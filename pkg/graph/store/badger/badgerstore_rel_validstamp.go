package badger

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Inline valid-time adjacency stamps (OPT15 — LiveGraph VLDB 2020 inline-edge
// timestamps, in-memory arm).
//
// A temporal adjacency traversal (give me nid's neighbours valid at time T)
// today fetches and msgpack-decodes EVERY incident relationship row just to
// read its valid interval, then applies MatchesTemporalFilter. Profiling found
// the decode is the dominant per-edge cost (see ForEachAdjacentEndpoint). For a
// hub node whose edges are mostly expired versions, almost all of those decodes
// are wasted on rows the filter rejects.
//
// relValidIdx keeps each relationship's EFFECTIVE valid interval alongside the
// adjacency index so the traversal can reject (or admit) an edge from the inline
// stamp WITHOUT decoding its row. It is a small parallel map (RelID -> 16 bytes),
// deliberately NOT a change to outIdx's value type (which would ripple through
// every adjacency reader). Soundness is "a hit must be fresh; a miss falls back
// to the decode path" — so it is correct even on a partial/cross-shard store
// where an incoming edge's entity lives on another shard (no local stamp ->
// miss -> decode, exactly as the existing scan already degrades).
//
// LAZY: the index is nil until the first temporal traversal builds it
// (ensureRelValidIdxBuilt). Until then setRelValidStampLocked is a no-op, so a
// graph that never does temporal adjacency — or a tiered store, which does not
// expose ForEachAdjacentEndpointAt at all — pays NOTHING for the per-rel stamps
// (no memory, no per-write cost). Once built, every rel lifecycle site keeps it
// fresh incrementally (the divergence gate covers that), and the build captures
// the current rel set in one pass.

// relValidStamp is one relationship's inline valid-time interval. vf is the
// EFFECTIVE valid-from: EntityValidFrom has already resolved the snowflake-ID
// fallback, so vf is always > 0 for a real relationship. vt is valid-to with 0
// meaning open-ended. Storing the resolved vf lets the fast path reuse the
// canonical MatchesTemporalFilter via a synthetic TemporalMetadata, so the
// inline decision can never drift from the decode path's semantics.
type relValidStamp struct {
	vf int64
	vt int64
}

// setRelValidStampLocked records (or refreshes) the rel's inline valid interval.
// Caller holds idxMu write lock. It MUST be called at every site that creates a
// relationship row OR rewrites it in place: ReplaceRelationship and
// ReplaceRelWithHistory close the interval on a version update and touch no
// other index, so a missed call leaves a stale stamp = silently wrong temporal
// results. That bug class is exactly what the divergence gate
// (badgerstore_rel_validstamp_test.go) is built to catch.
func (bs *Store) setRelValidStampLocked(rid types.RelID, r *types.Relationship) {
	if bs.relValidIdx == nil {
		return // not built yet — temporal traversal will build it on first use
	}
	bs.relValidIdx[rid] = relValidStampFor(rid, r)
}

// relValidStampFor computes a rel's effective valid interval (EntityValidFrom
// resolves the snowflake fallback; vt==0 is open-ended).
func relValidStampFor(rid types.RelID, r *types.Relationship) relValidStamp {
	tm := r.Temporal()
	vf := storepkg.EntityValidFrom(rid.SnowflakeID(), tm)
	var vt types.Instant
	if tm != nil {
		vt = tm.ValidTo
	}
	return relValidStamp{vf: int64(vf), vt: int64(vt)}
}

// ensureRelValidIdxBuilt builds the inline stamp index from the current rel set
// on the first temporal traversal, decoding each live rel once. After this every
// lifecycle site maintains it incrementally. The atomic flag keeps the steady
// state lock-free; the build itself runs under idxMu.Lock so it is consistent
// with concurrent mutations (a writer either contributes a stamp post-build or
// is captured by the build, never lost).
func (bs *Store) ensureRelValidIdxBuilt() {
	if bs.relValidIdxBuilt.Load() {
		return
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.relValidIdx != nil {
		return // built by a racing caller while we waited for the lock
	}
	idx := make(map[types.RelID]relValidStamp, len(bs.relIDs))
	for rid := range bs.relIDs {
		r, err := bs.getRelLocked(rid)
		if err != nil {
			continue // gone since the snapshot — a miss falls back to a decode
		}
		idx[rid] = relValidStampFor(rid, r)
	}
	bs.relValidIdx = idx
	bs.relValidIdxBuilt.Store(true)
}

// ForEachAdjacentEndpointAt is ForEachAdjacentEndpoint with an inline temporal
// filter: it streams (relID, otherEndpoint) for nid's adjacency in the given
// direction, yielding ONLY edges whose valid interval passes opts' temporal
// filter, and — for any edge whose interval is recorded in the inline stamp
// index — it makes that decision WITHOUT decoding the relationship row. An edge
// whose stamp is absent (e.g. a cross-shard incoming rel whose entity lives on
// another shard) falls back to a row decode so the result is always exact. With
// no temporal filter set this is exactly ForEachAdjacentEndpoint.
//
// Equivalence with the decode path is pinned by the divergence gate
// (badgerstore_rel_validstamp_test.go) across random create/close/replace/delete
// sequences. fn returning false stops the scan. Order is unspecified.
func (bs *Store) ForEachAdjacentEndpointAt(nid types.NodeID, typeToken uint16, incoming bool, opts QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return err
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	if hasTemporal {
		bs.ensureRelValidIdxBuilt() // lazy first-use build (no-op once built)
	}

	bs.idxMu.RLock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		bs.idxMu.RUnlock()
		return ErrNodeNotFound
	}
	metas, snapErr := bs.adjacentRelMetasSnapshotLocked(nid, typeToken, incoming)
	// Apply the inline temporal filter WHILE holding the lock (the predicate is
	// pure) so only survivors leave the locked section — for a hub whose edges are
	// mostly expired, that collapses the post-lock slice from every edge to the
	// few that pass. A stampless edge (e.g. a cross-shard incoming rel with no
	// local entity) cannot be decided inline; it is deferred to a post-lock
	// decode. The stamp read is consistent with the adjacency snapshot moment.
	var survivors []adjMeta
	var deferred []adjMeta
	if snapErr == nil {
		survivors = make([]adjMeta, 0, len(metas))
		for _, m := range metas {
			if !hasTemporal {
				survivors = append(survivors, m)
				continue
			}
			s, ok := bs.relValidIdx[m.rel]
			if !ok {
				deferred = append(deferred, m)
				continue
			}
			// Synthetic metadata with the resolved vf (>0) so
			// MatchesTemporalFilter uses it directly; the snowflake-ID arg is
			// therefore irrelevant.
			tm := types.TemporalMetadata{ValidFrom: types.Instant(s.vf), ValidTo: types.Instant(s.vt)}
			if storepkg.MatchesTemporalFilter(m.rel.SnowflakeID(), &tm, opts) {
				survivors = append(survivors, m)
			}
		}
	}
	bs.idxMu.RUnlock()
	if snapErr != nil {
		return snapErr
	}

	// Decode fallback for stampless edges (rare — cross-shard incoming only).
	for _, m := range deferred {
		r, err := bs.prefetchRelScan(m.rel)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted since snapshot — relaxed isolation
			}
			return fmt.Errorf("graph: scan relationship %d: %w", m.rel.SnowflakeID(), err)
		}
		if storepkg.MatchesTemporalFilter(m.rel.SnowflakeID(), r.Temporal(), opts) {
			survivors = append(survivors, m)
		}
	}

	for _, m := range survivors {
		if !fn(m.rel, m.other) {
			return nil
		}
	}
	return nil
}

// ForEachAdjacentRelAt is the decode-arm sibling of ForEachAdjacentEndpointAt:
// it streams the DECODED relationship rows for nid's adjacency in the given
// direction, but uses the inline valid-time stamps to SKIP the msgpack decode of
// any edge the temporal filter already rejects — so a traversal that needs the
// relationship (its properties / identity) still pays the decode only for edges
// valid at the query time, not for the expired versions on the same hub. A
// stampless edge (cross-shard incoming) falls back to a decode + the canonical
// filter. With no temporal filter set this is exactly ForEachOutgoingRel /
// ForEachIncomingRel. fn returning false stops the scan; rows are frozen.
func (bs *Store) ForEachAdjacentRelAt(nid types.NodeID, typeToken uint16, incoming bool, opts QueryOpts, fn func(*types.Relationship) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return err
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	if hasTemporal {
		bs.ensureRelValidIdxBuilt() // lazy first-use build (no-op once built)
	}

	bs.idxMu.RLock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		bs.idxMu.RUnlock()
		return ErrNodeNotFound
	}
	rids, snapErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, incoming)
	// Mark rids whose inline stamp PROVES the edge out-of-window, under the same
	// lock as the snapshot — those are skipped below WITHOUT a decode (the win).
	var skipDecode map[types.RelID]struct{}
	if snapErr == nil && hasTemporal {
		for _, rid := range rids {
			if s, ok := bs.relValidIdx[rid]; ok {
				tm := types.TemporalMetadata{ValidFrom: types.Instant(s.vf), ValidTo: types.Instant(s.vt)}
				if !storepkg.MatchesTemporalFilter(rid.SnowflakeID(), &tm, opts) {
					if skipDecode == nil {
						skipDecode = make(map[types.RelID]struct{})
					}
					skipDecode[rid] = struct{}{}
				}
			}
		}
	}
	bs.idxMu.RUnlock()
	if snapErr != nil {
		return snapErr
	}

	if len(rids) == 0 {
		return nil
	}
	storepkg.SortRelIDs(rids)

	for _, rid := range rids {
		if _, skip := skipDecode[rid]; skip {
			continue // inline stamp proved out-of-window — no decode
		}
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: scan relationship %d: %w", rid.SnowflakeID(), err)
		}
		match := relationshipMatchesOutgoing(r, nid, typeToken)
		if incoming {
			match = relationshipMatchesIncoming(r, nid, typeToken)
		}
		if !match {
			continue
		}
		// Valid-stamped edges always pass this re-check (cheap int compares on the
		// already-decoded row); it is the ONLY temporal check for a stampless edge.
		if hasTemporal && !storepkg.MatchesTemporalFilter(rid.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		if !fn(r) {
			return nil
		}
	}
	return nil
}
