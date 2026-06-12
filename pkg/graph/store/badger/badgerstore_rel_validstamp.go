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
	tm := r.Temporal()
	vf := storepkg.EntityValidFrom(rid.SnowflakeID(), tm)
	var vt types.Instant
	if tm != nil {
		vt = tm.ValidTo
	}
	bs.relValidIdx[rid] = relValidStamp{vf: int64(vf), vt: int64(vt)}
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
