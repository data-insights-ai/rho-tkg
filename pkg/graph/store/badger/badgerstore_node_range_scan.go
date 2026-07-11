package badger

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabelPropertyRange streams the label's nodes whose NUMERIC
// propKey value lies within [min, max] (per the inclusivity flags) to fn in
// snowflake-ID order — the range-scan capability. Candidates come
// from the property index's ordered numeric view; the view over-selects by
// design (float64 sort keys, ulp-widened bounds), so fn receives CANDIDATES
// and callers MUST re-check the predicate with exact comparison semantics.
// fn returning false stops early.
//
// Returns ErrIndexNotFound when no property index exists for (token,
// propKey) or its ordered view is unavailable (cardinality-capped) —
// callers fall back to a label scan. Same isolation and frozen-row
// contract as ForEachNodeByLabel.
// NodeRangeCardinality returns the COUNT of the label's nodes whose numeric
// propKey value lies within [min, max] (per the inclusivity flags), summed from
// the property index's sorted per-value bucket sizes (R1) — O(distinct values in
// range), no node fetches. exact=false declines (no usable index / poisoned by an
// integer past 2^53) and the caller must scan-and-count. This answers
// `count(p) WHERE p.k > x` without re-fetching every candidate node.
//
// Disk mode (PropertyIndexOnDisk) always declines (exact=false): the
// persisted keyspace does not maintain the RAM ordered view's per-value
// bucket sizes, so an O(1) exact sum isn't available there. Callers already
// handle decline by falling back to ForEachNodeByLabelPropertyRange + an
// exact count, so this is a pure availability difference, never a wrong
// answer.
func (bs *Store) NodeRangeCardinality(token uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	bs.idxMu.RLock()
	idx, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}]
	var count int64
	exact := false
	if ok && !bs.propIdxOnDisk {
		count, exact = idx.RangeCardinality(min, max, inclMin, inclMax)
	}
	bs.idxMu.RUnlock()
	return count, exact, nil
}

func (bs *Store) ForEachNodeByLabelPropertyRange(token uint16, propKey string, min, max float64, inclMin, inclMax bool, opts QueryOpts, fn func(*types.Node) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return err
	}

	bs.idxMu.RLock()
	idx, ok := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}]
	var nids []types.NodeID
	supported := false
	var rangeErr error
	if ok {
		if bs.propIdxOnDisk {
			nids, supported, rangeErr = bs.propertyIndexDiskRangeLocked(propKey, min, max)
		} else {
			nids, supported = idx.RangeNodeIDs(min, max, inclMin, inclMax)
		}
	}
	bs.idxMu.RUnlock()
	if rangeErr != nil {
		return fmt.Errorf("graph: range-scan property index: %w", rangeErr)
	}
	if !ok || !supported {
		return storecontract.ErrIndexNotFound
	}

	if len(nids) == 0 {
		return nil
	}
	storepkg.SortNodeIDs(nids)
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)

	hasTemporal := storepkg.HasTemporalFilter(opts)
	emitted := 0
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted since snapshot, or orphaned index entry
			}
			return fmt.Errorf("graph: range-scan node %d: %w", nid.SnowflakeID(), err)
		}
		if !n.HasLabelTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(nid.SnowflakeID(), n.Temporal(), opts) {
			continue
		}
		if !fn(n) {
			return nil
		}
		emitted++
		if opts.Limit > 0 && emitted >= opts.Limit {
			return nil
		}
	}
	return nil
}

// orderedRangePageSize bounds the candidate IDs the RAM-mode ordered scan
// snapshots under idxMu per page before releasing the lock to materialize +
// emit rows — so a top-k caller (fn stops after k rows) touches
// O(pageSize + log n) index entries, never the whole range.
const orderedRangePageSize = 256

// ForEachNodeByLabelPropertyRangeOrdered streams the label's nodes whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE
// ORDER — ascending, or descending when desc — with ties (equal values)
// always broken by node ID ASCENDING in both directions. It is the ordered /
// top-k access path serving ORDER BY prop [LIMIT k]: fn returning false stops
// the scan at the index level so a LIMIT is pushed down.
//
// Candidates come from the property index's ordered numeric view (RAM mode)
// or the persisted 0x0A keyspace (PropertyIndexOnDisk), which OVER-SELECT by
// design (float64 sort keys, ulp-widened bounds): fn receives CANDIDATES and
// MUST re-check the predicate with exact comparison semantics (lesson 23; the
// door never skips a boundary bucket because int64 magnitudes past 2^53
// collapse onto neighbouring sort keys, lesson 25, so exact inclusivity is
// fn's responsibility).
//
// Current-state only (no temporal QueryOpts parameter). Returns
// ErrIndexNotFound when no property index exists for (token, propKey) or its
// ordered view is unavailable. Same isolation and frozen-row contract as
// ForEachNodeByLabel: fn runs with no store lock held and may call back into
// the store.
//
// RAM mode is fully lazy/paged (O(k + log n) index work for a top-k). Disk
// mode snapshots the ordered candidate IDs (cheap 8-byte IDs, overlay-merged)
// up front, then streams the expensive node materialization with the same
// fn-driven early stop — so node fetches (the costly decode) stay bounded by
// what fn consumes.
func (bs *Store) ForEachNodeByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Node) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}

	if bs.propIdxOnDisk {
		return bs.forEachNodeRangeOrderedDisk(token, propKey, key, min, max, desc, fn)
	}
	return bs.forEachNodeRangeOrderedRAM(token, key, min, max, desc, fn)
}

// forEachNodeRangeOrderedRAM serves the ordered scan from the in-memory
// ordered numeric view via RangeOrderedPage, page by page.
func (bs *Store) forEachNodeRangeOrderedRAM(token uint16, key indexpkg.PropertyIndexKey, min, max float64, desc bool, fn func(*types.Node) bool) error {
	var cur indexpkg.RangeOrderedCursor
	for {
		bs.idxMu.RLock()
		idx, ok := bs.propertyIndexes[key]
		if !ok {
			bs.idxMu.RUnlock()
			return storecontract.ErrIndexNotFound
		}
		ids, next, done, supported := idx.RangeOrderedPage(min, max, desc, cur, orderedRangePageSize)
		bs.idxMu.RUnlock()
		if !supported {
			return storecontract.ErrIndexNotFound
		}
		stop, err := bs.emitOrderedCandidates(token, ids, fn)
		if err != nil {
			return err
		}
		if stop || done {
			return nil
		}
		cur = next
	}
}

// forEachNodeRangeOrderedDisk serves the ordered scan from the persisted 0x0A
// keyspace. It snapshots the ordered candidate IDs (overlay-merged) once, then
// streams node materialization with fn-driven early stop.
func (bs *Store) forEachNodeRangeOrderedDisk(token uint16, propKey string, key indexpkg.PropertyIndexKey, min, max float64, desc bool, fn func(*types.Node) bool) error {
	bs.idxMu.RLock()
	_, ok := bs.propertyIndexes[key]
	if !ok {
		bs.idxMu.RUnlock()
		return storecontract.ErrIndexNotFound
	}
	ids, supported, err := bs.propertyIndexDiskRangeOrderedLocked(propKey, min, max, desc)
	bs.idxMu.RUnlock()
	if err != nil {
		return fmt.Errorf("graph: ordered range-scan property index: %w", err)
	}
	if !supported {
		return storecontract.ErrIndexNotFound
	}
	_, err = bs.emitOrderedCandidates(token, ids, fn)
	return err
}

// emitOrderedCandidates fetches each candidate node (outside idxMu) and hands
// live, label-carrying rows to fn in the given order. stop=true when fn asked
// to stop (returned false).
func (bs *Store) emitOrderedCandidates(token uint16, ids []snowflake.ID, fn func(*types.Node) bool) (stop bool, err error) {
	for _, rawID := range ids {
		n, ferr := bs.prefetchNodeScan(types.NodeID(rawID))
		if ferr != nil {
			if errors.Is(ferr, ErrNodeNotFound) {
				continue // deleted since snapshot, or orphaned ordered-view entry
			}
			return false, fmt.Errorf("graph: ordered range-scan node %d: %w", rawID, ferr)
		}
		if !n.HasLabelTokenRaw(token) {
			continue
		}
		if !fn(n) {
			return true, nil
		}
	}
	return false, nil
}
