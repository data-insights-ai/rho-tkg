package badger

import (
	"errors"
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// NodePropertyStats returns NDV/min/max/count planner statistics for
// (labelToken, propertyKey). Missing/unpopulated pairs return a zero-value
// PropertyStats (Count 0, NDV 0, Min/Max nil) — not an error, mirroring
// NodeCountByLabelAndPropertyKey's "unregistered → 0" convention.
//
// Locking is DELIBERATELY fine-grained rather than one idxMu.Lock() held for
// the whole call: on a rescan (see PropertyStatsAccumulator.Dirty/Rescan),
// fetching a node NOT resident in the LRU cache
// (prefetchNodeScan -> prefetchNodeNoFill) itself takes idxMu.RLock() to
// check existence before the Badger read — holding idxMu.Lock() across that
// call would self-deadlock (sync.RWMutex is not reentrant). So the rescan's
// node-fetch loop (collectCurrentPropertyValues) runs with idxMu NOT held by
// this goroutine.
//
// That unlocked collect window would, naïvely, let a concurrent PutNode land a
// NEW live extremum between the collect and the Rescan commit, and the stale
// snapshot would OVERWRITE it and clear dirty — persisting the wrong exact
// Min/Max forever (not a data race; a lost-update ordering bug). The guard is
// an optimistic retry keyed on the accumulator's write generation
// (PropertyStatsAccumulator.WriteGen, bumped under idxMu.Lock() on every
// Observe/Forget): resolveNodePropertyStats reads the generation under the
// lock BEFORE the collect and re-reads it under the lock BEFORE committing —
// if it moved, a concurrent mutation invalidated the freshly collected values,
// so it redoes the collect (bounded by propertyStatsRescanMaxAttempts). On
// exhaustion it returns the LIVE snapshot without committing a possibly-stale
// Rescan and leaves dirty set, so a later quiescent read reconciles exactly
// (Observe keeps Min/Max monotonically correct for additions, so the fallback
// never under-reports a live extremum). See lesson 62 and
// docs/query-planners.md "Deletion semantics".
//
// This is coarser-grained than the memory backend's NodePropertyStats (which
// holds one lock for the whole call — its node lookups are direct in-process
// map reads with no recursive-locking risk, so it needs no generation guard).
//
// propertyStatsRescanMaxAttempts bounds that optimistic-retry loop (lesson 24:
// an optimistic retry over an unlocked window must be bounded).
const propertyStatsRescanMaxAttempts = 3

func (bs *Store) NodePropertyStats(labelToken uint16, propertyKey string) (storecontract.PropertyStats, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyStats{}, err
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	var count int64
	if v, ok := bs.propertyKeyCounts.Load(key); ok {
		if c := v.(*atomic.Int64).Load(); c > 0 {
			count = c
		}
	}

	bs.idxMu.RLock()
	acc := bs.propertyStats[key]
	bs.idxMu.RUnlock()

	if acc == nil {
		return storecontract.PropertyStats{Count: count}, nil
	}

	ndv, min, max, err := bs.resolveNodePropertyStats(labelToken, propertyKey, acc)
	if err != nil {
		return storecontract.PropertyStats{}, err
	}
	return storecontract.PropertyStats{NDV: ndv, Min: min, Max: max, Count: count}, nil
}

// resolveNodePropertyStats returns the accumulator's NDV/min/max, running a
// dirty-triggered Rescan under the write-generation guard described on
// NodePropertyStats. acc is non-nil.
func (bs *Store) resolveNodePropertyStats(labelToken uint16, propertyKey string, acc *indexpkg.PropertyStatsAccumulator) (ndv int64, min, max any, err error) {
	for attempt := 0; attempt < propertyStatsRescanMaxAttempts; attempt++ {
		bs.idxMu.RLock()
		dirty := acc.Dirty()
		genBefore := acc.WriteGen()
		bs.idxMu.RUnlock()

		if !dirty {
			bs.idxMu.RLock()
			ndv, min, max = acc.Snapshot()
			bs.idxMu.RUnlock()
			return ndv, min, max, nil
		}

		// Collect the current population with idxMu NOT held (a cache-cold node
		// fetch needs its own RLock; see the doc comment above).
		values, cerr := bs.collectCurrentPropertyValues(labelToken, propertyKey)
		if cerr != nil {
			return 0, nil, nil, cerr
		}
		if bs.rescanTestHook != nil {
			bs.rescanTestHook(attempt)
		}

		bs.idxMu.Lock()
		if acc.WriteGen() == genBefore {
			// No Observe/Forget landed during the unlocked collect — the values
			// are consistent, so commit the recomputed exact Min/Max.
			acc.Rescan(values)
			ndv, min, max = acc.Snapshot()
			bs.idxMu.Unlock()
			return ndv, min, max, nil
		}
		// The generation moved: the collected values are stale relative to the
		// live population. Discard them (do NOT commit) and retry.
		bs.idxMu.Unlock()
	}

	// Bounded retries exhausted (a sustained write storm on this pair). Return
	// the LIVE snapshot without committing a stale Rescan; dirty stays set so a
	// later quiescent read reconciles. Observe keeps Min/Max monotonically
	// correct for additions, so this never under-reports a live extremum.
	bs.idxMu.RLock()
	ndv, min, max = acc.Snapshot()
	bs.idxMu.RUnlock()
	return ndv, min, max, nil
}

// NodePropertyStatsSketch is a store-INTERNAL accessor (NOT part of the
// public store.NodePropertyStatsCapability contract) exposing the RAW
// per-(labelToken, propertyKey) HyperLogLog sketch alongside the exact
// min/max/count. It exists for the tiered backend's cross-shard PropertyStats
// fold: NDV cannot be folded by summing per-shard Estimate()s (that
// over-counts a value present on more than one shard), so tiered.Store calls
// this on each concrete shard, register-max Merges the returned sketches
// (indexpkg.HyperLogLog.Merge), and calls Estimate() exactly once on the
// merged result — see docs/adr/0005-tiered-parity.md §3.1. The returned
// sketch is a CLONE (independent of this store's internal accumulator
// state), safe for the caller to Merge/mutate concurrently with further
// mutation of this store. Missing/unpopulated pairs return
// (nil, nil, nil, 0, nil) — not an error, mirroring NodePropertyStats' own
// "unregistered → zero value" convention. Runs the SAME dirty-triggered
// Rescan (with its write-generation guard) as NodePropertyStats so Min/Max
// reflect the current live population exactly as the public door would
// report them.
func (bs *Store) NodePropertyStatsSketch(labelToken uint16, propertyKey string) (sketch *indexpkg.HyperLogLog, min, max any, count int64, err error) {
	if err := bs.checkOpen(); err != nil {
		return nil, nil, nil, 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, nil, nil, 0, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, nil, nil, 0, err
	}

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}

	var c int64
	if v, ok := bs.propertyKeyCounts.Load(key); ok {
		if cc := v.(*atomic.Int64).Load(); cc > 0 {
			c = cc
		}
	}

	bs.idxMu.RLock()
	acc := bs.propertyStats[key]
	bs.idxMu.RUnlock()
	if acc == nil {
		return nil, nil, nil, c, nil
	}

	_, mn, mx, rerr := bs.resolveNodePropertyStats(labelToken, propertyKey, acc)
	if rerr != nil {
		return nil, nil, nil, 0, rerr
	}

	bs.idxMu.RLock()
	sk := acc.Sketch()
	bs.idxMu.RUnlock()
	return sk, mn, mx, c, nil
}

// collectCurrentPropertyValues returns propertyKey's value from every
// CURRENT node carrying labelToken. Takes idxMu.RLock() ONLY for the
// label-membership snapshot (labelNodeIDsSnapshotLocked's own contract);
// each per-node fetch (prefetchNodeScan) runs with idxMu NOT held by this
// goroutine, because a cache miss inside it takes idxMu.RLock() itself.
func (bs *Store) collectCurrentPropertyValues(labelToken uint16, propertyKey string) ([]any, error) {
	bs.idxMu.RLock()
	nids, err := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if err != nil {
		return nil, err
	}
	values := make([]any, 0, len(nids))
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned label-index entry
			}
			return nil, err
		}
		if v, ok := n.GetProperty(propertyKey); ok {
			values = append(values, v)
		}
	}
	return values, nil
}
