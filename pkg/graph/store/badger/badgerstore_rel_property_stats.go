package badger

import (
	"errors"
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelPropertyStats returns NDV/min/max/count planner statistics for
// (relTypeToken, propertyKey) — the relationship mirror of NodePropertyStats
// (BACKLOG 21a). Missing/unpopulated pairs return a zero-value PropertyStats,
// not an error, mirroring RelPropertyTypeClassCounts' "unregistered → zero"
// convention. Locking follows the exact same fine-grained, write-generation-
// guarded rescan discipline as NodePropertyStats — see its doc comment for
// the full rationale (a cache-cold prefetchRelScan takes its own idxMu.RLock,
// so the rescan collect must run with idxMu NOT held by this goroutine).
func (bs *Store) RelPropertyStats(relTypeToken uint16, propertyKey string) (storecontract.PropertyStats, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyStats{}, storecontract.ErrCapabilityNotSupported
	}
	if err := bs.ensurePlannerStats(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyStats{}, err
	}

	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}

	var count int64
	if v, ok := bs.relPropertyKeyCounts.Load(key); ok {
		if c := v.(*atomic.Int64).Load(); c > 0 {
			count = c
		}
	}

	bs.idxMu.RLock()
	acc := bs.relPropertyStats[key]
	bs.idxMu.RUnlock()

	if acc == nil {
		return storecontract.PropertyStats{Count: count}, nil
	}

	ndv, min, max, err := bs.resolveRelPropertyStats(relTypeToken, propertyKey, acc)
	if err != nil {
		return storecontract.PropertyStats{}, err
	}
	return storecontract.PropertyStats{NDV: ndv, Min: min, Max: max, Count: count}, nil
}

// resolveRelPropertyStats is the relationship mirror of
// resolveNodePropertyStats — same dirty-triggered Rescan under the same
// write-generation optimistic-retry guard, bounded by the same
// propertyStatsRescanMaxAttempts. acc is non-nil.
func (bs *Store) resolveRelPropertyStats(relTypeToken uint16, propertyKey string, acc *indexpkg.PropertyStatsAccumulator) (ndv int64, min, max any, err error) {
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

		values, cerr := bs.collectCurrentRelPropertyValues(relTypeToken, propertyKey)
		if cerr != nil {
			return 0, nil, nil, cerr
		}
		if bs.rescanTestHook != nil {
			bs.rescanTestHook(attempt)
		}

		bs.idxMu.Lock()
		if acc.WriteGen() == genBefore {
			acc.Rescan(values)
			ndv, min, max = acc.Snapshot()
			bs.idxMu.Unlock()
			return ndv, min, max, nil
		}
		bs.idxMu.Unlock()
	}

	bs.idxMu.RLock()
	ndv, min, max = acc.Snapshot()
	bs.idxMu.RUnlock()
	return ndv, min, max, nil
}

// collectCurrentRelPropertyValues returns propertyKey's value from every
// CURRENT relationship carrying relTypeToken — the relationship mirror of
// collectCurrentPropertyValues. Takes idxMu.RLock() ONLY for the
// type-membership snapshot; each per-rel fetch (prefetchRelScan) runs with
// idxMu NOT held by this goroutine (a cache miss inside it takes its own
// idxMu.RLock()).
func (bs *Store) collectCurrentRelPropertyValues(relTypeToken uint16, propertyKey string) ([]any, error) {
	bs.idxMu.RLock()
	rids := bs.relTypeRelIDsSnapshotLocked(relTypeToken)
	bs.idxMu.RUnlock()
	values := make([]any, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // orphaned type-index entry
			}
			return nil, err
		}
		if v, ok := r.GetProperty(propertyKey); ok {
			values = append(values, v)
		}
	}
	return values, nil
}

func (bs *Store) getOrCreateRelPropertyKeyCounter(relTypeToken uint16, propertyKey string) *atomic.Int64 {
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if v, ok := bs.relPropertyKeyCounts.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.relPropertyKeyCounts.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// addRelPropertyStatsCounts counts a relationship row entering the current
// set: the per-(relType, propKey) presence counter and the NDV+min/max
// accumulator. The relationship mirror of adjustNodePropertyKeyCounts(+1);
// called at every door that makes a row current AND by the loadIndexes
// rebuild, alongside addRelPropertyTypeClassCounts. Caller MUST already hold
// bs.idxMu.Lock().
func (bs *Store) addRelPropertyStatsCounts(r *types.Relationship) {
	bs.adjustRelPropertyStatsCounts(r, 1)
}

// removeRelPropertyStatsCounts takes a row leaving the current set back out:
// presence counters decrement and each value is Forgotten by the
// accumulator. The caller passes the row it removes, so no per-relationship
// memo is kept (backlog 53). Caller MUST already hold bs.idxMu.Lock().
func (bs *Store) removeRelPropertyStatsCounts(r *types.Relationship) {
	bs.adjustRelPropertyStatsCounts(r, -1)
}

func (bs *Store) adjustRelPropertyStatsCounts(r *types.Relationship, delta int64) {
	if bs.plannerStatsOff() || r == nil {
		return
	}
	relType := r.TypeToken().Value()
	if relType == 0 {
		return
	}
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		value, ok := r.GetProperty(propertyKey)
		if !ok {
			return true
		}
		bs.getOrCreateRelPropertyKeyCounter(relType, propertyKey).Add(delta)
		bs.adjustRelPropertyStatsOne(relType, propertyKey, valueKey, value, delta)
		return true
	})
}

// adjustRelPropertyStatsOne folds one (relType, property key) observation
// into the NDV+min/max accumulator (bs.relPropertyStats) — the relationship
// mirror of adjustNodePropertyStatsOne. Caller MUST already hold bs.idxMu
// (any mode is fine for a fresh map read; every actual mutation call site
// holds idxMu.Lock()). delta>0 means the value is ENTERING the population
// (Observe); delta<0 means it is LEAVING (Forget).
func (bs *Store) adjustRelPropertyStatsOne(relTypeToken uint16, propertyKey, valueKey string, value any, delta int64) {
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	if delta > 0 {
		acc := bs.relPropertyStats[key]
		if acc == nil {
			acc = indexpkg.NewPropertyStatsAccumulator()
			bs.relPropertyStats[key] = acc
		}
		acc.Observe(valueKey, value)
		return
	}
	if acc := bs.relPropertyStats[key]; acc != nil {
		acc.Forget(value)
	}
}

// RelPropertyNDV implements store.RelPropertyNDVCapability: the NDV and
// Count RelPropertyStats would return, without the dirty-triggered rescan of
// the exact Min/Max (no relationship is read).
func (bs *Store) RelPropertyNDV(relTypeToken uint16, propertyKey string) (storecontract.PropertyNDV, error) {
	if err := bs.checkOpen(); err != nil {
		return storecontract.PropertyNDV{}, err
	}
	if bs.disablePlannerStats {
		return storecontract.PropertyNDV{}, storecontract.ErrCapabilityNotSupported
	}
	if err := bs.ensurePlannerStats(); err != nil {
		return storecontract.PropertyNDV{}, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyNDV{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyNDV{}, err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}
	var out storecontract.PropertyNDV
	if v, ok := bs.relPropertyKeyCounts.Load(key); ok {
		if c := v.(*atomic.Int64).Load(); c > 0 {
			out.Count = c
		}
	}
	bs.idxMu.RLock()
	if acc := bs.relPropertyStats[key]; acc != nil {
		out.NDV, _, _ = acc.Snapshot()
	}
	bs.idxMu.RUnlock()
	return out, nil
}
