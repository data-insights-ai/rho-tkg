package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// adjustRelPropertyKeyCounts runs the full indexable-property sweep for one
// entering (delta>0) or leaving (delta<0) relationship row, incrementing the
// presence counter and folding the observation into the NDV+min/max
// accumulator on the SAME call — the relationship mirror of
// adjustNodePropertyKeyCounts. The memory store holds live rels, so a delete
// path decrements with the OLD rel directly (no memoized-contribution
// sidecar, unlike badger's read-free deleteRelByInfo — see
// relPropertyTypeClassCounts's doc comment for the identical precedent).
// Caller holds ms.mu (write mode).
func (ms *Store) adjustRelPropertyKeyCounts(r *types.Relationship, delta int) {
	if ms.disablePlannerStats || r == nil || delta == 0 {
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
		counts := ms.relPropertyKeyCounts[relType]
		if counts == nil {
			if delta < 0 {
				return true
			}
			counts = make(map[string]int)
			ms.relPropertyKeyCounts[relType] = counts
		}
		next := counts[propertyKey] + delta
		if next <= 0 {
			delete(counts, propertyKey)
			if len(counts) == 0 {
				delete(ms.relPropertyKeyCounts, relType)
			}
		} else {
			counts[propertyKey] = next
		}
		ms.adjustRelPropertyStatsOne(relType, propertyKey, valueKey, value, delta)
		return true
	})
}

// adjustRelPropertyStatsOne folds one (relType, property key) observation
// into the NDV+min/max accumulator — the relationship mirror of
// adjustNodePropertyStatsOne. delta>0 means the value is ENTERING the
// population (Observe); delta<0 means it is LEAVING (Forget).
func (ms *Store) adjustRelPropertyStatsOne(relTypeToken uint16, propertyKey, valueKey string, value any, delta int) {
	if delta > 0 {
		entries := ms.relPropertyStats[relTypeToken]
		if entries == nil {
			entries = make(map[string]*indexpkg.PropertyStatsAccumulator)
			ms.relPropertyStats[relTypeToken] = entries
		}
		acc := entries[propertyKey]
		if acc == nil {
			acc = indexpkg.NewPropertyStatsAccumulator()
			entries[propertyKey] = acc
		}
		acc.Observe(valueKey, value)
		return
	}
	if entries := ms.relPropertyStats[relTypeToken]; entries != nil {
		if acc := entries[propertyKey]; acc != nil {
			acc.Forget(value)
		}
	}
}

// RelPropertyStats returns NDV/min/max/count planner statistics for
// (relTypeToken, propertyKey) — the relationship mirror of NodePropertyStats.
// Missing/unpopulated pairs return a zero-value PropertyStats, not an error.
//
// Takes the exclusive write lock for the same reason NodePropertyStats does:
// a dirty accumulator triggers an in-place Rescan over the type's current
// relationship values, and the memory backend keeps this fully race-free with
// no generation guard (direct in-process map reads, no re-entrant locking
// risk).
func (ms *Store) RelPropertyStats(relTypeToken uint16, propertyKey string) (storecontract.PropertyStats, error) {
	if ms == nil {
		return storecontract.PropertyStats{}, ErrNilStore
	}
	if ms.disablePlannerStats {
		return storecontract.PropertyStats{}, storecontract.ErrCapabilityNotSupported
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyStats{}, err
	}

	var count int
	if counts := ms.relPropertyKeyCounts[relTypeToken]; counts != nil {
		count = counts[propertyKey]
	}
	var acc *indexpkg.PropertyStatsAccumulator
	if entries := ms.relPropertyStats[relTypeToken]; entries != nil {
		acc = entries[propertyKey]
	}
	if acc == nil {
		return storecontract.PropertyStats{Count: int64(count)}, nil
	}
	if acc.Dirty() {
		acc.Rescan(ms.collectCurrentRelPropertyValuesLocked(relTypeToken, propertyKey))
	}
	ndv, min, max := acc.Snapshot()
	return storecontract.PropertyStats{NDV: ndv, Min: min, Max: max, Count: int64(count)}, nil
}

// collectCurrentRelPropertyValuesLocked returns propertyKey's value from
// every CURRENT relationship carrying relTypeToken. Callers must already
// hold ms.mu (any mode) — it reads ms.typeIdx/ms.rels directly rather than
// calling a public Store method, so as not to re-enter ms.mu from within
// RelPropertyStats.
func (ms *Store) collectCurrentRelPropertyValuesLocked(relTypeToken uint16, propertyKey string) []any {
	ids := ms.typeIdx[relTypeToken]
	values := make([]any, 0, len(ids))
	for id := range ids {
		r := ms.rels[id]
		if r == nil {
			continue
		}
		if v, ok := r.GetProperty(propertyKey); ok {
			values = append(values, v)
		}
	}
	return values
}
