package badger

import (
	"errors"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// relStatsEntry is one property's (valueKey, value) pair for a relationship,
// memoized by rel ID (relStatsContrib) so the read-free deleteRelByInfo —
// which carries no property values — can Forget() the NDV+min/max
// accumulator precisely by ID. The stats-maintenance analogue of
// relClassEntry (badgerstore_rel_type_class_counts.go); a SEPARATE structure
// because ForEachPropertyTypeClass deliberately never exposes the raw value
// (see types.Relationship.ForEachPropertyTypeClass doc), so the type-class
// memoization cannot be reused here.
type relStatsEntry struct {
	key      string
	valueKey string
	value    any
}

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

// addRelPropertyStatsCounts increments the per-(relType, propKey) presence
// counter and folds each observation into the NDV+min/max accumulator,
// MEMOIZING the (key, valueKey, value) triples by rel ID so a later
// delete-by-ID (deleteRelByInfo) can Forget() precisely. The relationship
// mirror of adjustNodePropertyKeyCounts(+1); called at every full-rel-write
// ADD site AND the loadIndexes rebuild, alongside addRelPropertyTypeClassCounts.
// Caller MUST already hold bs.idxMu.Lock() (every call site here does — see
// the grep audit in badgerstore_rel_type_class_counts.go for the identical
// requirement on its sibling).
func (bs *Store) addRelPropertyStatsCounts(r *types.Relationship) {
	if bs.disablePlannerStats || r == nil {
		return
	}
	relType := r.TypeToken().Value()
	if relType == 0 {
		return
	}
	var contrib []relStatsEntry
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		value, ok := r.GetProperty(propertyKey)
		if !ok {
			return true
		}
		bs.getOrCreateRelPropertyKeyCounter(relType, propertyKey).Add(1)
		bs.adjustRelPropertyStatsOne(relType, propertyKey, valueKey, value, 1)
		contrib = append(contrib, relStatsEntry{key: propertyKey, valueKey: valueKey, value: value})
		return true
	})
	if contrib != nil {
		bs.relStatsContrib.Store(r.ID().SnowflakeID(), contrib)
	}
}

// removeRelPropertyStatsCountsByID decrements the presence counters and
// Forgets each memoized value from the NDV+min/max accumulator for the
// relationship identified by relID (relType from the caller's
// RelDeleteInfo / old row) — the relationship mirror of
// removeRelPropertyTypeClassCountsByID. A rel with no memoized contribution
// is a no-op. Caller MUST already hold bs.idxMu.Lock().
func (bs *Store) removeRelPropertyStatsCountsByID(relID snowflake.ID, relType uint16) {
	if bs.disablePlannerStats {
		return
	}
	v, ok := bs.relStatsContrib.LoadAndDelete(relID)
	if !ok {
		return
	}
	for _, e := range v.([]relStatsEntry) {
		bs.getOrCreateRelPropertyKeyCounter(relType, e.key).Add(-1)
		bs.adjustRelPropertyStatsOne(relType, e.key, e.valueKey, e.value, -1)
	}
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
