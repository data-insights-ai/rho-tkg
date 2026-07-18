package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// adjustRelPropertyTypeClassOne folds one (relType, property key, class) observation
// into the exact rel type-class counters. The relationship mirror of
// adjustNodePropertyTypeClassOne; maintained on the same rel-mutation doors as the
// rel property index (co-located with AddRel/RemoveRelFromPropertyIndexes). Caller
// holds ms.mu (write mode). Empty entries are pruned so a fully-departed pair does
// not leak.
func (ms *Store) adjustRelPropertyTypeClassOne(relTypeToken uint16, propertyKey string, class types.PropertyTypeClass, delta int64) {
	if class >= types.NumPropertyTypeClasses {
		return
	}
	if ms.relPropertyTypeClassCounts == nil {
		if delta < 0 {
			return
		}
		ms.relPropertyTypeClassCounts = make(map[uint16]map[string]*[types.NumPropertyTypeClasses]int64)
	}
	byKey := ms.relPropertyTypeClassCounts[relTypeToken]
	if byKey == nil {
		if delta < 0 {
			return
		}
		byKey = make(map[string]*[types.NumPropertyTypeClasses]int64)
		ms.relPropertyTypeClassCounts[relTypeToken] = byKey
	}
	counts := byKey[propertyKey]
	if counts == nil {
		if delta < 0 {
			return
		}
		counts = &[types.NumPropertyTypeClasses]int64{}
		byKey[propertyKey] = counts
	}
	counts[class] += delta
	if counts[class] < 0 {
		counts[class] = 0
	}
	for _, c := range counts {
		if c > 0 {
			return
		}
	}
	delete(byKey, propertyKey)
	if len(byKey) == 0 {
		delete(ms.relPropertyTypeClassCounts, relTypeToken)
	}
}

// adjustRelPropertyTypeClassCounts runs the full-property class sweep for one entering
// (delta>0) or leaving (delta<0) relationship row. Called at every memory rel-mutation
// site (co-located with the rel property index maintenance). Caller holds ms.mu.
func (ms *Store) adjustRelPropertyTypeClassCounts(r *types.Relationship, delta int64) {
	if ms.disablePlannerStats || r == nil || delta == 0 {
		return
	}
	relType := r.TypeToken().Value()
	if relType == 0 {
		return
	}
	r.ForEachPropertyTypeClass(func(propertyKey string, class types.PropertyTypeClass) bool {
		ms.adjustRelPropertyTypeClassOne(relType, propertyKey, class, delta)
		return true
	})
}

// RelPropertyTypeClassCounts satisfies the optional
// store.RelPropertyTypeClassCountsCapability — the relationship mirror of
// NodePropertyTypeClassCounts (rule 2, BACKLOG 5B). Unregistered pairs return the zero
// value, not an error.
func (ms *Store) RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if ms == nil {
		return storecontract.PropertyTypeClassCounts{}, ErrNilStore
	}
	if ms.disablePlannerStats {
		return storecontract.PropertyTypeClassCounts{}, storecontract.ErrCapabilityNotSupported
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	byKey := ms.relPropertyTypeClassCounts[relTypeToken]
	if byKey == nil {
		return storecontract.PropertyTypeClassCounts{}, nil
	}
	counts := byKey[propertyKey]
	if counts == nil {
		return storecontract.PropertyTypeClassCounts{}, nil
	}
	return storecontract.PropertyTypeClassCounts{
		Numeric: counts[types.ClassNumeric],
		NaN:     counts[types.ClassNaN],
		String:  counts[types.ClassString],
		Bool:    counts[types.ClassBool],
		Other:   counts[types.ClassOther],
	}, nil
}
