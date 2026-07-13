package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// adjustNodePropertyTypeClassOne folds one (label, property key, class)
// observation into the exact type-class counters. Maintained on the SAME
// node-mutation call as propertyKeyCounts (adjustNodePropertyKeyCounts), so
// exactness is inherited door-for-door from the presence counter's audit.
// Caller holds ms.mu (write mode on every mutation site).
func (ms *Store) adjustNodePropertyTypeClassOne(labelToken uint16, propertyKey string, class types.PropertyTypeClass, delta int64) {
	if class >= types.NumPropertyTypeClasses {
		return
	}
	byKey := ms.propertyTypeClassCounts[labelToken]
	if byKey == nil {
		if delta < 0 {
			return
		}
		byKey = make(map[string]*[types.NumPropertyTypeClasses]int64)
		ms.propertyTypeClassCounts[labelToken] = byKey
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
	// Prune empty entries so a fully-departed (label, key) pair does not leak.
	for _, c := range counts {
		if c > 0 {
			return
		}
	}
	delete(byKey, propertyKey)
	if len(byKey) == 0 {
		delete(ms.propertyTypeClassCounts, labelToken)
	}
}

// NodePropertyTypeClassCounts satisfies the optional
// store.NodePropertyTypeClassCountsCapability — the exact O(1) per-(label,
// property key) type-class partition. Missing is always 0 at this boundary
// (graph-layer computed). Unregistered pairs return the zero value, not an
// error, mirroring NodeCountByLabelAndPropertyKey.
func (ms *Store) NodePropertyTypeClassCounts(labelToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if ms == nil {
		return storecontract.PropertyTypeClassCounts{}, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	byKey := ms.propertyTypeClassCounts[labelToken]
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

// ListCompositePropertyIndexes satisfies the optional
// store.CompositeIndexIntrospectionCapability: the declared, order-preserving
// key tuple of every composite definition under labelToken. Caller-owned
// copies; unregistered labels return an empty slice.
func (ms *Store) ListCompositePropertyIndexes(labelToken uint16) ([][]string, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	defs := ms.compositeIndexesByLabel[labelToken]
	out := make([][]string, 0, len(defs))
	for _, defKey := range defs {
		idx := ms.compositeIndexes[defKey]
		if idx == nil {
			continue
		}
		keys := make([]string, len(idx.Keys))
		copy(keys, idx.Keys)
		out = append(out, keys)
	}
	return out, nil
}
