package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodePropertyBounds satisfies store.NodePropertyBoundsCapability: the
// numeric bounds of (label, key) from the property index's ordered view
// (first and last key), the type-class counts and the label count, all under
// one read lock (every node write changes the three under the write lock).
// ok=false without an index on the pair, for an index that cannot state the
// bounds exactly (PropertyIndex.NumericBounds) and with planner statistics
// disabled.
func (ms *Store) NodePropertyBounds(labelToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	if ms == nil {
		return storecontract.PropertyBounds{}, false, ErrNilStore
	}
	if err := validateIndexTarget(storecontract.ValidateLabelToken(labelToken), propertyKey); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if ms.disablePlannerStats {
		return storecontract.PropertyBounds{}, false, nil
	}
	idx := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}]
	return boundsLocked(idx, ms.propertyTypeClassCounts[labelToken][propertyKey], int64(len(ms.labelIdx[labelToken])))
}

// RelPropertyBounds is NodePropertyBounds over a relationship type's property
// index, type-class counts and type count (sealed rows included).
func (ms *Store) RelPropertyBounds(relTypeToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	if ms == nil {
		return storecontract.PropertyBounds{}, false, ErrNilStore
	}
	if err := validateIndexTarget(storecontract.ValidateRelTypeToken(relTypeToken), propertyKey); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	if ms.disablePlannerStats {
		return storecontract.PropertyBounds{}, false, nil
	}
	idx := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}]
	total := int64(len(ms.typeIdx[relTypeToken]) + ms.liveSealedOfLocked(relTypeToken))
	return boundsLocked(idx, ms.relPropertyTypeClassCounts[relTypeToken][propertyKey], total)
}

// boundsLocked assembles PropertyBounds from an index, the pair's class
// counters (nil: none) and the label or type count; the caller holds ms.mu.
func boundsLocked(idx *indexpkg.PropertyIndex, counts *[types.NumPropertyTypeClasses]int64, total int64) (storecontract.PropertyBounds, bool, error) {
	var classes storecontract.PropertyTypeClassCounts
	if counts != nil {
		classes = storecontract.PropertyTypeClassCounts{
			Numeric: counts[types.ClassNumeric],
			NaN:     counts[types.ClassNaN],
			String:  counts[types.ClassString],
			Bool:    counts[types.ClassBool],
			Other:   counts[types.ClassOther],
		}
	}
	out, ok := indexpkg.PropertyBoundsOf(idx, classes, total)
	return out, ok, nil
}
