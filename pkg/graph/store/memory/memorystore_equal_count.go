package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
)

// NodeEqualCount counts the label's nodes whose propertyKey value equals
// value under the property equality (types.EqualIndexValues), from the property index's value sets
// (PropertyIndex.EqualCount): no node is read. exact=false (no error) when
// no index on (label, key) exists or the index cannot say (see EqualCount).
func (ms *Store) NodeEqualCount(labelToken uint16, propertyKey string, value any) (int64, bool, error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false, err
	}
	count, exact := ms.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}].EqualCount(value)
	return count, exact, nil
}

// RelEqualCount is NodeEqualCount for a relationship type's property index.
func (ms *Store) RelEqualCount(relTypeToken uint16, propertyKey string, value any) (int64, bool, error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false, err
	}
	count, exact := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}].EqualCount(value)
	return count, exact, nil
}
