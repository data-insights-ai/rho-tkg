package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
)

// NodeEqualCount counts the label's nodes whose propertyKey value equals
// value under the property equality (types.EqualIndexValues), from the RAM property index's value sets
// (PropertyIndex.EqualCount): no node and no index key is read. exact=false
// (no error) when no index on (label, key) exists, for an index kept on disk
// (the index keys live in badger, not as counted sets), or when the index
// cannot say (see EqualCount).
func (bs *Store) NodeEqualCount(labelToken uint16, propertyKey string, value any) (int64, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	count, exact := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}].EqualCount(value)
	return count, exact, nil
}

// RelEqualCount is NodeEqualCount for a relationship type's property index
// (kept in RAM in every mode).
func (bs *Store) RelEqualCount(relTypeToken uint16, propertyKey string, value any) (int64, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	count, exact := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}].EqualCount(value)
	return count, exact, nil
}
