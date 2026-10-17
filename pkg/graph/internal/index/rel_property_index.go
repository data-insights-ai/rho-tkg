package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelPropertyIndexKey identifies a relationship property index by rel-type
// token and property key (mirror of PropertyIndexKey; the value store is the
// same *PropertyIndex).
type RelPropertyIndexKey struct {
	RelTypeToken uint16
	PropertyKey  string
}

// RangeRelIDs is the rel mirror of RangeNodeIDs: over-selecting candidates, so
// callers must post-filter exactly. supported=false where the index does not
// serve (serves): nil, Elements, or being built.
func (pi *PropertyIndex) RangeRelIDs(min, max float64, inclMin, inclMax bool) (ids []types.RelID, supported bool) {
	if !pi.serves() {
		return nil, false
	}
	if pi.numKeys.n == 0 {
		return nil, true
	}
	nodeIDs, _ := pi.RangeNodeIDs(min, max, inclMin, inclMax)
	if len(nodeIDs) == 0 {
		return nil, true
	}
	ids = make([]types.RelID, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		ids = append(ids, types.RelID(nid.SnowflakeID()))
	}
	return ids, true
}

// AddRelToPropertyIndexes indexes a relationship's properties (at most one
// matching index: a rel has one type token). Caller must hold the store's write lock.
func AddRelToPropertyIndexes(indexes map[RelPropertyIndexKey]*PropertyIndex, r *types.Relationship, id snowflake.ID) {
	if len(indexes) == 0 || r == nil {
		return
	}
	typeToken := r.TypeToken().Value()
	r.ForEachIndexKey(RelElementsOf(indexes, typeToken), func(propertyKey, valueKey string) bool {
		key := RelPropertyIndexKey{RelTypeToken: typeToken, PropertyKey: propertyKey}
		if idx, ok := indexes[key]; ok {
			idx.AddKey(id, valueKey)
		}
		return true
	})
}

// RemoveRelFromPropertyIndexes removes a relationship's properties from the
// matching rel property index. Mirror of RemoveNodeFromPropertyIndexes.
// Caller must hold the store's write lock.
func RemoveRelFromPropertyIndexes(indexes map[RelPropertyIndexKey]*PropertyIndex, r *types.Relationship, id snowflake.ID) {
	if len(indexes) == 0 || r == nil {
		return
	}
	typeToken := r.TypeToken().Value()
	r.ForEachIndexKey(RelElementsOf(indexes, typeToken), func(propertyKey, valueKey string) bool {
		key := RelPropertyIndexKey{RelTypeToken: typeToken, PropertyKey: propertyKey}
		if idx, ok := indexes[key]; ok {
			idx.removeKey(id, valueKey)
		}
		return true
	})
}

// PurgeRelFromAllPropertyIndexes removes a relationship ID from every rel
// property index. Brute-force O(V) fallback when the relationship's data is
// unavailable (deleteRelByInfo carries no property values) or on the corruption
// path. Caller must hold the store's write lock.
func PurgeRelFromAllPropertyIndexes(indexes map[RelPropertyIndexKey]*PropertyIndex, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, idx := range indexes {
		if idx == nil {
			continue
		}
		for valKey, idSet := range idx.Entries {
			if idSet.remove(id) && idSet.Len() == 0 {
				idx.dropKey(valKey)
			}
		}
		idx.purgeOrdered(id)
		idx.valueStats.Store(nil)
		if idx.Mutated != nil {
			idx.Mutated[id] = struct{}{}
		}
	}
}
