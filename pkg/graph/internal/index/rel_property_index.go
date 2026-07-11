package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RelPropertyIndexKey uniquely identifies a relationship property index by
// rel-type token and property key. It is the relationship mirror of
// PropertyIndexKey (which is keyed by label token). The value store itself is
// the same ID-generic *PropertyIndex — only the keying and the typed accessor
// helpers below differ, so the range/ordered-view machinery is shared verbatim
// (Node/Rel parity, house rule / Testing Rule 2).
type RelPropertyIndexKey struct {
	RelTypeToken uint16
	PropertyKey  string
}

// RelIDs returns the relationship IDs matching the given value as a
// caller-owned slice — the rel-typed mirror of PropertyIndex.NodeIDs. Store
// query code needs IDs but must not receive the mutable index set.
func (pi *PropertyIndex) RelIDs(value any) []types.RelID {
	if pi == nil {
		return nil
	}
	vk := PropertyValueKey(value)
	if vk == "" {
		return nil
	}
	set := pi.Entries[vk]
	if len(set) == 0 {
		return nil
	}
	out := make([]types.RelID, 0, len(set))
	for id := range set {
		out = append(out, types.RelID(id))
	}
	return out
}

// RangeRelIDs is the rel-typed mirror of RangeNodeIDs — the candidate
// relationship IDs whose numeric property sort key lies within [min, max]
// (bound inclusivity per flags), as a caller-owned slice. Bounds are WIDENED
// by one ulp on each side before the search, so the returned set OVER-SELECTS
// by design: callers must post-filter with exact comparison semantics.
// supported=false only when the index is nil; an enabled view with no numeric
// keys returns an authoritative empty result.
func (pi *PropertyIndex) RangeRelIDs(min, max float64, inclMin, inclMax bool) (ids []types.RelID, supported bool) {
	if pi == nil {
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

// AddRelToPropertyIndexes indexes a relationship's properties into the matching
// rel property index (there is at most one — the type token is a single value,
// unlike a node's label set). Mirror of AddNodeToPropertyIndexes.
// Caller must hold the store's write lock.
func AddRelToPropertyIndexes(indexes map[RelPropertyIndexKey]*PropertyIndex, r *types.Relationship, id snowflake.ID) {
	if len(indexes) == 0 || r == nil {
		return
	}
	typeToken := r.TypeToken().Value()
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
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
	r.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		key := RelPropertyIndexKey{RelTypeToken: typeToken, PropertyKey: propertyKey}
		if idx, ok := indexes[key]; ok {
			idx.removeKey(id, valueKey)
		}
		return true
	})
}

// PurgeRelFromAllPropertyIndexes removes a relationship ID from every value set
// in every rel property index. Brute-force O(V) fallback used when the
// relationship's data is unavailable (the shared deleteRelByInfo seam carries
// only RelDeleteInfo, no property values) or on the corruption path. Mirror of
// PurgeNodeFromAllPropertyIndexes. Caller must hold the store's write lock.
func PurgeRelFromAllPropertyIndexes(indexes map[RelPropertyIndexKey]*PropertyIndex, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, idx := range indexes {
		if idx == nil {
			continue
		}
		for valKey, idSet := range idx.Entries {
			delete(idSet, id)
			if len(idSet) == 0 {
				delete(idx.Entries, valKey)
			}
		}
		idx.purgeOrdered(id)
		if idx.Mutated != nil {
			idx.Mutated[id] = struct{}{}
		}
	}
}
