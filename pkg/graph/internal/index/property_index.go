package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PropertyIndexKey uniquely identifies a property index by label token and property key.
type PropertyIndexKey struct {
	LabelToken  uint16
	PropertyKey string
}

// PropertyIndex stores a reverse mapping from canonical value keys to sets of node IDs.
type PropertyIndex struct {
	Entries map[string]map[snowflake.ID]struct{}
	Mutated map[snowflake.ID]struct{} // non-nil during index creation Phase 2
}

// NewPropertyIndex creates an empty property index.
func NewPropertyIndex() *PropertyIndex {
	return &PropertyIndex{
		Entries: make(map[string]map[snowflake.ID]struct{}),
	}
}

// Add inserts a node ID into the index for the given property value.
// No-op if the value type is not indexable (complex types).
func (pi *PropertyIndex) Add(id snowflake.ID, value any) {
	if pi == nil {
		return
	}
	vk := PropertyValueKey(value)
	pi.AddKey(id, vk)
}

// AddKey inserts a node ID for a precomputed property value key.
func (pi *PropertyIndex) AddKey(id snowflake.ID, vk string) {
	if pi == nil {
		return
	}
	if vk == "" {
		return
	}
	if pi.Entries == nil {
		pi.Entries = make(map[string]map[snowflake.ID]struct{})
	}
	if pi.Entries[vk] == nil {
		pi.Entries[vk] = make(map[snowflake.ID]struct{})
	}
	pi.Entries[vk][id] = struct{}{}
	if pi.Mutated != nil {
		pi.Mutated[id] = struct{}{}
	}
}

// Remove deletes a node ID from the index for the given property value.
// No-op if the value type is not indexable or the ID is not in the index.
func (pi *PropertyIndex) Remove(id snowflake.ID, value any) {
	if pi == nil {
		return
	}
	vk := PropertyValueKey(value)
	pi.removeKey(id, vk)
}

func (pi *PropertyIndex) removeKey(id snowflake.ID, vk string) {
	if pi == nil {
		return
	}
	if vk == "" {
		return
	}
	if set, ok := pi.Entries[vk]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(pi.Entries, vk)
		}
	}
	if pi.Mutated != nil {
		pi.Mutated[id] = struct{}{}
	}
}

// Lookup returns the set of node IDs matching the given value.
// Returns nil if no matches. The returned map is a copy; mutating it cannot
// alter the index.
func (pi *PropertyIndex) Lookup(value any) map[snowflake.ID]struct{} {
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
	out := make(map[snowflake.ID]struct{}, len(set))
	for id := range set {
		out[id] = struct{}{}
	}
	return out
}

// NodeIDs returns the node IDs matching the given value as a caller-owned
// slice. It is the allocation-conscious lookup path for store query code,
// which needs IDs but must not receive the mutable index set.
func (pi *PropertyIndex) NodeIDs(value any) []types.NodeID {
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
	out := make([]types.NodeID, 0, len(set))
	for id := range set {
		out = append(out, types.NodeID(id))
	}
	return out
}

// PropertyValueKey computes a canonical, type-safe string key for a property value.
// Type-prefixed to prevent cross-type collisions (int(1) vs string("1")).
// Only primitive types are indexed; complex types (maps, slices) return "" (not indexed).
func PropertyValueKey(v any) string {
	return types.IndexablePropertyValueKey(v)
}

// AddNodeToPropertyIndexes indexes a node's properties into all matching property indexes.
// Caller must hold the store's write lock.
func AddNodeToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for i := 0; i < n.LabelTokenCount(); i++ {
		labelToken := n.LabelTokenRawAt(i)
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			key := PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
			if idx, ok := indexes[key]; ok {
				idx.AddKey(id, valueKey)
			}
			return true
		})
	}
}

// PurgeNodeFromAllPropertyIndexes removes a node ID from every value set in
// every property index. Brute-force O(V) fallback used when the node's data
// is unavailable (corruption path). The normal path uses RemoveNodeFromPropertyIndexes
// which is O(L*P) but requires the node object.
// Caller must hold the store's write lock.
func PurgeNodeFromAllPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, id snowflake.ID) {
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
		if idx.Mutated != nil {
			idx.Mutated[id] = struct{}{}
		}
	}
}

// RemoveNodeFromPropertyIndexes removes a node's properties from all matching property indexes.
// Caller must hold the store's write lock.
func RemoveNodeFromPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for i := 0; i < n.LabelTokenCount(); i++ {
		labelToken := n.LabelTokenRawAt(i)
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			key := PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
			if idx, ok := indexes[key]; ok {
				idx.removeKey(id, valueKey)
			}
			return true
		})
	}
}
