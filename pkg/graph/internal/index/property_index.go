package index

import (
	"fmt"
	"strconv"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
// Returns nil if no matches.
func (pi *PropertyIndex) Lookup(value any) map[snowflake.ID]struct{} {
	if pi == nil {
		return nil
	}
	vk := PropertyValueKey(value)
	if vk == "" {
		return nil
	}
	return pi.Entries[vk]
}

// PropertyValueKey computes a canonical, type-safe string key for a property value.
// Type-prefixed to prevent cross-type collisions (int(1) vs string("1")).
// Only primitive types are indexed; complex types (maps, slices) return "" (not indexed).
func PropertyValueKey(v any) string {
	switch val := v.(type) {
	case string:
		return "s:" + val
	case int:
		return fmt.Sprintf("i:%d", val)
	case int8:
		return fmt.Sprintf("i8:%d", val)
	case int16:
		return fmt.Sprintf("i16:%d", val)
	case int32:
		return fmt.Sprintf("i32:%d", val)
	case int64:
		return fmt.Sprintf("i64:%d", val)
	case uint:
		return fmt.Sprintf("u:%d", val)
	case uint8:
		return fmt.Sprintf("u8:%d", val)
	case uint16:
		return fmt.Sprintf("u16:%d", val)
	case uint32:
		return fmt.Sprintf("u32:%d", val)
	case uint64:
		return fmt.Sprintf("u64:%d", val)
	case float32:
		return "f32:" + strconv.FormatFloat(float64(val), 'g', -1, 32)
	case float64:
		return "f64:" + strconv.FormatFloat(val, 'g', -1, 64)
	case bool:
		if val {
			return "b:true"
		}
		return "b:false"
	default:
		return ""
	}
}

// AddNodeToPropertyIndexes indexes a node's properties into all matching property indexes.
// Caller must hold the store's write lock.
func AddNodeToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		for _, p := range n.Properties() {
			key := PropertyIndexKey{LabelToken: tv, PropertyKey: p.Key}
			if idx, ok := indexes[key]; ok {
				idx.Add(id, p.Value)
			}
		}
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
	}
}

// RemoveNodeFromPropertyIndexes removes a node's properties from all matching property indexes.
// Caller must hold the store's write lock.
func RemoveNodeFromPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		for _, p := range n.Properties() {
			key := PropertyIndexKey{LabelToken: tv, PropertyKey: p.Key}
			if idx, ok := indexes[key]; ok {
				idx.Remove(id, p.Value)
			}
		}
	}
}
