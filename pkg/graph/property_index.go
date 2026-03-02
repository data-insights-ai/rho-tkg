package graph

import (
	"fmt"
	"strconv"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// propertyIndexKey uniquely identifies a property index by label token and property key.
type propertyIndexKey struct {
	labelToken  uint16
	propertyKey string
}

// propertyIndex stores a reverse mapping from canonical value keys to sets of node IDs.
type propertyIndex struct {
	entries map[string]map[snowflake.ID]struct{}
	mutated map[snowflake.ID]struct{} // non-nil during index creation Phase 2
}

// newPropertyIndex creates an empty property index.
func newPropertyIndex() *propertyIndex {
	return &propertyIndex{
		entries: make(map[string]map[snowflake.ID]struct{}),
	}
}

// add inserts a node ID into the index for the given property value.
// No-op if the value type is not indexable (complex types).
func (pi *propertyIndex) add(id snowflake.ID, value any) {
	vk := propertyValueKey(value)
	if vk == "" {
		return
	}
	if pi.entries[vk] == nil {
		pi.entries[vk] = make(map[snowflake.ID]struct{})
	}
	pi.entries[vk][id] = struct{}{}
	if pi.mutated != nil {
		pi.mutated[id] = struct{}{}
	}
}

// remove deletes a node ID from the index for the given property value.
// No-op if the value type is not indexable or the ID is not in the index.
func (pi *propertyIndex) remove(id snowflake.ID, value any) {
	vk := propertyValueKey(value)
	if vk == "" {
		return
	}
	if set, ok := pi.entries[vk]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(pi.entries, vk)
		}
	}
	if pi.mutated != nil {
		pi.mutated[id] = struct{}{}
	}
}

// lookup returns the set of node IDs matching the given value.
// Returns nil if no matches.
func (pi *propertyIndex) lookup(value any) map[snowflake.ID]struct{} {
	vk := propertyValueKey(value)
	if vk == "" {
		return nil
	}
	return pi.entries[vk]
}

// propertyValueKey computes a canonical, type-safe string key for a property value.
// Type-prefixed to prevent cross-type collisions (int(1) vs string("1")).
// Only primitive types are indexed; complex types (maps, slices) return "" (not indexed).
func propertyValueKey(v any) string {
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

// addNodeToPropertyIndexes indexes a node's properties into all matching property indexes.
// Caller must hold the store's write lock.
func addNodeToPropertyIndexes(indexes map[propertyIndexKey]*propertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		for _, p := range n.Properties() {
			key := propertyIndexKey{labelToken: tv, propertyKey: p.Key}
			if idx, ok := indexes[key]; ok {
				idx.add(id, p.Value)
			}
		}
	}
}

// purgeNodeFromAllPropertyIndexes removes a node ID from every value set in
// every property index. Brute-force O(V) fallback used when the node's data
// is unavailable (corruption path). The normal path uses removeNodeFromPropertyIndexes
// which is O(L*P) but requires the node object.
// Caller must hold the store's write lock.
func purgeNodeFromAllPropertyIndexes(indexes map[propertyIndexKey]*propertyIndex, id snowflake.ID) {
	for _, idx := range indexes {
		for valKey, idSet := range idx.entries {
			delete(idSet, id)
			if len(idSet) == 0 {
				delete(idx.entries, valKey)
			}
		}
	}
}

// removeNodeFromPropertyIndexes removes a node's properties from all matching property indexes.
// Caller must hold the store's write lock.
func removeNodeFromPropertyIndexes(indexes map[propertyIndexKey]*propertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		for _, p := range n.Properties() {
			key := propertyIndexKey{labelToken: tv, propertyKey: p.Key}
			if idx, ok := indexes[key]; ok {
				idx.remove(id, p.Value)
			}
		}
	}
}
