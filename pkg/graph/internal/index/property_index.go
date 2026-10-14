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

	// Ordered numeric view — see property_index_range.go.
	numKeys    sortedChunks[float64]
	numBuckets map[float64]map[snowflake.ID]struct{}

	// Ordered STRING view — see property_index_string_range.go. Maintained
	// alongside numKeys inside AddKey/removeKey.
	strKeys    sortedChunks[string]
	strBuckets map[string]map[snowflake.ID]struct{}

	// numImpreciseCount counts currently indexed entries whose integer
	// magnitude exceeds 2^53 (float64 sort key may collide). While > 0,
	// RangeCardinality declines and the caller falls back to the exact scan.
	// A count, not a sticky bool (BACKLOG 16j): add/removeOrdered keep it
	// symmetric per (id, vk). purgeOrdered does not decrement it (original vk
	// unknown; staying imprecise is the safe direction).
	numImpreciseCount int

	// rangeCounts (opt-in per index, fixed at creation): numKeys in counted
	// mode, so RangeCardinality is two prefix sums instead of a walk.
	rangeCounts bool

	// elements (backlog 63, opt-in, fixed at creation): each list element is
	// a key. No ordered views: range and prefix accessors report supported=false.
	elements bool
}

// NewPropertyIndex creates an empty property index.
func NewPropertyIndex() *PropertyIndex {
	return &PropertyIndex{
		Entries: make(map[string]map[snowflake.ID]struct{}),
	}
}

// NewPropertyIndexWith creates an empty property index; rangeCounts selects
// the counted ordered view (see the rangeCounts field), elements the
// per-element keys (see the elements field). The store refuses both together.
func NewPropertyIndexWith(rangeCounts, elements bool) *PropertyIndex {
	pi := NewPropertyIndex()
	pi.rangeCounts = rangeCounts
	pi.elements = elements
	return pi
}

// RangeCounts reports whether the index was created with range counts.
func (pi *PropertyIndex) RangeCounts() bool {
	return pi != nil && pi.rangeCounts
}

// Elements reports whether the index keys each list element (backlog 63).
func (pi *PropertyIndex) Elements() bool {
	return pi != nil && pi.elements
}

// orderedViews reports whether the index maintains its ordered numeric and
// string views (every index but an Elements one).
func (pi *PropertyIndex) orderedViews() bool {
	return !pi.elements
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
	if pi.orderedViews() {
		pi.addOrdered(id, vk)
		pi.addOrderedStr(id, vk)
	}
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
	if pi.orderedViews() {
		pi.removeOrdered(id, vk)
		pi.removeOrderedStr(id, vk)
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

// ElementsOf returns the selector types.Node.ForEachIndexKey takes: whether
// (labelToken, propertyKey) is an Elements index. Consulted for list values only.
func ElementsOf(indexes map[PropertyIndexKey]*PropertyIndex, labelToken uint16) func(propertyKey string) bool {
	return func(propertyKey string) bool {
		return indexes[PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}].Elements()
	}
}

// RelElementsOf is ElementsOf for the rel property indexes of one type.
func RelElementsOf(indexes map[RelPropertyIndexKey]*PropertyIndex, relTypeToken uint16) func(propertyKey string) bool {
	return func(propertyKey string) bool {
		return indexes[RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propertyKey}].Elements()
	}
}

// AddEntityKeys indexes one entity's value: the scalar key, or every element
// key of an Elements index. Backfill and rebuild use it to key rows as writes do.
func (pi *PropertyIndex) AddEntityKeys(id snowflake.ID, propertyKey string, keys func(propertyKey string, elements bool, fn func(valueKey string) bool)) {
	if pi == nil {
		return
	}
	keys(propertyKey, pi.elements, func(vk string) bool {
		pi.AddKey(id, vk)
		return true
	})
}

// AddNodeToPropertyIndexes indexes a node's properties into all matching property indexes.
// Caller must hold the store's write lock.
func AddNodeToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	if len(indexes) == 0 {
		return
	}
	for i := 0; i < n.LabelTokenCount(); i++ {
		labelToken := n.LabelTokenRawAt(i)
		n.ForEachIndexKey(ElementsOf(indexes, labelToken), func(propertyKey, valueKey string) bool {
			key := PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
			if idx, ok := indexes[key]; ok {
				idx.AddKey(id, valueKey)
			}
			return true
		})
	}
}

// PurgeNodeFromAllPropertyIndexes removes a node ID from every property index.
// Brute-force O(V) fallback for when the node is unavailable (corruption path).
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
		idx.purgeOrdered(id)
		idx.purgeOrderedStr(id)
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
		n.ForEachIndexKey(ElementsOf(indexes, labelToken), func(propertyKey, valueKey string) bool {
			key := PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
			if idx, ok := indexes[key]; ok {
				idx.removeKey(id, valueKey)
			}
			return true
		})
	}
}
