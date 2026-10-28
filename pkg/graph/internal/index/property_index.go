package index

import (
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PropertyIndexKey uniquely identifies a property index by label token and property key.
type PropertyIndexKey struct {
	LabelToken  uint16
	PropertyKey string
}

// PropertyIndex stores a reverse mapping from canonical value keys to the
// sorted posting list of their entity IDs (backlog 72).
type PropertyIndex struct {
	Entries map[string]*IDPostings
	Mutated map[snowflake.ID]struct{} // non-nil during index creation Phase 2

	// Ordered numeric view — see property_index_range.go.
	numKeys    sortedChunks[float64]
	numBuckets map[float64]*IDPostings

	// Ordered STRING view — see property_index_string_range.go: the distinct
	// string values; a value's IDs are its Entries posting list ("s:"+value).
	// Maintained inside AddKey/removeKey.
	strKeys sortedChunks[string]

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

	// onDisk (PROPDISKLABEL, fixed at creation): badger keeps the entries in
	// its on-disk keyspace and this index holds none; the maintenance helpers
	// below skip it and no door reads it (serves).
	onDisk bool

	// valueStats caches ValueStats until the next mutation (round 9).
	valueStats atomic.Pointer[ValueStats]
}

// NewPropertyIndex creates an empty property index.
func NewPropertyIndex() *PropertyIndex {
	return &PropertyIndex{
		Entries: make(map[string]*IDPostings),
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

// NewPropertyIndexOnDisk creates the RAM handle of an index whose entries the
// store keeps on disk (badger PropertyIndexOnDisk, PropertyIndexOnDiskLabels).
func NewPropertyIndexOnDisk() *PropertyIndex {
	pi := NewPropertyIndex()
	pi.onDisk = true
	return pi
}

// OnDisk reports whether the store keeps the index's entries on disk.
func (pi *PropertyIndex) OnDisk() bool {
	return pi != nil && pi.onDisk
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
		pi.Entries = make(map[string]*IDPostings)
	}
	set := pi.Entries[vk]
	if set == nil {
		set = &IDPostings{}
		pi.Entries[vk] = set
		if pi.orderedViews() {
			pi.addOrderedStr(vk)
		}
	}
	set.add(id)
	pi.valueStats.Store(nil)
	if pi.orderedViews() {
		pi.addOrdered(id, vk)
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
	pi.valueStats.Store(nil)
	if set, ok := pi.Entries[vk]; ok {
		set.remove(id)
		if set.Len() == 0 {
			pi.dropKey(vk)
		}
	}
	if pi.orderedViews() {
		pi.removeOrdered(id, vk)
	}
	if pi.Mutated != nil {
		pi.Mutated[id] = struct{}{}
	}
}

// dropKey deletes the emptied value key vk, and its string from the ordered
// string view.
func (pi *PropertyIndex) dropKey(vk string) {
	delete(pi.Entries, vk)
	if pi.orderedViews() {
		pi.removeOrderedStr(vk)
	}
}

// Postings returns the sorted posting list of value (nil when no entity holds
// it or the value is not indexable). The list is the index's own: the caller
// reads it under the store's index lock and does not change it.
func (pi *PropertyIndex) Postings(value any) *IDPostings {
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

// forEachNodeIndexKey calls fn with each index of indexes on a label n
// carries and each value key n has under it (an Elements index: each element's
// key). A value key is built for an indexed property only (IDXKEYLAZY: walking
// n's properties built one per property and label, a copy of every value per
// write).
func forEachNodeIndexKey(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, fn func(key PropertyIndexKey, idx *PropertyIndex, valueKey string)) {
	for key, idx := range indexes {
		if !n.HasLabelTokenRaw(key.LabelToken) {
			continue
		}
		if !idx.Elements() {
			if vk, _ := n.IndexablePropertyValueKey(key.PropertyKey); vk != "" {
				fn(key, idx, vk)
			}
			continue
		}
		n.ForEachPropertyIndexKey(key.PropertyKey, true, func(vk string) bool {
			fn(key, idx, vk)
			return true
		})
	}
}

// AddNodeToPropertyIndexes indexes a node's properties into all matching property indexes
// in RAM. Caller must hold the store's write lock.
func AddNodeToPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	forEachNodeIndexKey(indexes, n, func(_ PropertyIndexKey, idx *PropertyIndex, valueKey string) {
		if !idx.onDisk {
			idx.AddKey(id, valueKey)
		}
	})
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

// RemoveNodeFromPropertyIndexes removes a node's properties from all matching property indexes
// in RAM. Caller must hold the store's write lock.
func RemoveNodeFromPropertyIndexes(indexes map[PropertyIndexKey]*PropertyIndex, n *types.Node, id snowflake.ID) {
	forEachNodeIndexKey(indexes, n, func(_ PropertyIndexKey, idx *PropertyIndex, valueKey string) {
		if !idx.onDisk {
			idx.removeKey(id, valueKey)
		}
	})
}
