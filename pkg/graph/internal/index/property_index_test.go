package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Contains returns true if the given node ID exists in any value bucket.
// Test-only helper used during property index verification.
func (pi *PropertyIndex) Contains(id snowflake.ID) bool {
	for _, idSet := range pi.Entries {
		if idSet.Has(id) {
			return true
		}
	}
	return false
}

// lookupIDs (test-only) returns value's IDs ascending as a fresh slice, nil
// when none.
func lookupIDs(pi *PropertyIndex, value any) []snowflake.ID {
	return pi.Postings(value).AppendAfter(nil, 0, 0)
}

func TestPropertyIndex_Contains(t *testing.T) {
	t.Parallel()
	idx := NewPropertyIndex()

	if idx.Contains(snowflake.ID(1)) {
		t.Error("empty index should not contain anything")
	}

	idx.Add(snowflake.ID(1), "Alice")
	idx.Add(snowflake.ID(2), "Bob")

	if !idx.Contains(snowflake.ID(1)) {
		t.Error("should contain ID 1")
	}
	if !idx.Contains(snowflake.ID(2)) {
		t.Error("should contain ID 2")
	}
	if idx.Contains(snowflake.ID(3)) {
		t.Error("should not contain ID 3")
	}
}

func TestPropertyIndexZeroValueAndNilReceiverNoop(t *testing.T) {
	t.Parallel()

	var idx PropertyIndex
	idx.Add(snowflake.ID(1), "Alice")
	if got := lookupIDs(&idx, "Alice"); len(got) != 1 {
		t.Fatalf("zero-value Lookup after Add len = %d, want 1", len(got))
	}
	idx.Remove(snowflake.ID(1), "Alice")
	if got := lookupIDs(&idx, "Alice"); got != nil {
		t.Fatalf("zero-value Lookup after Remove = %v, want nil", got)
	}

	var nilIdx *PropertyIndex
	nilIdx.Add(snowflake.ID(1), "Alice")
	nilIdx.Remove(snowflake.ID(1), "Alice")
	if got := lookupIDs(nilIdx, "Alice"); got != nil {
		t.Fatalf("nil Lookup = %v, want nil", got)
	}

	PurgeNodeFromAllPropertyIndexes(map[PropertyIndexKey]*PropertyIndex{
		{LabelToken: 1, PropertyKey: "name"}: nil,
	}, snowflake.ID(1))
}

func TestPropertyIndexAddKey(t *testing.T) {
	t.Parallel()

	var idx PropertyIndex
	idx.AddKey(snowflake.ID(1), "s:Alice")
	if got := lookupIDs(&idx, "Alice"); len(got) != 1 {
		t.Fatalf("zero-value Lookup after AddKey len = %d, want 1", len(got))
	}

	idx.AddKey(snowflake.ID(2), "")
	if idx.Contains(snowflake.ID(2)) {
		t.Fatal("AddKey with empty key inserted an ID")
	}

	idx.Mutated = make(map[snowflake.ID]struct{})
	idx.AddKey(snowflake.ID(3), "s:Bob")
	if _, ok := idx.Mutated[snowflake.ID(3)]; !ok {
		t.Fatal("AddKey did not mark Mutated ID")
	}

	var nilIdx *PropertyIndex
	nilIdx.AddKey(snowflake.ID(4), "s:Carol")
}

func TestPropertyIndexPurgeMarksMutated(t *testing.T) {
	t.Parallel()

	id := snowflake.ID(7)
	idx := NewPropertyIndex()
	idx.Mutated = make(map[snowflake.ID]struct{})
	idx.AddKey(id, "s:Alice")
	delete(idx.Mutated, id)

	PurgeNodeFromAllPropertyIndexes(map[PropertyIndexKey]*PropertyIndex{
		{LabelToken: 1, PropertyKey: "name"}: idx,
	}, id)
	if idx.Contains(id) {
		t.Fatal("PurgeNodeFromAllPropertyIndexes left ID in index")
	}
	if _, ok := idx.Mutated[id]; !ok {
		t.Fatal("PurgeNodeFromAllPropertyIndexes did not mark Mutated ID")
	}
}

func TestPropertyIndexPostingsMisses(t *testing.T) {
	t.Parallel()

	var nilIdx *PropertyIndex
	if got := nilIdx.Postings("Alice"); got != nil {
		t.Fatalf("nil Postings = %v, want nil", got)
	}

	idx := NewPropertyIndex()
	idx.Add(snowflake.ID(2), "Alice")
	idx.Add(snowflake.ID(1), "Alice")
	if got := idx.Postings([]string{"not", "indexable"}); got != nil {
		t.Fatalf("unindexable Postings = %v, want nil", got)
	}
	if got := idx.Postings("Bob"); got != nil {
		t.Fatalf("missing Postings = %v, want nil", got)
	}
	if got := lookupIDs(idx, "Alice"); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("Postings(Alice) = %v, want [1 2] ascending", got)
	}
}

func TestAddAndRemoveNodeFromPropertyIndexes(t *testing.T) {
	t.Parallel()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, []uint16{20})
	if err := n.SetProperty("name", "Alice"); err != nil {
		t.Fatal(err)
	}
	if err := n.SetProperty("meta", map[string]any{"tags": []any{"alpha"}}); err != nil {
		t.Fatal(err)
	}

	nameIdx := NewPropertyIndex()
	metaIdx := NewPropertyIndex()
	otherLabelIdx := NewPropertyIndex()
	indexes := map[PropertyIndexKey]*PropertyIndex{
		{LabelToken: 10, PropertyKey: "name"}: nameIdx,
		{LabelToken: 20, PropertyKey: "meta"}: metaIdx,
		{LabelToken: 30, PropertyKey: "name"}: otherLabelIdx,
	}

	AddNodeToPropertyIndexes(indexes, n, snowflake.ID(1))
	if got := lookupIDs(nameIdx, "Alice"); len(got) != 1 {
		t.Fatalf("name index len = %d, want 1", len(got))
	}
	if metaIdx.Contains(snowflake.ID(1)) {
		t.Fatal("unindexable map property was added to property index")
	}
	if otherLabelIdx.Contains(snowflake.ID(1)) {
		t.Fatal("index for unmatched label received node")
	}

	RemoveNodeFromPropertyIndexes(indexes, n, snowflake.ID(1))
	if got := lookupIDs(nameIdx, "Alice"); got != nil {
		t.Fatalf("name index after remove = %v, want nil", got)
	}
}

func TestNodePropertyIndexHelpersNilNodeNoop(t *testing.T) {
	t.Parallel()

	idx := NewPropertyIndex()
	indexes := map[PropertyIndexKey]*PropertyIndex{
		{LabelToken: 10, PropertyKey: "name"}: idx,
	}

	AddNodeToPropertyIndexes(indexes, nil, snowflake.ID(1))
	RemoveNodeFromPropertyIndexes(indexes, nil, snowflake.ID(1))
	if idx.Contains(snowflake.ID(1)) {
		t.Fatal("nil node helper call mutated index")
	}
}

// TestIndexMaintenanceBuildsIndexedKeysOnly (IDXKEYLAZY, ai-soc's BO A/B
// kpdl: GC mark +20 s with DerivedPage's index on disk): adding a node or a
// relationship to the indexes builds the value key of an indexed property
// only. Before, each of a row's properties got one per label (a copy of the
// value), indexed or not: 20 unindexed properties cost 20 allocations or more.
func TestIndexMaintenanceBuildsIndexedKeysOnly(t *testing.T) {
	const unindexed = 20
	n := types.NewNode(1, 7, nil)
	r := types.NewRelationship(2, 3, 1, 1)
	for _, set := range []func(string, any) error{n.SetProperty, r.SetProperty} {
		if err := set("k", "indexed"); err != nil {
			t.Fatal(err)
		}
		for i := range unindexed {
			if err := set(string(rune('a'+i))+"p", "a value nobody indexes"); err != nil {
				t.Fatal(err)
			}
		}
	}
	nodes := map[PropertyIndexKey]*PropertyIndex{{LabelToken: 7, PropertyKey: "k"}: NewPropertyIndex()}
	rels := map[RelPropertyIndexKey]*PropertyIndex{{RelTypeToken: 3, PropertyKey: "k"}: NewPropertyIndex()}
	for name, add := range map[string]func(){
		"node": func() { AddNodeToPropertyIndexes(nodes, n, 1) },
		"rel":  func() { AddRelToPropertyIndexes(rels, r, 2) },
	} {
		if allocs := testing.AllocsPerRun(50, add); allocs >= unindexed/2 {
			t.Errorf("%s: %.0f allocations per add with %d unindexed properties", name, allocs, unindexed)
		}
	}
	if !nodes[PropertyIndexKey{LabelToken: 7, PropertyKey: "k"}].Contains(1) || !rels[RelPropertyIndexKey{RelTypeToken: 3, PropertyKey: "k"}].Contains(2) {
		t.Fatal("the indexed property was not indexed")
	}
}

// TestRelElementsIndexKeysEachElement: a relationship Elements index keys
// each element of a list value (the node twin: pkg/graph's Elements tests),
// and removing the relationship drops them.
func TestRelElementsIndexKeysEachElement(t *testing.T) {
	r := types.NewRelationship(2, 3, 1, 1)
	if err := r.SetProperty("tags", []any{"a", "b", "a"}); err != nil {
		t.Fatal(err)
	}
	idx := NewPropertyIndexWith(false, true)
	rels := map[RelPropertyIndexKey]*PropertyIndex{{RelTypeToken: 3, PropertyKey: "tags"}: idx}
	AddRelToPropertyIndexes(rels, r, 2)
	for _, v := range []string{"a", "b"} {
		if ids := idx.Entries[types.IndexablePropertyValueKey(v)]; ids == nil || !ids.Has(2) {
			t.Fatalf("element %q not indexed: %v", v, idx.Entries)
		}
	}
	if len(idx.Entries) != 2 {
		t.Fatalf("%d keys, want 2", len(idx.Entries))
	}
	RemoveRelFromPropertyIndexes(rels, r, 2)
	if len(idx.Entries) != 0 {
		t.Fatalf("after remove: %d keys, want 0", len(idx.Entries))
	}
}
