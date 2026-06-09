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
		if _, ok := idSet[id]; ok {
			return true
		}
	}
	return false
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
	if got := idx.Lookup("Alice"); len(got) != 1 {
		t.Fatalf("zero-value Lookup after Add len = %d, want 1", len(got))
	}
	idx.Remove(snowflake.ID(1), "Alice")
	if got := idx.Lookup("Alice"); got != nil {
		t.Fatalf("zero-value Lookup after Remove = %v, want nil", got)
	}

	var nilIdx *PropertyIndex
	nilIdx.Add(snowflake.ID(1), "Alice")
	nilIdx.Remove(snowflake.ID(1), "Alice")
	if got := nilIdx.Lookup("Alice"); got != nil {
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
	if got := idx.Lookup("Alice"); len(got) != 1 {
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

func TestPropertyIndexLookupReturnsIndependentMap(t *testing.T) {
	t.Parallel()

	idx := NewPropertyIndex()
	idx.Add(snowflake.ID(1), "Alice")
	idx.Add(snowflake.ID(2), "Alice")

	got := idx.Lookup("Alice")
	if len(got) != 2 {
		t.Fatalf("Lookup len = %d, want 2", len(got))
	}
	delete(got, snowflake.ID(1))
	got[snowflake.ID(99)] = struct{}{}

	again := idx.Lookup("Alice")
	if _, ok := again[snowflake.ID(1)]; !ok {
		t.Fatal("mutating Lookup result removed ID from index")
	}
	if _, ok := again[snowflake.ID(99)]; ok {
		t.Fatal("mutating Lookup result inserted ID into index")
	}
}

func TestPropertyIndexNodeIDsReturnsIndependentSlice(t *testing.T) {
	t.Parallel()

	idx := NewPropertyIndex()
	idx.Add(snowflake.ID(1), "Alice")
	idx.Add(snowflake.ID(2), "Alice")

	got := idx.NodeIDs("Alice")
	if len(got) != 2 {
		t.Fatalf("NodeIDs len = %d, want 2", len(got))
	}
	got[0] = 99

	again := idx.Lookup("Alice")
	if _, ok := again[snowflake.ID(99)]; ok {
		t.Fatal("mutating NodeIDs result inserted ID into index")
	}
	if len(again) != 2 {
		t.Fatalf("Lookup after NodeIDs mutation len = %d, want 2", len(again))
	}
}

func TestPropertyIndexNodeIDsMisses(t *testing.T) {
	t.Parallel()

	var nilIdx *PropertyIndex
	if got := nilIdx.NodeIDs("Alice"); got != nil {
		t.Fatalf("nil NodeIDs = %v, want nil", got)
	}

	idx := NewPropertyIndex()
	idx.Add(snowflake.ID(1), "Alice")
	if got := idx.NodeIDs([]string{"not", "indexable"}); got != nil {
		t.Fatalf("unindexable NodeIDs = %v, want nil", got)
	}
	if got := idx.NodeIDs("Bob"); got != nil {
		t.Fatalf("missing NodeIDs = %v, want nil", got)
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
	if got := nameIdx.Lookup("Alice"); len(got) != 1 {
		t.Fatalf("name index len = %d, want 1", len(got))
	}
	if metaIdx.Contains(snowflake.ID(1)) {
		t.Fatal("unindexable map property was added to property index")
	}
	if otherLabelIdx.Contains(snowflake.ID(1)) {
		t.Fatal("index for unmatched label received node")
	}

	RemoveNodeFromPropertyIndexes(indexes, n, snowflake.ID(1))
	if got := nameIdx.Lookup("Alice"); got != nil {
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
