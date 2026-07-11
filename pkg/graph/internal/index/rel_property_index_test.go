package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relWithProps(t *testing.T, id snowflake.ID, typeToken uint16, props map[string]any) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(id), typeToken, types.NodeID(1000), types.NodeID(2000))
	for k, v := range props {
		if err := r.SetProperty(k, v); err != nil {
			t.Fatalf("SetProperty(%q): %v", k, err)
		}
	}
	return r
}

func relIDSet(ids []types.RelID) map[types.RelID]struct{} {
	out := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}

func TestRelPropertyIndexAddRemoveLookup(t *testing.T) {
	const knows = uint16(7)
	indexes := map[RelPropertyIndexKey]*PropertyIndex{
		{RelTypeToken: knows, PropertyKey: "weight"}: NewPropertyIndex(),
	}

	r1 := relWithProps(t, 100, knows, map[string]any{"weight": int64(5)})
	r2 := relWithProps(t, 200, knows, map[string]any{"weight": int64(5)})
	r3 := relWithProps(t, 300, knows, map[string]any{"weight": int64(9)})
	// Different type token — must not land in the knows/weight index.
	r4 := relWithProps(t, 400, uint16(8), map[string]any{"weight": int64(5)})

	for _, r := range []*types.Relationship{r1, r2, r3, r4} {
		AddRelToPropertyIndexes(indexes, r, r.ID().SnowflakeID())
	}

	idx := indexes[RelPropertyIndexKey{RelTypeToken: knows, PropertyKey: "weight"}]
	got := relIDSet(idx.RelIDs(int64(5)))
	want := relIDSet([]types.RelID{100, 200})
	if len(got) != len(want) {
		t.Fatalf("RelIDs(5) = %v, want %v", got, want)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("RelIDs(5) missing %d; got %v", id, got)
		}
	}
	if r4id := (types.RelID(400)); func() bool { _, ok := got[r4id]; return ok }() {
		t.Fatalf("RelIDs(5) must NOT contain the different-type rel 400")
	}

	// Type-safe key: string "5" must not collide with int 5.
	if ids := idx.RelIDs("5"); len(ids) != 0 {
		t.Fatalf("RelIDs(string \"5\") = %v, want empty (type-prefixed keys)", ids)
	}

	// Remove r1 — value bucket 5 keeps only r2.
	RemoveRelFromPropertyIndexes(indexes, r1, r1.ID().SnowflakeID())
	if ids := idx.RelIDs(int64(5)); len(ids) != 1 || ids[0] != 200 {
		t.Fatalf("after remove, RelIDs(5) = %v, want [200]", ids)
	}
}

func TestRelPropertyIndexRangeRelIDs(t *testing.T) {
	const knows = uint16(7)
	idx := NewPropertyIndex()
	for id, w := range map[snowflake.ID]int64{10: 1, 20: 5, 30: 9, 40: 15} {
		idx.AddKey(id, PropertyValueKey(w))
	}

	ids, supported := idx.RangeRelIDs(5, 12, true, true)
	if !supported {
		t.Fatal("RangeRelIDs supported=false on populated numeric view")
	}
	got := relIDSet(ids)
	// Over-selecting filter: candidates within widened [5,12]. 20(w=5),30(w=9)
	// are in range; caller post-filters exact bounds. 40(w=15) is out.
	if _, ok := got[types.RelID(20)]; !ok {
		t.Fatalf("RangeRelIDs missing rel 20 (weight 5); got %v", got)
	}
	if _, ok := got[types.RelID(30)]; !ok {
		t.Fatalf("RangeRelIDs missing rel 30 (weight 9); got %v", got)
	}
	if _, ok := got[types.RelID(40)]; ok {
		t.Fatalf("RangeRelIDs must NOT contain rel 40 (weight 15 > 12); got %v", got)
	}

	// Empty numeric view returns authoritative empty (supported=true).
	empty := NewPropertyIndex()
	if ids, supported := empty.RangeRelIDs(0, 100, true, true); !supported || len(ids) != 0 {
		t.Fatalf("empty RangeRelIDs = (%v, %v), want ([], true)", ids, supported)
	}

	// Nil index: unsupported.
	var nilIdx *PropertyIndex
	if _, supported := nilIdx.RangeRelIDs(0, 1, true, true); supported {
		t.Fatal("nil RangeRelIDs supported=true, want false")
	}
}

func TestRelPropertyIndexPurge(t *testing.T) {
	const knows = uint16(7)
	indexes := map[RelPropertyIndexKey]*PropertyIndex{
		{RelTypeToken: knows, PropertyKey: "weight"}: NewPropertyIndex(),
		{RelTypeToken: knows, PropertyKey: "since"}:  NewPropertyIndex(),
	}
	r := relWithProps(t, 500, knows, map[string]any{"weight": int64(3), "since": int64(2020)})
	AddRelToPropertyIndexes(indexes, r, r.ID().SnowflakeID())

	PurgeRelFromAllPropertyIndexes(indexes, r.ID().SnowflakeID())
	for key, idx := range indexes {
		if len(idx.Entries) != 0 {
			t.Fatalf("after purge, index %v still has entries %v", key, idx.Entries)
		}
	}
}

func TestRelPropertyIndexNilAndEmptyGuards(t *testing.T) {
	// Nil map / nil rel are no-ops (must not panic).
	AddRelToPropertyIndexes(nil, nil, 1)
	RemoveRelFromPropertyIndexes(nil, nil, 1)
	PurgeRelFromAllPropertyIndexes(nil, 1)

	var nilIdx *PropertyIndex
	if ids := nilIdx.RelIDs(int64(1)); ids != nil {
		t.Fatalf("nil RelIDs = %v, want nil", ids)
	}

	// Unindexable value returns nil.
	idx := NewPropertyIndex()
	idx.AddKey(1, PropertyValueKey(int64(1)))
	if ids := idx.RelIDs([]int{1, 2}); ids != nil {
		t.Fatalf("RelIDs(slice) = %v, want nil (unindexable)", ids)
	}
}
