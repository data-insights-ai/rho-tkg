package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestMemoryStoreOrdinals(t *testing.T) {
	ms := New()
	if !ms.HasOrdinals() || ms.MaxNodeOrdinal() != 0 || ms.MaxRelOrdinal() != 0 {
		t.Fatal("a new store has ordinals and has handed out none")
	}
	a := types.NewNode(10, 1, nil)
	b := types.NewNode(20, 1, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.PutRelationship(types.NewRelationship(100, 1, a.ID(), b.ID())); err != nil {
		t.Fatal(err)
	}
	if ms.MaxNodeOrdinal() != 2 || ms.MaxRelOrdinal() != 1 {
		t.Fatalf("Max = %d/%d, want 2/1", ms.MaxNodeOrdinal(), ms.MaxRelOrdinal())
	}
	if a.Ordinal() != 0 {
		t.Fatal("the store must not write the ordinal into the caller's object")
	}
	// A caller-set ordinal is ignored.
	c := types.NewNode(30, 1, nil)
	_ = c.SetOrdinal(999)
	if err := ms.PutNode(c); err != nil {
		t.Fatal(err)
	}
	if got, _ := ms.LendNode(c.ID()); got.Ordinal() != 3 {
		t.Fatalf("stored ordinal %d, want 3 (the caller's 999 ignored)", got.Ordinal())
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if ms.MaxNodeOrdinal() != 0 || ms.MaxRelOrdinal() != 0 {
		t.Fatal("a closed store reports 0")
	}
	var nilStore *Store
	if nilStore.HasOrdinals() || nilStore.MaxNodeOrdinal() != 0 || nilStore.MaxRelOrdinal() != 0 {
		t.Fatal("nil store")
	}
}
