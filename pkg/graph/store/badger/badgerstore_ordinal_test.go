package badger

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreOrdinals(t *testing.T) {
	shared := &OrdinalAllocator{}
	for _, cfg := range []Config{
		{InMemory: true, FlushInterval: 1<<63 - 1},
		{InMemory: true, FlushInterval: 1<<63 - 1, DisableOrdinals: true},
		{InMemory: true, FlushInterval: 1<<63 - 1, SharedOrdinals: shared},
	} {
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		gen := newTestGen(t, 0)
		a := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
		b := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
		for _, n := range []*types.Node{a, b} {
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
		}
		r := types.NewRelationship(types.RelID(newTestGen(t, 1).Generate()), 1, a.ID(), b.ID())
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
		gotA, _ := bs.GetNode(a.ID())
		gotR, _ := bs.GetRelationship(r.ID())
		if cfg.DisableOrdinals {
			if bs.HasOrdinals() || bs.MaxNodeOrdinal() != 0 || gotA.Ordinal() != 0 || gotR.Ordinal() != 0 {
				t.Fatal("DisableOrdinals must hand out none")
			}
		} else {
			if !bs.HasOrdinals() || bs.MaxNodeOrdinal() != 2 || bs.MaxRelOrdinal() != 1 || gotA.Ordinal() != 1 || gotR.Ordinal() != 1 {
				t.Fatalf("ordinals: max %d/%d, a %d, r %d", bs.MaxNodeOrdinal(), bs.MaxRelOrdinal(), gotA.Ordinal(), gotR.Ordinal())
			}
			if err := bs.DeleteRelationship(r.ID()); err != nil {
				t.Fatal(err)
			}
			if err := bs.DeleteNode(a.ID()); err != nil {
				t.Fatal(err)
			}
			c := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
			if err := bs.PutNode(c); err != nil {
				t.Fatal(err)
			}
			if got, _ := bs.GetNode(c.ID()); got.Ordinal() != 3 {
				t.Fatalf("after a delete the next node gets %d, want 3 (1 is retired)", got.Ordinal())
			}
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		if bs.MaxNodeOrdinal() != 0 || bs.MaxRelOrdinal() != 0 {
			t.Fatal("a closed store reports 0")
		}
	}
	if shared.MaxNodeOrdinal() != 3 || shared.MaxRelOrdinal() != 1 {
		t.Fatalf("the shared allocator handed out %d/%d", shared.MaxNodeOrdinal(), shared.MaxRelOrdinal())
	}
	var nilAlloc *OrdinalAllocator
	var zero Store
	if nilAlloc.MaxNodeOrdinal() != 0 || nilAlloc.MaxRelOrdinal() != 0 || zero.HasOrdinals() || zero.MaxNodeOrdinal() != 0 || zero.MaxRelOrdinal() != 0 {
		t.Fatal("nil allocator / zero-value store")
	}
}
