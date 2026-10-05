package badger

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreLend(t *testing.T) {
	bs, err := New(Config{InMemory: true, FlushInterval: 1<<63 - 1, CacheCapacity: 2})
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
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ { // the second round reads the rows the first cached
		n, err := bs.LendNode(a.ID())
		if err != nil || !n.IsFrozen() || n.ID() != a.ID() {
			t.Fatalf("LendNode: %v frozen=%v", err, n != nil && n.IsFrozen())
		}
		rel, err := bs.LendRelationship(r.ID())
		if err != nil || !rel.IsFrozen() || rel.EndNodeID() != b.ID() {
			t.Fatalf("LendRelationship: %v", err)
		}
	}
	n1, _ := bs.LendNode(a.ID())
	n2, _ := bs.LendNode(a.ID())
	if n1 != n2 {
		t.Fatal("a cached node is lent as the cached object, not a copy")
	}
	if _, err := bs.LendNode(types.NodeID(gen.Generate())); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("LendNode(missing): %v", err)
	}
	if _, err := bs.LendRelationship(types.RelID(gen.Generate())); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("LendRelationship(missing): %v", err)
	}
	if _, err := bs.LendNode(-1); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendNode(-1): %v", err)
	}
	if _, err := bs.LendRelationship(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendRelationship(0): %v", err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.LendNode(a.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendNode after Close: %v", err)
	}
	if _, err := bs.LendRelationship(r.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendRelationship after Close: %v", err)
	}
}
