package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// LendNode / LendRelationship route like GetNode / GetRelationship: a
// reference node, an event node, a same-shard and a cross-shard
// relationship (entity on the event shard, in/ on the reference shard).
func TestTieredStoreLend(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen, rgen := tieredNodeGen(t), tieredRelGen(t)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	ev1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	ev2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{ref, ev1, ev2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	same := types.NewRelationship(types.RelID(rgen.Generate()), 1, ev1.ID(), ev2.ID())
	cross := types.NewRelationship(types.RelID(rgen.Generate()), 1, ev1.ID(), ref.ID())
	for _, r := range []*types.Relationship{same, cross} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []*types.Node{ref, ev1} {
		a, err := ts.LendNode(n.ID())
		if err != nil || !a.IsFrozen() || a.ID() != n.ID() {
			t.Fatalf("LendNode(%d): %v", n.ID(), err)
		}
		b, _ := ts.LendNode(n.ID())
		if a != b {
			t.Fatalf("LendNode(%d) returned a copy", n.ID())
		}
	}
	for _, r := range []*types.Relationship{same, cross} {
		a, err := ts.LendRelationship(r.ID())
		if err != nil || !a.IsFrozen() || a.EndNodeID() != r.EndNodeID() {
			t.Fatalf("LendRelationship(%d): %v", r.ID(), err)
		}
		b, _ := ts.LendRelationship(r.ID())
		if a != b {
			t.Fatalf("LendRelationship(%d) returned a copy", r.ID())
		}
	}
	if _, err := ts.LendNode(types.NodeID(gen.Generate())); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("LendNode(missing): %v", err)
	}
	if _, err := ts.LendRelationship(types.RelID(rgen.Generate())); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("LendRelationship(missing): %v", err)
	}
	if _, err := ts.LendNode(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendNode(0): %v", err)
	}
	if _, err := ts.LendRelationship(-5); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("LendRelationship(-5): %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.LendNode(ref.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendNode after Close: %v", err)
	}
	if _, err := ts.LendRelationship(same.ID()); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("LendRelationship after Close: %v", err)
	}
}

func TestTieredLendHelpersAndNilEpochs(t *testing.T) {
	var nilStore *Store
	if nilStore.NodeMutationEpoch() != 0 || nilStore.RelMutationEpoch() != 0 {
		t.Fatal("nil store epochs must be 0")
	}
	if r, found, err := lendRelationshipRow(nil, types.RelID(1)); r != nil || found || err != nil {
		t.Fatal("a nil shard holds no row")
	}
	ts, _, signalTok := setupBatchDelete(t)
	gen, rgen := tieredNodeGen(t), tieredRelGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	r := types.NewRelationship(types.RelID(rgen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	shard := ts.HotShardForTest().Store()
	if got, found, err := lendRelationshipRow(shard, r.ID()); err != nil || !found || got.ID() != r.ID() {
		t.Fatalf("lendRelationshipRow = %v %v %v", got, found, err)
	}
	if err := ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := lendRelationshipRow(shard, r.ID()); found || err != nil {
		t.Fatalf("a deleted relationship: found=%v err=%v", found, err)
	}
}
