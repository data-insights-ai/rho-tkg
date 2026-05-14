package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type failPutNodeTieredWrapper struct {
	*tiered.Store
	err error
}

func (s *failPutNodeTieredWrapper) PutNode(*types.Node) error {
	return s.err
}

type failPutNodesBatchTieredWrapper struct {
	*tiered.Store
	err error
}

func (s *failPutNodesBatchTieredWrapper) PutNodesBatch([]*types.Node) error {
	return s.err
}

type failPutRelationshipTieredWrapper struct {
	*tiered.Store
	err error
}

func (s *failPutRelationshipTieredWrapper) PutRelationship(*types.Relationship) error {
	return s.err
}

type generatedCreateRelationshipRecorder struct {
	rel   *types.Relationship
	proof generatedcreate.Proof
}

func (r *generatedCreateRelationshipRecorder) PutNodeGeneratedID(*types.Node, generatedcreate.Proof) error {
	panic("PutNodeGeneratedID should not be called")
}

func (r *generatedCreateRelationshipRecorder) PutRelationshipGeneratedID(rel *types.Relationship, proof generatedcreate.Proof) error {
	r.rel = rel
	r.proof = proof
	return nil
}

func (r *generatedCreateRelationshipRecorder) PutNodesBatchGeneratedID([]*types.Node, generatedcreate.Proof) error {
	panic("PutNodesBatchGeneratedID should not be called")
}

func TestPutGeneratedRelationshipUsesGeneratedCreateCapability(t *testing.T) {
	rec := &generatedCreateRelationshipRecorder{}
	c := &Core{generatedCreate: rec}
	rel := types.NewRelationship(types.RelID(123), 1, types.NodeID(10), types.NodeID(20))

	if err := c.putGeneratedRelationship(rel); err != nil {
		t.Fatalf("putGeneratedRelationship: %v", err)
	}
	if rec.rel != rel {
		t.Fatalf("generated create rel = %p, want %p", rec.rel, rel)
	}
	if !rec.proof.Valid() {
		t.Fatal("generated create proof is not FreshGraphID")
	}
}

func TestGeneratedCreateFastPath_IgnoresTieredWrapperForNodeAdd(t *testing.T) {
	injected := errors.New("injected wrapper PutNode failure")
	wrapper := &failPutNodeTieredWrapper{Store: newTestTieredStore(t), err: injected}
	if _, ok := any(wrapper).(generatedcreate.Capability); !ok {
		t.Fatal("test wrapper must inherit generatedcreate.Capability from tiered.Store")
	}

	g, err := New(Config{Store: wrapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.generatedCreate != nil {
		t.Fatal("wrapped tiered.Store must not enable the generated-create fast path")
	}

	const label = "WrapperNodeCreate"
	if _, err := g.Nodes.Add([]string{label}, nil); !errors.Is(err, injected) {
		t.Fatalf("Nodes.Add error = %v, want injected wrapper error", err)
	}
	if _, ok := g.labels.Lookup(label); ok {
		t.Fatalf("label %q remained registered after failed wrapper PutNode", label)
	}
}

func TestGeneratedCreateFastPath_IgnoresTieredWrapperForBatchNodeAdd(t *testing.T) {
	injected := errors.New("injected wrapper PutNodesBatch failure")
	wrapper := &failPutNodesBatchTieredWrapper{Store: newTestTieredStore(t), err: injected}
	if _, ok := any(wrapper).(generatedcreate.Capability); !ok {
		t.Fatal("test wrapper must inherit generatedcreate.Capability from tiered.Store")
	}

	g, err := New(Config{Store: wrapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.generatedCreate != nil {
		t.Fatal("wrapped tiered.Store must not enable the generated-create fast path")
	}

	const label = "WrapperBatchNodeCreate"
	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := bb.AddNode([]string{label}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	res, err := bb.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if res.Created != 0 || res.Failed != 1 {
		t.Fatalf("Execute result created=%d failed=%d, want created=0 failed=1", res.Created, res.Failed)
	}
	if len(res.Errors) != 1 || !errors.Is(res.Errors[0].Err, injected) {
		t.Fatalf("Execute errors = %#v, want injected wrapper error", res.Errors)
	}
	if _, ok := g.labels.Lookup(label); ok {
		t.Fatalf("label %q remained registered after failed wrapper PutNodesBatch", label)
	}
}

func TestGeneratedCreateFastPath_IgnoresTieredWrapperForRelationshipAdd(t *testing.T) {
	injected := errors.New("injected wrapper PutRelationship failure")
	wrapper := &failPutRelationshipTieredWrapper{Store: newTestTieredStore(t), err: injected}
	if _, ok := any(wrapper).(generatedcreate.Capability); !ok {
		t.Fatal("test wrapper must inherit generatedcreate.Capability from tiered.Store")
	}

	g, err := New(Config{Store: wrapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.generatedCreate != nil {
		t.Fatal("wrapped tiered.Store must not enable the generated-create fast path")
	}

	a, err := g.Nodes.Add([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add start node: %v", err)
	}
	b, err := g.Nodes.Add([]string{"User"}, nil)
	if err != nil {
		t.Fatalf("Add end node: %v", err)
	}

	const typ = "WRAPPER_REL_CREATE"
	if _, err := g.Rels.Add(typ, a, b, nil); !errors.Is(err, injected) {
		t.Fatalf("Rels.Add error = %v, want injected wrapper error", err)
	}
	if _, ok := g.relTypes.Lookup(typ); ok {
		t.Fatalf("relationship type %q remained registered after failed wrapper PutRelationship", typ)
	}
}
