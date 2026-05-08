// Tests in this file pin the F5 fix from the 2026-05-08 maintainability
// review: relationship endpoint hash refresh must surface every store error
// other than ErrNodeNotFound. Before the fix, both the standalone update
// path (updateRelationshipInternal) and the batch path (BatchBuilder.runRels)
// silently swallowed any non-nil GetNode error and wrote the relationship
// with empty FromNodeHash / ToNodeHash, making operational faults
// indistinguishable from a legitimate cascade-deleted endpoint.

package core

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// endpointFaultStore wraps a Store and, once enabled, returns a fixed
// non-sentinel error from GetNode for a single target node. Used to drive
// the F5 endpoint hash-refresh paths into their error branches without
// disturbing any other store interaction.
type endpointFaultStore struct {
	storepkg.Store
	target  types.NodeID
	err     error
	enabled atomic.Bool
}

func (s *endpointFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.enabled.Load() && id == s.target {
		return nil, s.err
	}
	return s.Store.GetNode(id)
}

func TestRelUpdate_EndpointReadFailure_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic disk fault")
	fs := &endpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	updated, err := g.Rels.UpdateWithContext(context.Background(), r.ID(), map[string]any{"since": 2025})
	if err == nil {
		t.Fatalf("UpdateWithContext: got nil error, want non-nil (synthetic GetNode failure must propagate)")
	}
	if !errors.Is(err, injected) {
		t.Fatalf("err = %v, want errors.Is(err, injected)", err)
	}
	if updated != nil {
		t.Fatalf("updated rel = %v, want nil on failure", updated)
	}

	fs.enabled.Store(false)
	got, err := g.Rels.GetWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after failed update: %v", err)
	}
	if v, ok := got.GetProperty("since"); ok {
		t.Fatalf("rel acquired property 'since' = %v after failed update", v)
	}
	if got.Version() != r.Version() {
		t.Fatalf("rel version = %d, want %d (no version bump on failed update)", got.Version(), r.Version())
	}
}

func TestRelUpdate_EndpointNotFound_StaysSilent(t *testing.T) {
	t.Parallel()
	fs := &endpointFaultStore{Store: memory.New(), err: storepkg.ErrNodeNotFound}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	updated, err := g.Rels.UpdateWithContext(context.Background(), r.ID(), map[string]any{"since": 2025})
	if err != nil {
		t.Fatalf("UpdateWithContext: %v (ErrNodeNotFound on endpoint must be tolerated silently)", err)
	}
	if updated == nil {
		t.Fatal("updated rel = nil, want non-nil (silent path returns the new rel)")
	}
	if v, ok := updated.GetProperty("since"); !ok || v != 2025 {
		t.Fatalf("rel.since = %v (%v), want 2025 / true", v, ok)
	}
}

func TestBatch_EndpointReadFailure_RecordsBatchError(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic disk fault")
	fs := &endpointFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	c, err := g.Nodes.Add([]string{"C"}, nil)
	if err != nil {
		t.Fatalf("AddNode C: %v", err)
	}

	bb := NewBatchBuilder(g)
	rFail, err := bb.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("queue rFail: %v", err)
	}
	if _, err := bb.AddRelationship("KNOWS", a, c, nil); err != nil {
		t.Fatalf("queue rOK: %v", err)
	}

	fs.target = b.ID()
	fs.enabled.Store(true)

	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if res.Failed != 1 {
		t.Fatalf("res.Failed = %d, want 1 (only the b-endpointed rel should fail)", res.Failed)
	}
	if res.Created < 1 {
		t.Fatalf("res.Created = %d, want >= 1 (the c-endpointed rel must still commit)", res.Created)
	}

	var matched *BatchError
	for i := range res.Errors {
		if res.Errors[i].ID == types.EntityID(rFail.ID()) {
			matched = &res.Errors[i]
			break
		}
	}
	if matched == nil {
		t.Fatalf("res.Errors did not include the failed rel; got %+v", res.Errors)
	}
	if matched.Op != "AddRelationship" {
		t.Errorf("Op = %q, want %q", matched.Op, "AddRelationship")
	}
	if !errors.Is(matched.Err, injected) {
		t.Errorf("Err = %v, want errors.Is(err, injected)", matched.Err)
	}
}
