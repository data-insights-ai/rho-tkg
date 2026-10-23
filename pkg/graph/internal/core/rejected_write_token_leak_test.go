// Tests in this file pin R4-F14 and R4-F15 from the 2026-05-08
// maintainability review:
//
//   - R4-F14: rejected create/import operations must not allocate
//     registry tokens or consume IDs. Pre-fix, allocating tokens before
//     the rejection gate (self-loop, ID==0, duplicate ID) permanently
//     polluted the label/rel-type registry on every rejection.
//
//   - R4-F15: collision probes (GetNode / GetRelationship) on the
//     import path must surface non-not-found store errors, not treat
//     every error as absence and proceed with the create.
package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R4-F14: a self-loop rejection in Rels.Add must not register the
// relationship type token. Verify by adding two distinct unused rel
// types — one rejected (self-loop) and one accepted — and asserting
// the rejected type is NOT in the registry.
func TestR4_RejectedRelAdd_DoesNotAllocateRelTypeToken(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := g.Rels.Add(context.Background(), "REJECTED_TYPE", n, n, nil); !errors.Is(err, ErrSelfLoop) {
		t.Fatalf("expected ErrSelfLoop, got %v", err)
	}

	if _, ok := g.relTypes.Lookup("REJECTED_TYPE"); ok {
		t.Errorf("REJECTED_TYPE token registered despite self-loop rejection (R4-F14)")
	}
}

// R4-F14 batch counterpart: same invariant for BatchBuilder.AddRelationship.
func TestR4_RejectedBatchAddRel_DoesNotAllocateRelTypeToken(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	n, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddRelationship("REJECTED_BATCH_TYPE", n, n, nil); !errors.Is(err, ErrSelfLoop) {
		t.Fatalf("expected ErrSelfLoop, got %v", err)
	}

	if _, ok := g.relTypes.Lookup("REJECTED_BATCH_TYPE"); ok {
		t.Errorf("REJECTED_BATCH_TYPE token registered despite self-loop rejection (R4-F14 batch)")
	}
}

// R4-F14: duplicate-ID rejection in Nodes.Import must not register the
// node's labels. Pre-fix, the labels were registered before the
// collision probe, so a duplicate import permanently polluted the
// label registry.
func TestR4_DuplicateImportNode_DoesNotAllocateLabelTokens(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	defer g.Close()

	first, err := g.Nodes.Add(context.Background(), []string{"Existing"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := g.Nodes.Import(context.Background(), first.ID(), []string{"NEW_LABEL_REJECTED"}, nil); !errors.Is(err, storepkg.ErrNodeExists) {
		t.Fatalf("expected ErrNodeExists, got %v", err)
	}

	if _, ok := g.labels.Lookup("NEW_LABEL_REJECTED"); ok {
		t.Errorf("NEW_LABEL_REJECTED token registered despite duplicate-ID rejection (R4-F14)")
	}
}

// R4-F15: a non-not-found error from the store's GetRelationship probe
// must propagate to the caller, not be silently swallowed.
//
// Achieve this by wrapping a memory store with a probe-fault store
// that injects a non-sentinel error from GetRelationship for one
// target ID, then attempt Import on that ID.
func TestR4_ImportRel_StoreProbeError_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic probe fault")
	fs := &relProbeFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	relID := g.Rels.NextID()
	fs.target = relID
	fs.enabled = true

	_, importErr := g.Rels.Import(context.Background(), relID, "LINK", a, b, nil)
	if importErr == nil {
		t.Fatal("expected probe-fault to surface, got nil error")
	}
	if !errors.Is(importErr, injected) {
		t.Errorf("expected wrapped %v, got %v", injected, importErr)
	}
}

// R4-F15 node counterpart.
func TestR4_ImportNode_StoreProbeError_Propagates(t *testing.T) {
	t.Parallel()
	injected := errors.New("synthetic probe fault")
	fs := &nodeProbeFaultStore{Store: memory.New(), err: injected}
	g, err := New(Config{Store: fs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	id := g.Nodes.NextID()
	fs.target = id
	fs.enabled = true

	_, importErr := g.Nodes.Import(context.Background(), id, []string{"X"}, nil)
	if importErr == nil {
		t.Fatal("expected probe-fault to surface, got nil error")
	}
	if !errors.Is(importErr, injected) {
		t.Errorf("expected wrapped %v, got %v", injected, importErr)
	}
}

// relProbeFaultStore returns the configured non-sentinel err from a
// single GetRelationship probe so the import collision-probe path can
// be driven into its error branch deterministically.
type relProbeFaultStore struct {
	storepkg.Store
	target  types.RelID
	err     error
	enabled bool
}

func (s *relProbeFaultStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if s.enabled && id == s.target {
		s.enabled = false
		return nil, s.err
	}
	return s.Store.GetRelationship(id)
}

type nodeProbeFaultStore struct {
	storepkg.Store
	target  types.NodeID
	err     error
	enabled bool
}

func (s *nodeProbeFaultStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.enabled && id == s.target {
		s.enabled = false
		return nil, s.err
	}
	return s.Store.GetNode(id)
}
