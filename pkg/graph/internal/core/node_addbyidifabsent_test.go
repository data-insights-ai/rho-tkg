package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// S3 contract: Nodes.AddByIDIfAbsent is the Node parallel of
// Rels.AddByIDIfAbsent. If a node with the supplied id already exists,
// return (existingNode, false, nil). Otherwise, create + return (newNode,
// true, nil).

func TestNodeAddByIDIfAbsent_Inserts(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	id := g.Nodes.NextID()
	got, created, err := g.Nodes.AddByIDIfAbsent(context.Background(), id, []string{"Case"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("AddByIDIfAbsent fresh: %v", err)
	}
	if !created {
		t.Fatalf("created = false on fresh id, want true")
	}
	if got == nil || got.ID() != id {
		t.Fatalf("returned node = %+v, want id %d", got, id)
	}
}

func TestNodeAddByIDIfAbsent_ReturnsExistingWhenPresent(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	id := g.Nodes.NextID()

	first, created, err := g.Nodes.AddByIDIfAbsent(context.Background(), id, []string{"Case"}, map[string]any{"k": "v1"})
	if err != nil {
		t.Fatalf("first AddByIDIfAbsent: %v", err)
	}
	if !created {
		t.Fatalf("first call created = false, want true")
	}

	// Second call with different props must NOT mutate; must return the existing.
	second, created2, err := g.Nodes.AddByIDIfAbsent(context.Background(), id, []string{"Case"}, map[string]any{"k": "v2"})
	if err != nil {
		t.Fatalf("second AddByIDIfAbsent: %v", err)
	}
	if created2 {
		t.Fatalf("second call created = true on existing id, want false")
	}
	if second.ID() != first.ID() {
		t.Fatalf("returned ID %d, want %d", second.ID(), first.ID())
	}
	// Properties of the existing node are preserved — the second call's
	// props are intentionally ignored when the node already exists.
	if v, ok := second.GetProperty("k"); !ok || v != "v1" {
		t.Fatalf("existing prop k = (%v, %v), want (v1, true)", v, ok)
	}
}

func TestNodeAddByIDIfAbsent_RejectsZeroAndNegativeIDs(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	if _, _, err := g.Nodes.AddByIDIfAbsent(context.Background(), types.NodeID(0), []string{"Case"}, nil); !errors.Is(err, ErrZeroID) {
		t.Fatalf("zero id = %v, want ErrZeroID", err)
	}
	if _, _, err := g.Nodes.AddByIDIfAbsent(context.Background(), types.NodeID(-1), []string{"Case"}, nil); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("negative id = %v, want ErrInvalidID", err)
	}
}

func TestNodeAddByIDIfAbsent_ClosedGraph(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	id := g.Nodes.NextID()
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, _, err := g.Nodes.AddByIDIfAbsent(context.Background(), id, []string{"Case"}, nil); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("AddByIDIfAbsent after Close = %v, want ErrGraphClosed", err)
	}
}

// Suppress the unused-import lint for storepkg — we keep it imported for
// parity with sister tests in this package.
var _ = storepkg.ErrNodeNotFound
