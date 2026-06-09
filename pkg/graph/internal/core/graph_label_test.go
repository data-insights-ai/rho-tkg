package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestGraphNodeLabels(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})

	personTok, _ := g.Resolve.GetOrCreateLabel("Person")
	actorTok, _ := g.Resolve.GetOrCreateLabel("Actor")

	n := types.NewNode(types.NodeID(snowflake.ID(1)), personTok, []uint16{actorTok})
	labels := g.Nodes.Labels(n)

	if len(labels) != 2 {
		t.Fatalf("NodeLabels len = %d, want 2", len(labels))
	}
	if labels[0] != "Person" || labels[1] != "Actor" {
		t.Errorf("NodeLabels = %v, want [Person Actor]", labels)
	}
}

func TestGraphLookupLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")

	tok, ok := g.Resolve.LookupLabel("Person")
	if !ok {
		t.Fatal("LookupLabel(\"Person\") should return true")
	}
	if tok == 0 {
		t.Fatal("LookupLabel should return non-zero token")
	}

	_, ok = g.Resolve.LookupLabel("Unknown")
	if ok {
		t.Fatal("LookupLabel(\"Unknown\") should return false")
	}
}

func TestGraphUpdateNodeLabelsUnchanged(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add(context.Background(), []string{"Person", "Actor"}, map[string]any{"name": "Alice"})
	id := n.ID()

	g.Nodes.Update(context.Background(), id, map[string]any{"name": "Bob"})

	got, _ := g.Nodes.Get(context.Background(), id)
	labels := g.Nodes.Labels(got)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Actor" {
		t.Fatalf("labels after update = %v, want [Person Actor]", labels)
	}
}

// ─── UpdateRelationship tests ────────────────────────────────────────────────

func TestMemStoreNodeCountByLabel_AfterPut(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2})
	if err := ms.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	c1, _ := ms.NodeCountByLabel(1)
	c2, _ := ms.NodeCountByLabel(2)
	c3, _ := ms.NodeCountByLabel(3) // non-existent
	if c1 != 1 {
		t.Fatalf("label 1 count = %d, want 1", c1)
	}
	if c2 != 1 {
		t.Fatalf("label 2 count = %d, want 1", c2)
	}
	if c3 != 0 {
		t.Fatalf("label 3 count = %d, want 0", c3)
	}
}

func TestMemStoreNodeCountByLabel_AfterDelete(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, nil)
	ms.PutNode(n)
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	ms.PutNode(n2)

	ms.DeleteNode(types.NodeID(100))

	c, _ := ms.NodeCountByLabel(1)
	if c != 1 {
		t.Fatalf("label 1 count after delete = %d, want 1", c)
	}
}

func TestMemStoreNodeCountByLabel_MultiLabel(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	// Node with labels 1, 2, 3 — each label counter incremented.
	n := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2, 3})
	ms.PutNode(n)

	for _, tok := range []uint16{1, 2, 3} {
		c, _ := ms.NodeCountByLabel(tok)
		if c != 1 {
			t.Fatalf("label %d count = %d, want 1", tok, c)
		}
	}
}

func TestMemStoreNodeCountByLabel_CascadeDelete(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	n1 := types.NewNode(types.NodeID(snowflake.ID(100)), 1, []uint16{2})
	n2 := types.NewNode(types.NodeID(snowflake.ID(200)), 1, nil)
	ms.PutNode(n1)
	ms.PutNode(n2)

	r := types.NewRelationship(types.RelID(snowflake.ID(300)), 5, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	ms.PutRelationship(r)

	// Cascade deletes n1 and its relationship.
	ms.DeleteNodeCascade(types.NodeID(100))

	c1, _ := ms.NodeCountByLabel(1)
	if c1 != 1 {
		t.Fatalf("label 1 count after cascade = %d, want 1", c1)
	}
	c2, _ := ms.NodeCountByLabel(2)
	if c2 != 0 {
		t.Fatalf("label 2 count after cascade = %d, want 0", c2)
	}
	ct, _ := ms.RelCountByType(5)
	if ct != 0 {
		t.Fatalf("type 5 count after cascade = %d, want 0", ct)
	}
}

// --- Graph-level CountByLabel after batch add and cascade delete ---
