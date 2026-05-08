package core

import (
	"errors"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

func TestMemStoreCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	err := g.Index.CreateProperty("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}
}

func TestMemStoreCreatePropertyIndex_Duplicate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.CreateProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexExists) {
		t.Fatalf("expected storepkg.ErrIndexExists, got %v", err)
	}
}

func TestMemStoreDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DropProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}
}

func TestMemStoreDropPropertyIndex_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")

	err := g.Index.DropProperty("Person", "name")
	if !errors.Is(err, storepkg.ErrIndexNotFound) {
		t.Fatalf("expected storepkg.ErrIndexNotFound, got %v", err)
	}
}

func TestMemStorePropertyIndex_AutoUpdate(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	// Verify index finds Alice.
	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after add: expected 1, got %d", len(nodes))
	}

	// Update the property.
	id := n.ID()
	g.Nodes.Update(id, map[string]any{"name": "Alicia"})

	// Old value should be gone.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after update: old value still found, got %d", len(nodes))
	}

	// New value should be found.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("after update: new value not found, got %d", len(nodes))
	}

	// Delete the node.
	g.Nodes.Delete(id)

	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("after delete: node still in index, got %d", len(nodes))
	}
}

// --- Graph-layer Property Index tests ---

func TestGraphCreatePropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")

	err := g.Index.CreateProperty("Person", "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex failed: %v", err)
	}

	// Unregistered label → no-op (no error).
	err = g.Index.CreateProperty("Unknown", "name")
	if err != nil {
		t.Fatalf("unregistered label should return nil, got %v", err)
	}
}

func TestGraphDropPropertyIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Resolve.GetOrCreateLabel("Person")
	g.Index.CreateProperty("Person", "name")

	err := g.Index.DropProperty("Person", "name")
	if err != nil {
		t.Fatalf("DropPropertyIndex failed: %v", err)
	}

	// Unregistered label → no-op.
	err = g.Index.DropProperty("Unknown", "name")
	if err != nil {
		t.Fatalf("unregistered label should return nil, got %v", err)
	}
}

func TestGraphPropertyIndex_MultipleValues(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Index.CreateProperty("Person", "name")

	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 Alices, got %d", len(nodes))
	}

	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Bob", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("expected 1 Bob, got %d", len(nodes))
	}
}

func TestGraphPropertyIndex_UpdateReflected(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	id := n.ID()
	g.Nodes.Update(id, map[string]any{"name": "Alicia"})

	// Old value gone.
	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("old value still found: %d", len(nodes))
	}

	// New value present.
	nodes, _ = g.Nodes.ByLabelAndProperty("Person", "name", "Alicia", storepkg.QueryOpts{})
	if len(nodes) != 1 {
		t.Fatalf("new value not found: %d", len(nodes))
	}
}

func TestGraphPropertyIndex_DeleteRemoves(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	g.Nodes.Delete(n.ID())

	nodes, _ := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if len(nodes) != 0 {
		t.Fatalf("deleted node still in index: %d", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_Hit(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	name, _ := nodes[0].GetProperty("name")
	if name != "Alice" {
		t.Fatalf("expected name=Alice, got %v", name)
	}
}

func TestMemStoreNodesByLabelAndProperty_Miss(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Bob", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestMemStoreNodesByLabelAndProperty_NoIndex(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})

	// No index — should fall back to scan.
	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("fallback scan: expected 1 node, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_Found(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob", "age": int64(25)})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected 1, got %d", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_NotFound(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	g.Index.CreateProperty("Person", "name")

	nodes, err := g.Nodes.ByLabelAndProperty("Person", "name", "Charlie", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil, got %d nodes", len(nodes))
	}
}

func TestGraphNodesByLabelAndProperty_UnregisteredLabel(t *testing.T) {
	t.Parallel()

	g, _ := New(Config{Store: memory.New()})
	nodes, err := g.Nodes.ByLabelAndProperty("Unknown", "name", "Alice", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nodes != nil {
		t.Fatalf("expected nil for unregistered label, got %d", len(nodes))
	}
}
