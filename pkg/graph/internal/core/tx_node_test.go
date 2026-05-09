package core

import (
	"errors"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// --- GraphTx CRUD tests (update/delete with rollback) ---

func TestGraphTx_UpdateNode_Commit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	updated, err := tx.UpdateNode(nodeID, map[string]any{"name": "Alicia"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	_ = updated
	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	val, ok := got.GetProperty("name")
	if !ok || val != "Alicia" {
		t.Errorf("name = %v, want Alicia", val)
	}
}

func TestGraphTx_UpdateNode_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	if _, err := tx.UpdateNode(nodeID, map[string]any{"name": "Alicia"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	val, ok := got.GetProperty("name")
	if !ok || val != "Alice" {
		t.Errorf("name after rollback = %v, want Alice", val)
	}
}

func TestGraphTx_UpdateNode_MultipleTimes(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice", "age": int64(30)})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	if _, err := tx.UpdateNode(nodeID, map[string]any{"name": "Bob"}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.UpdateNode(nodeID, map[string]any{"age": int64(99)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Both updates should be reverted — original state restored.
	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	name, _ := got.GetProperty("name")
	age, _ := got.GetProperty("age")
	if name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
	if age != int64(30) {
		t.Errorf("age = %v, want 30", age)
	}
}

func TestGraphTx_DeleteNode_Commit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add("KNOWS", n1, n2, nil); err != nil {
		t.Fatal(err)
	}

	tx := g.BeginTx()
	if err := tx.DeleteNode(n1.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Node and cascade-deleted relationship should be gone.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 1 {
		t.Errorf("node count after delete commit: got %d, want 1", nc)
	}
	if rc != 0 {
		t.Errorf("rel count after delete commit: got %d, want 0", rc)
	}
}

func TestGraphTx_DeleteNode_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Add("KNOWS", n1, n2, nil); err != nil {
		t.Fatal(err)
	}

	tx := g.BeginTx()
	if err := tx.DeleteNode(n1.ID()); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Node and cascade-deleted relationship should be restored.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 2 {
		t.Errorf("node count after rollback: got %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("rel count after rollback: got %d, want 1", rc)
	}

	// Verify the node's properties are intact.
	got, err := g.Nodes.Get(n1.ID())
	if err != nil {
		t.Fatalf("GetNode after rollback: %v", err)
	}
	name, ok := got.GetProperty("name")
	if !ok || name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
}

func TestGraphTx_SetNodeProperty_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	if err := tx.SetNodeProperty(nodeID, "age", int64(42)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	_, ok := got.GetProperty("age")
	if ok {
		t.Error("age should not exist after rollback")
	}
}

// --- GraphTx ByID relationship tests ---

func TestTxGetNode(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	got, err := tx.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode in tx: %v", err)
	}
	name, ok := got.GetProperty("name")
	if !ok || name != "Alice" {
		t.Errorf("name = %v, want Alice", name)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTxGetNode_AfterDone(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_, err = tx.GetNode(n.ID())
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("GetNode after commit: got %v, want storepkg.ErrTxDone", err)
	}
}
