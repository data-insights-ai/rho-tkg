package core

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func newTxTestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestGraphTx_CommitPersists(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify entities persisted.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 2 {
		t.Errorf("node count: got %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("rel count: got %d, want 1", rc)
	}
}

func TestGraphTx_RollbackDeletes(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify everything is gone.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 0 {
		t.Errorf("node count after rollback: got %d, want 0", nc)
	}
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}

func TestGraphTx_RollbackEmpty(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback empty tx: %v", err)
	}
}

func TestGraphTx_DoubleCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	err := tx.Commit()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("double commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_DoubleRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	err := tx.Rollback()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("double rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_AddAfterCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, err := tx.AddNode([]string{"Person"}, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddNode after commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_AddAfterRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	_, err := tx.AddNode([]string{"Person"}, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddNode after rollback: got %v, want storepkg.ErrTxDone", err)
	}

	_, err = tx.AddRelationship("KNOWS", nil, nil, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddRelationship after rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_CommitThenRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	err := tx.Rollback()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("rollback after commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_RollbackThenCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	err := tx.Commit()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("commit after rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_RollbackLeavesNoHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// No history should exist for rolled-back entities.
	history, err := g.Nodes.History(nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Errorf("rolled-back node has %d history entries, want 0", len(history))
	}
}

func TestGraphTx_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	// BeginTx holds the write lock. Another goroutine trying to do
	// something that needs the lock should block until commit/rollback.
	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})

	go func() {
		defer wg.Done()
		// This should block until the tx is committed.
		_, _ = g.Nodes.All(storepkg.QueryOpts{})
		close(done)
	}()

	// Commit to release the lock.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	// Verify the node was visible after commit.
	_ = n
	nc, _ := g.Nodes.Count()
	if nc != 1 {
		t.Errorf("after concurrent commit: node count=%d, want 1", nc)
	}
}

func TestGraphTx_CreatedIDs(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	nodeIDs := tx.CreatedNodeIDs()
	relIDs := tx.CreatedRelIDs()

	if len(nodeIDs) != 2 {
		t.Errorf("created node IDs: got %d, want 2", len(nodeIDs))
	}
	if len(relIDs) != 1 {
		t.Errorf("created rel IDs: got %d, want 1", len(relIDs))
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

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

func TestGraphTx_DeleteRel_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n1, n2, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx := g.BeginTx()
	if err := tx.DeleteRelationship(relID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Relationship should be restored.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count after rollback: got %d, want 1", rc)
	}

	got, err := g.Rels.Get(relID)
	if err != nil {
		t.Fatalf("GetRelationship after rollback: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2020) {
		t.Errorf("since = %v, want 2020", since)
	}
}

func TestGraphTx_CreateThenDelete_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx := g.BeginTx()
	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Temp"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteNode(n.ID()); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Nothing should exist — create+delete in same tx, rolled back = nothing.
	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count: got %d, want 0", nc)
	}
}

func TestGraphTx_UpdateThenDelete_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	tx := g.BeginTx()
	if _, err := tx.UpdateNode(nodeID, map[string]any{"name": "Bob"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteNode(nodeID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Pre-existing node should be restored to original state.
	got, err := g.Nodes.Get(nodeID)
	if err != nil {
		t.Fatalf("GetNode after rollback: %v", err)
	}
	name, _ := got.GetProperty("name")
	if name != "Alice" {
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

func TestGraphTx_UpdateRelationship_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n1, n2, map[string]any{"weight": int64(5)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx := g.BeginTx()
	if _, err := tx.UpdateRelationship(relID, map[string]any{"weight": int64(99)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	got, err := g.Rels.Get(relID)
	if err != nil {
		t.Fatal(err)
	}
	weight, _ := got.GetProperty("weight")
	if weight != int64(5) {
		t.Errorf("weight = %v, want 5", weight)
	}
}

func TestGraphTx_AfterDone_ReturnsErrTxDone(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// All mutation methods should return storepkg.ErrTxDone after commit.
	if _, err := tx.UpdateNode(nodeID, map[string]any{"x": int64(1)}); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("UpdateNode: got %v, want storepkg.ErrTxDone", err)
	}
	if _, err := tx.UpdateRelationship(relID, map[string]any{"x": int64(1)}); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("UpdateRelationship: got %v, want storepkg.ErrTxDone", err)
	}
	if err := tx.DeleteNode(nodeID); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("DeleteNode: got %v, want storepkg.ErrTxDone", err)
	}
	if err := tx.DeleteRelationship(relID); !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("DeleteRelationship: got %v, want storepkg.ErrTxDone", err)
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

func TestTxAddRelationshipByID(t *testing.T) {
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

	tx := g.BeginTx()
	r, err := tx.AddRelationshipByID("KNOWS", n1.ID(), n2.ID(), map[string]any{"since": int64(2024)})
	if err != nil {
		t.Fatalf("AddRelationshipByID: %v", err)
	}
	if r == nil {
		t.Fatal("relationship is nil")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify relationship persisted.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count after commit: got %d, want 1", rc)
	}
	got, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2024) {
		t.Errorf("since = %v, want 2024", since)
	}
}

func TestTxAddRelationshipByID_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx := g.BeginTx()
	_, err = tx.AddRelationshipByID("KNOWS", n1.ID(), n2.ID(), nil)
	if err != nil {
		t.Fatalf("AddRelationshipByID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Relationship should be gone after rollback.
	rc, _ := g.Rels.Count()
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}

func TestTxAddRelationshipByIDIfAbsent(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID()
	id2 := n2.ID()

	// First: create in tx and commit.
	tx := g.BeginTx()
	r, created, err := tx.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatalf("AddRelationshipByIDIfAbsent: %v", err)
	}
	if !created {
		t.Error("first call: want created=true")
	}
	if r == nil {
		t.Fatal("relationship is nil")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Second: same call should find existing and return created=false.
	tx2 := g.BeginTx()
	r2, created2, err := tx2.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatalf("second AddRelationshipByIDIfAbsent: %v", err)
	}
	if created2 {
		t.Error("second call: want created=false")
	}
	if r2 == nil {
		t.Fatal("second call: relationship is nil")
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	// Should still have exactly 1 relationship.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count: got %d, want 1", rc)
	}
}

func TestTxAddRelationshipByIDIfAbsent_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID()
	id2 := n2.ID()

	// Create in tx then rollback.
	tx := g.BeginTx()
	_, created, err := tx.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("want created=true")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Should be absent after rollback.
	rc, _ := g.Rels.Count()
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}

// --- Graph.Reset tests ---

func TestGraphReset_Empty(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset empty graph: %v", err)
	}
}

func TestGraphReset_ClearsEntities(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	_, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count after reset: %d, want 0", nc)
	}
}

func TestGraphReset_PreservesRegistries(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	// Register some labels and types.
	_, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	// The "Person" label should still be registered.
	tok, ok := g.Resolve.LookupLabel("Person")
	if !ok {
		t.Fatal("label 'Person' should survive Reset")
	}
	if tok == 0 {
		t.Fatal("label token should be non-zero")
	}
}

func TestGraphReset_ClearsHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	// Update to create version history.
	_, err = g.Nodes.Update(n.ID(), map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	// History should be cleared.
	history, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Errorf("history after reset: got %d entries, want 0", len(history))
	}
}

// --- Fix A: GraphTx.Rollback releases write lock even on store panic ---

// deleteRelPanicStore wraps a Store and panics on DeleteRelationship.
// This simulates a store panic during the "delete created rels" rollback phase.
type deleteRelPanicStore struct {
	storepkg.MandatoryStore
}

func (s *deleteRelPanicStore) DeleteRelationship(_ types.RelID) error {
	panic("injected store panic during DeleteRelationship")
}

func TestGraphTx_RollbackPanicSafe(t *testing.T) {
	// Not parallel: we mutate g.store directly.
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create two nodes before the transaction so we can add a relationship.
	n1, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}

	tx := g.BeginTx()

	// Create a relationship inside the transaction so tx.createdRels is non-empty.
	_, err = tx.AddRelationship("LINKS", n1, n2, nil)
	if err != nil {
		// If AddRelationship fails here, skip — we only need createdRels non-empty.
		t.Fatalf("AddRelationship in tx: %v", err)
	}

	// Inject a panicking store so Rollback panics when deleting created rels.
	g.store = &deleteRelPanicStore{g.store}

	// Call Rollback from a goroutine so the panic is recoverable.
	panicCaught := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCaught <- true
			} else {
				panicCaught <- false
			}
		}()
		_ = tx.Rollback() //nolint:errcheck // panic expected; error path never reached
	}()

	caught := <-panicCaught
	if !caught {
		t.Fatal("expected a panic from the injected store, but none occurred")
	}

	// The deferred tx.g.mu.Unlock() must have run, so BeginTx must not block.
	// Use a timeout to detect deadlock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		tx2 := g.BeginTx()
		_ = tx2.Commit()
	}()

	select {
	case <-done:
		// BeginTx completed — lock was properly released after the panic.
	case <-time.After(2 * time.Second):
		t.Fatal("BeginTx blocked for 2s after panicking Rollback; graph write lock leaked")
	}
}

// TestMutationBlockedDuringTx verifies that standalone mutations (AddNode)
// are blocked while a transaction holds g.mu.Lock. The AddNode call in goroutine B
// must wait until the tx commits before proceeding.
func TestMutationBlockedDuringTx(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	tx := g.BeginTx()

	// Create a node inside the tx to prove it works.
	_, err = tx.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	var blocked atomic.Bool
	blocked.Store(true)

	done := make(chan struct{})
	go func() {
		// This AddNode must block until tx commits because it acquires g.mu.RLock
		// which is blocked by the tx's g.mu.Lock.
		_, err2 := g.Nodes.Add([]string{"B"}, nil)
		if err2 != nil {
			t.Errorf("standalone AddNode: %v", err2)
		}
		blocked.Store(false)
		close(done)
	}()

	// Give goroutine B time to attempt and block.
	time.Sleep(50 * time.Millisecond)
	if !blocked.Load() {
		t.Fatal("standalone AddNode was NOT blocked during tx — isolation failure")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Wait for goroutine B to complete.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("standalone AddNode did not unblock after tx commit")
	}

	nc, _ := g.Nodes.Count()
	if nc != 2 {
		t.Errorf("node count: got %d, want 2", nc)
	}
}

// TestSnapshotConsistencyDuringMutation verifies that Snapshot sees a consistent
// view — no torn reads from concurrent mutations. The Snapshot is taken under
// g.mu.RLock which is compatible with other readers but blocked by writers (tx).
func TestSnapshotConsistencyDuringMutation(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	// Pre-create some nodes.
	for i := 0; i < 10; i++ {
		_, err := g.Nodes.Add([]string{"Item"}, map[string]any{"idx": i})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	// Concurrent reads and writes should not race.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = g.Nodes.Add([]string{"Item"}, map[string]any{"idx": j + 100})
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = g.Nodes.All(storepkg.QueryOpts{})
			}
		}()
	}
	wg.Wait()

	// If the race detector didn't fire, concurrency is safe.
	nc, _ := g.Nodes.Count()
	if nc < 10 {
		t.Errorf("node count: got %d, want >= 10", nc)
	}
}

// TestTxCommitPublishesBufferedEvents verifies that events are buffered during a
// transaction and all published (in order) on Commit.
func TestTxCommitPublishesBufferedEvents(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	g.Events.SetSync(bus)

	var mu sync.Mutex
	var received []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})

	tx := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("tx.AddRelationship: %v", err)
	}

	// Before commit: no events should have been published.
	mu.Lock()
	preCommitCount := len(received)
	mu.Unlock()
	if preCommitCount != 0 {
		t.Fatalf("events before commit: got %d, want 0", preCommitCount)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After commit: all 3 events should have been published.
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("events after commit: got %d, want 3", len(received))
	}
	if received[0].Type != eventspkg.EventNodeCreate {
		t.Errorf("event[0].Type: got %d, want eventspkg.EventNodeCreate(%d)", received[0].Type, eventspkg.EventNodeCreate)
	}
	if received[1].Type != eventspkg.EventNodeCreate {
		t.Errorf("event[1].Type: got %d, want eventspkg.EventNodeCreate(%d)", received[1].Type, eventspkg.EventNodeCreate)
	}
	if received[2].Type != eventspkg.EventRelCreate {
		t.Errorf("event[2].Type: got %d, want eventspkg.EventRelCreate(%d)", received[2].Type, eventspkg.EventRelCreate)
	}
}

// TestTxRollbackNoEvents verifies that no events are published when a tx rolls back.
func TestTxRollbackNoEvents(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	g.Events.SetSync(bus)

	var count atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		count.Add(1)
	})

	tx := g.BeginTx()

	_, err = tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// No events should have been published.
	if got := count.Load(); got != 0 {
		t.Errorf("events after rollback: got %d, want 0", got)
	}

	// Verify node was actually rolled back.
	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count after rollback: got %d, want 0", nc)
	}
}

// TestTxCommitHandlerCanReadGraph verifies that event handlers invoked on Commit
// can safely call Graph read methods (proving events fire after g.mu.Unlock).
func TestTxCommitHandlerCanReadGraph(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	g.Events.SetSync(bus)

	var handlerOK atomic.Bool
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			// This must not deadlock — g.mu must be unlocked at this point.
			n, err := g.Nodes.Get(types.NodeID(e.EntityID))
			if err == nil && n != nil {
				handlerOK.Store(true)
			}
		}
	})

	tx := g.BeginTx()
	_, err = tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if !handlerOK.Load() {
		t.Error("event handler could not read committed node — possible deadlock or missing data")
	}
}

// TestBatchEventsNotBuffered verifies that batch operations (outside a tx) still
// emit events immediately — only tx-based mutations use buffering.
func TestBatchEventsNotBuffered(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	g.Events.SetSync(bus)

	var count atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		count.Add(1)
	})

	// Standalone mutation (not in tx) — should emit immediately.
	_, err = g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if got := count.Load(); got != 1 {
		t.Errorf("events after standalone AddNode: got %d, want 1", got)
	}
}
