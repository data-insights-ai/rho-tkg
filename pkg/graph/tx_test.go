package graph

import (
	"errors"
	"sync"
	"testing"
)

func newTxTestGraph(t *testing.T) *Graph {
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
	nc, _ := g.NodeCount()
	rc, _ := g.RelationshipCount()
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
	nc, _ := g.NodeCount()
	rc, _ := g.RelationshipCount()
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("double commit: got %v, want ErrTxDone", err)
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("double rollback: got %v, want ErrTxDone", err)
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("AddNode after commit: got %v, want ErrTxDone", err)
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("AddNode after rollback: got %v, want ErrTxDone", err)
	}

	_, err = tx.AddRelationship("KNOWS", nil, nil, nil)
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("AddRelationship after rollback: got %v, want ErrTxDone", err)
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("rollback after commit: got %v, want ErrTxDone", err)
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
	if !errors.Is(err, ErrTxDone) {
		t.Errorf("commit after rollback: got %v, want ErrTxDone", err)
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
	nodeID := n.InternalID().SnowflakeID()

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// No history should exist for rolled-back entities.
	history, err := g.GetNodeHistory(nodeID)
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
		_, _ = g.AllNodes(QueryOpts{})
		close(done)
	}()

	// Commit to release the lock.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	// Verify the node was visible after commit.
	_ = n
	nc, _ := g.NodeCount()
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

// --- Graph.Reset tests ---

func TestGraphReset_Empty(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	if err := g.Reset(); err != nil {
		t.Fatalf("Reset empty graph: %v", err)
	}
}

func TestGraphReset_ClearsEntities(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	_, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Reset(); err != nil {
		t.Fatal(err)
	}

	nc, _ := g.NodeCount()
	if nc != 0 {
		t.Errorf("node count after reset: %d, want 0", nc)
	}
}

func TestGraphReset_PreservesRegistries(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	// Register some labels and types.
	_, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Reset(); err != nil {
		t.Fatal(err)
	}

	// The "Person" label should still be registered.
	tok, ok := g.LookupLabel("Person")
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

	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	// Update to create version history.
	_, err = g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Reset(); err != nil {
		t.Fatal(err)
	}

	// History should be cleared.
	history, err := g.GetNodeHistory(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Errorf("history after reset: got %d entries, want 0", len(history))
	}
}
