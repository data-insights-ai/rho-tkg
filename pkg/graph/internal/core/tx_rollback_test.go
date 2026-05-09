package core

import (
	"errors"
	"sync/atomic"
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

func TestGraphTx_RollbackDeletes(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()

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

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback empty tx: %v", err)
	}
}

func TestGraphTx_DoubleRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	err := tx.Rollback()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("double rollback: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_AddAfterRollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()

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

func TestGraphTx_CreateThenDelete_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()
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

	tx, _ := g.BeginTx()

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
