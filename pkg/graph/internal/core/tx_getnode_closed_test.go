package core

import (
	"context"
	"errors"
	"testing"
)

// BACKLOG 11a: GraphTx.GetNode/GetRelationship used bare lockActive() (only
// tx.mu) instead of lockActiveCore() (tx.mu + c.mu.RLock() + a closed check)
// — the pattern every OTHER tx read mirror (Export, Snapshot, VerifyShard,
// Labels, ...) uses. Since a v4.1.0 Path-B tx does NOT hold c.mu for its
// whole lifetime (only per-call), a concurrent Close() could complete (or be
// mid-flight) while GetNode/GetRelationship read from tx.g.store directly,
// with nothing checking tx.g.closed or excluding the read via c.mu — an
// inconsistency with every sibling method's documented ErrGraphClosed
// contract, and (under real concurrency) a race on the underlying store
// itself.
//
// This reproduces the logical half of the bug deterministically: begin a tx,
// Close() the graph out from under it (without committing/rolling back —
// Close() does not wait for open transactions, it only briefly takes
// c.mu.Lock to tear down index providers), then call GetNode/GetRelationship
// and assert the same ErrGraphClosed contract every sibling tx read method
// already honors.

func TestGraphTx_GetNode_AfterCloseReturnsErrGraphClosed(t *testing.T) {
	g := newTxTestGraph(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := tx.GetNode(n.ID()); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("tx.GetNode after Close = %v, want ErrGraphClosed — BACKLOG 11a regression", err)
	}
}

func TestGraphTx_GetRelationship_AfterCloseReturnsErrGraphClosed(t *testing.T) {
	g := newTxTestGraph(t)
	ctx := context.Background()
	a, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add b: %v", err)
	}
	r, err := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add rel: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}

	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := tx.GetRelationship(r.ID()); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("tx.GetRelationship after Close = %v, want ErrGraphClosed — BACKLOG 11a regression", err)
	}
}

// TestGraphTx_GetNode_StillWorksInsideOpenTx is the non-regression
// counterpart: the fix must not break the ordinary, still-open-tx read path.
func TestGraphTx_GetNode_StillWorksInsideOpenTx(t *testing.T) {
	g := newTxTestGraph(t)
	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Ada"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	got, err := tx.GetNode(n.ID())
	if err != nil {
		t.Fatalf("tx.GetNode: %v", err)
	}
	if got.ID() != n.ID() {
		t.Fatalf("tx.GetNode returned wrong node: %v", got.ID())
	}
}
