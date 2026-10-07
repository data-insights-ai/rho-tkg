package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func addNodesProps(n int) []map[string]any {
	props := make([]map[string]any, n)
	for i := range props {
		props[i] = map[string]any{"i": int64(i)}
	}
	return props
}

func nodeCount(t *testing.T, g *Core) int {
	t.Helper()
	n, err := g.Nodes.Count()
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// A refused element leaves no node and no new label behind.
func TestAddNodesInternalRestoresLabels(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	props := addNodesProps(3)
	props[2] = map[string]any{"bad": struct{}{}}
	if nodes, err := tx.AddNodes([]string{"Fresh"}, props); err == nil || nodes != nil {
		t.Fatalf("AddNodes = %d nodes, %v; want nil and an error", len(nodes), err)
	} else if !strings.Contains(err.Error(), "node 2 of 3") {
		t.Fatalf("err %q does not name the element", err)
	}
	if _, ok := g.labels.Lookup("Fresh"); ok {
		t.Fatal("a refused call left its label")
	}
	if n := nodeCount(t, g); n != 0 {
		t.Fatalf("%d nodes", n)
	}
}

// A store batch that fails after writing: the written nodes are removed, the
// new label is restored, the add counter does not move. When the cleanup
// itself fails the error says so.
func TestAddNodesInternalStoreFailure(t *testing.T) {
	t.Parallel()
	g, fs := newNodeCreateRollbackGraph(t)
	fs.failBatch = true
	before, _ := g.Stats.Get()
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := tx.AddNodes([]string{"Fresh"}, addNodesProps(4))
	if !errors.Is(err, fs.err) || nodes != nil {
		t.Fatalf("AddNodes = %d nodes, %v; want nil and the injected error", len(nodes), err)
	}
	if n := nodeCount(t, g); n != 0 {
		t.Fatalf("%d nodes stayed after the failed batch", n)
	}
	if _, ok := g.labels.Lookup("Fresh"); ok {
		t.Fatal("the failed batch left its label")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	after, _ := g.Stats.Get()
	if after.NodesAdded != before.NodesAdded {
		t.Fatalf("NodesAdded %d -> %d", before.NodesAdded, after.NodesAdded)
	}

	fs.failDelete = true
	tx, err = g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.AddNodes([]string{"Fresh"}, addNodesProps(2))
	if !errors.Is(err, fs.err) || !strings.Contains(err.Error(), "failed to remove partial node") {
		t.Fatalf("err = %v, want the injected error and the cleanup failure", err)
	}
	fs.failDelete, fs.failBatch = false, false
	_ = tx.Rollback()
}

// A panic in the store's batch write releases the registry lock and restores
// the label; the graph keeps working.
func TestAddNodesInternalPanic(t *testing.T) {
	t.Parallel()
	g, fs := newNodeCreateRollbackGraph(t)
	fs.panicBatch = true
	func() {
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if recover() == nil {
				t.Fatal("the injected panic did not surface")
			}
			_ = tx.Rollback()
		}()
		_, _ = tx.AddNodes([]string{"Fresh"}, addNodesProps(2))
	}()
	if _, ok := g.labels.Lookup("Fresh"); ok {
		t.Fatal("the panicking batch left its label")
	}
	fs.panicBatch = false
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddNodes([]string{"Fresh"}, addNodesProps(2)); err != nil {
		t.Fatalf("after the panic: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := nodeCount(t, g); n != 2 {
		t.Fatalf("%d nodes, want 2", n)
	}
}

// Under a scoped write token every node takes AddNode's path; a cancelled
// context creates nothing.
func TestAddNodesInternalScopedAndCancelled(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)
	ctx := withScopeToken(context.Background(), 7)
	if g.canBatchNodeCreates(ctx) {
		t.Fatal("a scoped write must not batch")
	}
	if !g.canBatchNodeCreates(context.Background()) {
		t.Fatal("a plain write without constraints batches")
	}
	var got int
	if _, err := g.runUnderRLock(func() {
		created, err := g.addNodesInternal(ctx, []string{"S"}, addNodesProps(3))
		if err != nil {
			t.Errorf("scoped AddNodes: %v", err)
		}
		got = len(created)
	}); err != nil {
		t.Fatal(err)
	}
	if got != 3 || nodeCount(t, g) != 3 {
		t.Fatalf("scoped path created %d (store %d), want 3", got, nodeCount(t, g))
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := g.runUnderRLock(func() {
		created, err := g.addNodesInternal(cancelled, []string{"S"}, addNodesProps(3))
		if !errors.Is(err, context.Canceled) || created != nil {
			t.Errorf("cancelled: %d nodes, %v", len(created), err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if n := nodeCount(t, g); n != 3 {
		t.Fatalf("%d nodes after the cancelled call, want 3", n)
	}
	if _, err := g.runUnderRLock(func() {
		props := addNodesProps(2)
		props[1] = map[string]any{"x": struct{}{}}
		created, err := g.addNodesInternal(ctx, []string{"S"}, props)
		if err == nil || len(created) != 1 {
			t.Errorf("per-node path with a bad second element: %d nodes, %v; want the first and an error", len(created), err)
		}
	}); err != nil {
		t.Fatal(err)
	}
}

// The temporal shadow keys of each element: valid time, creation time and,
// where the graph allows backfill, the transaction time; refused otherwise.
func TestAddNodesInternalTemporalKeys(t *testing.T) {
	t.Parallel()
	g, err := New(Config{AllowTxBackfill: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	props := []map[string]any{
		{"tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(200), "tkg_created_at": types.Instant(50)},
		{"tkg_tx_from": types.Instant(5000)},
		{},
	}
	nodes, err := tx.AddNodes([]string{"T"}, props)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tm := nodes[0].Temporal()
	if tm.ValidFrom != 100 || tm.ValidTo != 200 || tm.CreatedAt != 50 {
		t.Fatalf("node 0 temporal %+v", *tm)
	}
	if got := nodes[1].Temporal().TxFrom; got != 5000 {
		t.Fatalf("node 1 TxFrom %d, want the backfilled 5000", got)
	}
	if got := nodes[2].Temporal().TxFrom; got <= 5000 {
		t.Fatalf("node 2 TxFrom %d, want the clock", got)
	}

	plain := newTxTestGraph(t)
	tx, err = plain.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.AddNodes([]string{"T"}, props[1:2]); !errors.Is(err, ErrTxBackfillDisabled) {
		t.Fatalf("backfill without the gate: %v, want ErrTxBackfillDisabled", err)
	}
	if _, err := tx.AddNodes([]string{"T"}, []map[string]any{{"tkg_valid_from": "x"}}); err == nil {
		t.Fatal("a malformed valid time passed")
	}
	if _, err := tx.AddNodes([]string{"T"}, []map[string]any{{"tkg_author_id": int64(1)}}); err == nil {
		t.Fatal("a malformed provenance key passed")
	}
}
