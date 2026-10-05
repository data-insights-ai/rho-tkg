package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func txFromOf(t *testing.T, g *graphpkg.Graph, id types.NodeID) types.Instant {
	t.Helper()
	n, err := g.Nodes().Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return n.Temporal().TxFrom
}

// StartInstant is strictly after every stamp reserved before Begin and
// strictly before every stamp of the transaction's own writes; an as-of read
// pinned at it sees the graph as the transaction found it (two-phase: a node
// that existed and was then updated inside the transaction reads with its
// old value; a node the transaction created is absent).
func TestGraphTxStartInstant(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, _ storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		before, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"v": int64(1)})
		if err != nil {
			t.Fatal(err)
		}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		start := tx.StartInstant()
		if start <= txFromOf(t, g, before.ID()) {
			t.Fatalf("StartInstant %d not after an earlier write's TxFrom %d", start, txFromOf(t, g, before.ID()))
		}
		created, err := tx.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.UpdateNode(before.ID(), map[string]any{"v": int64(2)}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if tx.StartInstant() != start {
			t.Fatal("StartInstant changed at Commit")
		}
		if got := txFromOf(t, g, created.ID()); got <= start {
			t.Fatalf("a write of the transaction is stamped %d, not after its start %d", got, start)
		}
		if got := txFromOf(t, g, before.ID()); got <= start {
			t.Fatalf("the transaction's update is stamped %d, not after its start %d", got, start)
		}
		asOf, err := g.Temporal().NodesAsOf(start)
		if err != nil {
			t.Fatal(err)
		}
		var sawBefore bool
		for _, n := range asOf {
			if n.ID() == created.ID() {
				t.Fatal("a node the transaction created is visible as of its start")
			}
			if n.ID() == before.ID() {
				sawBefore = true
				if v, _ := n.GetProperty("v"); v != int64(1) {
					t.Fatalf("as of the start v = %v, want 1 (the value before the transaction)", v)
				}
			}
		}
		if !sawBefore {
			t.Fatal("a node that existed before the transaction is missing as of its start")
		}

		// Later transactions start later; a rolled-back one keeps its start.
		tx2, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		if tx2.StartInstant() <= start || tx2.StartInstant() <= txFromOf(t, g, created.ID()) {
			t.Fatalf("a later transaction starts at %d, not after %d", tx2.StartInstant(), start)
		}
		s2 := tx2.StartInstant()
		if err := tx2.Rollback(); err != nil {
			t.Fatal(err)
		}
		if tx2.StartInstant() != s2 {
			t.Fatal("StartInstant changed at Rollback")
		}
		var inRun types.Instant
		if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { inRun = tx.StartInstant(); return nil }); err != nil {
			t.Fatal(err)
		}
		if inRun <= s2 {
			t.Fatalf("Run's transaction starts at %d, not after %d", inRun, s2)
		}
	})
	var nilTx *graphpkg.GraphTx
	if nilTx.StartInstant() != 0 {
		t.Fatal("a nil transaction's StartInstant must be 0")
	}
}
