package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func newForEachTestGraph(t *testing.T, n int, cacheCapacity int) *graph.Graph {
	t.Helper()
	g, err := graph.New(graph.Config{
		SnowflakeNodeID: 1,
		BadgerInMemory:  true,
		CacheCapacity:   cacheCapacity,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	for i := 0; i < n; i++ {
		if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"idx": int64(i)}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	return g
}

// TestForEachByLabel_MatchesByLabel pins that the streaming scan emits
// exactly the rows the materializing ByLabel returns, in the same order —
// including past the entity-cache capacity, where the streaming arm's
// no-fill reads must still decode every row correctly.
func TestForEachByLabel_MatchesByLabel(t *testing.T) {
	g := newForEachTestGraph(t, 40, 8) // cardinality 5x the cache capacity

	want, err := g.Nodes().ByLabel("Person", graph.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(want) != 40 {
		t.Fatalf("ByLabel returned %d rows, want 40", len(want))
	}

	var got []*types.Node
	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{}, func(n *types.Node) bool {
		got = append(got, n)
		return true
	}); err != nil {
		t.Fatalf("ForEachByLabel: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("streamed %d rows, ByLabel returned %d", len(got), len(want))
	}
	for i := range want {
		if got[i].ID() != want[i].ID() {
			t.Fatalf("row %d: streamed ID %d, ByLabel ID %d", i, got[i].ID(), want[i].ID())
		}
		gv, _ := got[i].GetProperty("idx")
		wv, _ := want[i].GetProperty("idx")
		if gv != wv {
			t.Fatalf("row %d: streamed idx %v, ByLabel idx %v", i, gv, wv)
		}
		if !got[i].IsFrozen() {
			t.Fatalf("row %d: streamed row not frozen", i)
		}
	}
}

// TestForEachByLabel_EarlyStopAndLimit pins fn=false termination and the
// Limit opt — both must stop the scan without error.
func TestForEachByLabel_EarlyStopAndLimit(t *testing.T) {
	g := newForEachTestGraph(t, 20, 0)

	seen := 0
	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{}, func(*types.Node) bool {
		seen++
		return seen < 5
	}); err != nil {
		t.Fatalf("early stop: %v", err)
	}
	if seen != 5 {
		t.Fatalf("early stop saw %d rows, want 5", seen)
	}

	seen = 0
	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{Limit: 7}, func(*types.Node) bool {
		seen++
		return true
	}); err != nil {
		t.Fatalf("limit: %v", err)
	}
	if seen != 7 {
		t.Fatalf("Limit=7 saw %d rows", seen)
	}
}

// TestForEachByLabel_NilCallbackAndUnknownLabel pins the error and empty
// edges: nil fn is rejected; an unregistered label streams nothing.
func TestForEachByLabel_NilCallbackAndUnknownLabel(t *testing.T) {
	g := newForEachTestGraph(t, 3, 0)

	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{}, nil); err == nil {
		t.Fatal("nil callback accepted")
	}
	calls := 0
	if err := g.Nodes().ForEachByLabel("NoSuchLabel", graph.QueryOpts{}, func(*types.Node) bool {
		calls++
		return true
	}); err != nil {
		t.Fatalf("unknown label: %v", err)
	}
	if calls != 0 {
		t.Fatalf("unknown label streamed %d rows", calls)
	}
}

// TestForEachByLabel_CallbackReentry pins the relaxed-isolation promise:
// fn may call back into the graph (the materializing path under c.mu could
// not allow this — RWMutex readers deadlock behind queued writers).
func TestForEachByLabel_CallbackReentry(t *testing.T) {
	g := newForEachTestGraph(t, 10, 0)

	var reErr error
	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{}, func(n *types.Node) bool {
		if _, err := g.Nodes().Get(context.Background(), n.ID()); err != nil {
			reErr = fmt.Errorf("re-entrant Get(%d): %w", n.ID(), err)
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachByLabel: %v", err)
	}
	if reErr != nil {
		t.Fatal(reErr)
	}
}

// TestForEachByLabel_TemporalFallback pins that a temporal filter routes
// through the history-aware materializing path and still streams rows.
func TestForEachByLabel_TemporalFallback(t *testing.T) {
	g := newForEachTestGraph(t, 6, 0)

	nodes, err := g.Nodes().ByLabel("Person", graph.QueryOpts{})
	if err != nil || len(nodes) == 0 {
		t.Fatalf("seed read: %v (%d)", err, len(nodes))
	}
	at := nodes[0].Temporal().TxFrom
	want, err := g.Nodes().ByLabel("Person", graph.QueryOpts{ValidAt: at})
	if err != nil {
		t.Fatalf("ByLabel temporal: %v", err)
	}
	var got int
	if err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{ValidAt: at}, func(*types.Node) bool {
		got++
		return true
	}); err != nil {
		t.Fatalf("ForEachByLabel temporal: %v", err)
	}
	if got != len(want) {
		t.Fatalf("temporal stream %d rows, ByLabel %d", got, len(want))
	}
}

// guard against accidental sentinel removal — the nil-callback error must
// stay distinguishable.
func TestForEachByLabel_NilCallbackSentinel(t *testing.T) {
	g := newForEachTestGraph(t, 1, 0)
	err := g.Nodes().ForEachByLabel("Person", graph.QueryOpts{}, nil)
	if err == nil || errors.Is(err, graph.ErrNodeNotFound) {
		t.Fatalf("unexpected nil-callback error: %v", err)
	}
}
