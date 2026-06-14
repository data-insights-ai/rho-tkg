package core

import (
	"context"
	"errors"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeAt_CurrentRowFastPathSkipsHistory proves the current-row short-circuit:
// a point query whose validAt the open current row covers is answered WITHOUT
// reading version history, while a historical validAt falls through and reads it.
// A store that faults on every history read for the node makes the distinction
// observable — the fast path succeeds, the slow path surfaces the fault.
func TestNodeAt_CurrentRowFastPathSkipsHistory(t *testing.T) {
	errBoom := errors.New("history read on the fast path")
	store := &filteredHistoryFaultStore{Store: memory.New(), err: errBoom}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	clk := useTestClock(t, g)
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(1)})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	store.failID = n.ID() // every history read for this node now faults

	// Current/future/valid-from-boundary valid times: covered by the open
	// current row → no history read, so no fault.
	for _, validAt := range []types.Instant{1000, clk.PeekInstant(), 1 << 40} {
		got, err := g.Temporal.NodeAt(n.ID(), validAt)
		if err != nil {
			t.Fatalf("NodeAt(%d) hit history (fast path missed): %v", validAt, err)
		}
		if v, _ := got.Properties().Get("x"); v != int64(1) {
			t.Fatalf("NodeAt(%d) x = %v, want 1", validAt, v)
		}
	}

	// Historical valid time (before valid-from): the current row cannot answer,
	// so history is read and the fault surfaces.
	if _, err := g.Temporal.NodeAt(n.ID(), 500); !errors.Is(err, errBoom) {
		t.Fatalf("NodeAt(historical) err = %v, want history fault %v", err, errBoom)
	}
}

// deepHistoryNode builds a node with `versions` tiled versions via repeated
// Update with increasing valid-from, returning the id and the latest valid-from.
func deepHistoryNode(b *testing.B, g *Core, versions int) (types.NodeID, types.Instant) {
	b.Helper()
	ctx := context.Background()
	n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(0)})
	if err != nil {
		b.Fatalf("Add: %v", err)
	}
	vf := types.Instant(1000)
	for i := 1; i < versions; i++ {
		vf += 1000
		if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": vf, "x": int64(i)}); err != nil {
			b.Fatalf("Update %d: %v", i, err)
		}
	}
	return n.ID(), vf
}

// BenchmarkNodeAt_DeepHistory contrasts the current-row fast path (NodeAt at the
// latest valid time, which the open current row answers without loading history)
// against the slow path (NodeAt at an early valid time, which materializes and
// scans the whole version chain). The fast path is flat in history depth; the
// slow path scales with it.
func BenchmarkNodeAt_DeepHistory(b *testing.B) {
	for _, depth := range []int{8, 64, 256} {
		bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: 1 << 62})
		if err != nil {
			b.Fatalf("badger.New: %v", err)
		}
		g, err := New(Config{Store: bs})
		if err != nil {
			b.Fatalf("New: %v", err)
		}
		id, latestVF := deepHistoryNode(b, g, depth)

		b.Run("fastpath_current/depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Temporal.NodeAt(id, latestVF+1); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("slowpath_historical/depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := g.Temporal.NodeAt(id, 1500); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = g.Close()
	}
}
