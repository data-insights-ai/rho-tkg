package bench

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// temporalNodeCount is the fixture size for the two history-aware
// scenarios below — large enough to exercise a real merge-history-with-
// current fold, small enough to keep fixture build (nodeCount * 5 ops,
// standalone Add/Update — Update isn't batchable) fast.
const temporalNodeCount = 1500

// buildValidTimeChain creates temporalNodeCount nodes, each carrying an
// explicit 5-version valid-time (world-time) chain via tkg_valid_from:
// version v's ValidFrom is validFromSteps[v]. Returns a pin instant strictly
// between version 2's and version 3's ValidFrom, so g.Temporal().NodesAt(pin)
// must resolve every node to its version-2 belief state — exercising a real
// "predicate held earlier in the interval, not on the most-recent version"
// path (Testing Rule 16), not just a happy-path current-state read.
func buildValidTimeChain(tb testing.TB, g *graph.Graph) types.Instant {
	tb.Helper()
	ctx := benchCtx()
	validFromSteps := [5]types.Instant{1000, 2000, 3000, 4000, 5000}

	for i := 0; i < temporalNodeCount; i++ {
		n, err := g.Nodes().Add(ctx, []string{"Chrono"}, map[string]any{
			"seq":            i,
			"version":        0,
			"tkg_valid_from": validFromSteps[0],
		})
		if err != nil {
			tb.Fatalf("add chrono node %d: %v", i, err)
		}
		id := n.ID()
		for v := 1; v < len(validFromSteps); v++ {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{
				"version":        v,
				"tkg_valid_from": validFromSteps[v],
			}); err != nil {
				tb.Fatalf("update chrono node %d to v%d: %v", i, v, err)
			}
		}
	}
	return (validFromSteps[2] + validFromSteps[3]) / 2
}

// BenchmarkTemporalPoint measures g.Temporal().NodesAt (valid-time point
// query) against a graph where every node has a 5-version chain, pinned
// mid-chain so the resolver must reconstruct a non-current belief state
// for each entity rather than short-circuit on the current row.
func BenchmarkTemporalPoint(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			pin := buildValidTimeChain(b, g)

			b.ReportAllocs()
			for b.Loop() {
				nodes, err := g.Temporal().NodesAt(pin)
				if err != nil {
					b.Fatalf("NodesAt: %v", err)
				}
				if len(nodes) != temporalNodeCount {
					b.Fatalf("got %d nodes at pin, want %d", len(nodes), temporalNodeCount)
				}
			}
		})
	}
}

// buildTxTimeChain creates temporalNodeCount nodes and advances each
// through 5 transaction-time versions in lockstep rounds (round 0 = create,
// rounds 1-4 = Update), capturing a NowTx() pin after round 2 and before
// round 3 — so g.Temporal().NodesAsOf(pin) must reconstruct each entity's
// round-2 belief state from mid-history, not its current (round-4) state.
func buildTxTimeChain(tb testing.TB, g *graph.Graph) types.Instant {
	tb.Helper()
	ctx := benchCtx()
	ids := make([]types.NodeID, temporalNodeCount)
	for i := range ids {
		n, err := g.Nodes().Add(ctx, []string{"Chrono"}, map[string]any{"round": 0, "seq": i})
		if err != nil {
			tb.Fatalf("add chrono node %d: %v", i, err)
		}
		ids[i] = n.ID()
	}
	for round := 1; round <= 2; round++ {
		for _, id := range ids {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"round": round}); err != nil {
				tb.Fatalf("update node %d to round %d: %v", id, round, err)
			}
		}
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		tb.Fatalf("NowTx: %v", err)
	}
	for round := 3; round <= 4; round++ {
		for _, id := range ids {
			if _, err := g.Nodes().Update(ctx, id, map[string]any{"round": round}); err != nil {
				tb.Fatalf("update node %d to round %d: %v", id, round, err)
			}
		}
	}
	return pin
}

// BenchmarkAsOfPin measures g.Temporal().NodesAsOf (bitemporal, named
// as-of-tag style transaction-time query) pinned to the middle of each
// entity's 5-round transaction history.
func BenchmarkAsOfPin(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			pin := buildTxTimeChain(b, g)

			b.ReportAllocs()
			for b.Loop() {
				nodes, err := g.Temporal().NodesAsOf(pin)
				if err != nil {
					b.Fatalf("NodesAsOf: %v", err)
				}
				if len(nodes) != temporalNodeCount {
					b.Fatalf("got %d nodes as of pin, want %d", len(nodes), temporalNodeCount)
				}
			}
		})
	}
}
