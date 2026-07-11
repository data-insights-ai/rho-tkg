// Package bench holds cross-backend performance benchmarks for the graph
// engine, kept separate from the main module tree so a routine `go test
// ./...` run never pays their fixture-build cost.
//
// Every scenario in this package runs against BOTH shipped backends
// (in-memory MemoryStore and in-memory BadgerStore) through the shared
// harness below, using b.Run("memory", ...) / b.Run("badger", ...)
// sub-benchmarks. Fixtures are built once per (scenario, backend) pair
// BEFORE the timed loop, using the b.Loop() protocol (Go 1.24+) so the
// one-time setup cost is never re-paid across the framework's iteration
// calibration passes — see https://pkg.go.dev/testing#B.Loop.
//
// Run: `make bench` (informal, -benchtime=0.3s) or the mandatory gate
// `go test -bench=. -benchtime=1x ./bench` (every benchmark at least once).
// See bench/README.md for the regression-check workflow.
package bench

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// backendCase names one Store construction strategy under benchmark.
type backendCase struct {
	name          string
	snowflakeNode int64
	badger        bool
}

// backendCases is the shared backend matrix every scenario in this package
// iterates over. Benchmarks in this package never run concurrently (no
// b.RunParallel, no -parallel above the default), so reusing a fixed
// SnowflakeNodeID per backend name across sequential sub-benchmarks is safe:
// each graph is closed (via b.Cleanup) before the next one with the same ID
// opens.
var backendCases = []backendCase{
	{name: "memory", snowflakeNode: 0, badger: false},
	{name: "badger", snowflakeNode: 1, badger: true},
}

// newBenchGraph constructs a fresh graph for bc and registers b.Cleanup to
// close it. tb is typically the *testing.B of the innermost b.Run — passing
// it (rather than the top-level *testing.B) scopes the close to that
// sub-benchmark's lifetime.
func newBenchGraph(tb testing.TB, bc backendCase) *graph.Graph {
	tb.Helper()
	cfg := graph.Config{SnowflakeNodeID: bc.snowflakeNode}
	if bc.badger {
		cfg.BadgerInMemory = true
	}
	g, err := graph.New(cfg)
	if err != nil {
		tb.Fatalf("%s: graph.New: %v", bc.name, err)
	}
	tb.Cleanup(func() {
		if err := g.Close(); err != nil {
			tb.Logf("%s: graph.Close: %v", bc.name, err)
		}
	})
	return g
}

// benchCtx is the shared background context for fixture and benchmark ops
// (none of the exercised doors do anything context-cancellation-sensitive).
func benchCtx() context.Context { return context.Background() }

// addLabeledNodes creates n nodes carrying label via the standalone Add
// door and returns their IDs in creation order. Used by fixtures that need
// individually addressable nodes (point reads, temporal chains).
func addLabeledNodes(tb testing.TB, g *graph.Graph, label string, n int) []types.NodeID {
	tb.Helper()
	ctx := benchCtx()
	ids := make([]types.NodeID, n)
	for i := 0; i < n; i++ {
		node, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"seq": i})
		if err != nil {
			tb.Fatalf("add node %d: %v", i, err)
		}
		ids[i] = node.ID()
	}
	return ids
}
