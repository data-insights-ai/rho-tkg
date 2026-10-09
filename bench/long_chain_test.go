package bench

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Long chains (review of the effective timeline, 2026-10-09): one node with n
// Updates (no valid-from: each version starts at its write) and one bounded
// correction [1500,1600) over the genesis, so the chain is non-monotonic and
// every point read takes the resolver's own-bounds arm, where the supersession
// rule runs. n = 300, 1000, 3000. NodeAtTxLongChain is the point door at
// valid 1550 (the correction), NodeEffectiveTimelineLongChain the whole
// timeline (n+2 segments).

var longChainSizes = []int{300, 1000, 3000}

func longChainFixture(b *testing.B, bc backendCase, n int) (*graph.Graph, types.NodeID, types.Instant) {
	b.Helper()
	g := newBenchGraph(b, bc)
	ctx := benchCtx()
	nd, err := g.Nodes().Add(ctx, []string{"L"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(0)})
	if err != nil {
		b.Fatalf("add: %v", err)
	}
	for i := 1; i <= n; i++ {
		if _, err := g.Nodes().Update(ctx, nd.ID(), map[string]any{"x": int64(i)}); err != nil {
			b.Fatalf("update %d: %v", i, err)
		}
	}
	if _, err := g.Temporal().SetNodeVersionInterval(ctx, nd.ID(), 1500, 1600, map[string]any{"x": int64(-1)}); err != nil {
		b.Fatalf("cascade: %v", err)
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		b.Fatalf("NowTx: %v", err)
	}
	return g, nd.ID(), pin
}

func BenchmarkNodeAtTxLongChain(b *testing.B) {
	for _, bc := range backendCases {
		for _, n := range longChainSizes {
			b.Run(fmt.Sprintf("%s/n=%d", bc.name, n), func(b *testing.B) {
				g, id, pin := longChainFixture(b, bc, n)
				b.ReportAllocs()
				for b.Loop() {
					if _, err := g.Temporal().NodeAtTx(id, 1550, pin); err != nil {
						b.Fatalf("NodeAtTx: %v", err)
					}
				}
			})
		}
	}
}

func BenchmarkNodeEffectiveTimelineLongChain(b *testing.B) {
	for _, bc := range backendCases {
		for _, n := range longChainSizes {
			b.Run(fmt.Sprintf("%s/n=%d", bc.name, n), func(b *testing.B) {
				g, id, pin := longChainFixture(b, bc, n)
				b.ReportAllocs()
				for b.Loop() {
					segs, err := g.Temporal().NodeEffectiveTimeline(id, pin)
					if err != nil || len(segs) != n+3 {
						b.Fatalf("timeline: %d segments, %v; want %d", len(segs), err, n+3)
					}
				}
			})
		}
	}
}
