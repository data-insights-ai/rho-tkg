package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// benchEnvelopeQueryAt is the instant the benchmark queries at — far in the
// future so every "cold" node's bounded window is provably non-overlapping and
// prunable, while "live" nodes (open-ended) still match.
const benchEnvelopeQueryAt = types.Instant(1_000_000)

// buildEnvelopeBenchGraph creates a graph with `total` "Doc" nodes of which
// ~coldFrac are cold (valid [1000,2000) — far past, bounded envelope, prunable)
// and the rest are live (valid_from 1000, open-ended, match at query time). When
// withIndex is set, a temporal index is created so the B4 prune fires.
func buildEnvelopeBenchGraph(b *testing.B, total int, coldFrac float64, withIndex bool) *Core {
	b.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	ctx := context.Background()
	coldEvery := int(1.0 / (1.0 - coldFrac)) // 1-in-N nodes are live
	if coldEvery < 1 {
		coldEvery = 1
	}
	for i := 0; i < total; i++ {
		live := i%coldEvery == 0
		var props map[string]any
		if live {
			props = map[string]any{"tkg_valid_from": types.Instant(1000)}
		} else {
			props = map[string]any{"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000)}
		}
		if _, err := g.Nodes.Add(ctx, []string{"Doc"}, props); err != nil {
			b.Fatalf("Add[%d]: %v", i, err)
		}
	}
	if withIndex {
		if err := g.Index.CreateTemporal("Doc"); err != nil {
			b.Fatalf("CreateTemporal: %v", err)
		}
	}
	b.Cleanup(func() { _ = g.Close() })
	return g
}

// BenchmarkTemporalEnvelopePrune measures NodesByLabelAt over a label where 90%
// of members fall outside the query window. WithIndex the B4 envelope prune drops
// those 90% before the per-id chain resolve; NoIndex resolves every candidate.
// The two subtests return the identical result — only the work differs.
func BenchmarkTemporalEnvelopePrune(b *testing.B) {
	const total = 5000
	const coldFrac = 0.9

	b.Run("NoIndex", func(b *testing.B) {
		g := buildEnvelopeBenchGraph(b, total, coldFrac, false)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nodes, err := g.Temporal.NodesByLabelAt("Doc", benchEnvelopeQueryAt)
			if err != nil {
				b.Fatalf("NodesByLabelAt: %v", err)
			}
			_ = nodes
		}
	})

	b.Run("WithIndex", func(b *testing.B) {
		g := buildEnvelopeBenchGraph(b, total, coldFrac, true)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			nodes, err := g.Temporal.NodesByLabelAt("Doc", benchEnvelopeQueryAt)
			if err != nil {
				b.Fatalf("NodesByLabelAt: %v", err)
			}
			_ = nodes
		}
	})
}
