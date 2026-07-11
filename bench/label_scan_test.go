package bench

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// labelScanFixtureSize matches the WP scenario name (10k nodes under one label).
const labelScanFixtureSize = 10000

// BenchmarkLabelScan10k measures a full label scan (g.Nodes().ByLabel) over
// 10k same-labeled nodes, with two sub-variants: the default sorted-by-ID
// materialization and the QueryOpts.NoSort fast path that skips the O(n log n)
// sort term for order-independent consumers.
func BenchmarkLabelScan10k(b *testing.B) {
	const label = "Scan"
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			addLabeledNodes(b, g, label, labelScanFixtureSize)

			b.Run("sorted", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					nodes, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{})
					if err != nil {
						b.Fatalf("ByLabel: %v", err)
					}
					if len(nodes) != labelScanFixtureSize {
						b.Fatalf("got %d nodes, want %d", len(nodes), labelScanFixtureSize)
					}
				}
			})

			b.Run("nosort", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					nodes, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{NoSort: true})
					if err != nil {
						b.Fatalf("ByLabel NoSort: %v", err)
					}
					if len(nodes) != labelScanFixtureSize {
						b.Fatalf("got %d nodes, want %d", len(nodes), labelScanFixtureSize)
					}
				}
			})
		})
	}
}
