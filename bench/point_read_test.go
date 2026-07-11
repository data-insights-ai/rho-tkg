package bench

import "testing"

// pointReadFixtureSize is the working set of nodes cycled through by
// BenchmarkPointReadHit — large enough that the read path isn't trivially
// a single-entry cache hit, small enough that fixture build stays fast.
const pointReadFixtureSize = 2000

// BenchmarkPointReadHit measures a warm cache-hit point read
// (g.Nodes().Get) cycling through a fixed working set of existing node
// IDs — the common "fetch by ID" access pattern.
func BenchmarkPointReadHit(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			ids := addLabeledNodes(b, g, "Person", pointReadFixtureSize)
			ctx := benchCtx()

			b.ReportAllocs()
			i := 0
			for b.Loop() {
				if _, err := g.Nodes().Get(ctx, ids[i%len(ids)]); err != nil {
					b.Fatalf("get %d: %v", ids[i%len(ids)], err)
				}
				i++
			}
		})
	}
}
