package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkMemoryUpdateWritePath: a plain memory Update, which writes one
// history row; the store's history generation is an added cost on this path
// (one atomic add per history row).
func BenchmarkMemoryUpdateWritePath(b *testing.B) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"v": int64(0)})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	i := int64(0)
	for b.Loop() {
		i++
		if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"v": i}); err != nil {
			b.Fatal(err)
		}
	}
}
