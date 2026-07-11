package bench

import (
	"math/rand/v2"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// annFixtureSize / annDims match the requested scenario name (10k x 128-dim).
const (
	annFixtureSize   = 10000
	annDims          = 128
	annQueryPoolSize = 50
)

// annVectors deterministically builds n dims-dimensional vectors from a
// fixed seed (math/rand/v2) — reproducible across runs, same generation
// shape as pkg/graph/internal/index's hnsw_test.go corpus helper.
func annVectors(seed uint64, n, dims int) [][]float32 {
	rng := rand.New(rand.NewPCG(seed, seed^0xD1B54A32D192ED03))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dims)
		for j := range v {
			v[j] = float32(rng.NormFloat64())
		}
		vecs[i] = v
	}
	return vecs
}

// BenchmarkANNSearch10k compares the default approximate HNSW vector-index
// engine against the exact brute-force escape hatch
// (VectorIndexOptions.UseBruteForce) at 10k x 128-dim scale. The vector
// index lives entirely at the store layer in memory regardless of which
// Store backend hosts the node rows, so only a single backend variant is
// exercised here ("index is store-level in memory for both; one
// variant suffices") rather than the full backendCases matrix.
func BenchmarkANNSearch10k(b *testing.B) {
	const label = "Vec"
	const propKey = "embedding"
	vecs := annVectors(1, annFixtureSize, annDims)
	queries := annVectors(2, annQueryPoolSize, annDims)

	buildFixture := func(b *testing.B, useBruteForce bool) *graph.Graph {
		b.Helper()
		g := newBenchGraph(b, backendCases[0]) // memory — see doc comment above.
		ctx := benchCtx()
		for i, v := range vecs {
			if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{propKey: v, "seq": i}); err != nil {
				b.Fatalf("add node %d: %v", i, err)
			}
		}
		opts := storepkg.VectorIndexOptions{UseBruteForce: useBruteForce}
		if err := g.Index().CreateVectorWithOptions(label, propKey, annDims, storepkg.DistanceEuclidean, opts); err != nil {
			b.Fatalf("CreateVectorWithOptions(UseBruteForce=%v): %v", useBruteForce, err)
		}
		return g
	}

	b.Run("hnsw", func(b *testing.B) {
		g := buildFixture(b, false)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			q := queries[i%len(queries)]
			if _, err := g.Index().SearchNearest(label, propKey, q, 10, storepkg.QueryOpts{}); err != nil {
				b.Fatalf("SearchNearest: %v", err)
			}
			i++
		}
	})

	b.Run("bruteforce", func(b *testing.B) {
		g := buildFixture(b, true)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			q := queries[i%len(queries)]
			if _, err := g.Index().SearchNearest(label, propKey, q, 10, storepkg.QueryOpts{}); err != nil {
				b.Fatalf("SearchNearest: %v", err)
			}
			i++
		}
	})
}
