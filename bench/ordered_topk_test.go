package bench

import (
	"math"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// orderedTopKFixtureSize is the value-range cardinality the top-k scan runs
// over — 100k distinct numeric values under one label.
const orderedTopKFixtureSize = 100000

// orderedTopKK is the LIMIT (top-10 by value).
const orderedTopKK = 10

// BenchmarkOrderedTopK contrasts the K3a ordered / top-k access path
// (ForEachByLabelPropertyRangeOrdered, LIMIT pushed into the index) against
// the pre-K3a "collect-then-limit" shape a query layer had to use — a full
// label scan materialized, sorted by value, then truncated to k. The ordered
// arm is expected to be orders of magnitude faster: it touches O(k + log n)
// index entries and materializes only k rows, while collect-then-limit
// materializes and sorts all 100k.
func BenchmarkOrderedTopK(b *testing.B) {
	const label, key = "Score", "v"
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			if err := g.Index().CreateProperty(label, key); err != nil {
				b.Fatalf("CreateProperty: %v", err)
			}
			ctx := benchCtx()
			// Scramble insertion order so the k smallest values are spread
			// across the fixture, not clustered at one end.
			for i := 0; i < orderedTopKFixtureSize; i++ {
				v := (i*2654435761 + 12345) % orderedTopKFixtureSize
				if _, err := g.Nodes().Add(ctx, []string{label}, map[string]any{key: v}); err != nil {
					b.Fatalf("add node %d: %v", i, err)
				}
			}

			// Ordered arm: ascending top-k with LIMIT pushdown (fn stops at k).
			b.Run("ordered", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					got := make([]int, 0, orderedTopKK)
					err := g.Nodes().ForEachByLabelPropertyRangeOrdered(label, key, math.Inf(-1), math.Inf(1), true, true, false, storepkg.QueryOpts{}, func(n *types.Node) bool {
						got = append(got, valueOfInt(n, key))
						return len(got) < orderedTopKK
					})
					if err != nil {
						b.Fatalf("ordered scan: %v", err)
					}
					if len(got) != orderedTopKK || got[0] != 0 {
						b.Fatalf("ordered top-k wrong: %v", got)
					}
				}
			})

			// Baseline arm: collect-then-limit — full label scan, sort by
			// value, truncate to k (the pre-K3a top-k compilation).
			b.Run("collect-then-limit", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					all, err := g.Nodes().ByLabel(label, storepkg.QueryOpts{})
					if err != nil {
						b.Fatalf("ByLabel: %v", err)
					}
					sort.Slice(all, func(i, j int) bool {
						return valueOfInt(all[i], key) < valueOfInt(all[j], key)
					})
					if len(all) > orderedTopKK {
						all = all[:orderedTopKK]
					}
					if len(all) != orderedTopKK || valueOfInt(all[0], key) != 0 {
						b.Fatalf("collect-then-limit top-k wrong")
					}
				}
			})
		})
	}
}

func valueOfInt(n *types.Node, key string) int {
	v, ok := n.PropertiesMap()[key]
	if !ok {
		return math.MaxInt
	}
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	default:
		return math.MaxInt
	}
}
