package bench

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// relPropertyFixtureSize matches the WP scenario name: 10k KNOWS relationships
// carrying a numeric weight, one of which is highly selective.
const relPropertyFixtureSize = 10000

// BenchmarkRelPropertyLookup10k compares the accelerated rel-property-index
// lookup (g.Rels().ByTypeAndProperty with an index present) against the
// unaccelerated type-scan+filter path (same door, index dropped) over 10k
// KNOWS relationships where a selective weight matches exactly one rel (K3b).
func BenchmarkRelPropertyLookup10k(b *testing.B) {
	const relType = "KNOWS"
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			ctx := benchCtx()

			// Two endpoint nodes shared by every relationship.
			a, err := g.Nodes().Add(ctx, []string{"P"}, nil)
			if err != nil {
				b.Fatalf("add node a: %v", err)
			}
			c, err := g.Nodes().Add(ctx, []string{"P"}, nil)
			if err != nil {
				b.Fatalf("add node c: %v", err)
			}

			// 10k rels: weight is mostly a common bucket, plus one selective
			// value (weight=selective) carried by exactly one relationship.
			const selective = int64(999999)
			for i := 0; i < relPropertyFixtureSize; i++ {
				w := int64(i % 100) // 100 common buckets
				if i == relPropertyFixtureSize/2 {
					w = selective
				}
				if _, err := g.Rels().Add(ctx, relType, a, c, map[string]any{"weight": w}); err != nil {
					b.Fatalf("add rel %d: %v", i, err)
				}
			}

			b.Run("scan", func(b *testing.B) {
				// No index — the door falls back to a type scan + filter.
				b.ReportAllocs()
				for b.Loop() {
					rels, err := g.Rels().ByTypeAndProperty(relType, "weight", selective, storepkg.QueryOpts{})
					if err != nil || len(rels) != 1 {
						b.Fatalf("scan ByTypeAndProperty: len=%d err=%v", len(rels), err)
					}
				}
			})

			if err := g.Index().CreateRelProperty(relType, "weight"); err != nil {
				b.Fatalf("CreateRelProperty: %v", err)
			}

			b.Run("indexed", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					rels, err := g.Rels().ByTypeAndProperty(relType, "weight", selective, storepkg.QueryOpts{})
					if err != nil || len(rels) != 1 {
						b.Fatalf("indexed ByTypeAndProperty: len=%d err=%v", len(rels), err)
					}
				}
			})
		})
	}
}
