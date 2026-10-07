package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelColumnOrdinals: a relationship type's identities read with its
// column scan (36,000 relationships over 20,000 nodes, one property), per
// backend: the scan alone, the scan plus a Lend per relationship for its
// ordinal (what a consumer numbering relationships paid), and the scan's own
// Ordinals column (sigma-tkgd C4d store request 5: a CSR build from the column
// scan).
func BenchmarkRelColumnOrdinals(b *testing.B) {
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 0}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 0, BadgerInMemory: true}},
	} {
		b.Run(backend.name, func(b *testing.B) {
			g, err := graphpkg.New(backend.cfg)
			if err != nil {
				b.Fatal(err)
			}
			defer g.Close()
			ctx := context.Background()
			const nodes, rels = 20000, 36000
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			created := make([]*types.Node, nodes)
			for i := range created {
				if created[i], err = batch.AddNode([]string{"P"}, nil); err != nil {
					b.Fatal(err)
				}
			}
			for i := range rels {
				if _, err := batch.AddRelationship("KNOWS", created[i%nodes], created[(i*7+1)%nodes], map[string]any{"w": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			time.Sleep(300 * time.Millisecond)
			scan := func(b *testing.B, each func(*graphpkg.RelColumnBatch, int) uint64) {
				for b.Loop() {
					var seen int
					var sum uint64
					ok, err := g.ScanRelColumns("KNOWS", []string{"w"}, graphpkg.QueryOpts{}, func(rb *graphpkg.RelColumnBatch) bool {
						for i := range rb.IDs {
							sum += each(rb, i)
						}
						seen += len(rb.IDs)
						return true
					})
					if err != nil || !ok || seen != rels {
						b.Fatalf("%d rows, %v %v", seen, ok, err)
					}
					_ = sum
				}
			}
			b.Run("scan", func(b *testing.B) {
				scan(b, func(rb *graphpkg.RelColumnBatch, i int) uint64 { return uint64(rb.IDs[i].SnowflakeID()) })
			})
			b.Run("scan+lend", func(b *testing.B) {
				scan(b, func(rb *graphpkg.RelColumnBatch, i int) uint64 {
					r, err := g.Rels().Lend(ctx, rb.IDs[i])
					if err != nil {
						b.Fatal(err)
					}
					return uint64(r.Ordinal())
				})
			})
			b.Run("scan+ordinals", func(b *testing.B) {
				scan(b, func(rb *graphpkg.RelColumnBatch, i int) uint64 { return uint64(rb.Ordinals[i]) })
			})
		})
	}
}
