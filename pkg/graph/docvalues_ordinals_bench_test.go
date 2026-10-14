package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkDocValuesOrdinals: a label's 100,000 nodes read with one column
// and each node's ordinal (sigma-tkgd round 6 request 11), per backend:
// ForEachDocValues plus a Lend per row for the ordinal (what a consumer
// numbering the rows paid before), and ForEachDocValuesOrdinal.
func BenchmarkDocValuesOrdinals(b *testing.B) {
	const nodes = 100_000
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
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			for i := range nodes {
				if _, err := batch.AddNode([]string{"P"}, map[string]any{"w": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			keys := []string{"w"}
			marks := make([]bool, nodes+2)
			b.Run("values+lend", func(b *testing.B) {
				for b.Loop() {
					_, ok, err := g.Nodes().ForEachDocValues("P", keys, func(id types.NodeID, _ []any, _ []bool) bool {
						n, err := g.Nodes().Lend(ctx, id)
						if err != nil {
							b.Fatal(err)
						}
						marks[n.Ordinal()] = true
						return true
					})
					if err != nil || !ok {
						b.Fatal(ok, err)
					}
				}
			})
			b.Run("values+ordinal", func(b *testing.B) {
				for b.Loop() {
					_, ok, err := g.Nodes().ForEachDocValuesOrdinal("P", keys, func(_ types.NodeID, ord uint32, _ []any, _ []bool) bool {
						marks[ord] = true
						return true
					})
					if err != nil || !ok {
						b.Fatal(ok, err)
					}
				}
			})
		})
	}
}
