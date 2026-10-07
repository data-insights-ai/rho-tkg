package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkDocValuesPointLookup: a label column's point lookup by node ID
// (types.NodeColumnReader.Row) over 20,000 members, every member in turn and a
// non-member, per backend: the expand-aggregation target read (sigma-tkgd C4d
// store request 6).
func BenchmarkDocValuesPointLookup(b *testing.B) {
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
			const n = 20000
			ids := make([]types.NodeID, n)
			for i := range n {
				nd, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"age": int64(i % 90), "name": "x"})
				if err != nil {
					b.Fatal(err)
				}
				ids[i] = nd.ID()
			}
			other, err := g.Nodes().Add(ctx, []string{"Other"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			reader, _, ok, err := g.Nodes().DocValuesSnapshot("Person", []string{"age", "name"})
			if err != nil || !ok {
				b.Fatalf("snapshot: %v %v", ok, err)
			}
			vals, present := make([]any, 2), make([]bool, 2)
			b.Run("member", func(b *testing.B) {
				b.ReportAllocs()
				i := 0
				for b.Loop() {
					if !reader.Row(ids[i], vals, present) {
						b.Fatal("a member was not found")
					}
					i = (i + 7919) % n
				}
			})
			b.Run("non-member", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if reader.Row(other.ID(), vals, present) {
						b.Fatal("a non-member was found")
					}
				}
			})
		})
	}
}
