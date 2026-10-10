package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// BenchmarkIndexInventoryEpoch: what a kept snapshot pays on reopen to know
// its index inventory is unchanged (round 4 R1) — one InventoryEpoch load
// against one HasProperty probe per indexed property (here 4).
func BenchmarkIndexInventoryEpoch(b *testing.B) {
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
			keys := []string{"a", "b", "c", "d"}
			if _, err := g.Nodes().Add(context.Background(), []string{"P"}, map[string]any{"a": 1, "b": 2, "c": 3, "d": 4}); err != nil {
				b.Fatal(err)
			}
			for _, k := range keys {
				if err := g.Index().CreateProperty("P", k); err != nil {
					b.Fatal(err)
				}
			}
			ix := g.Index()
			b.Run("has-property-x4", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					for _, k := range keys {
						if ok, err := ix.HasProperty("P", k); err != nil || !ok {
							b.Fatal(ok, err)
						}
					}
				}
			})
			b.Run("epoch", func(b *testing.B) {
				b.ReportAllocs()
				want := ix.InventoryEpoch()
				for b.Loop() {
					if ix.InventoryEpoch() != want {
						b.Fatal("moved")
					}
				}
			})
		})
	}
}
