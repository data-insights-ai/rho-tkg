package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkPropertyNDVAfterExtremeWrite: 100,000 :P nodes with distinct
// "v"; before each timed call (timer stopped) the node holding the maximum
// gets a new, larger value, so the old maximum leaves the population and the
// next PropertyStats rescans the label (round 5 request 1, sigma-tkgd C4r:
// 470 ms per property on 100,000 badger nodes). "stats" reads
// PropertyStats, "ndv" PropertyNDV.
func BenchmarkPropertyNDVAfterExtremeWrite(b *testing.B) {
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
			const n = 100000
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			var top *types.Node
			for i := range n {
				if top, err = batch.AddNode([]string{"P"}, map[string]any{"v": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			next := int64(n)
			for _, arm := range []string{"stats", "ndv"} {
				b.Run(arm, func(b *testing.B) {
					for b.Loop() {
						b.StopTimer()
						if err := g.Nodes().SetProperty(ctx, top.ID(), "v", next); err != nil {
							b.Fatal(err)
						}
						next++
						b.StartTimer()
						if arm == "stats" {
							if s, err := g.Stats().PropertyStats("P", "v"); err != nil || s.Count != n {
								b.Fatalf("%+v %v", s, err)
							}
						} else if s, err := g.Stats().PropertyNDV("P", "v"); err != nil || s.Count != n {
							b.Fatalf("%+v %v", s, err)
						}
					}
				})
			}
		})
	}
}
