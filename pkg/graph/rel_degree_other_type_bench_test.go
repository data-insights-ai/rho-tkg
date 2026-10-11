package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkRelTypeDegreeStatsAfterOtherTypeWrite: 262,144 :SPOKE
// relationships from 64 hubs, and one :COLD_TICK relationship updated before
// each timed call (timer stopped), as sigma-tkgd's cold harness does (round
// 5 request 9: on memory the update moved the store-wide relationship epoch,
// so every call recounted the :SPOKE type, about 400 ms of 1,048,576).
func BenchmarkRelTypeDegreeStatsAfterOtherTypeWrite(b *testing.B) {
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
			const spokes, hubs = 262_144, 64
			batch, err := g.Batch().New()
			if err != nil {
				b.Fatal(err)
			}
			hubNodes := make([]*types.Node, hubs)
			for i := range hubNodes {
				if hubNodes[i], err = batch.AddNode([]string{"Hub"}, nil); err != nil {
					b.Fatal(err)
				}
			}
			for i := range spokes {
				s, err := batch.AddNode([]string{"Spoke"}, nil)
				if err != nil {
					b.Fatal(err)
				}
				// A skewed fan-in: hub h gets about spokes/2^(h+1).
				h := 0
				for x := i; x%2 == 1 && h < hubs-1; x /= 2 {
					h++
				}
				if _, err := batch.AddRelationship("SPOKE", hubNodes[h], s, nil); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			ta, err := g.Nodes().Add(ctx, []string{"ColdTick"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			tb, err := g.Nodes().Add(ctx, []string{"ColdTick"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			tick, err := g.Rels().Add(ctx, "COLD_TICK", ta, tb, map[string]any{"t": int64(0)})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := g.Stats().RelTypeDegreeStats("SPOKE"); err != nil {
				b.Fatal(err)
			}
			v := int64(0)
			for b.Loop() {
				b.StopTimer()
				v++
				if _, err := g.Rels().Update(ctx, tick.ID(), map[string]any{"t": v}); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				st, err := g.Stats().RelTypeDegreeStats("SPOKE")
				if err != nil || st.Rels != spokes {
					b.Fatalf("%+v %v", st, err)
				}
			}
		})
	}
}
