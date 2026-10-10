package graph_test

import (
	"context"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkAdjacentEndpointOrdinals: every node's outgoing neighbours with the
// relationship's and the neighbour's dense ordinals (20,000 nodes, 36,000
// relationships), per backend: ForEachAdjacentEndpoint plus a Lend of each
// relationship and neighbour for their ordinals (what a consumer numbering
// both paid), against ForEachAdjacentEndpointOrdinal (sigma-tkgd C3s open
// question 4: the adjacency handing out the other endpoint's ordinal).
func BenchmarkAdjacentEndpointOrdinals(b *testing.B) {
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
				if created[i], err = batch.AddNode([]string{"P"}, map[string]any{"i": int64(i)}); err != nil {
					b.Fatal(err)
				}
			}
			for i := range rels {
				if _, err := batch.AddRelationship("KNOWS", created[i%nodes], created[(i*7+1)%nodes], nil); err != nil {
					b.Fatal(err)
				}
			}
			if _, err := batch.Execute(); err != nil {
				b.Fatal(err)
			}
			ids := make([]types.NodeID, nodes)
			for i, n := range created {
				ids[i] = n.ID()
			}
			time.Sleep(300 * time.Millisecond)
			b.Run("endpoint+lend", func(b *testing.B) {
				for b.Loop() {
					var sum, seen uint64
					for _, id := range ids {
						if err := g.Rels().ForEachAdjacentEndpoint(id, "KNOWS", false, func(rel types.RelID, other types.NodeID) bool {
							r, err := g.Rels().Lend(ctx, rel)
							if err != nil {
								b.Fatal(err)
							}
							n, err := g.Nodes().Lend(ctx, other)
							if err != nil {
								b.Fatal(err)
							}
							sum += uint64(r.Ordinal()) + uint64(n.Ordinal())
							seen++
							return true
						}); err != nil {
							b.Fatal(err)
						}
					}
					if seen != rels || sum == 0 {
						b.Fatalf("%d edges", seen)
					}
				}
			})
			b.Run("ordinal-door", func(b *testing.B) {
				for b.Loop() {
					var sum, seen uint64
					for _, id := range ids {
						if err := g.Rels().ForEachAdjacentEndpointOrdinal(id, "KNOWS", false, func(_ types.RelID, relOrd uint32, _ types.NodeID, otherOrd uint32) bool {
							sum += uint64(relOrd) + uint64(otherOrd)
							seen++
							return true
						}); err != nil {
							b.Fatal(err)
						}
					}
					if seen != rels || sum == 0 {
						b.Fatalf("%d edges", seen)
					}
				}
			})
		})
	}
}

// BenchmarkAdjacentEndpointOrdinalCall: one ForEachAdjacentEndpointOrdinal
// call per op over a node with 2 outgoing KNOWS relationships, one callback
// built outside the loop, so B/op and allocs/op are the door's own (round 4
// R3: memory allocated three slices per call).
func BenchmarkAdjacentEndpointOrdinalCall(b *testing.B) {
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
			hub, err := g.Nodes().Add(ctx, []string{"P"}, nil)
			if err != nil {
				b.Fatal(err)
			}
			for range 2 {
				n, err := g.Nodes().Add(ctx, []string{"P"}, nil)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := g.Rels().Add(ctx, "KNOWS", hub, n, nil); err != nil {
					b.Fatal(err)
				}
			}
			var seen int
			fn := func(types.RelID, uint32, types.NodeID, uint32) bool { seen++; return true }
			id := hub.ID()
			b.ReportAllocs()
			for b.Loop() {
				if err := g.Rels().ForEachAdjacentEndpointOrdinal(id, "KNOWS", false, fn); err != nil {
					b.Fatal(err)
				}
			}
			if seen == 0 {
				b.Fatal("no edges")
			}
		})
	}
}
