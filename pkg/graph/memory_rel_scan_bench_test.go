package graph_test

import (
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// memoryStarGraph is sigma-tkgd's StarCandidatesSkewed shape on the memory
// store (round 5 request 2): 64 hubs with skewed fan-outs over 262,144 spokes
// of type SPOKE, each with an int "w".
func memoryStarGraph(b *testing.B) (*graphpkg.Graph, []types.NodeID, []types.NodeID) {
	b.Helper()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = g.Close() })
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
	spokeIDs := make([]*types.Node, spokes)
	for i := range spokes {
		if spokeIDs[i], err = batch.AddNode([]string{"Spoke"}, nil); err != nil {
			b.Fatal(err)
		}
		h := 0
		for x := i; x%2 == 1 && h < hubs-1; x /= 2 {
			h++
		}
		if _, err := batch.AddRelationship("SPOKE", hubNodes[h], spokeIDs[i], map[string]any{"w": int64(i % 97)}); err != nil {
			b.Fatal(err)
		}
	}
	if _, err := batch.Execute(); err != nil {
		b.Fatal(err)
	}
	hubIDs := make([]types.NodeID, hubs)
	for i, n := range hubNodes {
		hubIDs[i] = n.ID()
	}
	sp := make([]types.NodeID, spokes)
	for i, n := range spokeIDs {
		sp[i] = n.ID()
	}
	return g, hubIDs, sp
}

// BenchmarkMemoryStarDoors: the doors StarCandidatesSkewed reads on memory —
// ScanRelColumns over the type (w), the ordinal adjacency door out of every
// hub, and into every spoke.
func BenchmarkMemoryStarDoors(b *testing.B) {
	g, hubs, spokes := memoryStarGraph(b)
	b.Run("scan-rel-columns", func(b *testing.B) {
		for b.Loop() {
			var sum int64
			if _, err := g.ScanRelColumns("SPOKE", []string{"w"}, storepkg.QueryOpts{}, func(cb *storepkg.RelColumnBatch) bool {
				for _, v := range cb.Ints[0] {
					sum += v
				}
				return true
			}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ordinal-door-hubs-out", func(b *testing.B) {
		for b.Loop() {
			seen := 0
			for _, h := range hubs {
				if err := g.Rels().ForEachAdjacentEndpointOrdinal(h, "SPOKE", false, func(types.RelID, uint32, types.NodeID, uint32) bool {
					seen++
					return true
				}); err != nil {
					b.Fatal(err)
				}
			}
			if seen != len(spokes) {
				b.Fatalf("%d", seen)
			}
		}
	})
	b.Run("ordinal-door-spokes-in", func(b *testing.B) {
		for b.Loop() {
			seen := 0
			for _, s := range spokes {
				if err := g.Rels().ForEachAdjacentEndpointOrdinal(s, "SPOKE", true, func(types.RelID, uint32, types.NodeID, uint32) bool {
					seen++
					return true
				}); err != nil {
					b.Fatal(err)
				}
			}
			if seen != len(spokes) {
				b.Fatalf("%d", seen)
			}
		}
	})
}
