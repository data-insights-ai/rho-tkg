package bench

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// twoHopNodeCount / twoHopFanout match the WP scenario name: 10k nodes,
// 3-way fanout per node = 30k relationships.
const (
	twoHopNodeCount = 10000
	twoHopFanout    = 3
	twoHopRelType   = "KNOWS"
)

// buildTwoHopFixture creates twoHopNodeCount nodes and, for each node i,
// twoHopFanout outgoing KNOWS edges to nodes (i+1..i+fanout) mod
// twoHopNodeCount — a dense ring-with-chords graph with a predictable
// degree, built in one BatchBuilder.Execute for fixture-build speed.
func buildTwoHopFixture(tb testing.TB, g *graph.Graph) []types.NodeID {
	tb.Helper()
	ids := make([]types.NodeID, twoHopNodeCount)
	_, err := g.Batch().Run(func(bb *graph.BatchBuilder) error {
		nodes := make([]*types.Node, twoHopNodeCount)
		for i := 0; i < twoHopNodeCount; i++ {
			n, err := bb.AddNode([]string{"Person"}, map[string]any{"seq": i})
			if err != nil {
				return err
			}
			nodes[i] = n
			ids[i] = n.ID()
		}
		for i := 0; i < twoHopNodeCount; i++ {
			for f := 1; f <= twoHopFanout; f++ {
				target := nodes[(i+f)%twoHopNodeCount]
				if _, err := bb.AddRelationship(twoHopRelType, nodes[i], target, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		tb.Fatalf("build two-hop fixture: %v", err)
	}
	return ids
}

// BenchmarkTwoHop measures a decode-free two-hop expansion using
// g.Rels().ForEachAdjacentEndpoint (OPT: endpoint-carrying adjacency index,
// no relationship-row decode) over a 10k-node / 30k-relationship fixture.
func BenchmarkTwoHop(b *testing.B) {
	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := newBenchGraph(b, bc)
			ids := buildTwoHopFixture(b, g)

			b.ReportAllocs()
			i := 0
			for b.Loop() {
				start := ids[i%len(ids)]
				i++

				visited := 0
				err := g.Rels().ForEachAdjacentEndpoint(start, twoHopRelType, false, func(_ types.RelID, hop1 types.NodeID) bool {
					visited++
					innerErr := g.Rels().ForEachAdjacentEndpoint(hop1, twoHopRelType, false, func(_ types.RelID, _ types.NodeID) bool {
						visited++
						return true
					})
					if innerErr != nil {
						b.Fatalf("second hop from %d: %v", hop1, innerErr)
					}
					return true
				})
				if err != nil {
					b.Fatalf("first hop from %d: %v", start, err)
				}
				if visited == 0 {
					b.Fatal("expected at least one traversed edge")
				}
			}
		})
	}
}
