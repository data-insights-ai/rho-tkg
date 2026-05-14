package stats_test

import (
	"context"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/stats" // godoc anchor: ExampleAPI_<method> resolves against stats.API
)

// ExampleAPI_NodeCount demonstrates a simple count query across the graph.
func ExampleAPI_NodeCount() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})

	cnt, err := g.Stats.NodeCount()
	if err != nil {
		panic(err)
	}
	_ = cnt
}

// ExampleAPI_NodeCountByLabel demonstrates a per-label cardinality query.
func ExampleAPI_NodeCountByLabel() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	_, _ = g.Nodes.Add(context.Background(), []string{"Org"}, map[string]any{"name": "BDS"})

	cnt, err := g.Stats.NodeCountByLabel("Person")
	if err != nil {
		panic(err)
	}
	_ = cnt
}

// ExampleAPI_AllLabelCounts demonstrates retrieving cardinalities for every
// label in one call.
func ExampleAPI_AllLabelCounts() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	counts, err := g.Stats.AllLabelCounts()
	if err != nil {
		panic(err)
	}
	_ = counts
}
