package stats_test

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/stats" // godoc anchor: ExampleAPI_<method> resolves against stats.API
)

// ExampleAPI_NodeCount demonstrates a simple count query across the graph.
func ExampleAPI_NodeCount() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	for _, name := range []string{"Alice", "Bob"} {
		if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": name}); err != nil {
			panic(err)
		}
	}

	cnt, err := g.Stats().NodeCount()
	if err != nil {
		panic(err)
	}
	fmt.Println("nodes:", cnt)
	// Output: nodes: 2
}

// ExampleAPI_NodeCountByLabel demonstrates a per-label cardinality query.
func ExampleAPI_NodeCountByLabel() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		panic(err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Org"}, map[string]any{"name": "BDS"}); err != nil {
		panic(err)
	}

	cnt, err := g.Stats().NodeCountByLabel("Person")
	if err != nil {
		panic(err)
	}
	fmt.Println("Person:", cnt)
	// Output: Person: 1
}

// ExampleAPI_PropertyStats demonstrates the NDV/min/max/count planner
// statistics for a (label, property key) pair.
func ExampleAPI_PropertyStats() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	for _, age := range []int64{30, 45} {
		if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"age": age}); err != nil {
			panic(err)
		}
	}

	stats, err := g.Stats().PropertyStats("Person", "age")
	if err != nil {
		panic(err)
	}
	fmt.Println("count:", stats.Count, "ndv:", stats.NDV, "min:", stats.Min, "max:", stats.Max)
	// Output: count: 2 ndv: 2 min: 30 max: 45
}

// ExampleAPI_AllLabelCounts demonstrates retrieving cardinalities for every
// label in one call.
func ExampleAPI_AllLabelCounts() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		panic(err)
	}
	counts, err := g.Stats().AllLabelCounts()
	if err != nil {
		panic(err)
	}
	fmt.Println(counts)
	// Output: map[Person:1]
}
