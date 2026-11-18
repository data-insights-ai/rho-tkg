package temporal_test

import (
	"context"
	"fmt"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal" // godoc anchor: ExampleAPI_<method> resolves against temporal.API
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ExampleAPI_NodesAt demonstrates a point-in-time query against the graph.
func ExampleAPI_NodesAt() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		panic(err)
	}

	now := types.Instant(time.Now().UnixMilli())
	nodes, err := g.Temporal().NodesAt(now)
	if err != nil {
		panic(err)
	}
	fmt.Println(len(nodes), nodes[0].PropertiesMap()["name"])
	// Output: 1 Alice
}

// ExampleAPI_Snapshot demonstrates capturing the entire graph state at an
// instant.
func ExampleAPI_Snapshot() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if _, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		panic(err)
	}

	now := types.Instant(time.Now().UnixMilli())
	snap, err := g.Temporal().Snapshot(now)
	if err != nil {
		panic(err)
	}
	fmt.Println(snap.Timestamp == now, snap.NodeCount, snap.RelCount)
	// Output: true 1 0
}

// ExampleAPI_NodeAt demonstrates retrieving a specific node version at a
// point in time.
func ExampleAPI_NodeAt() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		panic(err)
	}
	now := types.Instant(time.Now().UnixMilli())
	v, err := g.Temporal().NodeAt(n.ID(), now)
	if err != nil {
		panic(err)
	}
	fmt.Println(v.ID() == n.ID(), v.PropertiesMap()["name"])
	// Output: true Alice
}
