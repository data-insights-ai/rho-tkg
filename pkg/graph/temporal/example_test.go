package temporal_test

import (
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal" // godoc anchor: ExampleAPI_<method> resolves against temporal.API
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ExampleAPI_NodesAt demonstrates a point-in-time query against the graph.
func ExampleAPI_NodesAt() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	now := types.Instant(time.Now().UnixMilli())
	nodes, err := g.Temporal.NodesAt(now)
	if err != nil {
		panic(err)
	}
	_ = nodes
}

// ExampleAPI_Snapshot demonstrates capturing the entire graph state at an
// instant.
func ExampleAPI_Snapshot() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})

	now := types.Instant(time.Now().UnixMilli())
	snap, err := g.Temporal.Snapshot(now)
	if err != nil {
		panic(err)
	}
	_ = snap
}

// ExampleAPI_NodeAt demonstrates retrieving a specific node version at a
// point in time.
func ExampleAPI_NodeAt() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	now := types.Instant(time.Now().UnixMilli())
	v, err := g.Temporal.NodeAt(n.ID(), now)
	if err != nil {
		panic(err)
	}
	_ = v
}
