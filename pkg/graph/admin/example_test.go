package admin_test

import (
	"context"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/admin" // godoc anchor: ExampleAPI_<method> resolves against admin.API
)

// ExampleAPI_DecomposeNodeID demonstrates extracting the timestamp and
// generator-node identifier from a NodeID. Works on every store backend,
// not just tiered.Store.
func ExampleAPI_DecomposeNodeID() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	comp := g.Admin.DecomposeNodeID(n.ID())
	_ = comp.CreatedAt
	_ = comp.NodeID
	_ = comp.Sequence
}

// ExampleAPI_Reset demonstrates clearing all entities while preserving
// label and relationship-type registries.
func ExampleAPI_Reset() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err := g.Admin.Reset(); err != nil {
		panic(err)
	}
}

