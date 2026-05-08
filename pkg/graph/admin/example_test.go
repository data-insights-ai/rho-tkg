package admin_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/admin" // godoc anchor: ExampleAPI_<method> resolves against admin.API
)

// ExampleAPI_DecomposeID demonstrates extracting the timestamp and node
// identifier from a snowflake ID. Works on every store backend, not just
// tiered.Store.
func ExampleAPI_DecomposeID() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	comp := g.Admin.DecomposeID(n.ID().SnowflakeID())
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

	_, _ = g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err := g.Admin.Reset(); err != nil {
		panic(err)
	}
}

// ExampleAPI_ListShards demonstrates inspecting shard metadata. Returns an
// error on non-tiered stores; the example handles that gracefully.
func ExampleAPI_ListShards() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	shards, _ := g.Admin.ListShards() // ignored: not a tiered store in this example
	_ = shards
}
