package resolve_test

import (
	"context"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/resolve" // godoc anchor: ExampleAPI_<method> resolves against resolve.API
)

// ExampleAPI_NodeProperty demonstrates resolving any user or shadow (tkg_*)
// property on a node by string key.
func ExampleAPI_NodeProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	v, ok := g.Resolve().NodeProperty(n, "name")
	_ = v
	_ = ok
}

// ExampleAPI_RelProperty demonstrates resolving any user or shadow (tkg_*)
// property on a relationship by string key.
func ExampleAPI_RelProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, _ := g.Rels().Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	v, ok := g.Resolve().RelProperty(r, "since")
	_ = v
	_ = ok
}
