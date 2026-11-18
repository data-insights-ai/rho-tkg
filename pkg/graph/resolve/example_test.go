package resolve_test

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/resolve" // godoc anchor: ExampleAPI_<method> resolves against resolve.API
)

// ExampleAPI_NodeProperty demonstrates resolving any user or shadow (tkg_*)
// property on a node by string key.
func ExampleAPI_NodeProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		panic(err)
	}
	v, ok := g.Resolve().NodeProperty(n, "name")
	fmt.Println(v, ok)
	_, ok = g.Resolve().NodeProperty(n, "missing")
	fmt.Println(ok)
	// Output:
	// Alice true
	// false
}

// ExampleAPI_RelProperty demonstrates resolving any user or shadow (tkg_*)
// property on a relationship by string key.
func ExampleAPI_RelProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	a, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		panic(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		panic(err)
	}
	r, err := g.Rels().Add(ctx, "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		panic(err)
	}
	v, ok := g.Resolve().RelProperty(r, "since")
	fmt.Println(v, ok)
	// Output: 2026 true
}
