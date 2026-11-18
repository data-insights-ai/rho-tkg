package hash_test

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/hash" // godoc anchor: ExampleAPI_<method> resolves against hash.API
)

// ExampleAPI_VerifyNodeChain demonstrates verifying the hash chain of a
// node's complete version history.
func ExampleAPI_VerifyNodeChain() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	n, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	n, _ = g.Nodes().Update(ctx, n.ID(), map[string]any{"name": "Alicia"})
	ok, err := g.Hash().VerifyNodeChain(n.ID())
	if err != nil {
		panic(err)
	}
	fmt.Println(ok)
	// Output: true
}

// ExampleAPI_VerifyRelChain demonstrates verifying the hash chain of a
// relationship's complete version history.
func ExampleAPI_VerifyRelChain() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, _ := g.Rels().Add(context.Background(), "KNOWS", a, b, nil)
	ok, err := g.Hash().VerifyRelChain(r.ID())
	if err != nil {
		panic(err)
	}
	fmt.Println(ok)
	// Output: true
}
