package hash_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/hash" // godoc anchor: ExampleAPI_<method> resolves against hash.API
)

// ExampleAPI_VerifyNodeChain demonstrates verifying the hash chain of a
// node's complete version history.
func ExampleAPI_VerifyNodeChain() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	ok, err := g.Hash.VerifyNodeChain(n.ID())
	if err != nil {
		panic(err)
	}
	_ = ok
}

// ExampleAPI_VerifyRelChain demonstrates verifying the hash chain of a
// relationship's complete version history.
func ExampleAPI_VerifyRelChain() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	ok, err := g.Hash.VerifyRelChain(r.ID())
	if err != nil {
		panic(err)
	}
	_ = ok
}
