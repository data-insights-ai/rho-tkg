package resolve_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/resolve" // godoc anchor: ExampleAPI_<method> resolves against resolve.API
)

// ExampleAPI_NodeProperty demonstrates resolving any user or shadow (tkg_*)
// property on a node by string key.
func ExampleAPI_NodeProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	v, ok := g.Resolve.NodeProperty(n, "name")
	_ = v
	_ = ok
}

// ExampleAPI_LookupLabel demonstrates a non-creating lookup of an existing
// label token.
func ExampleAPI_LookupLabel() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	tok, ok := g.Resolve.LookupLabel("Person")
	_ = tok
	_ = ok
}

// ExampleAPI_LabelToken demonstrates retrieving a label token, creating it
// if it does not yet exist.
func ExampleAPI_LabelToken() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	tok, err := g.Resolve.LabelToken("NewLabel")
	if err != nil {
		panic(err)
	}
	_ = tok
}
