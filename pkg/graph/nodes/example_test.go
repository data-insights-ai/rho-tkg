package nodes_test

import (
	"context"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/nodes" // godoc anchor: ExampleAPI_<method> resolves against nodes.API
)

// ExampleAPI_Add demonstrates the most common operation: creating a node
// with one label and a small property bag.
func ExampleAPI_Add() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		panic(err)
	}
	_ = n
}

// ExampleAPI_Update demonstrates an update that bumps the version chain.
func ExampleAPI_Update() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"name": "Alice (renamed)"}); err != nil {
		panic(err)
	}
}

// ExampleAPI_Delete demonstrates cascade deletion.
func ExampleAPI_Delete() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err := g.Nodes.Delete(context.Background(), n.ID()); err != nil {
		panic(err)
	}
}
