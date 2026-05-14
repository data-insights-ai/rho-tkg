package rels_test

import (
	"context"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/rels" // godoc anchor: ExampleAPI_<method> resolves against rels.API
)

// ExampleAPI_Add demonstrates the most common operation: creating a directed
// relationship between two nodes.
func ExampleAPI_Add() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, err := g.Rels.Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		panic(err)
	}
	_ = r
}

// ExampleAPI_Outgoing demonstrates traversing the outgoing edges of a node.
func ExampleAPI_Outgoing() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	_, _ = g.Rels.Add(context.Background(), "KNOWS", a, b, nil)

	out, err := g.Rels.Outgoing(a.ID(), "KNOWS")
	if err != nil {
		panic(err)
	}
	_ = out
}

// ExampleAPI_AddByID demonstrates the high-throughput path: create a
// relationship without first fetching the endpoint nodes.
func ExampleAPI_AddByID() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), nil)
	if err != nil {
		panic(err)
	}
	_ = r
}
