package rels_test

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/rels" // godoc anchor: ExampleAPI_<method> resolves against rels.API
)

// ExampleAPI_Add demonstrates the most common operation: creating a directed
// relationship between two nodes.
func ExampleAPI_Add() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	a, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, err := g.Rels().Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
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

	a, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	_, _ = g.Rels().Add(context.Background(), "KNOWS", a, b, nil)

	out, err := g.Rels().Outgoing(a.ID(), "KNOWS")
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

	a, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	r, err := g.Rels().AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), nil)
	if err != nil {
		panic(err)
	}
	_ = r
}

// ExampleAPI_Iter demonstrates the Go 1.23+ range-over-func form: Iter wraps
// ForEach so a caller can `for r, err := range g.Rels().Iter(ctx, opts)`
// directly, breaking out of the loop early stops the underlying scan.
func ExampleAPI_Iter() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	_, _ = g.Rels().Add(ctx, "KNOWS", a, b, nil)

	count := 0
	for r, err := range g.Rels().Iter(ctx, graph.QueryOpts{}) {
		if err != nil {
			panic(err)
		}
		_ = r
		count++
	}
	fmt.Println(count)
	// Output: 1
}
