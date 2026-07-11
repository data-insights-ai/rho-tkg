package graph_test

import (
	"context"
	"fmt"
	"sort"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

func ExampleNew() {
	g, _ := graphpkg.New(graphpkg.Config{})
	defer g.Close()
	alice, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	bob, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	_, _ = g.Rels().Add(context.Background(), "KNOWS", alice, bob, nil)
	persons, _ := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
	names := make([]string, len(persons))
	for i, p := range persons {
		name, _ := p.GetProperty("name")
		names[i] = name.(string)
	}
	sort.Strings(names)
	relCount, _ := g.Rels().Count()
	fmt.Println("Person nodes:", names)
	fmt.Println("Relationship count:", relCount)
	// Output:
	// Person nodes: [Alice Bob]
	// Relationship count: 1
}
