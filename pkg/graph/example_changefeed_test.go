package graph_test

import (
	"context"
	"fmt"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

func Example_changeFeed() {
	g, _ := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog())})
	defer g.Close()
	_, _ = g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"name": "Item1"})
	_, _ = g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"name": "Item2"})
	count := 0
	_ = g.Replication().ForEachChange(0, func(rec storepkg.ChangeRecord) bool {
		count++
		return true
	})
	fmt.Println("Change records:", count)
	// Output:
	// Change records: 2
}
