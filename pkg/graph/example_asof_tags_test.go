package graph_test

import (
	"context"
	"fmt"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func Example_asOfTags() {
	g, _ := graphpkg.New(graphpkg.Config{})
	defer g.Close()
	_, _ = g.Nodes().Add(context.Background(), []string{"Doc"}, map[string]any{"title": "Doc1"})
	_, _ = g.Nodes().Add(context.Background(), []string{"Doc"}, map[string]any{"title": "Doc2"})
	txNow, _ := g.Temporal().NowTx()
	_ = g.Temporal().TagAsOf("baseline", txNow)
	_, _ = g.Nodes().Add(context.Background(), []string{"Doc"}, map[string]any{"title": "Doc3"})
	resolved, ok, _ := g.Temporal().ResolveAsOf("baseline")
	if ok {
		fmt.Println("Baseline tag resolved:", resolved == txNow)
	}
	docsAtBaseline, _ := g.Temporal().NodesAsOf(txNow)
	fmt.Println("Docs at baseline:", len(docsAtBaseline))
	currentCount, _ := g.Nodes().Count()
	fmt.Println("Docs now:", currentCount)
	// Output:
	// Baseline tag resolved: true
	// Docs at baseline: 2
	// Docs now: 3
}
