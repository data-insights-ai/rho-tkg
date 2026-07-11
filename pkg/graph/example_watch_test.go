package graph_test

import (
	"context"
	"fmt"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// Example_watch demonstrates a live CDC consumer: Watch tails the change-log
// as a Go iter.Seq2, yielding new records as they commit until the caller
// cancels ctx or breaks. See replication.API.Watch's doc comment for the full
// termination/backoff contract.
func Example_watch() {
	g, _ := graphpkg.New(graphpkg.Config{Store: memory.New(memory.WithChangeLog())})
	defer g.Close()
	_, _ = g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"name": "Item1"})
	_, _ = g.Nodes().Add(context.Background(), []string{"Item"}, map[string]any{"name": "Item2"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	count := 0
	for rec, err := range g.Replication().Watch(ctx, 0) {
		if err != nil {
			break
		}
		fmt.Println("tag:", rec.Tag)
		count++
		if count == 2 {
			cancel()
		}
	}
	fmt.Println("Watched records:", count)
	// Output:
	// tag: NodePut
	// tag: NodePut
	// Watched records: 2
}
