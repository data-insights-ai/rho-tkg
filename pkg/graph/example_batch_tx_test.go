package graph_test

import (
	"fmt"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func Example_transactions() {
	g, _ := graphpkg.New(graphpkg.Config{})
	defer g.Close()
	_ = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, _ = tx.AddNode([]string{"Account"}, map[string]any{"id": "acc1"})
		_, _ = tx.AddNode([]string{"Account"}, map[string]any{"id": "acc2"})
		return nil
	})
	count1, _ := g.Nodes().Count()
	fmt.Println("After transaction:", count1)
	_, _ = g.Batch().Run(func(bb *graphpkg.BatchBuilder) error {
		_, _ = bb.AddNode([]string{"User"}, map[string]any{"name": "User1"})
		_, _ = bb.AddNode([]string{"User"}, map[string]any{"name": "User2"})
		_, _ = bb.AddNode([]string{"User"}, map[string]any{"name": "User3"})
		return nil
	})
	count2, _ := g.Nodes().Count()
	fmt.Println("After batch:", count2)
	// Output:
	// After transaction: 2
	// After batch: 5
}
