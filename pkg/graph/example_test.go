package graph_test

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
)

// ExampleTxAPI_Run demonstrates the closure-style transaction helper: open
// a transaction, do some mutations, and commit (or roll back on error).
func ExampleTxAPI_Run() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if err := g.Tx.Run(func(tx *graph.GraphTx) error {
		_, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
		return err
	}); err != nil {
		panic(err)
	}
}

// ExampleTxAPI_Begin demonstrates the explicit Begin / Commit / Rollback
// transaction lifecycle for callers that need finer control.
func ExampleTxAPI_Begin() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	tx := g.Tx.Begin()
	if _, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		_ = tx.Rollback()
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
	}
}

// ExampleTxAPI_RunContext demonstrates a transaction that honors a caller
// context for cancellation.
func ExampleTxAPI_RunContext() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	ctx := context.Background()
	if err := g.Tx.RunContext(ctx, func(tx *graph.GraphTx) error {
		_, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
		return err
	}); err != nil {
		panic(err)
	}
}

// ExampleBatchAPI_New demonstrates queueing a small batch of operations and
// applying them in one Execute call.
func ExampleBatchAPI_New() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	bb := g.Batch.New()
	if _, err := bb.AddNode([]string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		panic(err)
	}
	if _, err := bb.AddNode([]string{"Person"}, map[string]any{"name": "Bob"}); err != nil {
		panic(err)
	}
	if _, err := bb.Execute(); err != nil {
		panic(err)
	}
}
