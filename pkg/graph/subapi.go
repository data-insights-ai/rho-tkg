package graph

import (
	"context"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/core"
)

// Sub-API accessors that wrap pkg/graph-private types (GraphTx, BatchBuilder).
// These types live inside pkg/graph because they alias *core.GraphTx /
// *core.BatchBuilder; the wrappers route to the underlying *core.Core.

// TxAPI is the transaction sub-API accessor on Graph.Tx.
type TxAPI struct{ c *core.Core }

// Begin opens a new transaction. Forwards to (*core.Core).BeginTx.
func (a *TxAPI) Begin() *GraphTx { return a.c.BeginTx() }

// Run executes fn within a transaction. Commits if fn returns nil, otherwise
// rolls back and returns fn's error joined with any commit/rollback failure.
func (a *TxAPI) Run(fn func(*GraphTx) error) error {
	tx := a.c.BeginTx()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// RunContext executes fn within a transaction honoring ctx cancellation.
func (a *TxAPI) RunContext(ctx context.Context, fn func(*GraphTx) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx := a.c.BeginTx()
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// BatchAPI is the batch sub-API accessor on Graph.Batch.
type BatchAPI struct{ c *core.Core }

// New constructs a fresh BatchBuilder bound to the underlying graph.
func (a *BatchAPI) New() *BatchBuilder { return core.NewBatchBuilder(a.c) }
