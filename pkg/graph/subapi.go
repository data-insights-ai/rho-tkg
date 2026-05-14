package graph

import (
	"context"
	"errors"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/core"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// Sub-API accessors that wrap pkg/graph-private types (GraphTx, BatchBuilder).
// These types live inside pkg/graph because they alias *core.GraphTx /
// *core.BatchBuilder; the wrappers route to the underlying *core.Core.

// TxAPI is the transaction sub-API accessor on Graph.Tx.
type TxAPI struct{ c *core.Core }

// Begin opens a new transaction. Returns ErrGraphClosed if the graph
// has already been closed.
func (a *TxAPI) Begin() (*GraphTx, error) {
	if a == nil || a.c == nil {
		return nil, core.ErrNilGraph
	}
	return a.c.BeginTx()
}

// Run executes fn within a transaction. Commits if fn returns nil,
// otherwise rolls back. The returned error is fn's error joined with any
// rollback error.
//
// Panic safety: if fn panics, Rollback is invoked via defer to release the
// graph write lock and the panic is re-raised. Without this guard a panicking
// callback would leave c.mu held forever, deadlocking every subsequent
// mutation, transaction, and read.
func (a *TxAPI) Run(fn func(*GraphTx) error) (retErr error) {
	if a == nil || a.c == nil {
		return core.ErrNilGraph
	}
	if fn == nil {
		return core.ErrNilTxCallback
	}
	tx, err := a.c.BeginTx()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		// Either fn returned an error, fn panicked, or Commit was never reached.
		// Roll back to release the write lock; capture any rollback error.
		// (Rollback is idempotent through tx.done — safe even if fn rolled back.)
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, errTxDoneSentinel()) {
			retErr = errors.Join(retErr, rbErr)
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// RunContext executes fn within a transaction honoring ctx cancellation.
// Same panic-safety guarantees as Run; additionally, if ctx is already done
// before BeginTx or after fn returns nil but before Commit, the transaction
// is rolled back and ctx.Err() is returned.
func (a *TxAPI) RunContext(ctx context.Context, fn func(*GraphTx) error) (retErr error) {
	if a == nil || a.c == nil {
		return core.ErrNilGraph
	}
	if ctx == nil {
		return core.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return core.ErrNilTxCallback
	}
	tx, err := a.c.BeginTx()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		if rbErr := tx.Rollback(); rbErr != nil && !errors.Is(rbErr, errTxDoneSentinel()) {
			retErr = errors.Join(retErr, rbErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

// errTxDoneSentinel is the ErrTxDone sentinel that GraphTx returns after the
// caller has already committed or rolled back. The deferred Rollback in Run /
// RunContext expects this if fn rolled back manually; we silently swallow only
// that one sentinel and surface every other error.
func errTxDoneSentinel() error { return storepkg.ErrTxDone }

// BatchAPI is the batch sub-API accessor on Graph.Batch.
type BatchAPI struct{ c *core.Core }

// New constructs a fresh BatchBuilder bound to the underlying graph.
// Returns ErrGraphClosed if the graph has already been closed.
func (a *BatchAPI) New() (*BatchBuilder, error) {
	if a == nil || a.c == nil {
		return nil, core.ErrNilGraph
	}
	return core.NewBatchBuilder(a.c)
}
