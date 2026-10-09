package core

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// GraphTx twins of Nodes().DeleteWithTx / Nodes().UpdateWithTx. They share the
// plain twins' bodies (deleteNodeAt / updateNodeAt in tx_mutations.go), so the
// pre-mutation snapshot and the rollback are the plain twins' exactly; only the
// caller instant reaches the shared kernel (deleteNodeInternal,
// updateNodePreparedInternal), where it is checked under the entity locks.
//
// The instant is gated before the tx lock is taken — value first
// (ErrInvalidTxFrom), then Config.AllowTxBackfill (ErrTxBackfillDisabled) — as
// the standalone doors gate before c.mu. The past-dated write is reported to
// the as-of column cache after the write-through store write; a later Rollback
// invalidates the cache again (tx.Rollback bumps it).

// DeleteNodeWithTx removes a node and all connected relationships within the
// transaction like DeleteNode, stamping every tombstone of the cascade with the
// caller's transaction instant txTo (see NodeOps.DeleteWithTx). A refusal
// (ErrTxOrder) changes nothing and leaves the transaction open.
func (tx *GraphTx) DeleteNodeWithTx(id types.NodeID, txTo types.Instant) error {
	at, err := tx.g.resolveCallerTxInstant(txTo)
	if err != nil {
		return err
	}
	defer tx.g.notePastDatedWrite(at)
	return tx.deleteNodeAt(id, at)
}

// UpdateNodeWithTx applies property updates to a node within the transaction
// like UpdateNode, stamping the superseded version's TxTo and the new version's
// TxFrom and UpdatedAt with the caller's transaction instant txFrom (see
// NodeOps.UpdateWithTx). An update that changes nothing is refused (ErrTxOrder).
func (tx *GraphTx) UpdateNodeWithTx(id types.NodeID, updates map[string]any, txFrom types.Instant) (*types.Node, error) {
	at, err := tx.g.resolveCallerTxInstant(txFrom)
	if err != nil {
		return nil, err
	}
	defer tx.g.notePastDatedWrite(at)
	return tx.updateNodeAt(id, updates, at)
}
