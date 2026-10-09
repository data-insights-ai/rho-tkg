package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// LatestStamps returns the node's newest transaction stamps over ALL its rows
// (backlog 30): txFrom is the largest TxFrom and txTo the largest TxTo or
// DeletedAt (store.FoldTxStamps) over the current row and every history row
// — superseded versions, the rows a version-interval cascade appends after the
// current row's TxFrom while the current row stays, the delete tombstone —
// and deleted is true when the node has history rows but no current row. It
// equals that fold over Get and History at every moment without reading the
// history: the current row is read (lent, no copy), the history half comes
// from store.HistoryStampsCapability (memory, badger, sharded, tiered); a
// store without it is answered from History.
//
// Neither the current row's TxFrom alone (the cascade rows above it; an ended
// node has none) nor the top history version's row (TxFrom is not co-monotonic
// with version: validInstantAfter, tx_order.go) carries the answer.
//
// Errors: ErrNodeNotFound for an ID without any row, ErrGraphClosed, an
// invalid ID (ErrInvalidStoreMutation), a store error.
func (n *NodeOps) LatestStamps(id types.NodeID) (txFrom, txTo types.Instant, deleted bool, err error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return 0, 0, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, 0, false, ErrGraphClosed
	}
	// Current row first, history second: a with-history write publishes the
	// moved row to history before it replaces or removes the current row
	// (badger publishMoveLocked; memory writes both under one lock), so a row
	// is never in neither read.
	var cur *types.Node
	if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
		cur, err = lender.LendNode(id)
	} else {
		cur, err = c.getCurrentNode(id)
	}
	live := err == nil
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return 0, 0, false, err
	}
	if live {
		f, t, d := cur.TxStamps()
		txFrom, txTo = storepkg.FoldTxStamps(0, 0, f, t, d)
	}
	hf, ht, has, err := c.nodeHistoryStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if !live && !has {
		return 0, 0, false, storepkg.ErrNodeNotFound
	}
	txFrom, txTo = storepkg.FoldTxStamps(txFrom, txTo, hf, ht, 0)
	return txFrom, txTo, !live, nil
}

// nodeHistoryStamps is the history half: the store capability, or the fold of
// the history rows for a store without it. Caller holds c.mu (read).
func (c *Core) nodeHistoryStamps(id types.NodeID) (txFrom, txTo types.Instant, has bool, err error) {
	if p, ok := c.store.(storepkg.HistoryStampsCapability); ok {
		return p.NodeHistoryStamps(id)
	}
	history, err := c.getNodeHistory(id)
	if err != nil {
		return 0, 0, false, err
	}
	for _, h := range history {
		f, t, d := h.TxStamps()
		txFrom, txTo = storepkg.FoldTxStamps(txFrom, txTo, f, t, d)
	}
	return txFrom, txTo, len(history) > 0, nil
}

// LatestStamps is NodeOps.LatestStamps for relationships. Errors:
// ErrRelNotFound for an ID without any row, ErrGraphClosed, an invalid ID
// (ErrInvalidStoreMutation), a store error.
func (r *RelOps) LatestStamps(id types.RelID) (txFrom, txTo types.Instant, deleted bool, err error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return 0, 0, false, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, 0, false, ErrGraphClosed
	}
	var cur *types.Relationship
	if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
		cur, err = lender.LendRelationship(id)
	} else {
		cur, err = c.getCurrentRelationship(id)
	}
	live := err == nil
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return 0, 0, false, err
	}
	if live {
		f, t, d := cur.TxStamps()
		txFrom, txTo = storepkg.FoldTxStamps(0, 0, f, t, d)
	}
	hf, ht, has, err := c.relHistoryStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if !live && !has {
		return 0, 0, false, storepkg.ErrRelNotFound
	}
	txFrom, txTo = storepkg.FoldTxStamps(txFrom, txTo, hf, ht, 0)
	return txFrom, txTo, !live, nil
}

// relHistoryStamps mirrors nodeHistoryStamps for relationships.
func (c *Core) relHistoryStamps(id types.RelID) (txFrom, txTo types.Instant, has bool, err error) {
	if p, ok := c.store.(storepkg.HistoryStampsCapability); ok {
		return p.RelHistoryStamps(id)
	}
	history, err := c.getRelHistory(id)
	if err != nil {
		return 0, 0, false, err
	}
	for _, h := range history {
		f, t, d := h.TxStamps()
		txFrom, txTo = storepkg.FoldTxStamps(txFrom, txTo, f, t, d)
	}
	return txFrom, txTo, len(history) > 0, nil
}
