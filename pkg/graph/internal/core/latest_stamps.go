package core

import (
	"errors"
	"reflect"

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
	cur, live, err := c.nodeCurrentStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if live {
		txFrom, txTo = storepkg.FoldTxStamps(0, 0, cur.TxFrom, cur.TxTo, cur.DeletedAt)
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
	cur, live, err := c.relCurrentStamps(id)
	if err != nil {
		return 0, 0, false, err
	}
	if live {
		txFrom, txTo = storepkg.FoldTxStamps(0, 0, cur.TxFrom, cur.TxTo, cur.DeletedAt)
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

// currentStampsCapability admits store.CurrentStampsCapability for the
// current-row half of LatestStamps: exact native stores and stores that
// declare the methods themselves (sharded, a third-party backend). A wrapper
// that only inherits them from an embedded native store is declined, so its
// GetNode / GetRelationship override (fault injection, a policy check) stays
// the read LatestStamps makes — the same visibility guard as every cached
// capability handle.
func currentStampsCapability(store storepkg.MandatoryStore) storepkg.CurrentStampsCapability {
	cap, ok := store.(storepkg.CurrentStampsCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.CurrentStampsCapability)(nil)).Elem(),
		"NodeCurrentStamps", "RelCurrentStamps") {
		return nil
	}
	return cap
}

// nodeCurrentStamps is the current-row half of NodeOps.LatestStamps: the
// current row's stamps and whether there is one (live false, err nil for an
// ID without a current row). It reads store.CurrentStampsCapability where
// the store offers it, else the current row (lent on a trusted store, the
// validated row otherwise). Caller holds c.mu (read).
func (c *Core) nodeCurrentStamps(id types.NodeID) (storepkg.RowStamps, bool, error) {
	var s storepkg.RowStamps
	var err error
	if c.currentStamps != nil {
		s, err = c.currentStamps.NodeCurrentStamps(id)
	} else {
		var cur *types.Node
		if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
			cur, err = lender.LendNode(id)
		} else {
			cur, err = c.getCurrentNode(id)
		}
		s = storepkg.NodeRowStamps(cur)
	}
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		return storepkg.RowStamps{}, false, nil
	}
	if err != nil {
		return storepkg.RowStamps{}, false, err
	}
	return s, true, nil
}

// relCurrentStamps is nodeCurrentStamps for relationships.
func (c *Core) relCurrentStamps(id types.RelID) (storepkg.RowStamps, bool, error) {
	var s storepkg.RowStamps
	var err error
	if c.currentStamps != nil {
		s, err = c.currentStamps.RelCurrentStamps(id)
	} else {
		var cur *types.Relationship
		if lender, ok := c.store.(storepkg.EntityLendCapability); ok && c.storeRowsTrust {
			cur, err = lender.LendRelationship(id)
		} else {
			cur, err = c.getCurrentRelationship(id)
		}
		s = storepkg.RelRowStamps(cur)
	}
	if errors.Is(err, storepkg.ErrRelNotFound) {
		return storepkg.RowStamps{}, false, nil
	}
	if err != nil {
		return storepkg.RowStamps{}, false, err
	}
	return s, true, nil
}

// memberIDsCapability admits store.MemberIDsCapability for the effective
// scans' candidate gather on the same terms as currentStampsCapability: a
// wrapper that only inherits it keeps its NodesByLabel / RelationshipsByType
// override as the read.
func memberIDsCapability(store storepkg.MandatoryStore) storepkg.MemberIDsCapability {
	cap, ok := store.(storepkg.MemberIDsCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.MemberIDsCapability)(nil)).Elem(),
		"NodeIDsByLabel", "RelIDsByType") {
		return nil
	}
	return cap
}
