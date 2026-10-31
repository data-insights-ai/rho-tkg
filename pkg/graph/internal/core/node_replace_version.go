package core

import (
	"context"
	"maps"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Node — ReplaceVersion (ADR-0014, WHOLEROW)
// =============================================================================

// ReplaceVersion replaces the current row of node id whole as a new version:
// props is the new version's complete property set, [validFrom, validTo) its
// validity (validTo the valid axis's open end for an open row). The current
// version, closed or not, moves to history with TxTo at the new row's TxFrom;
// any start and end relative to it are accepted. Refused, nothing written: a
// node whose primary label is not in Config.ReplaceVersionLabels
// (ErrReplaceVersionNotListed), an unset start or an end not after it
// (ErrInvalidTimeRange), shadow keys in props (types.ErrReservedPrefix).
// A call that changes nothing returns the current row.
func (n *NodeOps) ReplaceVersion(ctx context.Context, id types.NodeID, props map[string]any, validFrom, validTo types.Instant) (*types.Node, error) {
	c := n.c
	if err := c.checkWritable(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	var (
		node    *types.Node
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, mutated, err = c.replaceNodeVersionInternal(ctx, id, props, validFrom, validTo)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return node, err
}

// ReplaceNodeVersion is Nodes().ReplaceVersion inside the transaction: the
// pre-mutation snapshot and the rollback are UpdateNode's.
func (tx *GraphTx) ReplaceNodeVersion(id types.NodeID, props map[string]any, validFrom, validTo types.Instant) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		return nil, err
	}
	n, mutated, err := tx.g.replaceNodeVersionInternal(tx.doorCtx(), id, props, validFrom, validTo)
	if err == nil && mutated {
		tx.g.publishEventNow(eventspkg.EventNodeUpdate, types.EntityID(id), eventspkg.PriorityNormal)
	}
	return n, err
}

// replaceNodeVersionInternal is the lock-free body of ReplaceVersion. Callers
// hold c.mu.RLock (standalone) or c.mu.Lock (tx).
//
// Red starting point (WHOLEROW step 2): the Update kernel with the validity as
// tkg_valid_from / tkg_valid_to, which refuses a closed row and a start at or
// before the previous one.
func (c *Core) replaceNodeVersionInternal(ctx context.Context, id types.NodeID, props map[string]any, validFrom, validTo types.Instant) (*types.Node, bool, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, false, err
	}
	updates := maps.Clone(props)
	if updates == nil {
		updates = map[string]any{}
	}
	updates["tkg_valid_from"] = validFrom
	if !c.axis().Open(validTo) {
		updates["tkg_valid_to"] = validTo
	}
	prov, tmp, updates, err := c.prepareUpdateProperties(updates, "replace node version")
	if err != nil {
		return nil, false, err
	}
	return c.updateNodePreparedInternal(ctx, id, prov, tmp, updates)
}
