package core

import (
	"context"
	"fmt"

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
	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil { // the store's read validates the ID
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
// Validation runs before the entity lock (shape, validity, properties), the
// listing and the no-op check under it. The new row is Update's new version
// with the one start rule relaxed: the chain's next version, TxFrom at the
// clock past the current row's belief, ValidFrom and ValidTo as given, the
// whole property set; the current row goes to history with TxTo at that
// instant, through ReplaceNodeWithHistory, so every index follows in the
// row's own batch.
func (c *Core) replaceNodeVersionInternal(ctx context.Context, id types.NodeID, props map[string]any, validFrom, validTo types.Instant) (*types.Node, bool, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, false, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	a := c.axis()
	if !a.Claimed(validFrom) {
		return nil, false, fmt.Errorf("%w: replace node version: valid-from %d is unset on this valid axis", ErrInvalidTimeRange, validFrom)
	}
	if !a.Open(validTo) {
		if err := checkCloseAfterStart("valid-to", validTo, validFrom, "node"); err != nil {
			return nil, false, err
		}
	}
	for key := range props {
		if types.IsShadowKey(key) {
			return nil, false, fmt.Errorf("graph: replace node version: %w: %q", types.ErrReservedPrefix, key)
		}
	}
	if err := c.validateProperties(props); err != nil {
		return nil, false, err
	}
	newProps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, false, fmt.Errorf("graph: replace node version: %w", err)
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	current, err := c.getCurrentNode(id)
	if err != nil {
		return nil, false, err
	}
	if _, ok := c.replaceVersionLabels[c.labels.Resolve(current.LabelTokenRawAt(0))]; !ok {
		return nil, false, fmt.Errorf("%w: node %d", ErrReplaceVersionNotListed, id)
	}
	if !replaceNodeVersionMutates(a, current, props, validFrom, validTo) {
		c.opNodeReads.Add(1)
		return current, false, nil
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("replace node version"); err != nil {
		return nil, false, err
	}

	prevVersion := current.Version()
	nextVersion, err := c.nextNodeVersion(id, current)
	if err != nil {
		return nil, false, err
	}
	prevState := current.DeepCopy()
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if err := current.SetOwnedProperties(newProps); err != nil {
		return nil, false, fmt.Errorf("graph: replace node version: %w", err)
	}
	current.SetVersion(nextVersion)

	// The transaction instant: the clock, past the current row's belief, so
	// the superseded row's TxTo never precedes its TxFrom (supersede's tx
	// rule); the valid start is the caller's claim, never derived.
	now := validInstantAfter(c.now(), beliefTx(current.Temporal()))
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.ValidFrom, tm.ValidTo = validFrom, validTo
	stampAppendedRow(tm, now)
	if ptm := prevState.Temporal(); ptm == nil {
		prevState.SetTemporal(&types.TemporalMetadata{TxTo: now})
	} else {
		ptm.TxTo = now
	}
	if err := c.checkNodeRowTemporalConstraints(id, current); err != nil {
		return nil, false, err
	}

	hash, err := c.nodeContentHash(current, c.nodeLabelsUnlocked(current))
	if err != nil {
		return nil, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	uniqueHold, uniqueErr := c.enforceUniqueForNode(current, prevState, id)
	if uniqueErr != nil {
		return nil, false, uniqueErr
	}
	defer uniqueHold.release()

	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, false, uniqueHold.storeWriteFailed(err)
	}
	c.opNodeUpdates.Add(1)
	return current, true, nil
}

// replaceNodeVersionMutates reports whether a whole-row replace changes the
// current row: a different property set, or a different start or end (the
// stored start: a claim differs from an unclaimed start at the same instant,
// which follows TxFrom; the end on the axis a).
func replaceNodeVersionMutates(a types.ValidAxis, current *types.Node, props map[string]any, validFrom, validTo types.Instant) bool {
	if current.PropertyCount() != len(props) {
		return true
	}
	for key, val := range props {
		if found, equal := current.PropertyValueEqual(key, val); !found || !equal {
			return true
		}
	}
	tm := current.Temporal()
	if tm == nil || tm.ValidFrom != validFrom {
		return true
	}
	return a.End(tm.ValidTo) != a.End(validTo)
}
