package core

import (
	"context"
	"fmt"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Node — Update
// =============================================================================

// Update applies property updates to an existing node with context support.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) Update(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
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
		node, mutated, err = c.updateNodeInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return node, err
}

// updateNodeInternal is the lock-free implementation of NodeOps.Update.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateNodeInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, false, err
	}

	if len(updates) == 0 {
		current, err := c.getCurrentNode(id)
		if err == nil {
			c.opNodeReads.Add(1)
		}
		return current, false, err
	}

	// The no-op check above uses the original map length; after extraction
	// the remaining updates may be empty (metadata-only update).
	prov, tmp, updates, err := c.prepareUpdateProperties(updates, "update node")
	if err != nil {
		return nil, false, err
	}
	return c.updateNodePreparedInternal(ctx, id, prov, tmp, updates)
}

// updateNodePreparedInternal applies a non-empty caller update after provenance
// extraction and property validation have already run. The prepared properties
// may be empty for metadata-only updates.
func (c *Core) updateNodePreparedInternal(ctx context.Context, id types.NodeID, prov updateProvenance, tmp updateTemporal, updates map[string]any) (*types.Node, bool, error) {
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, false, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Phase 2: Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil {
		return nil, false, err
	}
	if !nodePreparedUpdateMutates(current, prov, tmp, updates) {
		c.opNodeReads.Add(1)
		return current, false, nil
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return nil, false, err
	}
	// Phase 2: validate caller-supplied ValidFrom against the previous version's
	// effective ValidFrom. New ValidFrom must be strictly greater — otherwise
	// the prev tile's implicit ValidTo would land before its own ValidFrom,
	// corrupting the timeline. Bitemporal backdating uses Phase 3's cascade.
	if tmp.hasValidFrom && tmp.validFrom != 0 {
		prevEff := c.nodeValidFrom(current)
		if tmp.validFrom <= prevEff {
			return nil, false, fmt.Errorf("%w: %d <= prev %d", ErrValidFromBeforePrevious, tmp.validFrom, prevEff)
		}
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("update node"); err != nil {
		return nil, false, err
	}

	// Capture pre-mutation state for version history (deep copy before any mutations).
	prevVersion := current.Version()
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return nil, false, err
	}
	prevState := current.DeepCopy()

	// Capture current hash for the PrevHash chain.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, false, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, false, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		}
	}

	// Check final property count after mutations (under entity lock, before persist).
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(nextVersion)

	now := c.nodeVersionUpdateInstant(current)
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	// Phase 1: stop inheriting ValidFrom/ValidTo from the previous version
	// (clear to 0 so non-genesis resolver bounds use UpdatedAt). Phase 2:
	// honour caller-supplied tkg_valid_from / tkg_valid_to from `tmp`.
	tm.ValidFrom = 0
	tm.ValidTo = 0
	if tmp.hasValidFrom {
		tm.ValidFrom = tmp.validFrom
	}
	if tmp.hasValidTo {
		tm.ValidTo = tmp.validTo
	}
	tm.UpdatedAt = now

	// Set TxTo on the previous version (being superseded) using the same now.
	// TxFrom/TxTo are NOT hashed — safe to set before or after hash computation.
	if ptm := prevState.Temporal(); ptm == nil {
		ptm2 := &types.TemporalMetadata{}
		prevState.SetTemporal(ptm2)
		ptm2.TxTo = now
	} else {
		ptm.TxTo = now
	}

	// Set TxFrom on the new version (this is the commit time of the new version).
	tm.TxFrom = now

	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := integrity.ComputeNodeHashChecked(current, nodeLabels)
	if err != nil {
		return nil, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	// Preserve existing integrity metadata (Update previously wiped
	// AuthorID / Signature / AuthorizedBy / AuthorizationLevel whenever a
	// caller restated even one of them). Only overwrite the four caller-
	// supplied provenance fields that are explicitly present in `updates`.
	ig := nodeIntegrityWithHash(current.Integrity(), hash, prevHash)
	if prov.hasAuthorID {
		ig.AuthorID = prov.authorID
	}
	if prov.hasSignature {
		ig.Signature = prov.signature
	}
	if prov.hasAuthorizedBy {
		ig.AuthorizedBy = prov.authorizedBy
	}
	if prov.hasAuthLevel {
		ig.AuthorizationLevel = prov.authLevel
	}
	current.SetIntegrity(ig)

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Unique-constraint enforcement (standalone update door). Entity lock held;
	// take value stripe(s) across the check + write. prevState supplies the old
	// value so a changed constrained value also holds the freed value's stripe.
	uniqueRelease, uniqueErr := c.enforceUniqueForNode(current, prevState, id)
	if uniqueErr != nil {
		return nil, false, uniqueErr
	}
	defer uniqueRelease()

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, false, err
	}

	c.opNodeUpdates.Add(1)
	return current, true, nil
}

// UpdateInPlace applies property updates to a node without creating a version
// history entry. Version number is NOT incremented. PrevHash in the integrity
// chain is preserved. Use for high-frequency counter updates where history
// accumulation is undesirable. Returns storepkg.ErrNodeNotFound if the node
// does not exist. Empty updates map is a no-op. Acquires c.mu.RLock
// (panic-safe) for transaction isolation — blocked while a tx holds c.mu.Lock.
//
// tkg_valid_from/tkg_valid_to ARE accepted and rewrite the current row's
// TemporalMetadata directly — consistent with this door's no-history
// tradeoff, this does NOT stamp TxFrom or preserve the pre-correction belief
// anywhere: a bitemporal read pinned between the original write and this
// correction sees the corrected value. Callers that need the pre-correction
// belief recoverable must use Update instead.
func (n *NodeOps) UpdateInPlace(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
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
		node, mutated, err = c.updateNodeInPlaceInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return node, err
}

// updateNodeInPlaceInternal is the lock-free implementation of NodeOps.UpdateInPlace.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateNodeInPlaceInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, false, err
	}

	if len(updates) == 0 {
		current, err := c.getCurrentNode(id)
		if err == nil {
			c.opNodeReads.Add(1)
		}
		return current, false, err
	}

	// Phase 1: Pre-validate before acquiring entity lock. Strip reserved
	// temporal keys (tkg_valid_from / tkg_valid_to) into `tmp` so they pass
	// validation; UpdateInPlace honours them as direct rewrites of the
	// current row's TemporalMetadata (no version chain entry).
	tmp, updates, err := extractTemporalTracked(updates)
	if err != nil {
		return nil, false, err
	}
	if err := c.validatePropertyUpdates(updates, "update node in place"); err != nil {
		return nil, false, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Phase 2: Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	current, err := c.getCurrentNode(id)
	if err != nil {
		return nil, false, err
	}
	if !tmp.present && !nodePropertyUpdatesMutate(current, updates) {
		c.opNodeReads.Add(1)
		return current, false, nil
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return nil, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("update node in place"); err != nil {
		return nil, false, err
	}

	// Preserve existing PrevHash — no new chain link for in-place updates.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Capture the pre-mutation state so a constrained value CHANGED in place
	// still holds the freed old-value stripe (ADR-0002 Decision 3). Unlike
	// Update, UpdateInPlace mutates `current` directly and writes no history
	// row, so without this snapshot enforceUniqueForNode could not see the old
	// value to lock its stripe.
	var prevState *types.Node
	if c.hasUniqueConstraints.Load() {
		prevState = current.DeepCopy()
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, false, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, false, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		}
	}

	// Check final property count.
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	// NO version bump — in-place update preserves version.

	now := c.now()
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.UpdatedAt = now
	if tmp.hasValidFrom {
		tm.ValidFrom = tmp.validFrom
	}
	if tmp.hasValidTo {
		tm.ValidTo = tmp.validTo
	}

	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := integrity.ComputeNodeHashChecked(current, nodeLabels)
	if err != nil {
		return nil, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Unique-constraint enforcement (standalone in-place update door). Entity
	// lock held; take value stripe(s) across the check + write. prevState (nil
	// when no constraints exist) supplies the old value so a changed constrained
	// value also holds the freed value's stripe.
	uniqueRelease, uniqueErr := c.enforceUniqueForNode(current, prevState, id)
	if uniqueErr != nil {
		return nil, false, uniqueErr
	}
	defer uniqueRelease()

	// ReplaceNode instead of ReplaceNodeWithHistory — no history entry written.
	if err := c.store.ReplaceNode(current); err != nil {
		return nil, false, err
	}

	c.opNodeUpdates.Add(1)
	return current, true, nil
}

func nodePreparedUpdateMutates(current *types.Node, prov updateProvenance, tmp updateTemporal, updates map[string]any) bool {
	if prov.present || tmp.present {
		return true
	}
	return nodePropertyUpdatesMutate(current, updates)
}

func nodePropertyUpdatesMutate(current *types.Node, updates map[string]any) bool {
	for key, val := range updates {
		if val != nil {
			found, equal := current.PropertyValueEqual(key, val)
			if !found || !equal {
				return true
			}
			continue
		}
		found, _ := current.PropertyValueEqual(key, nil)
		if found {
			return true
		}
	}
	return false
}
