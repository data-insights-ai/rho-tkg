package core

import (
	"context"
	"fmt"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Node — Update
// =============================================================================

// UpdateWithContext applies property updates to an existing node with context support.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) UpdateWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
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

// updateNodeInternal is the lock-free implementation of NodeOps.UpdateWithContext.
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
	prov, updates, err := c.prepareUpdateProperties(updates, "update node")
	if err != nil {
		return nil, false, err
	}
	return c.updateNodePreparedInternal(ctx, id, prov, updates)
}

// updateNodePreparedInternal applies a non-empty caller update after provenance
// extraction and property validation have already run. The prepared properties
// may be empty for metadata-only updates.
func (c *Core) updateNodePreparedInternal(ctx context.Context, id types.NodeID, prov updateProvenance, updates map[string]any) (*types.Node, bool, error) {
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
	if !nodePreparedUpdateMutates(current, prov, updates) {
		c.opNodeReads.Add(1)
		return current, false, nil
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return nil, false, err
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
	current.SetIntegrity(&types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           prevHash,
		AuthorID:           prov.authorID,
		Signature:          prov.signature,
		AuthorizedBy:       prov.authorizedBy,
		AuthorizationLevel: prov.authLevel,
	})

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, false, err
	}

	c.opNodeUpdates.Add(1)
	return current, true, nil
}

// UpdateInPlace applies property updates to a node without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Use for high-frequency counter updates where history accumulation is undesirable.
// Returns storepkg.ErrNodeNotFound if the node does not exist. Empty updates map is a no-op.
func (n *NodeOps) UpdateInPlace(id types.NodeID, updates map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.Nodes.UpdateInPlaceWithContext(context.Background(), id, updates)
}

// UpdateInPlaceWithContext applies property updates to a node without history.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) UpdateInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
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

// updateNodeInPlaceInternal is the lock-free implementation of NodeOps.UpdateInPlaceWithContext.
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

	// Phase 1: Pre-validate before acquiring entity lock.
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
	if !nodePropertyUpdatesMutate(current, updates) {
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

	nodeLabels := c.nodeLabelsUnlocked(current)
	hash, err := integrity.ComputeNodeHashChecked(current, nodeLabels)
	if err != nil {
		return nil, false, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// ReplaceNode instead of ReplaceNodeWithHistory — no history entry written.
	if err := c.store.ReplaceNode(current); err != nil {
		return nil, false, err
	}

	c.opNodeUpdates.Add(1)
	return current, true, nil
}

func nodePreparedUpdateMutates(current *types.Node, prov updateProvenance, updates map[string]any) bool {
	if prov.present {
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
