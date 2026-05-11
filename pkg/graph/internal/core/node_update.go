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
	var (
		node *types.Node
		err  error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, err = c.updateNodeInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return node, err
}

// updateNodeInternal is the lock-free implementation of NodeOps.UpdateWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateNodeInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
		current, err := c.store.GetNode(id)
		if err == nil {
			c.opNodeReads.Add(1)
		}
		return current, err
	}

	// The no-op check above uses the original map length; after extraction
	// the remaining updates may be empty (metadata-only update).
	prov, updates, err := c.prepareUpdateProperties(updates, "update node")
	if err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := c.store.GetNode(id)
	if err != nil {
		return nil, err
	}

	// Capture pre-mutation state for version history (deep copy before any mutations).
	prevVersion := current.Version()
	nextVersion, err := nextEntityVersion(prevVersion)
	if err != nil {
		return nil, err
	}
	prevState := current.DeepCopy()

	// Capture current hash for the PrevHash chain.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.Hash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		}
	}

	// Check final property count after mutations (under entity lock, before persist).
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(nextVersion)

	now := c.now()
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
		return nil, fmt.Errorf("graph: compute node hash: %w", err)
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
		return nil, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := c.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, err
	}

	c.opNodeUpdates.Add(1)
	return current, nil
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
	var (
		node *types.Node
		err  error
	)
	ep, closeErr := c.runUnderRLock(func() {
		node, err = c.updateNodeInPlaceInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return node, err
}

// updateNodeInPlaceInternal is the lock-free implementation of NodeOps.UpdateInPlaceWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateNodeInPlaceInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
		current, err := c.store.GetNode(id)
		if err == nil {
			c.opNodeReads.Add(1)
		}
		return current, err
	}

	// Phase 1: Pre-validate before acquiring entity lock.
	if err := c.validatePropertyUpdates(updates, "update node in place"); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock → read-modify-write under serialization.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := c.store.GetNode(id)
	if err != nil {
		return nil, err
	}

	// Preserve existing PrevHash — no new chain link for in-place updates.
	prevHash := ""
	if ig := current.Integrity(); ig != nil {
		prevHash = ig.PrevHash
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	for key, val := range updates {
		if val == nil {
			if _, err := current.DeleteProperty(key); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		}
	}

	// Check final property count.
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
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
		return nil, fmt.Errorf("graph: compute node hash: %w", err)
	}
	current.SetIntegrity(nodeIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// ReplaceNode instead of ReplaceNodeWithHistory — no history entry written.
	if err := c.store.ReplaceNode(current); err != nil {
		return nil, err
	}

	c.opNodeUpdates.Add(1)
	return current, nil
}
