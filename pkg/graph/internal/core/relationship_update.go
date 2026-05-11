package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Relationship — Update
// =============================================================================

// UpdateWithContext applies property updates to an existing relationship with context support.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) UpdateWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.updateRelationshipInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return rel, err
}

// updateRelationshipInternal is the lock-free implementation of RelOps.UpdateWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateRelationshipInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
		current, err := c.store.GetRelationship(id)
		if err == nil {
			c.opRelReads.Add(1)
		}
		return current, err
	}

	prov, updates, err := c.prepareUpdateProperties(updates, "update relationship")
	if err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Acquire rel + both endpoint locks together. We need a peek
	// to discover the (immutable) endpoint IDs; we then re-acquire all three
	// locks together so the endpoint hash refresh below cannot race a
	// concurrent UpdateNode on either endpoint (R4-F7). Rel endpoints never
	// change after creation, so the peek-without-lock is benign — even if
	// the rel is replaced between peek and LockMany, the new version still
	// has the same start/end IDs. We re-fetch `current` under the proper
	// locks for a stable mutation snapshot.
	peek, err := c.store.GetRelationship(id)
	if err != nil {
		return nil, err
	}
	startID := peek.StartNodeID()
	endID := peek.EndNodeID()

	c.entityLocks.LockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := c.store.GetRelationship(id)
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
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
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

	relTypeName := c.relTypeUnlocked(current)
	hash, err := integrity.ComputeRelHashChecked(current, relTypeName)
	if err != nil {
		return nil, fmt.Errorf("graph: compute relationship hash: %w", err)
	}

	// Refresh endpoint hashes to capture the current state of the endpoint nodes.
	// These are NOT fed into ComputeRelHash to avoid cascading hash invalidation.
	relIG := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           prevHash,
		AuthorID:           prov.authorID,
		Signature:          prov.signature,
		AuthorizedBy:       prov.authorizedBy,
		AuthorizationLevel: prov.authLevel,
	}
	if err := c.refreshRelationshipEndpointHashes(current, relIG); err != nil {
		return nil, err
	}
	current.SetIntegrity(relIG)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := c.store.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		return nil, err
	}

	c.opRelUpdates.Add(1)
	return current, nil
}

func (c *Core) refreshRelationshipEndpointHashes(rel *types.Relationship, relIG *types.RelIntegrity) error {
	startID := rel.StartNodeID()
	endID := rel.EndNodeID()
	if c.endpointHash != nil {
		fromHash, toHash, err := c.endpointHash.EndpointIntegrityHashes(startID, endID)
		if err == nil {
			relIG.FromNodeHash = fromHash
			relIG.ToNodeHash = toHash
			return nil
		}
		if !errors.Is(err, storepkg.ErrNodeNotFound) {
			return fmt.Errorf("graph: refresh endpoint hashes: %w", err)
		}
	}

	fromHash, found, err := c.refreshNodeHash(startID)
	if err != nil {
		return fmt.Errorf("graph: refresh start-node hash: %w", err)
	}
	if found {
		relIG.FromNodeHash = fromHash
	}
	if startID == endID {
		relIG.ToNodeHash = relIG.FromNodeHash
		return nil
	}
	toHash, found, err := c.refreshNodeHash(endID)
	if err != nil {
		return fmt.Errorf("graph: refresh end-node hash: %w", err)
	}
	if found {
		relIG.ToNodeHash = toHash
	}
	return nil
}

func (c *Core) refreshNodeHash(id types.NodeID) (string, bool, error) {
	if c.nodeHash != nil {
		hash, err := c.nodeHash.NodeIntegrityHash(id)
		if err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				return "", false, nil
			}
			return "", false, err
		}
		return hash, true, nil
	}
	node, err := c.store.GetNode(id)
	if err != nil {
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return nodeIntegrityHash(node), true, nil
}

// UpdateInPlace applies property updates to a relationship without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Returns storepkg.ErrRelNotFound if the relationship does not exist. Empty updates map is a no-op.
func (r *RelOps) UpdateInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	return c.Rels.UpdateInPlaceWithContext(context.Background(), id, updates)
}

// UpdateInPlaceWithContext applies property updates to a relationship without history.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) UpdateInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.updateRelInPlaceInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return rel, err
}

// updateRelInPlaceInternal is the lock-free implementation of RelOps.UpdateInPlaceWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateRelInPlaceInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
		current, err := c.store.GetRelationship(id)
		if err == nil {
			c.opRelReads.Add(1)
		}
		return current, err
	}

	// Phase 1: Pre-validate before acquiring entity lock.
	if err := c.validatePropertyUpdates(updates, "update relationship in place"); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock on rel ID only.
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := c.store.GetRelationship(id)
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
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
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

	relTypeName := c.relTypeUnlocked(current)
	hash, err := integrity.ComputeRelHashChecked(current, relTypeName)
	if err != nil {
		return nil, fmt.Errorf("graph: compute relationship hash: %w", err)
	}
	current.SetIntegrity(relIntegrityWithHash(current.Integrity(), hash, prevHash))

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// ReplaceRelationship instead of ReplaceRelWithHistory — no history entry written.
	if err := c.store.ReplaceRelationship(current); err != nil {
		return nil, err
	}

	c.opRelUpdates.Add(1)
	return current, nil
}
