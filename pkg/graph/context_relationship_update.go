package graph

import (
	"context"
	"fmt"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// UpdateRelationshipWithContext applies property updates to an existing relationship with context support.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateRelationshipWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.updateRelationshipInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return r, err
}

// updateRelationshipInternal is the lock-free implementation of UpdateRelationshipWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateRelationshipInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetRelationshipWithContext(ctx, id)
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, authorizedBy, authLevel, updates, err := extractProvenance(updates)
	if err != nil {
		return nil, err
	}

	// Phase 1: Pre-validate before acquiring entity lock (fail fast).
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update relationship: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
			if err := g.validatePropertyEntry(key, val); err != nil {
				return nil, err
			}
		} else {
			// Even for deletions, check key length.
			if len(key) > g.validation.MaxPropertyKeyLength {
				return nil, fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), g.validation.MaxPropertyKeyLength)
			}
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock on rel ID only — property changes don't affect adjacency.
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetRelationship(id)
	if err != nil {
		return nil, err
	}

	// Capture pre-mutation state for version history (deep copy before any mutations).
	prevVersion := current.Version()
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
	if current.PropertyCount() > g.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), g.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(current.Version() + 1)

	now := types.Instant(time.Now().UnixMilli())
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

	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)

	// Refresh endpoint hashes to capture the current state of the endpoint nodes.
	// These are NOT fed into ComputeRelHash to avoid cascading hash invalidation.
	relIG := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           prevHash,
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	if sn, sErr := g.store.GetNode(current.StartNodeID()); sErr == nil {
		if sIg := sn.Integrity(); sIg != nil {
			relIG.FromNodeHash = sIg.Hash
		}
	}
	if en, eErr := g.store.GetNode(current.EndNodeID()); eErr == nil {
		if eIg := en.Integrity(); eIg != nil {
			relIG.ToNodeHash = eIg.Hash
		}
	}
	current.SetIntegrity(relIG)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := g.store.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		return nil, err
	}

	g.opRelUpdates.Add(1)
	return current, nil
}

// UpdateRelInPlace applies property updates to a relationship without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Returns ErrRelNotFound if the relationship does not exist. Empty updates map is a no-op.
func (g *Graph) UpdateRelInPlace(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	return g.UpdateRelInPlaceWithContext(context.Background(), id, updates)
}

// UpdateRelInPlaceWithContext applies property updates to a relationship without history.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateRelInPlaceWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.updateRelInPlaceInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return r, err
}

// updateRelInPlaceInternal is the lock-free implementation of UpdateRelInPlaceWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateRelInPlaceInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetRelationshipWithContext(ctx, id)
	}

	// Phase 1: Pre-validate before acquiring entity lock.
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update relationship in place: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
			if err := g.validatePropertyEntry(key, val); err != nil {
				return nil, err
			}
		} else {
			if len(key) > g.validation.MaxPropertyKeyLength {
				return nil, fmt.Errorf("%w: %q (%d > %d)", ErrKeyTooLong, key, len(key), g.validation.MaxPropertyKeyLength)
			}
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Phase 2: Entity lock on rel ID only.
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetRelationship(id)
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
	if current.PropertyCount() > g.validation.MaxPropertiesPerEntity {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), g.validation.MaxPropertiesPerEntity)
	}

	// NO version bump — in-place update preserves version.

	now := types.Instant(time.Now().UnixMilli())
	tm := current.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		current.SetTemporal(tm)
	}
	tm.UpdatedAt = now

	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)
	current.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: prevHash})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// ReplaceRelationship instead of ReplaceRelWithHistory — no history entry written.
	if err := g.store.ReplaceRelationship(current); err != nil {
		return nil, err
	}

	g.opRelUpdates.Add(1)
	return current, nil
}
