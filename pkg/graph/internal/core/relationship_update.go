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

// Update applies property updates to an existing relationship with context support.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) Update(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	var (
		rel     *types.Relationship
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, mutated, err = c.updateRelationshipInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return rel, err
}

// updateRelationshipInternal is the lock-free implementation of RelOps.Update.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateRelationshipInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, false, err
	}

	if len(updates) == 0 {
		current, err := c.getCurrentRelationship(id)
		if err == nil {
			c.opRelReads.Add(1)
		}
		return current, false, err
	}

	prov, updates, err := c.prepareUpdateProperties(updates, "update relationship")
	if err != nil {
		return nil, false, err
	}
	return c.updateRelationshipPreparedInternal(ctx, id, prov, updates)
}

// updateRelationshipPreparedInternal applies a non-empty caller update after
// provenance extraction and property validation have already run. The prepared
// properties may be empty for metadata-only updates.
func (c *Core) updateRelationshipPreparedInternal(ctx context.Context, id types.RelID, prov updateProvenance, updates map[string]any) (*types.Relationship, bool, error) {
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, false, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Phase 2: acquire rel + endpoint locks together. The first read only
	// discovers which endpoint locks to take. Caller-supplied relationship IDs
	// can be deleted and re-imported with different endpoints, so re-check the
	// endpoint identity after locking and retry if the peek was stale.
	current, startID, endID, err := c.lockRelationshipCurrentEndpoints(ctx, id)
	if err != nil {
		return nil, false, err
	}
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if !relPreparedUpdateMutates(current, prov, updates) {
		c.opRelReads.Add(1)
		return current, false, nil
	}
	if err := rejectClosedRelMutation(current); err != nil {
		return nil, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("update relationship"); err != nil {
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
				return nil, false, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, false, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		}
	}

	// Check final property count after mutations (under entity lock, before persist).
	if current.PropertyCount() > c.validation.MaxPropertiesPerEntity {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrTooManyProperties, current.PropertyCount(), c.validation.MaxPropertiesPerEntity)
	}

	current.SetVersion(nextVersion)

	now := c.relVersionUpdateInstant(current)
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
		return nil, false, fmt.Errorf("graph: compute relationship hash: %w", err)
	}

	// Refresh endpoint hashes to capture the current state of the endpoint nodes.
	// These are NOT fed into ComputeRelHash to avoid cascading hash invalidation.
	// Preserve existing integrity metadata (B3 — see node_update.go for
	// rationale). Only overwrite the four caller-supplied provenance fields
	// that are explicitly present in `updates`; FromNodeHash / ToNodeHash
	// are recomputed below by refreshRelationshipEndpointHashes.
	relIG := relIntegrityWithHash(current.Integrity(), hash, prevHash)
	if prov.hasAuthorID {
		relIG.AuthorID = prov.authorID
	}
	if prov.hasSignature {
		relIG.Signature = prov.signature
	}
	if prov.hasAuthorizedBy {
		relIG.AuthorizedBy = prov.authorizedBy
	}
	if prov.hasAuthLevel {
		relIG.AuthorizationLevel = prov.authLevel
	}
	if err := c.refreshRelationshipEndpointHashes(current, relIG); err != nil {
		return nil, false, err
	}
	current.SetIntegrity(relIG)

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := c.store.ReplaceRelWithHistory(current, prevVersion, prevState); err != nil {
		return nil, false, err
	}

	c.opRelUpdates.Add(1)
	return current, true, nil
}

// lockRelationshipCurrentEndpoints locks the relationship ID plus the endpoint
// IDs from a stable current row. Caller must unlock with the returned start/end
// IDs via c.entityLocks.UnlockThree.
//
// Bounded retry: peek-then-lock can race with a concurrent delete+reimport of
// the same RelID with different endpoints. We retry up to maxRetries times
// before giving up — parity with deleteNodeInternal's adjacency retry. An
// unbounded loop here lets a hostile concurrent workload deadlock the caller.
func (c *Core) lockRelationshipCurrentEndpoints(ctx context.Context, id types.RelID) (*types.Relationship, types.NodeID, types.NodeID, error) {
	const maxRetries = 10
	for range maxRetries {
		if err := checkCtx(ctx); err != nil {
			return nil, 0, 0, err
		}
		peek, err := c.getCurrentRelationship(id)
		if err != nil {
			return nil, 0, 0, err
		}
		startID := peek.StartNodeID()
		endID := peek.EndNodeID()

		c.entityLocks.LockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
		if err := checkCtx(ctx); err != nil {
			c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
			return nil, 0, 0, err
		}
		lockedCurrent, err := c.getCurrentRelationship(id)
		if err != nil {
			c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
			return nil, 0, 0, err
		}
		if err := checkCtx(ctx); err != nil {
			c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
			return nil, 0, 0, err
		}
		if lockedCurrent.StartNodeID() != startID || lockedCurrent.EndNodeID() != endID {
			c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())
			continue
		}
		return lockedCurrent, startID, endID, nil
	}
	return nil, 0, 0, fmt.Errorf("graph: relationship %d: endpoints changed after %d retries", id, maxRetries)
}

func (c *Core) refreshRelationshipEndpointHashes(rel *types.Relationship, relIG *types.RelIntegrity) error {
	startID := rel.StartNodeID()
	endID := rel.EndNodeID()
	if c.endpointHash != nil {
		fromHash, toHash, err := c.endpointHash.EndpointIntegrityHashes(startID, endID)
		if err == nil {
			if startID == endID {
				toHash = fromHash
			}
			relIG.FromNodeHash = fromHash
			relIG.ToNodeHash = toHash
			return nil
		}
		if !errors.Is(err, storepkg.ErrNodeNotFound) {
			return fmt.Errorf("graph: refresh endpoint hashes: %w", err)
		}
	}

	fromHash, err := c.refreshNodeHash(startID)
	if err != nil {
		return fmt.Errorf("graph: refresh start-node hash: %w", err)
	}
	relIG.FromNodeHash = fromHash
	if startID == endID {
		relIG.ToNodeHash = relIG.FromNodeHash
		return nil
	}
	toHash, err := c.refreshNodeHash(endID)
	if err != nil {
		return fmt.Errorf("graph: refresh end-node hash: %w", err)
	}
	relIG.ToNodeHash = toHash
	return nil
}

func (c *Core) refreshNodeHash(id types.NodeID) (string, error) {
	if c.nodeHash != nil {
		hash, err := c.nodeHash.NodeIntegrityHash(id)
		if err != nil {
			return "", err
		}
		return hash, nil
	}
	node, err := c.getCurrentNode(id)
	if err != nil {
		return "", err
	}
	return nodeIntegrityHash(node), nil
}

// UpdateInPlace applies property updates to a relationship without creating a
// version history entry. Version number is NOT incremented. PrevHash in the
// integrity chain is preserved. Returns storepkg.ErrRelNotFound if the
// relationship does not exist. Empty updates map is a no-op. Acquires
// c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) UpdateInPlace(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	var (
		rel     *types.Relationship
		mutated bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, mutated, err = c.updateRelInPlaceInternal(ctx, id, updates)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil && mutated {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityNormal})
	}
	return rel, err
}

// updateRelInPlaceInternal is the lock-free implementation of RelOps.UpdateInPlace.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) updateRelInPlaceInternal(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, false, err
	}

	if len(updates) == 0 {
		current, err := c.getCurrentRelationship(id)
		if err == nil {
			c.opRelReads.Add(1)
		}
		return current, false, err
	}

	// Phase 1: Pre-validate before acquiring entity lock.
	if err := c.validatePropertyUpdates(updates, "update relationship in place"); err != nil {
		return nil, false, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Phase 2: lock the relationship plus its current endpoints. In-place
	// updates still refresh endpoint hashes, so they need the same stable
	// endpoint snapshot as versioned relationship updates.
	current, startID, endID, err := c.lockRelationshipCurrentEndpoints(ctx, id)
	if err != nil {
		return nil, false, err
	}
	defer c.entityLocks.UnlockThree(id.SnowflakeID(), startID.SnowflakeID(), endID.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}
	if !relPropertyUpdatesMutate(current, updates) {
		c.opRelReads.Add(1)
		return current, false, nil
	}

	if err := rejectClosedRelMutation(current); err != nil {
		return nil, false, err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("update relationship in place"); err != nil {
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
				return nil, false, fmt.Errorf("graph: update relationship property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, false, fmt.Errorf("graph: update relationship property %q: %w", key, err)
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

	relTypeName := c.relTypeUnlocked(current)
	hash, err := integrity.ComputeRelHashChecked(current, relTypeName)
	if err != nil {
		return nil, false, fmt.Errorf("graph: compute relationship hash: %w", err)
	}
	relIG := relIntegrityWithHash(current.Integrity(), hash, prevHash)
	if err := c.refreshRelationshipEndpointHashes(current, relIG); err != nil {
		return nil, false, err
	}
	current.SetIntegrity(relIG)

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// ReplaceRelationship instead of ReplaceRelWithHistory — no history entry written.
	if err := c.store.ReplaceRelationship(current); err != nil {
		return nil, false, err
	}

	c.opRelUpdates.Add(1)
	return current, true, nil
}

func relPreparedUpdateMutates(current *types.Relationship, prov updateProvenance, updates map[string]any) bool {
	if prov.present {
		return true
	}
	return relPropertyUpdatesMutate(current, updates)
}

func relPropertyUpdatesMutate(current *types.Relationship, updates map[string]any) bool {
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
