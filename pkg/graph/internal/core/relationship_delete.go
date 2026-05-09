package core

import (
	"context"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Relationship — Read / Delete
// =============================================================================

// GetWithContext retrieves a relationship by snowflake ID with context support.
func (r *RelOps) GetWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	rel, err := c.store.GetRelationship(id)
	if err == nil {
		c.opRelReads.Add(1)
	}
	return rel, err
}

// DeleteWithContext removes a relationship from the store.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) DeleteWithContext(ctx context.Context, id types.RelID) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.deleteRelationshipInternal(ctx, id)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelDelete, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityCritical})
	}
	return err
}

// deleteRelationshipInternal is the lock-free implementation of RelOps.DeleteWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) deleteRelationshipInternal(ctx context.Context, id types.RelID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	// Read current state for tombstone.
	current, err := c.store.GetRelationship(id)
	if err != nil {
		return err
	}

	now := c.now()
	tombR := current.DeepCopy()
	tmR := tombR.Temporal()
	if tmR == nil {
		tmR = &types.TemporalMetadata{}
		tombR.SetTemporal(tmR)
	}
	tmR.DeletedAt = now
	tmR.ValidTo = now
	// Transaction time: this tombstone version was committed at now.
	tmR.TxFrom = now
	tmR.TxTo = now

	// Single atomic call: PutRelVersion + DeleteRelationship.
	if err := c.store.DeleteRelWithHistory(id, current.Version(), tombR); err != nil {
		return err
	}
	c.opRelDeletes.Add(1)
	return nil
}
