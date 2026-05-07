package graph

import (
	"context"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// GetRelationshipWithContext retrieves a relationship by snowflake ID with context support.
func (g *Graph) GetRelationshipWithContext(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	r, err := g.store.GetRelationship(id)
	if err == nil {
		g.opRelReads.Add(1)
	}
	return r, err
}

// DeleteRelationshipWithContext removes a relationship from the store.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) DeleteRelationshipWithContext(ctx context.Context, id types.RelID) error {
	g.mu.RLock()
	err := g.deleteRelationshipInternal(ctx, id)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelDelete, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityCritical})
	}
	return err
}

// deleteRelationshipInternal is the lock-free implementation of DeleteRelationshipWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) deleteRelationshipInternal(ctx context.Context, id types.RelID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

	// Read current state for tombstone.
	current, err := g.store.GetRelationship(id)
	if err != nil {
		return err
	}

	now := types.Instant(time.Now().UnixMilli())
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
	if err := g.store.DeleteRelWithHistory(id, current.Version(), tombR); err != nil {
		return err
	}
	g.opRelDeletes.Add(1)
	return nil
}
