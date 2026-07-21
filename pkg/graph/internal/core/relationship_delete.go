package core

import (
	"context"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Relationship — Read / Delete
// =============================================================================

// Get retrieves a relationship by snowflake ID with context support.
//
// Hot-path inlined: see NodeOps.Get for the rationale (B4).
func (r *RelOps) Get(ctx context.Context, id types.RelID) (*types.Relationship, error) {
	c := r.c
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return nil, ErrGraphClosed
	}
	rel, err := c.getCurrentRelationship(id)
	c.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if ctxErr := checkCtx(ctx); ctxErr != nil {
		return nil, ctxErr
	}
	c.opRelReads.Add(1)
	return rel, nil
}

// Delete removes a relationship from the store.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) Delete(ctx context.Context, id types.RelID) error {
	c := r.c
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
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

// deleteRelationshipInternal is the lock-free implementation of RelOps.Delete.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) deleteRelationshipInternal(ctx context.Context, id types.RelID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}

	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())

	if err := checkCtx(ctx); err != nil {
		return err
	}

	// Read current state for tombstone.
	current, err := c.getCurrentRelationship(id)
	if err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("delete relationship"); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}

	now := c.deleteInstantForRelationship(current)
	tmR := current.Temporal()
	if tmR == nil {
		tmR = &types.TemporalMetadata{}
		current.SetTemporal(tmR)
	}
	tmR.DeletedAt = now
	tmR.ValidTo = now
	if tmR.TxFrom == 0 {
		tmR.TxFrom = now
	}
	tmR.TxTo = now

	// Single atomic call: PutRelVersion + DeleteRelationship. Routes through
	// the BACKLOG 11f scoped sibling when ctx carries a scoped change-log
	// token and the store supports it — mirrors putGeneratedNode's routing
	// pattern. FOUNDATION ONLY: nothing constructs a token-carrying ctx yet
	// (see scope_token.go), so this branch is currently always dead in
	// production.
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		if scoped, ok := c.store.(storepkg.ScopedDeleteCapability); ok {
			if err := scoped.DeleteRelWithHistoryScoped(id, current.Version(), current, token); err != nil {
				return err
			}
			c.opRelDeletes.Add(1)
			return nil
		}
	}
	if err := c.store.DeleteRelWithHistory(id, current.Version(), current); err != nil {
		return err
	}
	c.opRelDeletes.Add(1)
	return nil
}
