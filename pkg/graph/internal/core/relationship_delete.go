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
	return r.c.endRelationship(ctx, id, 0, false, false)
}

// DeleteWithTx deletes a relationship like Delete but stamps the tombstone
// with the caller's transaction instant: TxTo = DeletedAt = txTo, ValidTo
// clamped to txTo when the row is open. Gates, in order: txTo must be a
// positive instant not in the future (ErrInvalidTxFrom), then
// Config.AllowTxBackfill (ErrTxBackfillDisabled); under the entity lock txTo
// must follow every TxFrom/TxTo recorded for the relationship and the current
// version's start, and no recorded close may lie at or after txTo (ErrTxOrder,
// wrapping ErrInvalidTxFrom). The commit clock is not moved by txTo.
func (r *RelOps) DeleteWithTx(ctx context.Context, id types.RelID, txTo types.Instant) error {
	return r.c.endRelationship(ctx, id, txTo, true, false)
}

// Retract ends belief in the relationship (backlog 43): its tombstone is
// Delete's (TxTo = DeletedAt = the delete instant T) marked Retracted, and a
// read pinned at or after T finds the relationship at NO valid time — not
// only after T, as after a Delete — while a read pinned before T answers
// exactly as before. Current-state reads lose it like after a Delete.
// Errors: ErrRelNotFound for an unknown ID; an ID already deleted or
// retracted is refused with an error matching both ErrEntityDeleted and
// ErrRelNotFound.
func (r *RelOps) Retract(ctx context.Context, id types.RelID) error {
	return r.c.endRelationship(ctx, id, 0, false, true)
}

// RetractWithTx is Retract at the caller's transaction instant txTo, with
// DeleteWithTx's gates and refusals (ErrInvalidTxFrom, ErrTxBackfillDisabled,
// ErrTxOrder against the whole chain, a recorded close at or after txTo); the
// commit clock is not moved by txTo.
func (r *RelOps) RetractWithTx(ctx context.Context, id types.RelID, txTo types.Instant) error {
	return r.c.endRelationship(ctx, id, txTo, true, true)
}

// endRelationship is the body of the four relationship end doors (Delete,
// DeleteWithTx, Retract, RetractWithTx): withTx gates txTo as a caller
// instant and reports the past-dated write after the store write.
func (c *Core) endRelationship(ctx context.Context, id types.RelID, txTo types.Instant, withTx, retract bool) error {
	if err := c.checkWritable(); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}
	spec := tombstoneSpec{retract: retract}
	if withTx {
		at, err := c.resolveCallerTxInstant(txTo)
		if err != nil {
			return err
		}
		spec.at = at
		defer c.notePastDatedWrite(at) // after the store write (as-of cache)
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.deleteRelationshipInternal(ctx, id, spec)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelDelete, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityCritical})
	}
	return err
}

// deleteRelationshipInternal is the lock-free implementation of the
// relationship end doors (endRelationship and the GraphTx, Batch and ingest
// twins). spec.at == 0 stamps the tombstone at deleteInstantForRelationship
// (the plain doors: clock floor, moved past a colliding close); spec.at != 0
// is a caller instant already gated by resolveCallerTxInstant, checked here
// under the entity lock (checkTxOrder, checkCallerDeleteCloses) and stamped
// verbatim, never moved. spec.retract marks the tombstone as a retraction.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) deleteRelationshipInternal(ctx context.Context, id types.RelID, spec tombstoneSpec) error {
	at := spec.at
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
	if at != 0 {
		if err := c.checkRelCallerDelete(id, current, at); err != nil {
			return err
		}
	}
	if err := c.checkpointDirtyRegistriesBeforeMutation("delete relationship"); err != nil {
		return err
	}
	if err := checkCtx(ctx); err != nil {
		return err
	}

	now := at
	if now == 0 {
		now = c.deleteInstantForRelationship(current)
	}
	tmR := current.Temporal()
	if tmR == nil {
		tmR = &types.TemporalMetadata{}
		current.SetTemporal(tmR)
	}
	stampDeleteTombstone(tmR, now, spec.retract)

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
