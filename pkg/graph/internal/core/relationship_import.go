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
// Relationship — Import (caller-specified ID)
// =============================================================================

// Import creates a relationship with a caller-specified snowflake ID.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) Import(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// importRelWithIDInternal is the lock-free implementation of RelOps.Import.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) importRelWithIDInternal(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, ErrZeroID
	}

	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	if err := c.validateName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()

	if startID == endID && !c.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	// R4-F14 / R5-F6: defer registry-token allocation until after the
	// cheap validation gates (self-loop, ID==0, nil endpoints), the
	// collision probe, AND the live-endpoint fetches. Every operational
	// failure path that can reject the import must run before we touch
	// the rel-type registry, so a rejected import never leaves a
	// permanent type registration behind.

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// Check for collision (R4-F14, R4-F15). Probe BEFORE allocating
	// the rel-type token so a duplicate import never pollutes the
	// registry. Probe must surface non-not-found errors instead of
	// silently treating them as absence — operational store failures
	// must not be hidden by the import path.
	if _, err := c.store.GetRelationship(id); err == nil {
		return nil, storepkg.ErrRelExists
	} else if !errors.Is(err, storepkg.ErrRelNotFound) {
		return nil, fmt.Errorf("graph: rel-id collision probe: %w", err)
	}

	// Fetch live endpoints under the endpoint locks (R4-F5). The
	// caller-supplied `startNode`/`endNode` pointers are advisory —
	// only their IDs are load-bearing for routing. Hashes and
	// constraint checks must use the current persisted state.
	liveStart, err := c.store.GetNode(startID)
	if err != nil {
		return nil, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
	}
	liveEnd, err := c.store.GetNode(endID)
	if err != nil {
		return nil, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
	}

	// R5-F6: only allocate the rel-type token now that every cheap and
	// operational rejection gate has been cleared.
	typeToken, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := integrity.ComputeRelHash(r, typeName)
	ig := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	if startIg := liveStart.Integrity(); startIg != nil {
		ig.FromNodeHash = startIg.Hash
	}
	if endIg := liveEnd.Integrity(); endIg != nil {
		ig.ToNodeHash = endIg.Hash
	}
	r.SetIntegrity(ig)

	txNow := c.now()
	tm := r.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		r.SetTemporal(tm)
	}
	tm.TxFrom = txNow
	if validFrom != 0 {
		tm.ValidFrom = validFrom
	}
	if validTo != 0 {
		tm.ValidTo = validTo
	}
	if createdAt != 0 {
		tm.CreatedAt = createdAt
	}

	if err := c.checkTemporalConstraints(r, liveStart, liveEnd); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := c.store.PutRelationship(r); err != nil {
		return nil, err
	}

	c.opRelAdds.Add(1)
	return r, nil
}
