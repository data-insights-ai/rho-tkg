package core

import (
	"context"
	"fmt"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Relationship — Add (Create / IfAbsent / ByID)
// =============================================================================

// AddWithContext creates a new directed relationship between two nodes.
// Acquires c.mu.RLock for transaction isolation — blocked while a tx holds c.mu.Lock.
func (r *RelOps) AddWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.addRelationshipInternal(ctx, typeName, startNode, endNode, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// addRelationshipInternal is the lock-free implementation of RelOps.AddWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipInternal(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields (tkg_valid_from, tkg_valid_to, tkg_created_at).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	// Validation limits.
	if err := c.validateName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()

	if startID == endID && !c.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// Fetch live endpoints from the store under the endpoint locks so
	// hash refresh and temporal-constraint checks see the current
	// state, not whatever the caller happened to hold (R4-F5). Stale
	// caller pointers can otherwise record FromNodeHash/ToNodeHash
	// values that were never true at write time, and bypass
	// ConstraintRelWithinEndpoints by checking against an
	// out-of-date validity window.
	liveStart, err := c.store.GetNode(startID)
	if err != nil {
		return nil, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
	}
	liveEnd, err := c.store.GetNode(endID)
	if err != nil {
		return nil, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
	}

	// R5-F6: defer rel-type token allocation past every endpoint-fetch
	// failure path. A missing endpoint, store error, or context
	// cancellation between the caller fetch and our endpoint-lock
	// window must NOT leave a permanent rel-type registration. R4-F14
	// already deferred past the cheap self-loop/validation gates;
	// R5-F6 finishes the job for the operational failure paths.
	typeToken, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	id := c.Rels.NextID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := integrity.ComputeRelHash(r, typeName)

	// Capture endpoint hashes at creation time for cross-validation.
	// FromNodeHash/ToNodeHash are NOT part of ComputeRelHash to avoid cascading
	// hash invalidation whenever endpoint nodes are updated.
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

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := c.now()
		rtm := r.Temporal()
		if rtm == nil {
			rtm = &types.TemporalMetadata{}
			r.SetTemporal(rtm)
		}
		rtm.TxFrom = txNow
		if validFrom != 0 {
			rtm.ValidFrom = validFrom
		}
		if validTo != 0 {
			rtm.ValidTo = validTo
		}
		if createdAt != 0 {
			rtm.CreatedAt = createdAt
		}
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

// AddByIDWithContext creates a relationship using endpoint snowflake IDs.
// This is the high-throughput path when the caller already knows both
// endpoint IDs and no graph-level constraints are configured.
//
// Behaviour vs RelOps.AddWithContext:
//   - When no endpoint-dependent constraints are configured, the live
//     endpoints are NOT fetched: FromNodeHash/ToNodeHash stay empty
//     and ConstraintRelWithinEndpoints is trivially satisfied (nothing
//     to check). This is the fast path the API name advertises.
//   - When ConstraintRelWithinEndpoints (or any other graph-level
//     constraint) is set on the graph, AddByID transparently fetches
//     the live endpoints under the endpoint lock and enforces the
//     constraint — silent bypass via this entry point is not possible.
func (r *RelOps) AddByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	var (
		rel *types.Relationship
		err error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, err = c.addRelationshipByIDInternal(ctx, typeName, startID, endID, props)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, err
}

// addRelationshipByIDInternal is the lock-free implementation of RelOps.AddByIDWithContext.
// Unlike addRelationshipInternal, it does NOT require pre-fetched endpoint nodes.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipByIDInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields (tkg_valid_from, tkg_valid_to, tkg_created_at).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	// Validation limits.
	if err := c.validateName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	if startID == endID && !c.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// R5-F7: when graph-level constraints are configured, fetch live
	// endpoints under the lock so the same enforcement that Rels.Add
	// performs runs here too. The "fast path" — no endpoint fetch,
	// no hash capture, no constraint check — applies only when there
	// is no constraint to enforce. Silent bypass via the ByID entry
	// point is no longer possible.
	var liveStart, liveEnd *types.Node
	if c.constraints.Len() > 0 {
		liveStart, err = c.store.GetNode(startID)
		if err != nil {
			return nil, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
		}
		if startID == endID {
			liveEnd = liveStart
		} else {
			liveEnd, err = c.store.GetNode(endID)
			if err != nil {
				return nil, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
			}
		}
	}

	// R5-F6: allocate rel-type token AFTER endpoint locks are held
	// (and, when constraints are active, AFTER the live-endpoint
	// fetches succeed) so a failed fetch does not leave a permanent
	// rel-type registration. The remaining post-allocation failure is
	// the store PutRelationship at the bottom of this function —
	// unavoidable because the relationship object literally needs the
	// token at construction time.
	typeToken, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	id := c.Rels.NextID()
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
	// Capture endpoint hashes when we have the live endpoints — this
	// matches Rels.Add's behaviour. When constraints are not configured
	// the hashes stay empty (the documented fast-path trade-off).
	if liveStart != nil {
		if startIg := liveStart.Integrity(); startIg != nil {
			ig.FromNodeHash = startIg.Hash
		}
	}
	if liveEnd != nil {
		if endIg := liveEnd.Integrity(); endIg != nil {
			ig.ToNodeHash = endIg.Hash
		}
	}
	r.SetIntegrity(ig)

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := c.now()
		rtm := r.Temporal()
		if rtm == nil {
			rtm = &types.TemporalMetadata{}
			r.SetTemporal(rtm)
		}
		rtm.TxFrom = txNow
		if validFrom != 0 {
			rtm.ValidFrom = validFrom
		}
		if validTo != 0 {
			rtm.ValidTo = validTo
		}
		if createdAt != 0 {
			rtm.CreatedAt = createdAt
		}
	}

	// R5-F7: enforce graph-level constraints when configured. Without
	// the live endpoints (no constraint set) there's nothing to check.
	if liveStart != nil && liveEnd != nil {
		if err := c.checkTemporalConstraints(r, liveStart, liveEnd); err != nil {
			return nil, err
		}
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

// AddByIDIfAbsentWithContext atomically creates a relationship using
// endpoint snowflake IDs only if no relationship of the same type between the same
// endpoints already exists. Returns (rel, created, err) where created is true if a
// new relationship was created, false if an existing one was returned.
//
// The existence check and creation are serialized under entity locks, preventing
// the TOCTOU race inherent in separate check-then-create calls.
//
// Constraint behaviour: same as AddByIDWithContext — when graph-level
// constraints are configured, live endpoints are fetched and the
// constraint is enforced; the fast path (no fetch, empty endpoint
// hashes) applies only when no constraint is set.
func (r *RelOps) AddByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, false, err
	}
	var (
		rel     *types.Relationship
		created bool
		err     error
	)
	ep, closeErr := c.runUnderRLock(func() {
		rel, created, err = c.addRelationshipByIDIfAbsentInternal(ctx, typeName, startID, endID, props)
	})
	if closeErr != nil {
		return nil, false, closeErr
	}
	if err == nil && created {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(rel.ID()), Timestamp: c.now(), Priority: eventspkg.PriorityHigh})
	}
	return rel, created, err
}

// addRelationshipByIDIfAbsentInternal is the lock-free implementation of
// AddRelationshipByIDIfAbsentWithContext. Under entity locks it checks for an
// existing relationship before creating, making the operation atomic.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
func (c *Core) addRelationshipByIDIfAbsentInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, false, err
	}

	// Extract reserved temporal fields.
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, false, err
	}

	// Validation limits.
	if err := c.validateName(typeName); err != nil {
		return nil, false, err
	}
	if err := c.validateProperties(props); err != nil {
		return nil, false, err
	}

	// Bulk-build properties first — fail fast before entity locking.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship properties: %w", err)
	}

	if startID == endID && !c.validation.AllowSelfLoops {
		return nil, false, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Lock both endpoints — serializes with concurrent Add/Delete on same endpoints.
	c.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer c.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// R5-F6: probe the registry for an existing token via Lookup
	// (zero side effect) before committing to GetOrCreate. If the type
	// was never registered, no relationship of this type can exist for
	// any endpoint pair, so the absence check is vacuous and we can
	// skip OutgoingRelationships entirely. This avoids polluting the
	// rel-type registry when a "duplicate-relationship" check that
	// would have hit the store-failure path returns early.
	if existingTok, ok := c.relTypes.Lookup(typeName); ok {
		existing, err := c.store.OutgoingRelationships(startID, existingTok)
		if err != nil {
			return nil, false, fmt.Errorf("graph: check existing relationships: %w", err)
		}
		for _, r := range existing {
			if r.EndNodeID() == endID {
				return r, false, nil
			}
		}
	}

	// R5-F7: when graph-level constraints are configured, fetch live
	// endpoints so the constraint is enforced uniformly with the rel
	// creation paths that DO fetch endpoints.
	var liveStart, liveEnd *types.Node
	if c.constraints.Len() > 0 {
		liveStart, err = c.store.GetNode(startID)
		if err != nil {
			return nil, false, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
		}
		if startID == endID {
			liveEnd = liveStart
		} else {
			liveEnd, err = c.store.GetNode(endID)
			if err != nil {
				return nil, false, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
			}
		}
	}

	// Not found — allocate the token now and create.
	typeToken, err := c.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship type: %w", err)
	}
	id := c.Rels.NextID()
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
	if liveStart != nil {
		if startIg := liveStart.Integrity(); startIg != nil {
			ig.FromNodeHash = startIg.Hash
		}
	}
	if liveEnd != nil {
		if endIg := liveEnd.Integrity(); endIg != nil {
			ig.ToNodeHash = endIg.Hash
		}
	}
	r.SetIntegrity(ig)

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := c.now()
		rtm := r.Temporal()
		if rtm == nil {
			rtm = &types.TemporalMetadata{}
			r.SetTemporal(rtm)
		}
		rtm.TxFrom = txNow
		if validFrom != 0 {
			rtm.ValidFrom = validFrom
		}
		if validTo != 0 {
			rtm.ValidTo = validTo
		}
		if createdAt != 0 {
			rtm.CreatedAt = createdAt
		}
	}

	// R5-F7: enforce graph-level constraints when live endpoints are
	// available.
	if liveStart != nil && liveEnd != nil {
		if err := c.checkTemporalConstraints(r, liveStart, liveEnd); err != nil {
			return nil, false, err
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	if err := c.store.PutRelationship(r); err != nil {
		return nil, false, err
	}

	c.opRelAdds.Add(1)
	return r, true, nil
}
