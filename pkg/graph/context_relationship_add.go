package graph

import (
	"context"
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// AddRelationshipWithContext creates a new directed relationship between two nodes.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.addRelationshipInternal(ctx, typeName, startNode, endNode, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return r, err
}

// addRelationshipInternal is the lock-free implementation of AddRelationshipWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addRelationshipInternal(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
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
	if err := g.validateName(typeName); err != nil {
		return nil, err
	}
	if err := g.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	typeToken, err := g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()

	if startID == endID && !g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	g.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer g.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)

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
	if startIg := startNode.Integrity(); startIg != nil {
		ig.FromNodeHash = startIg.Hash
	}
	if endIg := endNode.Integrity(); endIg != nil {
		ig.ToNodeHash = endIg.Hash
	}
	r.SetIntegrity(ig)

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := nowInstant()
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

	if err := g.checkTemporalConstraints(r, startNode, endNode); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	g.opRelAdds.Add(1)
	return r, nil
}

// AddRelationshipByIDWithContext creates a relationship using endpoint snowflake IDs
// without fetching the endpoint nodes. This is the high-throughput path when the
// caller already knows both endpoint IDs.
//
// Trade-offs vs AddRelationshipWithContext:
//   - FromNodeHash/ToNodeHash are not captured (empty in RelIntegrity)
//   - Temporal constraints against endpoint nodes are not checked
//
// Use AddRelationshipWithContext when endpoint integrity hashing or temporal
// constraint validation against endpoint nodes is required.
func (g *Graph) AddRelationshipByIDWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.addRelationshipByIDInternal(ctx, typeName, startID, endID, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return r, err
}

// addRelationshipByIDInternal is the lock-free implementation of AddRelationshipByIDWithContext.
// Unlike addRelationshipInternal, it does NOT require pre-fetched endpoint nodes.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addRelationshipByIDInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
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
	if err := g.validateName(typeName); err != nil {
		return nil, err
	}
	if err := g.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	typeToken, err := g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}

	if startID == endID && !g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	g.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer g.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)

	// Integrity metadata — endpoint hashes are left empty because we do not
	// have the endpoint nodes. This is the documented trade-off of the ByID path.
	ig := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	r.SetIntegrity(ig)

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := nowInstant()
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

	// NOTE: Temporal constraint checks against endpoint nodes are skipped.
	// The ByID path does not fetch endpoints — use AddRelationshipWithContext
	// when endpoint-relative temporal constraints are required.

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	g.opRelAdds.Add(1)
	return r, nil
}

// AddRelationshipByIDIfAbsentWithContext atomically creates a relationship using
// endpoint snowflake IDs only if no relationship of the same type between the same
// endpoints already exists. Returns (rel, created, err) where created is true if a
// new relationship was created, false if an existing one was returned.
//
// The existence check and creation are serialized under entity locks, preventing
// the TOCTOU race inherent in separate check-then-create calls.
//
// Trade-offs vs AddRelationshipByIDWithContext: same (no endpoint hashing, no
// temporal constraint checks against endpoint nodes).
func (g *Graph) AddRelationshipByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	g.mu.RLock()
	r, created, err := g.addRelationshipByIDIfAbsentInternal(ctx, typeName, startID, endID, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && created {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return r, created, err
}

// addRelationshipByIDIfAbsentInternal is the lock-free implementation of
// AddRelationshipByIDIfAbsentWithContext. Under entity locks it checks for an
// existing relationship before creating, making the operation atomic.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addRelationshipByIDIfAbsentInternal(ctx context.Context, typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
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
	if err := g.validateName(typeName); err != nil {
		return nil, false, err
	}
	if err := g.validateProperties(props); err != nil {
		return nil, false, err
	}

	// Bulk-build properties first — fail fast before entity locking.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship properties: %w", err)
	}

	typeToken, err := g.relTypes.GetOrCreate(typeName)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship type: %w", err)
	}

	if startID == endID && !g.validation.AllowSelfLoops {
		return nil, false, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	// Lock both endpoints — serializes with concurrent Add/Delete on same endpoints.
	g.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer g.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// Check for existing relationship under entity locks (atomic with creation).
	existing, err := g.store.OutgoingRelationships(startID, typeToken)
	if err != nil {
		return nil, false, fmt.Errorf("graph: check existing relationships: %w", err)
	}
	for _, r := range existing {
		if r.EndNodeID() == endID {
			return r, false, nil
		}
	}

	// Not found — create.
	id := g.NextRelID()
	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)

	// Integrity metadata — endpoint hashes left empty (ByID trade-off).
	ig := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	r.SetIntegrity(ig)

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := nowInstant()
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

	if err := checkCtx(ctx); err != nil {
		return nil, false, err
	}

	if err := g.store.PutRelationship(r); err != nil {
		return nil, false, err
	}

	g.opRelAdds.Add(1)
	return r, true, nil
}
