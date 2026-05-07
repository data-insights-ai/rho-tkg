package graph

import (
	"context"
	"fmt"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Relationship — Add (Create / IfAbsent / ByID)
// =============================================================================

// AddRelationshipWithContext creates a new directed relationship between two nodes.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.addRelationshipInternal(ctx, typeName, startNode, endNode, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: eventspkg.PriorityHigh})
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
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: eventspkg.PriorityHigh})
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

	hash := integrity.ComputeRelHash(r, typeName)

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
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(r.ID()), Timestamp: nowInstant(), Priority: eventspkg.PriorityHigh})
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

	hash := integrity.ComputeRelHash(r, typeName)

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

// =============================================================================
// Relationship — Update
// =============================================================================

// UpdateRelationshipWithContext applies property updates to an existing relationship with context support.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateRelationshipWithContext(ctx context.Context, id types.RelID, updates map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.updateRelationshipInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityNormal})
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
	hash := integrity.ComputeRelHash(current, relTypeName)

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
// Returns storepkg.ErrRelNotFound if the relationship does not exist. Empty updates map is a no-op.
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
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityNormal})
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
	hash := integrity.ComputeRelHash(current, relTypeName)
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

// =============================================================================
// Relationship — Read / Delete
// =============================================================================

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
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelDelete, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityCritical})
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

// =============================================================================
// Relationship — Import (caller-specified ID)
// =============================================================================

// ImportRelationshipWithID creates a relationship with a caller-specified snowflake ID.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) ImportRelationshipWithID(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventRelCreate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: eventspkg.PriorityHigh})
	}
	return r, err
}

// importRelWithIDInternal is the lock-free implementation of ImportRelationshipWithID.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) importRelWithIDInternal(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
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

	if err := g.validateName(typeName); err != nil {
		return nil, err
	}
	if err := g.validateProperties(props); err != nil {
		return nil, err
	}

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
	g.entityLocks.LockTwo(startID.SnowflakeID(), endID.SnowflakeID())
	defer g.entityLocks.UnlockTwo(startID.SnowflakeID(), endID.SnowflakeID())

	// Check for collision.
	if _, err := g.store.GetRelationship(id); err == nil {
		return nil, storepkg.ErrRelExists
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
	if startIg := startNode.Integrity(); startIg != nil {
		ig.FromNodeHash = startIg.Hash
	}
	if endIg := endNode.Integrity(); endIg != nil {
		ig.ToNodeHash = endIg.Hash
	}
	r.SetIntegrity(ig)

	txNow := nowInstant()
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
