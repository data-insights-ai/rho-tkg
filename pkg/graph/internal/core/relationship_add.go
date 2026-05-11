package core

import (
	"context"
	"fmt"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/integrity"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

const nonZeroRelTypeProbeToken uint16 = 1

func validateRelationshipEndpointIDs(startID, endID types.NodeID) error {
	if err := storepkg.ValidateNodeID(startID); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(endID); err != nil {
		return err
	}
	return nil
}

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
	if err == nil && ep != nil {
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
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	startID := startNode.ID()
	endID := endNode.ID()
	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return nil, err
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

	id := c.nextRelID()
	var fromHash, toHash string
	storeCanCaptureEndpointHashes := false
	if c.constraints.Len() > 0 {
		// Fetch live endpoints from the store under the endpoint locks so
		// hash refresh and temporal-constraint checks see the current state,
		// not whatever the caller happened to hold (R4-F5).
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return nil, err
		}
		probe := c.newRelConstraintProbe(id, startID, endID, validFrom, validTo, createdAt)
		if err := c.checkTemporalConstraints(probe, liveStart, liveEnd); err != nil {
			return nil, err
		}
		fromHash = nodeIntegrityHash(liveStart)
		toHash = nodeIntegrityHash(liveEnd)
	} else {
		if _, ok := c.store.(generatedcreate.RelationshipEndpointHashCapability); ok {
			storeCanCaptureEndpointHashes = true
		} else {
			fromHash, toHash, err = c.liveEndpointHashes(startID, endID)
			if err != nil {
				return nil, err
			}
		}
	}

	// R5-F6: defer rel-type token allocation past every endpoint-fetch
	// failure path. A missing endpoint, store error, or context
	// cancellation between the caller fetch and our endpoint-lock
	// window must NOT leave a permanent rel-type registration. R4-F14
	// already deferred past the cheap self-loop/validation gates;
	// R5-F6 finishes the job for the operational failure paths. The
	// allocation snapshot below rolls back a newly created type if the
	// final store write still fails.
	typeToken, relTypeSnapshot, allocatedRelType, err := c.getOrCreateRelTypeWithSnapshot(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}
	relTypeFinished := false
	finishRelType := func(err error) error {
		relTypeFinished = true
		return c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, err)
	}
	defer func() {
		if !relTypeFinished {
			_ = c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, fmt.Errorf("panic during relationship create"))
		}
	}()

	r := types.NewRelationship(id, typeToken, startID, endID)
	if err := r.SetOwnedProperties(ps); err != nil {
		return nil, finishRelType(fmt.Errorf("graph: relationship properties: %w", err))
	}

	hash, err := integrity.ComputeRelHashChecked(r, typeName)
	if err != nil {
		return nil, finishRelType(fmt.Errorf("graph: compute relationship hash: %w", err))
	}

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
	ig.FromNodeHash = fromHash
	ig.ToNodeHash = toHash
	r.SetIntegrity(ig)

	c.applyRelCreateTemporal(r, validFrom, validTo, createdAt)

	if err := checkCtx(ctx); err != nil {
		return nil, finishRelType(err)
	}

	if storeCanCaptureEndpointHashes {
		fromHash, toHash, _, err = putGeneratedRelationshipWithEndpointHashes(c.store, r)
		if err != nil {
			return nil, finishRelType(err)
		}
		ig.FromNodeHash = fromHash
		ig.ToNodeHash = toHash
	} else {
		if err := putGeneratedRelationship(c.store, r); err != nil {
			return nil, finishRelType(err)
		}
	}
	if err := finishRelType(nil); err != nil {
		return r, err
	}
	c.rememberRelType(typeName, typeToken)

	c.opRelAdds.Add(1)
	return r, nil
}

func (c *Core) newRelConstraintProbe(id types.RelID, startID, endID types.NodeID, validFrom, validTo, createdAt types.Instant) *types.Relationship {
	r := types.NewRelationship(id, nonZeroRelTypeProbeToken, startID, endID)
	c.applyRelCreateTemporal(r, validFrom, validTo, createdAt)
	return r
}

func (c *Core) applyRelCreateTemporal(r *types.Relationship, validFrom, validTo, createdAt types.Instant) {
	rtm := r.Temporal()
	if rtm == nil {
		rtm = &types.TemporalMetadata{}
		r.SetTemporal(rtm)
	}
	rtm.TxFrom = c.now()
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

func (c *Core) liveEndpointNodes(startID, endID types.NodeID) (*types.Node, *types.Node, error) {
	liveStart, err := c.store.GetNode(startID)
	if err != nil {
		return nil, nil, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
	}
	if startID == endID {
		return liveStart, liveStart, nil
	}
	liveEnd, err := c.store.GetNode(endID)
	if err != nil {
		return nil, nil, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
	}
	return liveStart, liveEnd, nil
}

func (c *Core) liveEndpointHashes(startID, endID types.NodeID) (string, string, error) {
	if c.endpointHash != nil {
		fromHash, toHash, err := c.endpointHash.EndpointIntegrityHashes(startID, endID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live endpoint integrity fetch under endpoint lock: %w", err)
		}
		return fromHash, toHash, nil
	}
	if c.nodeHash != nil {
		fromHash, err := c.nodeHash.NodeIntegrityHash(startID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live start-node integrity fetch under endpoint lock: %w", err)
		}
		if startID == endID {
			return fromHash, fromHash, nil
		}
		toHash, err := c.nodeHash.NodeIntegrityHash(endID)
		if err != nil {
			return "", "", fmt.Errorf("graph: live end-node integrity fetch under endpoint lock: %w", err)
		}
		return fromHash, toHash, nil
	}

	liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
	if err != nil {
		return "", "", err
	}
	return nodeIntegrityHash(liveStart), nodeIntegrityHash(liveEnd), nil
}

func nodeIntegrityHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// AddByIDWithContext creates a relationship using endpoint snowflake IDs.
// It has the same live-endpoint verification, endpoint-hash capture, and
// constraint enforcement semantics as RelOps.AddWithContext; it only changes
// the input form so callers do not need to carry endpoint node objects.
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
	if err == nil && ep != nil {
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
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship properties: %w", err)
	}

	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return nil, err
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

	id := c.nextRelID()
	var fromHash, toHash string
	storeCanCaptureEndpointHashes := false
	if c.constraints.Len() > 0 {
		liveStart, liveEnd, err := c.liveEndpointNodes(startID, endID)
		if err != nil {
			return nil, err
		}
		probe := c.newRelConstraintProbe(id, startID, endID, validFrom, validTo, createdAt)
		if err := c.checkTemporalConstraints(probe, liveStart, liveEnd); err != nil {
			return nil, err
		}
		fromHash = nodeIntegrityHash(liveStart)
		toHash = nodeIntegrityHash(liveEnd)
	} else {
		if _, ok := c.store.(generatedcreate.RelationshipEndpointHashCapability); ok {
			storeCanCaptureEndpointHashes = true
		} else {
			fromHash, toHash, err = c.liveEndpointHashes(startID, endID)
			if err != nil {
				return nil, err
			}
		}
	}

	// R5-F6: allocate rel-type token AFTER endpoint locks are held
	// (and, when constraints are active, AFTER the live-endpoint
	// fetches succeed) so a failed fetch does not leave a permanent
	// rel-type registration. Snapshot the registry at allocation time
	// so a final PutRelationship failure can roll back a newly created
	// type token.
	typeToken, relTypeSnapshot, allocatedRelType, err := c.getOrCreateRelTypeWithSnapshot(typeName)
	if err != nil {
		return nil, fmt.Errorf("graph: relationship type: %w", err)
	}
	relTypeFinished := false
	finishRelType := func(err error) error {
		relTypeFinished = true
		return c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, err)
	}
	defer func() {
		if !relTypeFinished {
			_ = c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, fmt.Errorf("panic during relationship create"))
		}
	}()

	r := types.NewRelationship(id, typeToken, startID, endID)
	if err := r.SetOwnedProperties(ps); err != nil {
		return nil, finishRelType(fmt.Errorf("graph: relationship properties: %w", err))
	}

	hash, err := integrity.ComputeRelHashChecked(r, typeName)
	if err != nil {
		return nil, finishRelType(fmt.Errorf("graph: compute relationship hash: %w", err))
	}

	ig := &types.RelIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	}
	ig.FromNodeHash = fromHash
	ig.ToNodeHash = toHash
	r.SetIntegrity(ig)

	c.applyRelCreateTemporal(r, validFrom, validTo, createdAt)

	if err := checkCtx(ctx); err != nil {
		return nil, finishRelType(err)
	}

	if storeCanCaptureEndpointHashes {
		fromHash, toHash, _, err = putGeneratedRelationshipWithEndpointHashes(c.store, r)
		if err != nil {
			return nil, finishRelType(err)
		}
		ig.FromNodeHash = fromHash
		ig.ToNodeHash = toHash
	} else {
		if err := putGeneratedRelationship(c.store, r); err != nil {
			return nil, finishRelType(err)
		}
	}
	if err := finishRelType(nil); err != nil {
		return r, err
	}
	c.rememberRelType(typeName, typeToken)

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
// Endpoint and constraint behaviour matches AddByIDWithContext.
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
	if err == nil && created && ep != nil {
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
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship properties: %w", err)
	}

	if err := validateRelationshipEndpointIDs(startID, endID); err != nil {
		return nil, false, err
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
	if existingTok, ok := c.lookupRelTypeLocked(typeName); ok {
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

	liveStart, err := c.store.GetNode(startID)
	if err != nil {
		return nil, false, fmt.Errorf("graph: live start-node fetch under endpoint lock: %w", err)
	}
	var liveEnd *types.Node
	if startID == endID {
		liveEnd = liveStart
	} else {
		liveEnd, err = c.store.GetNode(endID)
		if err != nil {
			return nil, false, fmt.Errorf("graph: live end-node fetch under endpoint lock: %w", err)
		}
	}

	id := c.nextRelID()
	if c.constraints.Len() > 0 {
		probe := c.newRelConstraintProbe(id, startID, endID, validFrom, validTo, createdAt)
		if err := c.checkTemporalConstraints(probe, liveStart, liveEnd); err != nil {
			return nil, false, err
		}
	}

	// Not found — allocate the token now and create. The allocation
	// snapshot lets a final PutRelationship failure roll back a newly
	// created type token.
	typeToken, relTypeSnapshot, allocatedRelType, err := c.getOrCreateRelTypeWithSnapshot(typeName)
	if err != nil {
		return nil, false, fmt.Errorf("graph: relationship type: %w", err)
	}
	relTypeFinished := false
	finishRelType := func(err error) error {
		relTypeFinished = true
		return c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, err)
	}
	defer func() {
		if !relTypeFinished {
			_ = c.restoreNewRelTypeOnError(relTypeSnapshot, allocatedRelType, typeName, fmt.Errorf("panic during relationship create"))
		}
	}()
	r := types.NewRelationship(id, typeToken, startID, endID)
	if err := r.SetOwnedProperties(ps); err != nil {
		return nil, false, finishRelType(fmt.Errorf("graph: relationship properties: %w", err))
	}

	hash, err := integrity.ComputeRelHashChecked(r, typeName)
	if err != nil {
		return nil, false, finishRelType(fmt.Errorf("graph: compute relationship hash: %w", err))
	}

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

	c.applyRelCreateTemporal(r, validFrom, validTo, createdAt)

	if err := checkCtx(ctx); err != nil {
		return nil, false, finishRelType(err)
	}

	if err := putGeneratedRelationship(c.store, r); err != nil {
		return nil, false, finishRelType(err)
	}
	if err := finishRelType(nil); err != nil {
		return r, true, err
	}
	c.rememberRelType(typeName, typeToken)

	c.opRelAdds.Add(1)
	return r, true, nil
}
