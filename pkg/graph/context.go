package graph

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// nowInstant returns the current time as a types.Instant (Unix milliseconds).
func nowInstant() types.Instant {
	return types.Instant(time.Now().UnixMilli())
}

// checkCtx performs a non-blocking context cancellation check.
// Returns ctx.Err() if the context is done, nil otherwise.
// Zero overhead when the context is not cancelled.
func checkCtx(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// extractTemporal removes the reserved temporal keys (tkg_valid_from,
// tkg_valid_to, tkg_created_at) from the props map and returns their values
// plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated.
func extractTemporal(props map[string]any) (validFrom, validTo, createdAt types.Instant, filtered map[string]any, err error) {
	_, hasVF := props["tkg_valid_from"]
	_, hasVT := props["tkg_valid_to"]
	_, hasCA := props["tkg_created_at"]
	if !hasVF && !hasVT && !hasCA {
		return 0, 0, 0, props, nil
	}

	validFrom, err = parseInstant(props["tkg_valid_from"], "tkg_valid_from")
	if err != nil {
		return 0, 0, 0, nil, err
	}
	validTo, err = parseInstant(props["tkg_valid_to"], "tkg_valid_to")
	if err != nil {
		return 0, 0, 0, nil, err
	}
	createdAt, err = parseInstant(props["tkg_created_at"], "tkg_created_at")
	if err != nil {
		return 0, 0, 0, nil, err
	}

	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_valid_from" && k != "tkg_valid_to" && k != "tkg_created_at" {
			filtered[k] = v
		}
	}
	return validFrom, validTo, createdAt, filtered, nil
}

// parseInstant converts a property value to types.Instant (Unix milliseconds).
// Accepts nil (returns 0), int64, float64, int, and types.Instant.
func parseInstant(v any, key string) (types.Instant, error) {
	if v == nil {
		return 0, nil
	}
	switch val := v.(type) {
	case types.Instant:
		return val, nil
	case int64:
		return types.Instant(val), nil
	case int:
		return types.Instant(val), nil
	case float64:
		if val != math.Trunc(val) {
			return 0, fmt.Errorf("graph: %s %g is not an integer", key, val)
		}
		return types.Instant(val), nil
	default:
		return 0, fmt.Errorf("graph: %s must be a number (Unix ms), got %T", key, v)
	}
}

// extractProvenance removes the reserved provenance keys (tkg_author_id,
// tkg_signature, tkg_authorized_by, tkg_auth_level) from the props map and
// returns their values plus a filtered props map without those keys.
// If none of the reserved keys are present, the original map is returned
// unchanged (no allocation). The caller's original map is never mutated (B23).
// Returns an error if tkg_auth_level is out of [0, 255] or has an unsupported type.
func extractProvenance(props map[string]any) (authorID string, sig []byte, authorizedBy string, authLevel uint8, filtered map[string]any, err error) {
	_, hasA := props["tkg_author_id"]
	_, hasS := props["tkg_signature"]
	_, hasABy := props["tkg_authorized_by"]
	_, hasAL := props["tkg_auth_level"]
	if !hasA && !hasS && !hasABy && !hasAL {
		return "", nil, "", 0, props, nil
	}
	authorID, _ = props["tkg_author_id"].(string)
	sig, _ = props["tkg_signature"].([]byte)
	sig = types.CloneBytes(sig)
	authorizedBy, _ = props["tkg_authorized_by"].(string)
	// Accept uint8 and all integer types for JSON round-trip safety.
	// Bounds are checked explicitly to prevent silent truncation via modulo.
	switch v := props["tkg_auth_level"].(type) {
	case uint8:
		authLevel = v
	case int:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case int32:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case int64:
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %d out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	case float64:
		if v != math.Trunc(v) {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %g is not an integer", v)
		}
		if v < 0 || v > 255 {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level %g out of range [0, 255]", v)
		}
		authLevel = uint8(v)
	default:
		if props["tkg_auth_level"] != nil {
			return "", nil, "", 0, nil, fmt.Errorf("graph: tkg_auth_level must be a number, got %T", props["tkg_auth_level"])
		}
	}
	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_author_id" && k != "tkg_signature" && k != "tkg_authorized_by" && k != "tkg_auth_level" {
			filtered[k] = v
		}
	}
	return authorID, sig, authorizedBy, authLevel, filtered, nil
}

// GetNodeWithContext retrieves a node by snowflake ID with context support.
func (g *Graph) GetNodeWithContext(ctx context.Context, id snowflake.ID) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	n, err := g.store.GetNode(id)
	if err == nil {
		g.opNodeReads.Add(1)
	}
	return n, err
}

// GetRelationshipWithContext retrieves a relationship by snowflake ID with context support.
func (g *Graph) GetRelationshipWithContext(ctx context.Context, id snowflake.ID) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	r, err := g.store.GetRelationship(id)
	if err == nil {
		g.opRelReads.Add(1)
	}
	return r, err
}

// AddNodeWithContext creates a new node with the given labels and properties.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddNodeWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.addNodeInternal(ctx, labels, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeCreate, EntityID: n.InternalID().SnowflakeID(), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return n, err
}

// addNodeInternal is the lock-free implementation of AddNodeWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addNodeInternal(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before validation so they are never
	// seen by PropertySlice.Set (which rejects the tkg_ prefix).
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return nil, err
	}

	// Extract reserved temporal fields (tkg_valid_from, tkg_valid_to, tkg_created_at).
	validFrom, validTo, createdAt, props, err := extractTemporal(props)
	if err != nil {
		return nil, err
	}

	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	// Validation limits.
	if len(labels) > g.validation.MaxLabelsPerNode {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(labels), g.validation.MaxLabelsPerNode)
	}
	for _, label := range labels {
		if err := g.validateName(label); err != nil {
			return nil, err
		}
	}
	if err := g.validateProperties(props); err != nil {
		return nil, err
	}

	// Bulk-build properties first — fail fast before generating an ID.
	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	// Resolve labels to tokens.
	primaryToken, err := g.labels.GetOrCreate(labels[0])
	if err != nil {
		return nil, fmt.Errorf("graph: primary label: %w", err)
	}

	var extraTokens []uint16
	for _, label := range labels[1:] {
		tok, err := g.labels.GetOrCreate(label)
		if err != nil {
			return nil, fmt.Errorf("graph: extra label %q: %w", label, err)
		}
		extraTokens = append(extraTokens, tok)
	}

	id := g.NextNodeID()
	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	// Hash from canonical (deduplicated) labels, not raw user input.
	// NewNode deduplicates tokens; NodeLabels resolves the canonical set.
	canonicalLabels := g.NodeLabels(n)
	hash := ComputeNodeHash(n, canonicalLabels)
	n.SetIntegrity(&types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           "",
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	})

	// Set transaction time + merge caller-provided temporal metadata.
	// TxFrom/TxTo are NOT hashed — must be set AFTER hash computation.
	{
		txNow := nowInstant()
		ntm := n.Temporal()
		if ntm == nil {
			ntm = &types.TemporalMetadata{}
			n.SetTemporal(ntm)
		}
		ntm.TxFrom = txNow
		if validFrom != 0 {
			ntm.ValidFrom = validFrom
		}
		if validTo != 0 {
			ntm.ValidTo = validTo
		}
		if createdAt != 0 {
			ntm.CreatedAt = createdAt
		}
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	g.opNodeAdds.Add(1)
	return n, nil
}

// AddRelationshipWithContext creates a new directed relationship between two nodes.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.addRelationshipInternal(ctx, typeName, startNode, endNode, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: r.InternalID().SnowflakeID(), Timestamp: nowInstant(), Priority: PriorityHigh})
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

	startID := startNode.InternalID().SnowflakeID()
	endID := endNode.InternalID().SnowflakeID()

	if startID == endID && !g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	// Lock ordering: ascending shard index — deadlock-free.
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

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
func (g *Graph) AddRelationshipByIDWithContext(ctx context.Context, typeName string, startID, endID snowflake.ID, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.addRelationshipByIDInternal(ctx, typeName, startID, endID, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: r.InternalID().SnowflakeID(), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return r, err
}

// addRelationshipByIDInternal is the lock-free implementation of AddRelationshipByIDWithContext.
// Unlike addRelationshipInternal, it does NOT require pre-fetched endpoint nodes.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addRelationshipByIDInternal(ctx context.Context, typeName string, startID, endID snowflake.ID, props map[string]any) (*types.Relationship, error) {
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
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

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
func (g *Graph) AddRelationshipByIDIfAbsentWithContext(ctx context.Context, typeName string, startID, endID snowflake.ID, props map[string]any) (*types.Relationship, bool, error) {
	g.mu.RLock()
	r, created, err := g.addRelationshipByIDIfAbsentInternal(ctx, typeName, startID, endID, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && created {
		dispatchEvent(ep, Event{Type: EventRelCreate, EntityID: r.InternalID().SnowflakeID(), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return r, created, err
}

// addRelationshipByIDIfAbsentInternal is the lock-free implementation of
// AddRelationshipByIDIfAbsentWithContext. Under entity locks it checks for an
// existing relationship before creating, making the operation atomic.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) addRelationshipByIDIfAbsentInternal(ctx context.Context, typeName string, startID, endID snowflake.ID, props map[string]any) (*types.Relationship, bool, error) {
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
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

	// Check for existing relationship under entity locks (atomic with creation).
	existing, err := g.store.OutgoingRelationships(startID, typeToken)
	if err != nil {
		return nil, false, fmt.Errorf("graph: check existing relationships: %w", err)
	}
	for _, r := range existing {
		if r.EndNodeID().SnowflakeID() == endID {
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

// DeleteNodeWithContext atomically removes a node and all connected relationships.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) DeleteNodeWithContext(ctx context.Context, id snowflake.ID) error {
	g.mu.RLock()
	err := g.deleteNodeInternal(ctx, id)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeDelete, EntityID: id, Timestamp: nowInstant(), Priority: PriorityCritical})
	}
	return err
}

// deleteNodeInternal is the lock-free implementation of DeleteNodeWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
//
// Two-phase locking with TOCTOU retry:
//
//	Phase A (node lock only): read node + adjacency, collect all entity IDs.
//	Phase B (all entities locked): re-read adjacency, verify unchanged, then mutate.
//	If adjacency changed between phases, retry from Phase A.
func (g *Graph) deleteNodeInternal(ctx context.Context, id snowflake.ID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	const maxRetries = 10
	for range maxRetries {

		// Phase A: read under node lock only.
		g.entityLocks.LockEntity(id)

		if err := checkCtx(ctx); err != nil {
			g.entityLocks.UnlockEntity(id)
			return err
		}

		current, err := g.store.GetNode(id)
		if err != nil {
			g.entityLocks.UnlockEntity(id)
			return err
		}

		outRels, err := g.store.OutgoingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockEntity(id)
			return err
		}
		inRels, err := g.store.IncomingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockEntity(id)
			return err
		}

		allIDs := collectDeleteIDs(id, outRels, inRels)
		g.entityLocks.UnlockEntity(id)

		// Phase B: lock ALL entities (node + rels), re-verify adjacency.
		g.entityLocks.LockMany(allIDs)

		// Re-read adjacency under full lock to detect TOCTOU changes.
		outRels2, err := g.store.OutgoingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockMany(allIDs)
			return err
		}
		inRels2, err := g.store.IncomingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockMany(allIDs)
			return err
		}

		allIDs2 := collectDeleteIDs(id, outRels2, inRels2)
		if !sameIDSet(allIDs, allIDs2) {
			// Adjacency changed — retry. Yield the goroutine so the competing
			// rel-writer can commit before we re-read adjacency.
			g.entityLocks.UnlockMany(allIDs)
			runtime.Gosched()
			continue
		}

		// Adjacency stable — perform deletion under full lock.
		err = g.deleteNodeLocked(ctx, id, current, outRels2, inRels2)
		g.entityLocks.UnlockMany(allIDs)
		return err
	}

	return fmt.Errorf("graph: delete node %d: adjacency changed after %d retries", id, maxRetries)
}

// collectDeleteIDs builds a deduplicated slice of all entity IDs involved in a
// node deletion: the node itself plus all connected relationship IDs.
func collectDeleteIDs(nodeID snowflake.ID, outRels, inRels []*types.Relationship) []snowflake.ID {
	seen := make(map[snowflake.ID]struct{}, 1+len(outRels)+len(inRels))
	seen[nodeID] = struct{}{}
	for _, r := range outRels {
		seen[r.InternalID().SnowflakeID()] = struct{}{}
	}
	for _, r := range inRels {
		seen[r.InternalID().SnowflakeID()] = struct{}{}
	}
	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// sameIDSet returns true if a and b contain the same set of IDs (order-independent).
func sameIDSet(a, b []snowflake.ID) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[snowflake.ID]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		if _, ok := set[id]; !ok {
			return false
		}
	}
	return true
}

// deleteNodeLocked performs the actual deletion under full entity lock.
// Builds tombstones for all connected rels and the node, then issues a single
// atomic DeleteNodeWithHistory call (replaces PutRelVersion×N + PutNodeVersion +
// DeleteNodeCascade with one compound store operation).
func (g *Graph) deleteNodeLocked(ctx context.Context, id snowflake.ID, current *types.Node, outRels, inRels []*types.Relationship) error {
	now := types.Instant(time.Now().UnixMilli())

	// Build relationship tombstones (dedup self-loops).
	seen := make(map[snowflake.ID]struct{})
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
	relTombstones := make([]RelTombstone, 0, len(allRels))
	for _, r := range allRels {
		rid := r.InternalID().SnowflakeID()
		if _, ok := seen[rid]; ok {
			continue // dedup self-loops
		}
		seen[rid] = struct{}{}
		tombR := r.DeepCopy()
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
		relTombstones = append(relTombstones, RelTombstone{
			ID:          rid,
			PrevVersion: r.Version(),
			Tombstone:   tombR,
		})
	}

	// Build node tombstone.
	tombN := current.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt = now
	tmN.ValidTo = now
	// Transaction time: this tombstone version was committed at now.
	tmN.TxFrom = now
	tmN.TxTo = now

	// Single atomic call: PutRelVersion×N + PutNodeVersion + DeleteNodeCascade.
	if err := g.store.DeleteNodeWithHistory(id, current.Version(), tombN, relTombstones); err != nil {
		return err
	}
	g.opNodeDeletes.Add(1)
	return nil
}

// DeleteRelationshipWithContext removes a relationship from the store.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) DeleteRelationshipWithContext(ctx context.Context, id snowflake.ID) error {
	g.mu.RLock()
	err := g.deleteRelationshipInternal(ctx, id)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelDelete, EntityID: id, Timestamp: nowInstant(), Priority: PriorityCritical})
	}
	return err
}

// deleteRelationshipInternal is the lock-free implementation of DeleteRelationshipWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) deleteRelationshipInternal(ctx context.Context, id snowflake.ID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

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

// UpdateNodeWithContext applies property updates to an existing node with context support.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateNodeWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.updateNodeInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: id, Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return n, err
}

// updateNodeInternal is the lock-free implementation of UpdateNodeWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateNodeInternal(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetNodeWithContext(ctx, id)
	}

	// Extract reserved provenance fields before validation.
	// The no-op check above uses the original map length; after extraction
	// the remaining updates may be empty (metadata-only update).
	authorID, sig, authorizedBy, authLevel, updates, err := extractProvenance(updates)
	if err != nil {
		return nil, err
	}

	// Phase 1: Pre-validate before acquiring entity lock (fail fast).
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update node: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
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

	// Phase 2: Entity lock → read-modify-write under serialization.
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetNode(id)
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
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
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

	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{
		Hash:               hash,
		PrevHash:           prevHash,
		AuthorID:           authorID,
		Signature:          sig,
		AuthorizedBy:       authorizedBy,
		AuthorizationLevel: authLevel,
	})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := g.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, err
	}

	g.opNodeUpdates.Add(1)
	return current, nil
}

// UpdateRelationshipWithContext applies property updates to an existing relationship with context support.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateRelationshipWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.updateRelationshipInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: id, Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return r, err
}

// updateRelationshipInternal is the lock-free implementation of UpdateRelationshipWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateRelationshipInternal(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
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
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

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
	hash := ComputeRelHash(current, relTypeName)

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
	if sn, sErr := g.store.GetNode(current.StartNodeID().SnowflakeID()); sErr == nil {
		if sIg := sn.Integrity(); sIg != nil {
			relIG.FromNodeHash = sIg.Hash
		}
	}
	if en, eErr := g.store.GetNode(current.EndNodeID().SnowflakeID()); eErr == nil {
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

// ImportNodeWithID creates a node with a caller-specified snowflake ID.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) ImportNodeWithID(ctx context.Context, id snowflake.ID, labels []string, props map[string]any) (*types.Node, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.importNodeWithIDInternal(ctx, id, labels, props)
}

// importNodeWithIDInternal is the lock-free implementation of ImportNodeWithID.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) importNodeWithIDInternal(ctx context.Context, id snowflake.ID, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if id == 0 {
		return nil, ErrZeroID
	}

	if len(labels) == 0 {
		return nil, ErrNoLabels
	}

	if len(labels) > g.validation.MaxLabelsPerNode {
		return nil, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, len(labels), g.validation.MaxLabelsPerNode)
	}
	for _, label := range labels {
		if err := g.validateName(label); err != nil {
			return nil, err
		}
	}
	if err := g.validateProperties(props); err != nil {
		return nil, err
	}

	ps, err := types.NewPropertySlice(props)
	if err != nil {
		return nil, fmt.Errorf("graph: node properties: %w", err)
	}

	primaryToken, err := g.labels.GetOrCreate(labels[0])
	if err != nil {
		return nil, fmt.Errorf("graph: primary label: %w", err)
	}

	var extraTokens []uint16
	for _, label := range labels[1:] {
		tok, err := g.labels.GetOrCreate(label)
		if err != nil {
			return nil, fmt.Errorf("graph: extra label %q: %w", label, err)
		}
		extraTokens = append(extraTokens, tok)
	}

	// Check for collision before creating.
	if _, err := g.store.GetNode(id); err == nil {
		return nil, ErrNodeExists
	}

	n := types.NewNode(id, primaryToken, extraTokens)
	n.SetProperties(ps)

	canonicalLabels := g.NodeLabels(n)
	hash := ComputeNodeHash(n, canonicalLabels)
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: ""})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	return n, nil
}

// ImportRelationshipWithID creates a relationship with a caller-specified snowflake ID.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) ImportRelationshipWithID(ctx context.Context, id snowflake.ID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
}

// importRelWithIDInternal is the lock-free implementation of ImportRelationshipWithID.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) importRelWithIDInternal(ctx context.Context, id snowflake.ID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
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

	startID := startNode.InternalID().SnowflakeID()
	endID := endNode.InternalID().SnowflakeID()

	if startID == endID && !g.validation.AllowSelfLoops {
		return nil, ErrSelfLoop
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Lock both endpoints to prevent write-skew with concurrent DeleteNode.
	g.entityLocks.LockTwo(startID, endID)
	defer g.entityLocks.UnlockTwo(startID, endID)

	// Check for collision.
	if _, err := g.store.GetRelationship(id); err == nil {
		return nil, ErrRelExists
	}

	r := types.NewRelationship(id, typeToken, startID, endID)
	r.SetProperties(ps)

	hash := ComputeRelHash(r, typeName)
	r.SetIntegrity(&types.RelIntegrity{Hash: hash, PrevHash: ""})

	if err := g.checkTemporalConstraints(r, startNode, endNode); err != nil {
		return nil, err
	}

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutRelationship(r); err != nil {
		return nil, err
	}

	return r, nil
}

// UpdateNodeInPlace applies property updates to a node without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Use for high-frequency counter updates where history accumulation is undesirable.
// Returns ErrNodeNotFound if the node does not exist. Empty updates map is a no-op.
func (g *Graph) UpdateNodeInPlace(id snowflake.ID, updates map[string]any) (*types.Node, error) {
	return g.UpdateNodeInPlaceWithContext(context.Background(), id, updates)
}

// UpdateRelInPlace applies property updates to a relationship without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Returns ErrRelNotFound if the relationship does not exist. Empty updates map is a no-op.
func (g *Graph) UpdateRelInPlace(id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	return g.UpdateRelInPlaceWithContext(context.Background(), id, updates)
}

// UpdateNodeInPlaceWithContext applies property updates to a node without history.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateNodeInPlaceWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.updateNodeInPlaceInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: id, Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return n, err
}

// updateNodeInPlaceInternal is the lock-free implementation of UpdateNodeInPlaceWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateNodeInPlaceInternal(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetNodeWithContext(ctx, id)
	}

	// Phase 1: Pre-validate before acquiring entity lock.
	for key, val := range updates {
		if types.IsShadowKey(key) {
			return nil, fmt.Errorf("graph: update node in place: %w: %q", types.ErrReservedPrefix, key)
		}
		if val != nil {
			if err := types.ValidatePropertyValue(val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
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

	// Phase 2: Entity lock → read-modify-write under serialization.
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	current, err := g.store.GetNode(id)
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
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
			}
		} else {
			if err := current.SetProperty(key, val); err != nil {
				return nil, fmt.Errorf("graph: update node property %q: %w", key, err)
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

	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// ReplaceNode instead of ReplaceNodeWithHistory — no history entry written.
	if err := g.store.ReplaceNode(current); err != nil {
		return nil, err
	}

	g.opNodeUpdates.Add(1)
	return current, nil
}

// UpdateRelInPlaceWithContext applies property updates to a relationship without history.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateRelInPlaceWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	g.mu.RLock()
	r, err := g.updateRelInPlaceInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventRelUpdate, EntityID: id, Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return r, err
}

// updateRelInPlaceInternal is the lock-free implementation of UpdateRelInPlaceWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateRelInPlaceInternal(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
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
	g.entityLocks.LockEntity(id)
	defer g.entityLocks.UnlockEntity(id)

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
	hash := ComputeRelHash(current, relTypeName)
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
