package graph

import (
	"context"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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

// extractProvenance removes the reserved tkg_author_id and tkg_signature keys
// from the props map and returns their values plus a props map without those keys.
// If neither key is present, the original map is returned unchanged (no allocation).
// The caller's original map is never mutated.
func extractProvenance(props map[string]any) (authorID string, sig []byte, filtered map[string]any) {
	_, hasA := props["tkg_author_id"]
	_, hasS := props["tkg_signature"]
	if !hasA && !hasS {
		return "", nil, props
	}
	authorID, _ = props["tkg_author_id"].(string)
	sig, _ = props["tkg_signature"].([]byte)
	filtered = make(map[string]any, len(props))
	for k, v := range props {
		if k != "tkg_author_id" && k != "tkg_signature" {
			filtered[k] = v
		}
	}
	return authorID, sig, filtered
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
// Checks context at entry and before the store write.
//
// Reserved keys tkg_author_id (string) and tkg_signature ([]byte) may be
// included in props to set provenance fields on the integrity struct. They are
// extracted before validation and never stored in the PropertySlice.
func (g *Graph) AddNodeWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Extract reserved provenance fields before validation so they are never
	// seen by PropertySlice.Set (which rejects the tkg_ prefix).
	authorID, sig, props := extractProvenance(props)

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
	n.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: "", AuthorID: authorID, Signature: sig})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	g.opNodeAdds.Add(1)
	g.publishEvent(EventNodeCreate, n.InternalID().SnowflakeID(), nowInstant())
	return n, nil
}

// AddRelationshipWithContext creates a new directed relationship between two nodes.
// Checks context at entry, before acquiring endpoint locks, and before the store write.
//
// Reserved keys tkg_author_id (string) and tkg_signature ([]byte) may be
// included in props to set provenance fields on the integrity struct. They are
// extracted before validation and never stored in the PropertySlice.
func (g *Graph) AddRelationshipWithContext(ctx context.Context, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if startNode == nil || endNode == nil {
		return nil, ErrNilNode
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, props := extractProvenance(props)

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
	ig := &types.RelIntegrity{Hash: hash, PrevHash: "", AuthorID: authorID, Signature: sig}
	if startIg := startNode.Integrity(); startIg != nil {
		ig.FromNodeHash = startIg.Hash
	}
	if endIg := endNode.Integrity(); endIg != nil {
		ig.ToNodeHash = endIg.Hash
	}
	r.SetIntegrity(ig)

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
	g.publishEvent(EventRelCreate, r.InternalID().SnowflakeID(), nowInstant())
	return r, nil
}

// DeleteNodeWithContext atomically removes a node and all connected relationships.
// Saves tombstone versions (with DeletedAt/ValidTo) for the node and all connected
// relationships before deletion, preserving temporal history for past-time queries.
//
// Two-phase locking with TOCTOU retry:
//
//	Phase A (node lock only): read node + adjacency, collect all entity IDs.
//	Phase B (all entities locked): re-read adjacency, verify unchanged, then mutate.
//	If adjacency changed between phases, retry from Phase A.
func (g *Graph) DeleteNodeWithContext(ctx context.Context, id snowflake.ID) error {
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
			// Adjacency changed — retry.
			g.entityLocks.UnlockMany(allIDs)
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
// Creates tombstones for all connected rels and the node, then cascade deletes.
func (g *Graph) deleteNodeLocked(ctx context.Context, id snowflake.ID, current *types.Node, outRels, inRels []*types.Relationship) error {
	now := types.Instant(time.Now().UnixMilli())

	// Save tombstone for all connected relationships first.
	seen := make(map[snowflake.ID]struct{})
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
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
		if err := g.store.PutRelVersion(rid, r.Version(), tombR); err != nil {
			return err
		}
	}

	// Save tombstone for the node itself.
	tombN := current.DeepCopy()
	tmN := tombN.Temporal()
	if tmN == nil {
		tmN = &types.TemporalMetadata{}
		tombN.SetTemporal(tmN)
	}
	tmN.DeletedAt = now
	tmN.ValidTo = now
	if err := g.store.PutNodeVersion(id, current.Version(), tombN); err != nil {
		return err
	}

	if err := g.store.DeleteNodeCascade(id); err != nil {
		return err
	}
	g.opNodeDeletes.Add(1)
	g.publishEvent(EventNodeDelete, id, now)
	return nil
}

// DeleteRelationshipWithContext removes a relationship from the store.
// Saves a tombstone version (with DeletedAt/ValidTo) before deletion,
// preserving temporal history for past-time queries.
func (g *Graph) DeleteRelationshipWithContext(ctx context.Context, id snowflake.ID) error {
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
	if err := g.store.PutRelVersion(id, current.Version(), tombR); err != nil {
		return err
	}

	if err := g.store.DeleteRelationship(id); err != nil {
		return err
	}
	g.opRelDeletes.Add(1)
	g.publishEvent(EventRelDelete, id, now)
	return nil
}

// UpdateNodeWithContext applies property updates to an existing node with context support.
// Checks context at entry, before acquiring the entity lock, before the store read,
// before saving version history, and before the final store write.
//
// Reserved keys tkg_author_id (string) and tkg_signature ([]byte) may be
// included in updates to set provenance fields on the new integrity struct.
// They are extracted before validation and never stored in the PropertySlice.
func (g *Graph) UpdateNodeWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetNodeWithContext(ctx, id)
	}

	// Extract reserved provenance fields before validation.
	// The no-op check above uses the original map length; after extraction
	// the remaining updates may be empty (metadata-only update).
	authorID, sig, updates := extractProvenance(updates)

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

	nodeLabels := g.NodeLabels(current)
	hash := ComputeNodeHash(current, nodeLabels)
	current.SetIntegrity(&types.NodeIntegrity{Hash: hash, PrevHash: prevHash, AuthorID: authorID, Signature: sig})

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	// Atomic replace + history — single store call prevents orphaned history entries.
	if err := g.store.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return nil, err
	}

	g.opNodeUpdates.Add(1)
	g.publishEvent(EventNodeUpdate, id, now)
	return current, nil
}

// UpdateRelationshipWithContext applies property updates to an existing relationship with context support.
// Checks context at entry, before acquiring the entity lock, before the store read,
// before saving version history, and before the final store write.
//
// Reserved keys tkg_author_id (string) and tkg_signature ([]byte) may be
// included in updates to set provenance fields on the new integrity struct.
// They are extracted before validation and never stored in the PropertySlice.
func (g *Graph) UpdateRelationshipWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if len(updates) == 0 {
		return g.GetRelationshipWithContext(ctx, id)
	}

	// Extract reserved provenance fields before validation.
	authorID, sig, updates := extractProvenance(updates)

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

	relTypeName := g.RelationshipType(current)
	hash := ComputeRelHash(current, relTypeName)

	// Refresh endpoint hashes to capture the current state of the endpoint nodes.
	// These are NOT fed into ComputeRelHash to avoid cascading hash invalidation.
	relIG := &types.RelIntegrity{Hash: hash, PrevHash: prevHash, AuthorID: authorID, Signature: sig}
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
	g.publishEvent(EventRelUpdate, id, now)
	return current, nil
}

// ImportNodeWithID creates a node with a caller-specified snowflake ID.
// Used for backup restore where ID preservation is required.
// Returns ErrNodeExists if the ID is already in use, ErrZeroID if id == 0.
func (g *Graph) ImportNodeWithID(ctx context.Context, id snowflake.ID, labels []string, props map[string]any) (*types.Node, error) {
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
// Used for backup restore where ID preservation is required.
// Returns ErrRelExists if the ID is already in use, ErrZeroID if id == 0.
func (g *Graph) ImportRelationshipWithID(ctx context.Context, id snowflake.ID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
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
// Identical to UpdateNodeWithContext except:
//  1. Version number is NOT incremented.
//  2. store.ReplaceNode is used instead of store.ReplaceNodeWithHistory.
//  3. PrevHash is preserved (not advanced) in the integrity chain.
func (g *Graph) UpdateNodeInPlaceWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Node, error) {
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
	g.publishEvent(EventNodeUpdate, id, now)
	return current, nil
}

// UpdateRelInPlaceWithContext applies property updates to a relationship without history.
// Identical to UpdateRelationshipWithContext except:
//  1. Version number is NOT incremented.
//  2. store.ReplaceRelationship is used instead of store.ReplaceRelWithHistory.
//  3. PrevHash is preserved (not advanced) in the integrity chain.
func (g *Graph) UpdateRelInPlaceWithContext(ctx context.Context, id snowflake.ID, updates map[string]any) (*types.Relationship, error) {
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
	g.publishEvent(EventRelUpdate, id, now)
	return current, nil
}
