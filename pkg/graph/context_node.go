package graph

import (
	"context"
	"fmt"
	"runtime"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// GetNodeWithContext retrieves a node by snowflake ID with context support.
func (g *Graph) GetNodeWithContext(ctx context.Context, id types.NodeID) (*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	n, err := g.store.GetNode(id)
	if err == nil {
		g.opNodeReads.Add(1)
	}
	return n, err
}

// AddNodeWithContext creates a new node with the given labels and properties.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) AddNodeWithContext(ctx context.Context, labels []string, props map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.addNodeInternal(ctx, labels, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeCreate, EntityID: types.EntityID(n.ID()), Timestamp: nowInstant(), Priority: PriorityHigh})
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
	n := types.NewNode(types.NodeID(id), primaryToken, extraTokens)
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

// DeleteNodeWithContext atomically removes a node and all connected relationships.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) DeleteNodeWithContext(ctx context.Context, id types.NodeID) error {
	g.mu.RLock()
	err := g.deleteNodeInternal(ctx, id)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeDelete, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityCritical})
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
func (g *Graph) deleteNodeInternal(ctx context.Context, id types.NodeID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	const maxRetries = 10
	for range maxRetries {

		// Phase A: read under node lock only.
		g.entityLocks.LockEntity(id.SnowflakeID())

		if err := checkCtx(ctx); err != nil {
			g.entityLocks.UnlockEntity(id.SnowflakeID())
			return err
		}

		current, err := g.store.GetNode(id)
		if err != nil {
			g.entityLocks.UnlockEntity(id.SnowflakeID())
			return err
		}

		outRels, err := g.store.OutgoingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockEntity(id.SnowflakeID())
			return err
		}
		inRels, err := g.store.IncomingRelationships(id, 0)
		if err != nil {
			g.entityLocks.UnlockEntity(id.SnowflakeID())
			return err
		}

		allIDs := collectDeleteIDs(id.SnowflakeID(), outRels, inRels)
		g.entityLocks.UnlockEntity(id.SnowflakeID())

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

		allIDs2 := collectDeleteIDs(id.SnowflakeID(), outRels2, inRels2)
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
//
// Returns raw snowflake.ID by design: the slice mixes a node ID with rel IDs
// for the LockMany locking surface, which uses a single 256-shard pool keyed by
// snowflake bits regardless of entity kind. See tasks/todo.md, Tier D.
func collectDeleteIDs(nodeID snowflake.ID, outRels, inRels []*types.Relationship) []snowflake.ID {
	seen := make(map[snowflake.ID]struct{}, 1+len(outRels)+len(inRels))
	seen[nodeID] = struct{}{}
	for _, r := range outRels {
		seen[r.ID().SnowflakeID()] = struct{}{}
	}
	for _, r := range inRels {
		seen[r.ID().SnowflakeID()] = struct{}{}
	}
	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	return ids
}

// sameIDSet returns true if a and b contain the same set of IDs (order-independent).
//
// Stays on raw snowflake.ID by design: callers pass a heterogeneous slice
// (node ID + rel IDs) produced by collectDeleteIDs and consumed by LockMany.
// A typed wrapper would be a lie — the slice is intentionally type-agnostic.
// See tasks/todo.md, Tier D.
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
func (g *Graph) deleteNodeLocked(ctx context.Context, id types.NodeID, current *types.Node, outRels, inRels []*types.Relationship) error {
	now := types.Instant(time.Now().UnixMilli())

	// Build relationship tombstones (dedup self-loops).
	seen := make(map[snowflake.ID]struct{})
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
	relTombstones := make([]RelTombstone, 0, len(allRels))
	for _, r := range allRels {
		rid := r.ID().SnowflakeID()
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
			ID:          types.RelID(rid),
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

// UpdateNodeWithContext applies property updates to an existing node with context support.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateNodeWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.updateNodeInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return n, err
}

// updateNodeInternal is the lock-free implementation of UpdateNodeWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateNodeInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
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
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

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

// ImportNodeWithID creates a node with a caller-specified snowflake ID.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) ImportNodeWithID(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.importNodeWithIDInternal(ctx, id, labels, props)
	ep := g.events
	g.mu.RUnlock()
	if err == nil {
		dispatchEvent(ep, Event{Type: EventNodeCreate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityHigh})
	}
	return n, err
}

// importNodeWithIDInternal is the lock-free implementation of ImportNodeWithID.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) importNodeWithIDInternal(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
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

	n := types.NewNode(types.NodeID(id), primaryToken, extraTokens)
	n.SetProperties(ps)

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

	txNow := nowInstant()
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		n.SetTemporal(tm)
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

	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := g.store.PutNode(n); err != nil {
		return nil, err
	}

	g.opNodeAdds.Add(1)
	return n, nil
}

// UpdateNodeInPlace applies property updates to a node without creating a version history entry.
// Version number is NOT incremented. PrevHash in the integrity chain is preserved.
// Use for high-frequency counter updates where history accumulation is undesirable.
// Returns ErrNodeNotFound if the node does not exist. Empty updates map is a no-op.
func (g *Graph) UpdateNodeInPlace(id types.NodeID, updates map[string]any) (*types.Node, error) {
	return g.UpdateNodeInPlaceWithContext(context.Background(), id, updates)
}

// UpdateNodeInPlaceWithContext applies property updates to a node without history.
// Acquires g.mu.RLock for transaction isolation — blocked while a tx holds g.mu.Lock.
func (g *Graph) UpdateNodeInPlaceWithContext(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
	g.mu.RLock()
	n, err := g.updateNodeInPlaceInternal(ctx, id, updates)
	ep := g.events
	g.mu.RUnlock()
	if err == nil && len(updates) > 0 {
		dispatchEvent(ep, Event{Type: EventNodeUpdate, EntityID: types.EntityID(id), Timestamp: nowInstant(), Priority: PriorityNormal})
	}
	return n, err
}

// updateNodeInPlaceInternal is the lock-free implementation of UpdateNodeInPlaceWithContext.
// Callers must hold g.mu.RLock (standalone) or g.mu.Lock (tx/batch).
func (g *Graph) updateNodeInPlaceInternal(ctx context.Context, id types.NodeID, updates map[string]any) (*types.Node, error) {
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
	g.entityLocks.LockEntity(id.SnowflakeID())
	defer g.entityLocks.UnlockEntity(id.SnowflakeID())

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
