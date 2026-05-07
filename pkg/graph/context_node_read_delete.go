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
