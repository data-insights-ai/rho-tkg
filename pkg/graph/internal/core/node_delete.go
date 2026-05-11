package core

import (
	"context"
	"fmt"
	"runtime"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Node — Read / Delete
// =============================================================================

// GetWithContext retrieves a node by snowflake ID with context support.
func (n *NodeOps) GetWithContext(ctx context.Context, id types.NodeID) (*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	var (
		node *types.Node
		err  error
	)
	_, closeErr := c.runUnderRLock(func() {
		node, err = c.store.GetNode(id)
	})
	if closeErr != nil {
		return nil, closeErr
	}
	if err == nil {
		c.opNodeReads.Add(1)
	}
	return node, err
}

// DeleteWithContext atomically removes a node and all connected relationships.
// Acquires c.mu.RLock (panic-safe) for transaction isolation — blocked
// while a tx holds c.mu.Lock.
func (n *NodeOps) DeleteWithContext(ctx context.Context, id types.NodeID) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	var err error
	ep, closeErr := c.runUnderRLock(func() {
		err = c.deleteNodeInternal(ctx, id)
	})
	if closeErr != nil {
		return closeErr
	}
	if err == nil {
		dispatchEvent(ep, eventspkg.Event{Type: eventspkg.EventNodeDelete, EntityID: types.EntityID(id), Timestamp: c.now(), Priority: eventspkg.PriorityCritical})
	}
	return err
}

// deleteNodeInternal is the lock-free implementation of NodeOps.DeleteWithContext.
// Callers must hold c.mu.RLock (standalone) or c.mu.Lock (tx/batch).
//
// Two-phase locking with TOCTOU retry:
//
//	Phase A (node lock only): confirm node exists, read adjacency, collect all entity IDs.
//	Phase B (all entities locked): re-read node + adjacency, verify adjacency unchanged, then mutate.
//	If adjacency changed between phases, retry from Phase A.
func (c *Core) deleteNodeInternal(ctx context.Context, id types.NodeID) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	const maxRetries = 10
	for range maxRetries {
		// Phase A: read under node lock only. The closure pattern keeps
		// the lock under defer so a panic from a custom Store does not
		// leak the shard lock.
		var (
			allIDs    []snowflake.ID
			phaseAErr error
		)
		func() {
			c.entityLocks.LockEntity(id.SnowflakeID())
			defer c.entityLocks.UnlockEntity(id.SnowflakeID())

			if err := checkCtx(ctx); err != nil {
				phaseAErr = err
				return
			}
			if _, err := c.store.GetNode(id); err != nil {
				phaseAErr = err
				return
			}
			outRels, err := c.store.OutgoingRelationships(id, 0)
			if err != nil {
				phaseAErr = err
				return
			}
			inRels, err := c.store.IncomingRelationships(id, 0)
			if err != nil {
				phaseAErr = err
				return
			}
			allIDs = collectDeleteIDs(id.SnowflakeID(), outRels, inRels)
		}()
		if phaseAErr != nil {
			return phaseAErr
		}

		// Phase B: lock ALL entities (node + rels), re-read node, and re-verify adjacency.
		// Same closure pattern — panic during the re-read or delete must
		// not leak LockMany.
		var (
			phaseBErr error
			retry     bool
			done      bool
		)
		func() {
			c.entityLocks.LockMany(allIDs)
			defer c.entityLocks.UnlockMany(allIDs)

			current, err := c.store.GetNode(id)
			if err != nil {
				phaseBErr = err
				return
			}
			outRels2, err := c.store.OutgoingRelationships(id, 0)
			if err != nil {
				phaseBErr = err
				return
			}
			inRels2, err := c.store.IncomingRelationships(id, 0)
			if err != nil {
				phaseBErr = err
				return
			}

			allIDs2 := collectDeleteIDs(id.SnowflakeID(), outRels2, inRels2)
			if !sameIDSet(allIDs, allIDs2) {
				// Adjacency changed — retry after releasing locks.
				retry = true
				return
			}

			phaseBErr = c.deleteNodeLocked(id, current, outRels2, inRels2)
			done = true
		}()
		if retry {
			runtime.Gosched()
			continue
		}
		if done {
			return phaseBErr
		}
		// phaseB returned without committing or retrying — propagate the
		// error.
		if phaseBErr != nil {
			return phaseBErr
		}
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
func (c *Core) deleteNodeLocked(id types.NodeID, current *types.Node, outRels, inRels []*types.Relationship) error {
	now := c.now()

	// Build relationship tombstones (dedup self-loops).
	seen := make(map[snowflake.ID]struct{})
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
	relTombstones := make([]storepkg.RelTombstone, 0, len(allRels))
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
		relTombstones = append(relTombstones, storepkg.RelTombstone{
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
	if err := c.store.DeleteNodeWithHistory(id, current.Version(), tombN, relTombstones); err != nil {
		return err
	}
	c.opNodeDeletes.Add(1)
	return nil
}
