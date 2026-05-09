package core

import (
	"errors"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"

	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Snapshot
// =============================================================================

// Snapshot returns a complete graph state at the given instant.
// Relationships are only included if both endpoints are valid at t.
//
// Takes c.mu.RLock for the duration. The RLock excludes tx/batch but
// NOT individual standalone mutations (which also take RLock), so
// concurrent writers can interleave between the node and rel reads.
// For a strict snapshot (no concurrent writers at all), call
// (*GraphTx).Snapshot from inside g.Tx.Run; the tx already holds
// c.mu.Lock and the underlying snapshotAt runs without re-entering
// the lock.
func (t *TempOps) Snapshot(at types.Instant) (*temporalpkg.GraphSnapshot, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotAt(at)
}

// snapshotAt computes the graph snapshot at t without acquiring any
// graph-level lock. Caller must hold c.mu.RLock OR c.mu.Lock — the
// standalone path uses RLock, the tx path uses Lock.
func (c *Core) snapshotAt(t types.Instant) (*temporalpkg.GraphSnapshot, error) {
	nodes, err := c.Temporal.NodesAt(t)
	if err != nil {
		return nil, err
	}

	// Build set of valid node IDs for endpoint filtering.
	nodeSet := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n.ID().SnowflakeID()] = struct{}{}
	}

	allRels, err := c.Temporal.RelationshipsAt(t)
	if err != nil {
		return nil, err
	}

	// Only include rels where both endpoints are in the valid node set.
	var rels []*types.Relationship
	for _, r := range allRels {
		_, startOK := nodeSet[r.StartNodeID().SnowflakeID()]
		_, endOK := nodeSet[r.EndNodeID().SnowflakeID()]
		if startOK && endOK {
			rels = append(rels, r)
		}
	}

	return &temporalpkg.GraphSnapshot{
		Timestamp:     t,
		Nodes:         nodes,
		Relationships: rels,
		NodeCount:     len(nodes),
		RelCount:      len(rels),
	}, nil
}

// =============================================================================
// Diff
// =============================================================================

// Diff returns the set of entity changes between t1 and t2.
// Entities valid at T2 but not T1 → Created.
// Entities valid at both but with different integrity hash → Updated.
// Entities valid at T1 but not T2 → Deleted.
// Returns ErrInvalidTimeRange if t1 >= t2 or either is zero.
//
// Note: the two snapshots are read independently without holding c.mu. A
// concurrent backdated write that commits between the two reads may appear as
// a spurious Created/Deleted entry. This is an acceptable trade-off against
// blocking all writes for the full O(N) snapshot duration.
//
// Implementation: delegates to DiffSnapshotsCallback with handlers that
// accumulate into a SnapshotDiff. The callback path resolves each entity
// version on demand instead of materialising two full snapshots, so the
// peak working set is one entity at a time plus the dedup ID set.
func (t *TempOps) Diff(t1, t2 types.Instant) (*temporalpkg.SnapshotDiff, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	diff := &temporalpkg.SnapshotDiff{T1: t1, T2: t2}
	handlers := temporalpkg.DiffHandlers{
		OnNodeCreated: func(after *types.Node) error {
			diff.NodesCreated = append(diff.NodesCreated, after)
			return nil
		},
		OnNodeUpdated: func(before, after *types.Node) error {
			diff.NodesUpdated = append(diff.NodesUpdated, temporalpkg.NodeUpdate{Before: before, After: after})
			return nil
		},
		OnNodeDeleted: func(before *types.Node) error {
			diff.NodesDeleted = append(diff.NodesDeleted, before)
			return nil
		},
		OnRelCreated: func(after *types.Relationship) error {
			diff.RelsCreated = append(diff.RelsCreated, after)
			return nil
		},
		OnRelUpdated: func(before, after *types.Relationship) error {
			diff.RelsUpdated = append(diff.RelsUpdated, temporalpkg.RelUpdate{Before: before, After: after})
			return nil
		},
		OnRelDeleted: func(before *types.Relationship) error {
			diff.RelsDeleted = append(diff.RelsDeleted, before)
			return nil
		},
	}
	if err := c.Temporal.DiffCallback(t1, t2, handlers); err != nil {
		return nil, err
	}
	return diff, nil
}

// DiffCallback streams entity changes between t1 (older) and t2
// (newer) via handler callbacks instead of materialising two full
// GraphSnapshot values. RAM usage is bounded by O(|distinct entity IDs| × 16B
// for the dedup set) plus the working memory for one entity version pair at
// a time, down from O(|entities valid at t1| + |entities valid at t2|).
//
// For every node ID known to the store (current ID set ∪ history ID set)
// the implementation calls GetNodeAt(id, t1) and GetNodeAt(id, t2). The
// result classifies each entity as Created (only at t2), Deleted (only at
// t1), Updated (present at both with a different integrity hash), or
// unchanged (skipped). Relationships follow the same pattern but are
// additionally subject to endpoint filtering: a relationship is treated as
// "present at t" only when both its start and end nodes are valid at t.
// This matches the snapshotAt rel-endpoint filter exactly and preserves
// behavioural parity with Temporal.Diff.
//
// nil handler fields are skipped cleanly. Returning a non-nil error from
// any handler halts iteration and returns that error. Order of delivery is
// implementation-defined; do not rely on it.
//
// DiffCallback ErrInvalidTimeRange if t1 == 0, t2 == 0, or t1 >= t2.
func (t *TempOps) DiffCallback(t1, t2 types.Instant, h temporalpkg.DiffHandlers) error {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if t1 == 0 || t2 == 0 || t1 >= t2 {
		return ErrInvalidTimeRange
	}

	// No c.mu.RLock: matches Temporal.Diff semantics. A concurrent backdated
	// write that commits between the per-entity GetNodeAt calls may appear
	// as a spurious Created/Deleted entry — same trade-off as before.

	// --- Nodes ---
	if err := c.forEachKnownNodeID(func(id types.NodeID) error {
		n1, err := c.lookupNodeAtForDiff(id, t1)
		if err != nil {
			return err
		}
		n2, err := c.lookupNodeAtForDiff(id, t2)
		if err != nil {
			return err
		}
		switch {
		case n1 == nil && n2 != nil:
			if h.OnNodeCreated != nil {
				return h.OnNodeCreated(n2)
			}
		case n1 != nil && n2 == nil:
			if h.OnNodeDeleted != nil {
				return h.OnNodeDeleted(n1)
			}
		case n1 != nil && n2 != nil:
			if nodeHash(n1) != nodeHash(n2) {
				if h.OnNodeUpdated != nil {
					return h.OnNodeUpdated(n1, n2)
				}
			}
		}
		// n1 == nil && n2 == nil: never visible in [t1, t2]; skip.
		return nil
	}); err != nil {
		return err
	}

	// --- Relationships ---
	// Endpoint validity is determined per-rel via GetNodeAt on the
	// resolved start/end at the queried time. We deliberately do NOT cache
	// node-validity decisions here: the dedup ID set already costs
	// O(|nodes|), and a second full-graph node-validity map would defeat
	// the bounded-RAM goal. The extra GetNodeAt calls are amortised into
	// the same chain reads that will also be needed if the rel itself
	// changes — Badger / TieredStore caches absorb most of the cost.
	return c.forEachKnownRelID(func(id types.RelID) error {
		r1, err := c.lookupRelAtForDiff(id, t1)
		if err != nil {
			return err
		}
		r2, err := c.lookupRelAtForDiff(id, t2)
		if err != nil {
			return err
		}
		switch {
		case r1 == nil && r2 != nil:
			if h.OnRelCreated != nil {
				return h.OnRelCreated(r2)
			}
		case r1 != nil && r2 == nil:
			if h.OnRelDeleted != nil {
				return h.OnRelDeleted(r1)
			}
		case r1 != nil && r2 != nil:
			if relHash(r1) != relHash(r2) {
				if h.OnRelUpdated != nil {
					return h.OnRelUpdated(r1, r2)
				}
			}
		}
		return nil
	})
}

// lookupNodeAtForDiff resolves the version of node id valid at t, returning
// (nil, nil) when no version covers t (entity not yet created or already
// deleted at that instant). All other errors are propagated. Used by
// DiffSnapshotsCallback so a missing-at-this-time signal is distinguishable
// from a real I/O error without sprinkling errors.Is checks at every call
// site.
func (c *Core) lookupNodeAtForDiff(id types.NodeID, t types.Instant) (*types.Node, error) {
	n, err := c.Temporal.NodeAt(id, t)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return n, nil
}

// lookupRelAtForDiff resolves the version of relationship id valid at t,
// applying the same endpoint-validity filter that snapshotAt uses: a
// relationship is reported as "present at t" only when its start and end
// nodes are both valid at t. Returns (nil, nil) when the rel itself is
// absent at t or either endpoint is missing. All other errors are
// propagated.
func (c *Core) lookupRelAtForDiff(id types.RelID, t types.Instant) (*types.Relationship, error) {
	r, err := c.Temporal.RelAt(id, t)
	if err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
			return nil, nil
		}
		return nil, err
	}
	// Endpoint filter — mirror snapshotAt's "both endpoints valid" rule.
	startOK, err := c.nodeExistsAt(r.StartNodeID(), t)
	if err != nil {
		return nil, err
	}
	if !startOK {
		return nil, nil
	}
	endOK, err := c.nodeExistsAt(r.EndNodeID(), t)
	if err != nil {
		return nil, err
	}
	if !endOK {
		return nil, nil
	}
	return r, nil
}

// nodeExistsAt reports whether some version of the node is valid at t.
// ErrNoVersionValidAt and ErrNodeNotFound both map to false; every other
// error propagates so a transient backend failure does not silently
// reclassify entities as "deleted" in the diff stream.
func (c *Core) nodeExistsAt(id types.NodeID, t types.Instant) (bool, error) {
	if _, err := c.Temporal.NodeAt(id, t); err != nil {
		if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// nodeHash returns the integrity hash of a node, or empty string if unset.
func nodeHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// relHash returns the integrity hash of a relationship, or empty string if unset.
func relHash(r *types.Relationship) string {
	if ig := r.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}
