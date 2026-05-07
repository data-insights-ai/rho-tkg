package graph

import (
	snowflake "github.com/bds421/rho-snowflake-2026"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// NodeUpdate pairs the before and after states of an updated node.
type NodeUpdate struct {
	Before *types.Node
	After  *types.Node
}

// RelUpdate pairs the before and after states of an updated relationship.
type RelUpdate struct {
	Before *types.Relationship
	After  *types.Relationship
}

// SnapshotDiff describes entity changes between two points in time.
// Entities valid at T2 but not T1 → Created.
// Entities valid at both with a different integrity hash → Updated.
// Entities valid at T1 but not T2 → Deleted.
type SnapshotDiff struct {
	T1, T2       types.Instant
	NodesCreated []*types.Node
	NodesUpdated []NodeUpdate
	NodesDeleted []*types.Node
	RelsCreated  []*types.Relationship
	RelsUpdated  []RelUpdate
	RelsDeleted  []*types.Relationship
}

// DiffSnapshots returns the set of entity changes between t1 and t2.
// Entities valid at T2 but not T1 → Created.
// Entities valid at both but with different integrity hash → Updated.
// Entities valid at T1 but not T2 → Deleted.
// Returns ErrInvalidTimeRange if t1 >= t2 or either is zero.
//
// Note: the two snapshots are read independently without holding g.mu. A
// concurrent backdated write that commits between the two reads may appear as
// a spurious Created/Deleted entry. This is an acceptable trade-off against
// blocking all writes for the full O(N) snapshot duration.
//
// TODO(v3.1.0): streaming DiffSnapshots to avoid O(N) RAM materialization.
func (g *Graph) DiffSnapshots(t1, t2 types.Instant) (*SnapshotDiff, error) {
	if t1 == 0 || t2 == 0 || t1 >= t2 {
		return nil, ErrInvalidTimeRange
	}

	// No g.mu.RLock — see doc comment above.
	snap1, err := g.snapshotAt(t1)
	if err != nil {
		return nil, err
	}
	snap2, err := g.snapshotAt(t2)
	if err != nil {
		return nil, err
	}

	return buildDiff(t1, t2, snap1, snap2), nil
}

// buildDiff computes the SnapshotDiff between two snapshots.
func buildDiff(t1, t2 types.Instant, snap1, snap2 *GraphSnapshot) *SnapshotDiff {
	diff := &SnapshotDiff{T1: t1, T2: t2}

	// --- Nodes ---
	nodes1 := make(map[snowflake.ID]*types.Node, len(snap1.Nodes))
	for _, n := range snap1.Nodes {
		nodes1[n.ID().SnowflakeID()] = n
	}
	nodes2 := make(map[snowflake.ID]*types.Node, len(snap2.Nodes))
	for _, n := range snap2.Nodes {
		nodes2[n.ID().SnowflakeID()] = n
	}

	for id, n2 := range nodes2 {
		if n1, ok := nodes1[id]; ok {
			// Present in both: compare hashes.
			h1 := nodeHash(n1)
			h2 := nodeHash(n2)
			if h1 != h2 {
				diff.NodesUpdated = append(diff.NodesUpdated, NodeUpdate{Before: n1, After: n2})
			}
			// identical hash → unchanged; skip
		} else {
			// Only in snap2 → Created.
			diff.NodesCreated = append(diff.NodesCreated, n2)
		}
	}
	for id, n1 := range nodes1 {
		if _, ok := nodes2[id]; !ok {
			// Only in snap1 → Deleted.
			diff.NodesDeleted = append(diff.NodesDeleted, n1)
		}
	}

	// --- Relationships ---
	rels1 := make(map[snowflake.ID]*types.Relationship, len(snap1.Relationships))
	for _, r := range snap1.Relationships {
		rels1[r.ID().SnowflakeID()] = r
	}
	rels2 := make(map[snowflake.ID]*types.Relationship, len(snap2.Relationships))
	for _, r := range snap2.Relationships {
		rels2[r.ID().SnowflakeID()] = r
	}

	for id, r2 := range rels2 {
		if r1, ok := rels1[id]; ok {
			h1 := relHash(r1)
			h2 := relHash(r2)
			if h1 != h2 {
				diff.RelsUpdated = append(diff.RelsUpdated, RelUpdate{Before: r1, After: r2})
			}
		} else {
			diff.RelsCreated = append(diff.RelsCreated, r2)
		}
	}
	for id, r1 := range rels1 {
		if _, ok := rels2[id]; !ok {
			diff.RelsDeleted = append(diff.RelsDeleted, r1)
		}
	}

	return diff
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
