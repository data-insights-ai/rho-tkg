package graph

import (
	"errors"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// GraphSnapshot represents the complete graph state at a point in time.
type GraphSnapshot struct {
	Timestamp     types.Instant
	Nodes         []*types.Node
	Relationships []*types.Relationship
	NodeCount     int
	RelCount      int
}

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

// --- Internal helpers ---

// nodeValidFrom returns the effective valid-from time for a node.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) nodeValidFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(g.nodeIDGen.CreatedAt(n.InternalID().SnowflakeID()).UnixMilli())
}

// relValidFrom returns the effective valid-from time for a relationship.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) relValidFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(g.relIDGen.CreatedAt(r.InternalID().SnowflakeID()).UnixMilli())
}

// isNodeValidAt checks if a node is valid at the given instant.
// Valid when: effectiveValidFrom <= t AND (ValidTo == 0 OR ValidTo > t).
func (g *Graph) isNodeValidAt(n *types.Node, t types.Instant) bool {
	from := g.nodeValidFrom(n)
	if from > t {
		return false
	}
	if tm := n.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// isRelValidAt checks if a relationship is valid at the given instant.
func (g *Graph) isRelValidAt(r *types.Relationship, t types.Instant) bool {
	from := g.relValidFrom(r)
	if from > t {
		return false
	}
	if tm := r.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// --- Public temporal query methods ---

// GetNodesValidAt returns all nodes valid at the given instant.
// History-aware: includes deleted nodes that were valid at time t by consulting
// version history in addition to current entities.
func (g *Graph) GetNodesValidAt(t types.Instant) ([]*types.Node, error) {
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id snowflake.ID) error {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// GetRelationshipsValidAt returns all relationships valid at the given instant.
// History-aware: includes deleted relationships that were valid at time t.
func (g *Graph) GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error) {
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id snowflake.ID) error {
		r, err := g.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortRelsByID(result)
	return result, nil
}

// GetNodesByLabelValidAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
// History-aware: includes historical versions whose label set matched at t.
func (g *Graph) GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id snowflake.ID) error {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		if n.HasLabelTokenRaw(tok) {
			result = append(result, n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// GetNodesValidDuring returns all nodes whose validity overlaps [start, end).
// History-aware: includes deleted or updated nodes that had any version valid
// during the interval.
func (g *Graph) GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error) {
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id snowflake.ID) error {
		n, err := g.getNodeVersionDuring(id, start, end)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// GetRelationshipsValidDuring returns all relationships whose validity overlaps [start, end).
// History-aware: includes deleted or updated relationships.
func (g *Graph) GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error) {
	var result []*types.Relationship
	err := g.forEachKnownRelID(func(id snowflake.ID) error {
		r, err := g.getRelVersionDuring(id, start, end)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortRelsByID(result)
	return result, nil
}

// GetNodeAt returns the version of a node that was valid at the given instant.
// Builds the full version chain (history + current) and finds the version
// whose validity period contains t.
//
// Version validity periods are derived from UpdatedAt timestamps:
//   - Genesis version: starts at nodeValidFrom (snowflake ID time or explicit ValidFrom)
//   - Subsequent versions: start at their UpdatedAt timestamp
//   - Each version's end time is the next version's start time (or open-ended for current)
//
// Explicit ValidFrom/ValidTo on the entity override derived values.
//
// Handles deleted entities: if the current node is gone but history exists,
// version resolution proceeds using only the history chain. The last entry
// in a deleted entity's history has ValidTo set (tombstone).
//
// Returns ErrNodeNotFound if the node never existed (no current, no history).
// Returns ErrNoVersionValidAt if no version covers the given time.
func (g *Graph) GetNodeAt(id snowflake.ID, t types.Instant) (*types.Node, error) {
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrNodeNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return g.resolveNodeVersionAt(chain, t)
}

// resolveNodeVersionAt finds the version valid at time t from a pre-built chain.
func (g *Graph) resolveNodeVersionAt(chain []*types.Node, t types.Instant) (*types.Node, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := g.nodeVersionBounds(chain, i)

		// Check: vStart <= t AND (vEnd == 0 OR vEnd > t).
		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, ErrNoVersionValidAt
}

// nodeVersionBounds computes the effective [vStart, vEnd) for chain[i].
func (g *Graph) nodeVersionBounds(chain []*types.Node, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	// Determine version start.
	if entry.Version() == 0 {
		vStart = g.nodeValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = g.nodeValidFrom(entry)
		}
	}

	// Determine version end.
	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = g.nodeValidFrom(next)
		}
	}
	// vEnd == 0 means open-ended (current version).

	// Explicit ValidFrom/ValidTo override derived values.
	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

// GetRelAt returns the version of a relationship that was valid at the given instant.
// Mirrors GetNodeAt for relationships. Handles deleted entities by consulting
// version history when the current relationship is gone.
//
// Returns ErrRelNotFound if the relationship never existed (no current, no history).
// Returns ErrNoVersionValidAt if no version covers the given time.
func (g *Graph) GetRelAt(id snowflake.ID, t types.Instant) (*types.Relationship, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}
	// current may be nil for deleted entities.

	history, err := g.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrRelNotFound
	}

	// Build chain: history (ascending version order) + current (if exists).
	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	return g.resolveRelVersionAt(chain, t)
}

// resolveRelVersionAt finds the version valid at time t from a pre-built chain.
func (g *Graph) resolveRelVersionAt(chain []*types.Relationship, t types.Instant) (*types.Relationship, error) {
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		vStart, vEnd := g.relVersionBounds(chain, i)

		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}
	return nil, ErrNoVersionValidAt
}

// relVersionBounds computes the effective [vStart, vEnd) for chain[i].
func (g *Graph) relVersionBounds(chain []*types.Relationship, i int) (types.Instant, types.Instant) {
	entry := chain[i]
	var vStart, vEnd types.Instant

	if entry.Version() == 0 {
		vStart = g.relValidFrom(entry)
	} else {
		if tm := entry.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vStart = tm.UpdatedAt
		} else {
			vStart = g.relValidFrom(entry)
		}
	}

	if i < len(chain)-1 {
		next := chain[i+1]
		if tm := next.Temporal(); tm != nil && tm.UpdatedAt != 0 {
			vEnd = tm.UpdatedAt
		} else {
			vEnd = g.relValidFrom(next)
		}
	}

	if tm := entry.Temporal(); tm != nil {
		if tm.ValidFrom != 0 {
			vStart = tm.ValidFrom
		}
		if tm.ValidTo != 0 {
			vEnd = tm.ValidTo
		}
	}

	return vStart, vEnd
}

// --- Private helpers for history-aware queries ---

// forEachKnownNodeID collects the union of current + history node IDs
// via lazy ForEach iteration, then calls fn for each unique ID.
// Two-phase: collect (under store locks), then process (locks released).
// This avoids materializing all IDs from all shards into giant slices.
//
// Memory note: the seen-map materialises O(N) IDs where N = total node count
// (current + historical). This is intentional: Phase 1 must hold store locks
// for consistency, but fn (Phase 2) may re-enter the store, which would
// deadlock. The map is the unavoidable bridge between the two phases.
// For history-unaware queries over only current entities, prefer the
// streaming ForEach iterators in the Store interface directly.
func (g *Graph) forEachKnownNodeID(fn func(snowflake.ID) error) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: collect unique IDs (no store method calls in callbacks — lock reentrancy).
	if err := g.store.ForEachNodeID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := g.store.ForEachNodeHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}

	// Phase 2: process (store locks released, safe to call GetNodeAt etc.).
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// forEachKnownRelID collects the union of current + history relationship IDs
// via lazy ForEach iteration, then calls fn for each unique ID.
// Two-phase: collect (under store locks), then process (locks released).
//
// Memory note: same O(N) materialisation trade-off as forEachKnownNodeID.
// See that function's doc comment for the rationale.
func (g *Graph) forEachKnownRelID(fn func(snowflake.ID) error) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: collect unique IDs.
	if err := g.store.ForEachRelID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}
	if err := g.store.ForEachRelHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	}); err != nil {
		return err
	}

	// Phase 2: process (store locks released).
	for id := range seen {
		if err := fn(id); err != nil {
			return err
		}
	}
	return nil
}

// getNodeVersionDuring returns the most recent version of a node whose validity
// overlaps [start, end). Returns ErrNoVersionValidAt if none overlap.
func (g *Graph) getNodeVersionDuring(id snowflake.ID, start, end types.Instant) (*types.Node, error) {
	current, err := g.store.GetNode(id)
	if err != nil && !errors.Is(err, ErrNodeNotFound) {
		return nil, err
	}

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrNodeNotFound
	}

	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	// Find the most recent version overlapping [start, end).
	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := g.nodeVersionBounds(chain, i)
		// Overlap: vStart < end AND (vEnd == 0 OR vEnd > start).
		if vStart < end && (vEnd == 0 || vEnd > start) {
			return chain[i], nil
		}
	}

	return nil, ErrNoVersionValidAt
}

// getRelVersionDuring returns the most recent version of a relationship whose
// validity overlaps [start, end). Returns ErrNoVersionValidAt if none overlap.
func (g *Graph) getRelVersionDuring(id snowflake.ID, start, end types.Instant) (*types.Relationship, error) {
	current, err := g.store.GetRelationship(id)
	if err != nil && !errors.Is(err, ErrRelNotFound) {
		return nil, err
	}

	history, err := g.store.GetRelHistory(id)
	if err != nil {
		return nil, err
	}

	if current == nil && len(history) == 0 {
		return nil, ErrRelNotFound
	}

	chain := make([]*types.Relationship, 0, len(history)+1)
	chain = append(chain, history...)
	if current != nil {
		chain = append(chain, current)
	}

	for i := len(chain) - 1; i >= 0; i-- {
		vStart, vEnd := g.relVersionBounds(chain, i)
		if vStart < end && (vEnd == 0 || vEnd > start) {
			return chain[i], nil
		}
	}

	return nil, ErrNoVersionValidAt
}

// GetNeighborsValidAt returns all neighbor nodes reachable from nodeID via
// relationships that are valid at the given instant, where the neighbor nodes
// themselves are also valid at that instant.
func (g *Graph) GetNeighborsValidAt(nodeID snowflake.ID, t types.Instant) ([]*types.Node, error) {
	neighborIDs := make(map[snowflake.ID]struct{})
	err := g.forEachKnownRelID(func(id snowflake.ID) error {
		r, err := g.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				return nil
			}
			return err
		}
		if r.StartNodeID().SnowflakeID() == nodeID {
			neighborIDs[r.EndNodeID().SnowflakeID()] = struct{}{}
		} else if r.EndNodeID().SnowflakeID() == nodeID {
			neighborIDs[r.StartNodeID().SnowflakeID()] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if len(neighborIDs) == 0 {
		return nil, nil
	}

	var result []*types.Node
	for id := range neighborIDs {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, n)
	}
	sortNodesByID(result)
	return result, nil
}

// Snapshot returns a complete graph state at the given instant.
// Relationships are only included if both endpoints are valid at t.
// Acquires g.mu.RLock to prevent torn reads from concurrent Batch execution.
func (g *Graph) Snapshot(t types.Instant) (*GraphSnapshot, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.snapshotAt(t)
}

// snapshotAt computes the graph snapshot at t. It does not acquire g.mu;
// callers that require strong snapshot consistency should hold g.mu.RLock.
func (g *Graph) snapshotAt(t types.Instant) (*GraphSnapshot, error) {
	nodes, err := g.GetNodesValidAt(t)
	if err != nil {
		return nil, err
	}

	// Build set of valid node IDs for endpoint filtering.
	nodeSet := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		nodeSet[n.InternalID().SnowflakeID()] = struct{}{}
	}

	allRels, err := g.GetRelationshipsValidAt(t)
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

	return &GraphSnapshot{
		Timestamp:     t,
		Nodes:         nodes,
		Relationships: rels,
		NodeCount:     len(nodes),
		RelCount:      len(rels),
	}, nil
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
		nodes1[n.InternalID().SnowflakeID()] = n
	}
	nodes2 := make(map[snowflake.ID]*types.Node, len(snap2.Nodes))
	for _, n := range snap2.Nodes {
		nodes2[n.InternalID().SnowflakeID()] = n
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
		rels1[r.InternalID().SnowflakeID()] = r
	}
	rels2 := make(map[snowflake.ID]*types.Relationship, len(snap2.Relationships))
	for _, r := range snap2.Relationships {
		rels2[r.InternalID().SnowflakeID()] = r
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

// --- Combined label + property + temporal queries ---

// NodesByLabelPropertyAndTime returns nodes matching the label and property value
// that are valid at the given instant. History-aware.
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyAndTime(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	targetKey := propertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id snowflake.ID) error {
		n, err := g.GetNodeAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		if !n.HasLabelTokenRaw(tok) {
			return nil
		}
		if v, found := n.GetProperty(key); found && propertyValueKey(v) == targetKey {
			result = append(result, n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}

// NodesByLabelPropertyDuring returns nodes matching the label and property value
// whose validity overlaps [start, end). History-aware.
// Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	targetKey := propertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	var result []*types.Node
	err := g.forEachKnownNodeID(func(id snowflake.ID) error {
		n, err := g.getNodeVersionDuring(id, start, end)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrNodeNotFound) {
				return nil
			}
			return err
		}
		if !n.HasLabelTokenRaw(tok) {
			return nil
		}
		if v, found := n.GetProperty(key); found && propertyValueKey(v) == targetKey {
			result = append(result, n)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sortNodesByID(result)
	return result, nil
}
