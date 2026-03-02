package graph

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// GraphSnapshot represents the complete graph state at a point in time.
type GraphSnapshot struct {
	Timestamp     types.Instant
	Nodes         []*types.Node
	Relationships []*types.Relationship
	NodeCount     int
	RelCount      int
}

// --- Internal helpers ---

// nodeValidFrom returns the effective valid-from time for a node.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) nodeValidFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	// Derive from snowflake ID — always available, millisecond precision.
	parts := g.nodeIDGen.Decompose(n.InternalID().SnowflakeID())
	return types.Instant(snowflakeEpoch.UnixMilli() + parts.Time)
}

// relValidFrom returns the effective valid-from time for a relationship.
// Uses explicit ValidFrom if set, falls back to snowflake ID timestamp.
func (g *Graph) relValidFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	parts := g.relIDGen.Decompose(r.InternalID().SnowflakeID())
	return types.Instant(snowflakeEpoch.UnixMilli() + parts.Time)
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

// isNodeValidDuring checks if a node's validity overlaps [start, end).
// Overlap: effectiveValidFrom < end AND (ValidTo == 0 OR ValidTo > start).
func (g *Graph) isNodeValidDuring(n *types.Node, start, end types.Instant) bool {
	from := g.nodeValidFrom(n)
	if from >= end {
		return false
	}
	if tm := n.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > start
	}
	return true
}

// isRelValidDuring checks if a relationship's validity overlaps [start, end).
func (g *Graph) isRelValidDuring(r *types.Relationship, start, end types.Instant) bool {
	from := g.relValidFrom(r)
	if from >= end {
		return false
	}
	if tm := r.Temporal(); tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > start
	}
	return true
}

// --- Public temporal query methods ---

// GetNodesValidAt returns all nodes valid at the given instant.
// Nodes without explicit temporal metadata derive their valid-from from the
// snowflake ID timestamp and are treated as open-ended (ValidTo=0).
func (g *Graph) GetNodesValidAt(t types.Instant) ([]*types.Node, error) {
	all, err := g.store.AllNodes()
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for _, n := range all {
		if g.isNodeValidAt(n, t) {
			result = append(result, n)
		}
	}
	return result, nil
}

// GetRelationshipsValidAt returns all relationships valid at the given instant.
func (g *Graph) GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error) {
	all, err := g.store.AllRelationships()
	if err != nil {
		return nil, err
	}

	var result []*types.Relationship
	for _, r := range all {
		if g.isRelValidAt(r, t) {
			result = append(result, r)
		}
	}
	return result, nil
}

// GetNodesByLabelValidAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
func (g *Graph) GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	nodes, err := g.store.NodesByLabel(tok)
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for _, n := range nodes {
		if g.isNodeValidAt(n, t) {
			result = append(result, n)
		}
	}
	return result, nil
}

// GetNodesValidDuring returns all nodes whose validity overlaps [start, end).
func (g *Graph) GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error) {
	all, err := g.store.AllNodes()
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for _, n := range all {
		if g.isNodeValidDuring(n, start, end) {
			result = append(result, n)
		}
	}
	return result, nil
}

// GetRelationshipsValidDuring returns all relationships whose validity overlaps [start, end).
func (g *Graph) GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error) {
	all, err := g.store.AllRelationships()
	if err != nil {
		return nil, err
	}

	var result []*types.Relationship
	for _, r := range all {
		if g.isRelValidDuring(r, start, end) {
			result = append(result, r)
		}
	}
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
// Returns ErrNodeNotFound if the node does not exist.
// Returns ErrNoVersionValidAt if no version covers the given time.
func (g *Graph) GetNodeAt(id snowflake.ID, t types.Instant) (*types.Node, error) {
	current, err := g.store.GetNode(id)
	if err != nil {
		return nil, err
	}

	history, err := g.store.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}

	// Build chain: history (ascending version order) + current.
	chain := make([]*types.Node, 0, len(history)+1)
	chain = append(chain, history...)
	chain = append(chain, current)

	// Compute each version's validity period and find a match.
	for i := len(chain) - 1; i >= 0; i-- {
		entry := chain[i]
		var vStart, vEnd types.Instant

		// Determine version start.
		if i == 0 {
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

		// Check: vStart <= t AND (vEnd == 0 OR vEnd > t).
		if vStart <= t && (vEnd == 0 || vEnd > t) {
			return entry, nil
		}
	}

	return nil, ErrNoVersionValidAt
}

// GetNeighborsValidAt returns all neighbor nodes reachable from nodeID via
// relationships that are valid at the given instant, where the neighbor nodes
// themselves are also valid at that instant.
func (g *Graph) GetNeighborsValidAt(nodeID snowflake.ID, t types.Instant) ([]*types.Node, error) {
	outRels, err := g.store.OutgoingRelationships(nodeID, 0)
	if err != nil {
		return nil, err
	}
	inRels, err := g.store.IncomingRelationships(nodeID, 0)
	if err != nil {
		return nil, err
	}

	// Collect neighbor IDs via valid relationships.
	neighborIDs := make(map[snowflake.ID]struct{})
	for _, r := range outRels {
		if g.isRelValidAt(r, t) {
			neighborIDs[r.EndNodeID().SnowflakeID()] = struct{}{}
		}
	}
	for _, r := range inRels {
		if g.isRelValidAt(r, t) {
			neighborIDs[r.StartNodeID().SnowflakeID()] = struct{}{}
		}
	}

	if len(neighborIDs) == 0 {
		return nil, nil
	}

	ids := make([]snowflake.ID, 0, len(neighborIDs))
	for id := range neighborIDs {
		ids = append(ids, id)
	}

	nodes, err := g.store.GetNodesByIDs(ids)
	if err != nil {
		return nil, err
	}

	// Filter to nodes valid at t.
	var result []*types.Node
	for _, n := range nodes {
		if g.isNodeValidAt(n, t) {
			result = append(result, n)
		}
	}
	return result, nil
}

// Snapshot returns a complete graph state at the given instant.
// Relationships are only included if both endpoints are valid at t.
func (g *Graph) Snapshot(t types.Instant) (*GraphSnapshot, error) {
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
