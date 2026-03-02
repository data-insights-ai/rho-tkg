package graph

import (
	"errors"

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
// History-aware: includes deleted nodes that were valid at time t by consulting
// version history in addition to current entities.
func (g *Graph) GetNodesValidAt(t types.Instant) ([]*types.Node, error) {
	allIDs, err := g.allKnownNodeIDs()
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for id := range allIDs {
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

// GetRelationshipsValidAt returns all relationships valid at the given instant.
// History-aware: includes deleted relationships that were valid at time t.
func (g *Graph) GetRelationshipsValidAt(t types.Instant) ([]*types.Relationship, error) {
	allIDs, err := g.allKnownRelIDs()
	if err != nil {
		return nil, err
	}

	var result []*types.Relationship
	for id := range allIDs {
		r, err := g.GetRelAt(id, t)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, r)
	}

	sortRelsByID(result)
	return result, nil
}

// GetNodesByLabelValidAt returns all nodes with the given label that are valid
// at the given instant. Returns nil if the label is not registered.
// Uses temporal query push-down — the Store filters before deep-copy.
func (g *Graph) GetNodesByLabelValidAt(label string, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	return g.store.NodesByLabel(tok, QueryOpts{ValidAt: t})
}

// GetNodesValidDuring returns all nodes whose validity overlaps [start, end).
// History-aware: includes deleted or updated nodes that had any version valid
// during the interval.
func (g *Graph) GetNodesValidDuring(start, end types.Instant) ([]*types.Node, error) {
	allIDs, err := g.allKnownNodeIDs()
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for id := range allIDs {
		n, err := g.getNodeVersionDuring(id, start, end)
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

// GetRelationshipsValidDuring returns all relationships whose validity overlaps [start, end).
// History-aware: includes deleted or updated relationships.
func (g *Graph) GetRelationshipsValidDuring(start, end types.Instant) ([]*types.Relationship, error) {
	allIDs, err := g.allKnownRelIDs()
	if err != nil {
		return nil, err
	}

	var result []*types.Relationship
	for id := range allIDs {
		r, err := g.getRelVersionDuring(id, start, end)
		if err != nil {
			if errors.Is(err, ErrNoVersionValidAt) || errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		result = append(result, r)
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

// allKnownNodeIDs returns the union of current node IDs and history node IDs.
// Uses AllNodeIDs (ID-only, no entity deserialization) instead of AllNodes
// to avoid O(N) deep-copy waste when only IDs are needed.
func (g *Graph) allKnownNodeIDs() (map[snowflake.ID]struct{}, error) {
	currentIDs, err := g.store.AllNodeIDs(QueryOpts{})
	if err != nil {
		return nil, err
	}
	ids := make(map[snowflake.ID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		ids[id] = struct{}{}
	}

	histIDs, err := g.store.AllNodeHistoryIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range histIDs {
		ids[id] = struct{}{}
	}

	return ids, nil
}

// allKnownRelIDs returns the union of current relationship IDs and history relationship IDs.
// Uses AllRelIDs (ID-only, no entity deserialization) instead of AllRelationships.
func (g *Graph) allKnownRelIDs() (map[snowflake.ID]struct{}, error) {
	currentIDs, err := g.store.AllRelIDs(QueryOpts{})
	if err != nil {
		return nil, err
	}
	ids := make(map[snowflake.ID]struct{}, len(currentIDs))
	for _, id := range currentIDs {
		ids[id] = struct{}{}
	}

	histIDs, err := g.store.AllRelHistoryIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range histIDs {
		ids[id] = struct{}{}
	}

	return ids, nil
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
// Acquires g.mu.RLock to prevent torn reads from concurrent Batch execution.
func (g *Graph) Snapshot(t types.Instant) (*GraphSnapshot, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

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

// --- Combined label + property + temporal queries ---

// NodesByLabelPropertyAndTime returns nodes matching the label and property value
// that are valid at the given instant. Composes the property index (or fallback
// scan) with temporal filtering. Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyAndTime(label, key string, value any, t types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}

	candidates, err := g.store.NodesByLabelAndProperty(tok, key, value, QueryOpts{})
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for _, n := range candidates {
		if g.isNodeValidAt(n, t) {
			result = append(result, n)
		}
	}
	return result, nil
}

// NodesByLabelPropertyDuring returns nodes matching the label and property value
// whose validity overlaps [start, end). Composes the property index (or fallback
// scan) with temporal interval filtering. Returns nil if the label is not registered.
func (g *Graph) NodesByLabelPropertyDuring(label, key string, value any, start, end types.Instant) ([]*types.Node, error) {
	tok, ok := g.labels.Lookup(label)
	if !ok {
		return nil, nil
	}

	candidates, err := g.store.NodesByLabelAndProperty(tok, key, value, QueryOpts{})
	if err != nil {
		return nil, err
	}

	var result []*types.Node
	for _, n := range candidates {
		if g.isNodeValidDuring(n, start, end) {
			result = append(result, n)
		}
	}
	return result, nil
}
