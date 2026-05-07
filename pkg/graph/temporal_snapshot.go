package graph

import (
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
		nodeSet[n.ID().SnowflakeID()] = struct{}{}
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
