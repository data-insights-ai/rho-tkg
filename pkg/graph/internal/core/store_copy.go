package core

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"

// Deep-copy helpers used when crossing the trust boundary from a store that
// returns aliased internal rows. Each helper preserves nil input as nil so
// callers can pass through untouched empty results.

func copyNodeRows(rows []*types.Node) []*types.Node {
	if rows == nil {
		return nil
	}
	out := make([]*types.Node, len(rows))
	for i, n := range rows {
		out[i] = n.DeepCopy()
	}
	return out
}

func copyRelationshipRows(rows []*types.Relationship) []*types.Relationship {
	if rows == nil {
		return nil
	}
	out := make([]*types.Relationship, len(rows))
	for i, r := range rows {
		out[i] = r.DeepCopy()
	}
	return out
}

func copyRelationshipRowMap(rows map[types.NodeID][]*types.Relationship) map[types.NodeID][]*types.Relationship {
	if rows == nil {
		return nil
	}
	out := make(map[types.NodeID][]*types.Relationship, len(rows))
	for nodeID, rels := range rows {
		out[nodeID] = copyRelationshipRows(rels)
	}
	return out
}
