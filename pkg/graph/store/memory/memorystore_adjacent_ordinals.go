package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.AdjacentEndpointOrdinalCapability = (*Store)(nil)

// ForEachAdjacentEndpointOrdinal streams nid's adjacency in the given
// direction as (relationship, its ordinal, other endpoint, its ordinal), in
// relationship-ID order, from the rows the store holds (no copy): the
// relationship's ordinal from its row, the other endpoint's from the node's
// current row, read under one lock for the node's whole adjacency.
func (ms *Store) ForEachAdjacentEndpointOrdinal(nid types.NodeID, typeToken uint16, incoming bool,
	fn func(rel types.RelID, relOrdinal uint32, other types.NodeID, otherOrdinal uint32) bool) error {
	var rows []*types.Relationship
	if err := ms.forEachAdjacentRel(nid, typeToken, incoming, func(r *types.Relationship) bool {
		rows = append(rows, r)
		return true
	}); err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	others := make([]uint32, len(rows))
	ms.mu.RLock()
	for i, r := range rows {
		if n := ms.nodes[adjacentOther(r, incoming)]; n != nil {
			others[i] = n.Ordinal()
		}
	}
	ms.mu.RUnlock()
	for i, r := range rows {
		if !fn(r.InternalID(), r.Ordinal(), adjacentOther(r, incoming), others[i]) {
			return nil
		}
	}
	return nil
}

func adjacentOther(r *types.Relationship, incoming bool) types.NodeID {
	if incoming {
		return r.StartNodeID()
	}
	return r.EndNodeID()
}
