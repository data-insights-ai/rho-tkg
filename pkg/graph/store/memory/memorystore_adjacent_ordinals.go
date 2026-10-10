package memory

import (
	"cmp"
	"slices"
	"sync"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.AdjacentEndpointOrdinalCapability = (*Store)(nil)

// adjacentOrdinalRow is one entry ForEachAdjacentEndpointOrdinal hands out,
// gathered under the store's read lock and emitted after it is released.
// Values only, so a pooled buffer keeps no row alive.
type adjacentOrdinalRow struct {
	rel      types.RelID
	relOrd   uint32
	otherOrd uint32
	other    types.NodeID
}

// adjacentOrdinalBufMax is the largest buffer returned to the pool (entries);
// a hub's larger buffer is left to the collector rather than kept for every
// later call.
const adjacentOrdinalBufMax = 1 << 14

// adjacentOrdinalBufs recycles the per-call buffer (round 4 R3: the door
// allocated three slices per call). A nested call from fn takes its own.
var adjacentOrdinalBufs = sync.Pool{New: func() any { return new([]adjacentOrdinalRow) }}

// ForEachAdjacentEndpointOrdinal streams nid's adjacency in the given
// direction as (relationship, its ordinal, other endpoint, its ordinal), in
// relationship-ID order, from the rows the store holds (no copy): the
// relationship's ordinal from its row, the other endpoint's from the node's
// current row, both read under one read lock for the node's whole adjacency,
// which is released before fn runs. The row-store path allocates nothing per
// call (a pooled buffer); a store with declared segment types takes the
// segment-aware path.
func (ms *Store) ForEachAdjacentEndpointOrdinal(nid types.NodeID, typeToken uint16, incoming bool,
	fn func(rel types.RelID, relOrdinal uint32, other types.NodeID, otherOrdinal uint32) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if len(ms.segTypes) > 0 {
		ms.mu.RUnlock()
		return ms.forEachAdjacentEndpointOrdinalByRows(nid, typeToken, incoming, fn)
	}
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if _, ok := ms.nodes[nid]; !ok {
		ms.mu.RUnlock()
		return ErrNodeNotFound
	}
	set := ms.outIdx[nid]
	if incoming {
		set = ms.inIdx[nid]
	}
	if set.len() == 0 || (typeToken != 0 && len(ms.typeIdx[typeToken]) == 0) {
		ms.mu.RUnlock()
		return nil
	}
	bufp := adjacentOrdinalBufs.Get().(*[]adjacentOrdinalRow)
	rows := (*bufp)[:0]
	add := func(id types.RelID) {
		r := ms.rels[id]
		if incoming {
			if !relationshipMatchesIncoming(r, nid, typeToken) {
				return
			}
		} else if !relationshipMatchesOutgoing(r, nid, typeToken) {
			return
		}
		other := adjacentOther(r, incoming)
		var otherOrd uint32
		if n := ms.nodes[other]; n != nil {
			otherOrd = n.Ordinal()
		}
		rows = append(rows, adjacentOrdinalRow{rel: id, relOrd: r.Ordinal(), otherOrd: otherOrd, other: other})
	}
	if set.m != nil {
		for id := range set.m {
			add(id)
		}
	} else {
		for _, id := range set.ids {
			add(id)
		}
	}
	ms.mu.RUnlock()
	slices.SortFunc(rows, func(a, b adjacentOrdinalRow) int { return cmp.Compare(a.rel, b.rel) })
	defer func() {
		if cap(rows) <= adjacentOrdinalBufMax {
			*bufp = rows[:0]
			adjacentOrdinalBufs.Put(bufp)
		}
	}()
	for _, e := range rows {
		if !fn(e.rel, e.relOrd, e.other, e.otherOrd) {
			return nil
		}
	}
	return nil
}

// forEachAdjacentEndpointOrdinalByRows is the door for a store with declared
// segment types: the adjacency resolved through the segment-aware scan, the
// other endpoints' ordinals read under one lock.
func (ms *Store) forEachAdjacentEndpointOrdinalByRows(nid types.NodeID, typeToken uint16, incoming bool,
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
