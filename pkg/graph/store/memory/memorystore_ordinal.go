package memory

import (
	"math"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.OrdinalCapability = (*Store)(nil)

// Dense ordinals (store.OrdinalCapability). The ordinal lives in the stored
// row (types.Node / types.Relationship), so the memory store keeps no map
// for it: every write derives the row's ordinal from the current row of the
// same ID, else from one of its history rows, else assigns the next one.
// Every write of a current or history row goes through storedNode /
// storedRel / historyNode / historyRel, under ms.mu held exclusively.
//
// A current relationship row also carries its endpoints' ordinals
// (storedRel); a history row carries none.
//
// Not covered: a relationship of a declared segment type (ADR-0011) carries
// 0 (its own and its endpoints') — its sealed rows are columns, not rows, and a row that moves between
// the memtable and a segment would change ordinal.

// MaxNodeOrdinal returns the largest node ordinal handed out (0 for none or a
// closed store).
func (ms *Store) MaxNodeOrdinal() uint32 {
	if ms == nil {
		return 0
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.closed {
		return 0
	}
	return ms.maxNodeOrdinal.Load()
}

// MaxRelOrdinal returns the largest relationship ordinal handed out.
func (ms *Store) MaxRelOrdinal() uint32 {
	if ms == nil {
		return 0
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if ms.closed {
		return 0
	}
	return ms.maxRelOrdinal.Load()
}

func (ms *Store) nodeOrdinalLocked(id types.NodeID) uint32 {
	if n := ms.nodes[id]; n != nil && n.Ordinal() != 0 {
		return n.Ordinal()
	}
	for _, h := range ms.nodeHistory[id] {
		if o := h.Ordinal(); o != 0 {
			return o
		}
	}
	next := ms.maxNodeOrdinal.Load()
	if next == math.MaxUint32 {
		return 0
	}
	ms.maxNodeOrdinal.Store(next + 1)
	return next + 1
}

func (ms *Store) relOrdinalLocked(id types.RelID, typeToken uint16) uint32 {
	if ms.segTypes[typeToken] != nil {
		return 0
	}
	if r := ms.rels[id]; r != nil && r.Ordinal() != 0 {
		return r.Ordinal()
	}
	for _, h := range ms.relHistory[id] {
		if o := h.Ordinal(); o != 0 {
			return o
		}
	}
	next := ms.maxRelOrdinal.Load()
	if next == math.MaxUint32 {
		return 0
	}
	ms.maxRelOrdinal.Store(next + 1)
	return next + 1
}

// storedNode is the frozen current row the store keeps for n.
func (ms *Store) storedNode(n *types.Node) *types.Node {
	return n.CompactFrozenCopyWithOrdinal(ms.nodeOrdinalLocked(n.ID()))
}

// storedRel is the frozen current row the store keeps for r, carrying its
// endpoints' ordinals from their current rows (0 for a declared segment type,
// whose rows carry no ordinal, and for an endpoint the store holds no current
// row for). An endpoint keeps its ordinal while it has a current row, and a
// current relationship's endpoints have theirs, so the stored values stay
// right for the row's life.
func (ms *Store) storedRel(r *types.Relationship) *types.Relationship {
	ord := ms.relOrdinalLocked(r.ID(), r.TypeToken().Value())
	var start, end uint32
	if ms.segTypes[r.TypeToken().Value()] == nil {
		start, end = ms.nodes[r.StartNodeID()].Ordinal(), ms.nodes[r.EndNodeID()].Ordinal()
	}
	return r.CompactFrozenCopyWithOrdinals(ord, start, end)
}

// historyNode is the history row the store keeps for a version of n.
func (ms *Store) historyNode(n *types.Node) *types.Node {
	return n.DeepCopyWithOrdinal(ms.nodeOrdinalLocked(n.ID()))
}

// historyRel is the history row the store keeps for a version of r.
func (ms *Store) historyRel(r *types.Relationship) *types.Relationship {
	return r.DeepCopyWithOrdinals(ms.relOrdinalLocked(r.ID(), r.TypeToken().Value()), 0, 0)
}

// HasOrdinals is true: the memory store numbers every node and every
// relationship of an undeclared type.
func (ms *Store) HasOrdinals() bool { return ms != nil }
