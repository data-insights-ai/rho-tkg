package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ExAdjacencyCapability = (*Store)(nil)

// Ex-adjacency (store.ExAdjacencyCapability, backlog 50): exOutIdx / exInIdx
// hold, per node, the relationships that named it as start / end in a row the
// live adjacency (outIdx / inIdx) no longer ties to it. deleteRelLocked moves
// both legs of the deleted row there (the adjacency entries it frees);
// putRelHistoryRowLocked adds both legs of a history row whose endpoints are not
// the memtable row's. A purge or an exact erasure drops a removed
// relationship's legs (dropExAdjRowsLocked). Under ms.mu.

// addExAdjLocked records both legs of r.
func (ms *Store) addExAdjLocked(id types.RelID, r *types.Relationship) {
	if ms.exOutIdx == nil {
		ms.exOutIdx = make(map[types.NodeID]*adjSet)
		ms.exInIdx = make(map[types.NodeID]*adjSet)
	}
	addAdjLocked(ms.exOutIdx, r.StartNodeID(), id)
	addAdjLocked(ms.exInIdx, r.EndNodeID(), id)
}

// noteHistoryRowExAdjLocked adds both legs of history row r of id unless the
// memtable row names the same endpoints (the live adjacency reaches it). A row
// of a relationship whose live row sits in a sealed segment is added too: a
// superset.
func (ms *Store) noteHistoryRowExAdjLocked(id types.RelID, r *types.Relationship) {
	if cur, ok := ms.rels[id]; ok && cur.StartNodeID() == r.StartNodeID() && cur.EndNodeID() == r.EndNodeID() {
		return
	}
	ms.addExAdjLocked(id, r)
}

// relRowsLocked returns id's memtable row (if any) and history rows: the rows
// whose legs a purge must drop.
func (ms *Store) relRowsLocked(id types.RelID) []*types.Relationship {
	var rows []*types.Relationship
	if r, ok := ms.rels[id]; ok {
		rows = append(rows, r)
	}
	for _, h := range ms.relHistory[id] {
		rows = append(rows, h)
	}
	return rows
}

// dropExAdjRowsLocked removes the legs of rows of a purged or erased id.
func (ms *Store) dropExAdjRowsLocked(id types.RelID, rows []*types.Relationship) {
	for _, r := range rows {
		removeAdjLocked(ms.exOutIdx, r.StartNodeID(), id)
		removeAdjLocked(ms.exInIdx, r.EndNodeID(), id)
	}
}

// dropExAdjRelEverywhereLocked removes id from every node's ex-adjacency
// (exact erasure: a relationship deleted without history rows left legs no
// row names any more). O(ex-adjacency size).
func (ms *Store) dropExAdjRelEverywhereLocked(id types.RelID) {
	for _, idx := range []map[types.NodeID]*adjSet{ms.exOutIdx, ms.exInIdx} {
		for nid, s := range idx {
			if s.remove(id) && s.len() == 0 {
				delete(idx, nid)
			}
		}
	}
}

// dropExAdjNodeLocked removes a purged or erased node's ex-adjacency.
func (ms *Store) dropExAdjNodeLocked(nid types.NodeID) {
	delete(ms.exOutIdx, nid)
	delete(ms.exInIdx, nid)
}

// ForEachExAdjacentRelID implements store.ExAdjacencyCapability (a snapshot
// taken under the lock, fn called outside it).
func (ms *Store) ForEachExAdjacentRelID(nid types.NodeID, incoming bool, fn func(types.RelID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		ms.mu.RUnlock()
		return err
	}
	idx := ms.exOutIdx
	if incoming {
		idx = ms.exInIdx
	}
	var ids []types.RelID
	for id := range idx[nid].all() {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// NoteExAdjacentRel implements store.ExAdjacencyCapability.
func (ms *Store) NoteExAdjacentRel(id types.RelID, start, end types.NodeID) error {
	if ms == nil {
		return ErrNilStore
	}
	if err := storecontract.ValidateRelationshipIndexEntry(start, end, 1, id); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	ms.addExAdjLocked(id, types.NewRelationship(id, 1, start, end))
	return nil
}
