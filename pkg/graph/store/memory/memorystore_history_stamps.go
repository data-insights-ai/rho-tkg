package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// History stamps (store.HistoryStampsCapability, backlog 30): per ID, the fold
// (store.FoldTxStamps) of its history rows, cached after the first call and
// kept exact without hooking the delete doors.
//
// An entry carries n, the ID's history row count when it was computed, and is
// used only while the ID still has n rows. Every history row write goes
// through putNodeHistoryRowLocked / putRelHistoryRowLocked, which folds the new
// row into a valid entry (count +1, or the same count for an overwrite) and
// drops an entry it cannot keep exact (an overwrite with lower stamps than the
// cached max: the overwritten row may have held it). Every other history
// change (trim, truncation, compaction, retention purge, exact erasure, the
// whole-map drops) only removes rows, so the count falls below n and the entry
// is stale from then on: a later row write sees count != n and drops it, a
// read sees count != n and recomputes. The count cannot climb back to n
// without such a write. A read recomputes in O(history of the ID) under the
// store's read lock and caches; a hit is a map lookup.
//
// All under ms.mu: writers hold it exclusively, readers share it and take
// histStampsMu for the cache maps.

// histStampsEntry is one ID's cached fold, valid while the ID has n rows.
type histStampsEntry struct {
	from, to types.Instant
	n        int
}

// putNodeHistoryRowLocked stores row as node nid's history version v: the one
// place a node history row is written. Caller holds ms.mu (write).
func (ms *Store) putNodeHistoryRowLocked(nid types.NodeID, v uint32, row *types.Node) {
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	before := len(inner)
	_, overwrite := inner[v]
	inner[v] = ms.historyNode(row)
	f, t, d := row.TxStamps()
	ms.histStampsMu.Lock()
	ms.nodeHistStamps = noteHistStamps(ms.nodeHistStamps, nid, before, len(inner), overwrite, f, t, d)
	ms.histStampsMu.Unlock()
}

// putRelHistoryRowLocked is putNodeHistoryRowLocked for relationships.
func (ms *Store) putRelHistoryRowLocked(rid types.RelID, v uint32, row *types.Relationship) {
	inner, ok := ms.relHistory[rid]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[rid] = inner
	}
	before := len(inner)
	_, overwrite := inner[v]
	inner[v] = ms.historyRel(row)
	f, t, d := row.TxStamps()
	ms.histStampsMu.Lock()
	ms.relHistStamps = noteHistStamps(ms.relHistStamps, rid, before, len(inner), overwrite, f, t, d)
	ms.histStampsMu.Unlock()
}

// noteHistStamps folds one written row into id's entry, if it has a valid one.
// before / after are the ID's row counts around the write. Caller holds
// histStampsMu.
func noteHistStamps[ID comparable](m map[ID]histStampsEntry, id ID, before, after int, overwrite bool, txFrom, txTo, deletedAt types.Instant) map[ID]histStampsEntry {
	e, ok := m[id]
	if !ok {
		return m
	}
	if e.n != before {
		delete(m, id) // rows were removed since it was computed
		return m
	}
	if overwrite && (txFrom < e.from || max(txTo, deletedAt) < e.to) {
		// The overwritten row may have held a max this row does not reach.
		delete(m, id)
		return m
	}
	from, to := storecontract.FoldTxStamps(e.from, e.to, txFrom, txTo, deletedAt)
	m[id] = histStampsEntry{from: from, to: to, n: after}
	return m
}

// invalidateNodeHistStampsLocked drops nid's entry (a test-only row rewrite).
// Caller holds ms.mu (write).
func (ms *Store) invalidateNodeHistStampsLocked(nid types.NodeID) {
	ms.histStampsMu.Lock()
	delete(ms.nodeHistStamps, nid)
	ms.histStampsMu.Unlock()
}

// NodeHistoryStamps returns the newest TxFrom and TxTo-or-DeletedAt over the
// node's history rows and whether it has any (store.HistoryStampsCapability):
// the fold of GetNodeHistory(id), without copying the rows.
func (ms *Store) NodeHistoryStamps(id types.NodeID) (types.Instant, types.Instant, bool, error) {
	if ms == nil {
		return 0, 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return 0, 0, false, err
	}
	inner := ms.nodeHistory[id]
	if len(inner) == 0 {
		return 0, 0, false, nil
	}
	ms.histStampsMu.RLock()
	e, ok := ms.nodeHistStamps[id]
	ms.histStampsMu.RUnlock()
	if ok && e.n == len(inner) {
		return e.from, e.to, true, nil
	}
	var from, to types.Instant
	for _, row := range inner {
		f, t, d := row.TxStamps()
		from, to = storecontract.FoldTxStamps(from, to, f, t, d)
	}
	ms.histStampsMu.Lock()
	if ms.nodeHistStamps == nil {
		ms.nodeHistStamps = make(map[types.NodeID]histStampsEntry)
	}
	ms.nodeHistStamps[id] = histStampsEntry{from: from, to: to, n: len(inner)}
	ms.histStampsMu.Unlock()
	return from, to, true, nil
}

// RelHistoryStamps is NodeHistoryStamps for relationships.
func (ms *Store) RelHistoryStamps(id types.RelID) (types.Instant, types.Instant, bool, error) {
	if ms == nil {
		return 0, 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return 0, 0, false, err
	}
	inner := ms.relHistory[id]
	if len(inner) == 0 {
		return 0, 0, false, nil
	}
	ms.histStampsMu.RLock()
	e, ok := ms.relHistStamps[id]
	ms.histStampsMu.RUnlock()
	if ok && e.n == len(inner) {
		return e.from, e.to, true, nil
	}
	var from, to types.Instant
	for _, row := range inner {
		f, t, d := row.TxStamps()
		from, to = storecontract.FoldTxStamps(from, to, f, t, d)
	}
	ms.histStampsMu.Lock()
	if ms.relHistStamps == nil {
		ms.relHistStamps = make(map[types.RelID]histStampsEntry)
	}
	ms.relHistStamps[id] = histStampsEntry{from: from, to: to, n: len(inner)}
	ms.histStampsMu.Unlock()
	return from, to, true, nil
}
