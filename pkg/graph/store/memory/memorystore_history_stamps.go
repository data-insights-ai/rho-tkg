package memory

import (
	"maps"
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// History stamps (store.HistoryStampsCapability): per ID, the fold
// (store.FoldTxStamps) of its history rows, cached after the first call and
// kept exact without hooking the delete doors.
//
// An entry carries n, the ID's history row count when it was computed, and is
// used only while the ID still has n rows. Every history row write goes
// through putNodeHistoryRowLocked / putRelHistoryRowLocked, which folds the new
// row into a valid entry (count +1, or the same count for an overwrite) and
// drops an entry it cannot keep exact (an overwrite with lower stamps than the
// cached max: the overwritten row may have held it) or whose ordered bit it
// cannot fold (a row below the top, an overwrite: the next read refolds the
// rows in version order instead of keeping "not proven"). Every other history
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
	ordered  bool // no row has a DeletedAt and TxFrom never falls with version (NodeHistoryNewestFirst)
}

// putNodeHistoryRowLocked stores row as node nid's history version v: the one
// place a node history row is written. by is the current row that replaced
// row (an update door's demoted row), else nil. Caller holds ms.mu (write).
func (ms *Store) putNodeHistoryRowLocked(nid types.NodeID, v uint32, row, by *types.Node) {
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
		ms.nodeHistoryIDs.Store(nil) // a new key
	}
	before := len(inner)
	_, overwrite := inner[v]
	inner[v] = ms.historyNode(row, by)
	top, ok := ms.nodeHistTop[nid]
	above := !ok || v > top
	if above {
		if ms.nodeHistTop == nil {
			ms.nodeHistTop = make(map[types.NodeID]uint32)
		}
		ms.nodeHistTop[nid] = v
	}
	if ms.nodeValidEnds.Active() {
		vr := indexpkg.ValidEndRowOfNode(row, true)
		vr.Version = v
		ms.nodeValidEnds.NoteRow(int64(nid), vr)
	}
	ms.nodeHistEpochs.Note(nid, len(inner))
	ms.histGen.Add(1)
	f, t, d := row.TxStamps()
	ms.histStampsMu.Lock()
	ms.nodeHistStamps = noteHistStamps(ms.nodeHistStamps, nid, before, len(inner), overwrite, above, f, t, d)
	ms.histStampsMu.Unlock()
}

// putRelHistoryRowLocked is putNodeHistoryRowLocked for relationships.
func (ms *Store) putRelHistoryRowLocked(rid types.RelID, v uint32, row, by *types.Relationship) {
	inner, ok := ms.relHistory[rid]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[rid] = inner
		ms.relHistoryIDs.Store(nil) // a new key
	}
	before := len(inner)
	_, overwrite := inner[v]
	ms.noteHistoryRowExAdjLocked(rid, row) // the row's legs enter the ex-adjacency
	inner[v] = ms.historyRel(row, by)
	top, ok := ms.relHistTop[rid]
	above := !ok || v > top
	if above {
		if ms.relHistTop == nil {
			ms.relHistTop = make(map[types.RelID]uint32)
		}
		ms.relHistTop[rid] = v
	}
	if ms.relValidEnds.Active() {
		vr := indexpkg.ValidEndRowOfRel(row, true)
		vr.Version = v
		ms.relValidEnds.NoteRow(int64(rid), vr)
	}
	ms.relHistEpochs.Note(rid, len(inner))
	ms.histGen.Add(1)
	f, t, d := row.TxStamps()
	ms.histStampsMu.Lock()
	ms.relHistStamps = noteHistStamps(ms.relHistStamps, rid, before, len(inner), overwrite, above, f, t, d)
	ms.histStampsMu.Unlock()
}

// noteHistStamps folds one written row into id's entry, if it has a valid one.
// before / after are the ID's row counts around the write. Caller holds
// histStampsMu.
func noteHistStamps[ID comparable](m map[ID]histStampsEntry, id ID, before, after int, overwrite, above bool, txFrom, txTo, deletedAt types.Instant) map[ID]histStampsEntry {
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
	if e.ordered && (overwrite || !above) {
		// A row below the top or a rewrite: whether TxFrom still never
		// falls with version needs the rows in order, the next read
		// refolds them (foldHistStamps) instead of keeping "not proven".
		delete(m, id)
		return m
	}
	ordered := e.ordered && deletedAt == 0 && txFrom >= e.from
	from, to := storecontract.FoldTxStamps(e.from, e.to, txFrom, txTo, deletedAt)
	m[id] = histStampsEntry{from: from, to: to, n: after, ordered: ordered}
	return m
}

// invalidateNodeHistStampsLocked drops nid's entry (a test-only row rewrite).
// Caller holds ms.mu (write).
func (ms *Store) invalidateNodeHistStampsLocked(nid types.NodeID) {
	ms.histStampsMu.Lock()
	delete(ms.nodeHistStamps, nid)
	ms.histStampsMu.Unlock()
	ms.nodeHistEpochs.Note(nid, len(ms.nodeHistory[nid]))
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
	e = ms.nodeHistStampsLocked(id, inner)
	return e.from, e.to, true, nil
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
	e = foldHistStamps(inner)
	ms.histStampsMu.Lock()
	if ms.relHistStamps == nil {
		ms.relHistStamps = make(map[types.RelID]histStampsEntry)
	}
	ms.relHistStamps[id] = e
	ms.histStampsMu.Unlock()
	return e.from, e.to, true, nil
}

// foldHistStamps folds inner's rows in version order: the stamps, the count
// and the ordered bit (no DeletedAt, TxFrom never falling with version).
func foldHistStamps[T interface {
	TxStamps() (types.Instant, types.Instant, types.Instant)
}](inner map[uint32]T) histStampsEntry {
	e := histStampsEntry{n: len(inner), ordered: true}
	for _, v := range slices.Sorted(maps.Keys(inner)) {
		f, t, d := inner[v].TxStamps()
		e.ordered = e.ordered && d == 0 && f >= e.from
		e.from, e.to = storecontract.FoldTxStamps(e.from, e.to, f, t, d)
	}
	return e
}

// nodeHistStampsLocked recomputes and caches nid's entry from inner (its
// history rows, non-empty). Caller holds ms.mu (read or write).
func (ms *Store) nodeHistStampsLocked(nid types.NodeID, inner map[uint32]*types.Node) histStampsEntry {
	e := foldHistStamps(inner)
	ms.histStampsMu.Lock()
	if ms.nodeHistStamps == nil {
		ms.nodeHistStamps = make(map[types.NodeID]histStampsEntry)
	}
	ms.nodeHistStamps[nid] = e
	ms.histStampsMu.Unlock()
	return e
}

var _ storecontract.HistorySuffixCapability = (*Store)(nil)

// nodeHistoryOrderedLocked reports the cached entry's ordered bit for inner
// (nid's history rows, non-empty), recomputing the entry as
// NodeHistoryStamps does when it is stale. Caller holds ms.mu.
func (ms *Store) nodeHistoryOrderedLocked(id types.NodeID, inner map[uint32]*types.Node) bool {
	ms.histStampsMu.RLock()
	e, ok := ms.nodeHistStamps[id]
	ms.histStampsMu.RUnlock()
	if !ok || e.n != len(inner) {
		e = ms.nodeHistStampsLocked(id, inner)
	}
	return e.ordered
}

// NodeHistoryNewestFirst visits the node's history skeletons by falling
// version with its ordered bit (nodeHistoryOrderedLocked;
// store.HistorySuffixCapability); each row is charged to t as copyHistoryMetas charges it, until visit returns
// false. Memory decodes nothing: the ordered bit is not charged.
func (ms *Store) NodeHistoryNewestFirst(id types.NodeID, t *storecontract.ReadTally, visit func(storecontract.VersionTemporalMeta, bool) bool) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	inner := ms.nodeHistory[id]
	ordered := len(inner) == 0 || ms.nodeHistoryOrderedLocked(id, inner)
	versions := slices.Sorted(maps.Keys(inner))
	for _, v := range slices.Backward(versions) {
		if err := t.History(1); err != nil {
			return ordered, err
		}
		if err := t.Buffer(16); err != nil {
			return ordered, err
		}
		m := storecontract.VersionTemporalMeta{Version: v}
		if tm, has := inner[v].TemporalByValue(); has {
			if err := t.Buffer(104); err != nil {
				return ordered, err
			}
			m.Temporal = new(types.TemporalMetadata)
			*m.Temporal = tm
		}
		ms.historyMetasListed.Add(1)
		if !visit(m, ordered) {
			return ordered, nil
		}
	}
	return ordered, nil
}
