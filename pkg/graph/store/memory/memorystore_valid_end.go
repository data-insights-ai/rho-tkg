package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-end indexes (backlog 51, store.RelValidEndCapability /
// NodeValidEndCapability; the fold and its soundness rules are
// index.ValidEndIndex's).
//
// LAZY, RAM-only: built on the first candidates call under ms.mu (write; later
// calls read under ms.mu.RLock) from
// every current row (sealed ones included) and every history row; afterwards
// every door that writes a row notes it under ms.mu: a current row put or
// replaced (noteRelCurrentLocked / noteNodeCurrentLocked), a history row
// (putRelHistoryRowLocked / putNodeHistoryRowLocked, the one place history is
// written), a current row removed (deleteRelLocked and the node delete doors),
// and a history row removed (truncate, trim, compaction, retention purge,
// exact erasure: the entity is re-read from its rows on the next call). A row
// sealed into or faulted in from a segment is the same logical row: no note.
// Clear drops both indexes.

var (
	_ storecontract.RelValidEndCapability  = (*Store)(nil)
	_ storecontract.NodeValidEndCapability = (*Store)(nil)
)

// noteRelCurrentLocked notes a put or replaced current relationship row.
// Caller holds ms.mu (write).
func (ms *Store) noteRelCurrentLocked(r *types.Relationship) {
	if ms.relValidEnds.Active() {
		ms.relValidEnds.NoteRow(int64(r.ID()), indexpkg.ValidEndRowOfRel(r, false))
	}
}

// noteNodeCurrentLocked notes a put or replaced current node row.
func (ms *Store) noteNodeCurrentLocked(n *types.Node) {
	if ms.nodeValidEnds.Active() {
		ms.nodeValidEnds.NoteRow(int64(n.ID()), indexpkg.ValidEndRowOfNode(n, false))
	}
}

// relHistoryRemovedLocked / nodeHistoryRemovedLocked note history rows of id
// removed (or the whole history).
func (ms *Store) relHistoryRemovedLocked(id types.RelID) {
	ms.relValidEnds.MarkUnknown(int64(id))
}

func (ms *Store) nodeHistoryRemovedLocked(id types.NodeID) {
	ms.nodeValidEnds.MarkUnknown(int64(id))
}

// relValidEndRowsLocked returns every stored row of rel id (none: gone).
// Caller holds ms.mu.
func (ms *Store) relValidEndRowsLocked(id types.RelID) ([]indexpkg.ValidEndRow, error) {
	var rows []indexpkg.ValidEndRow
	for v, r := range ms.relHistory[id] {
		row := indexpkg.ValidEndRowOfRel(r, true)
		row.Version = v
		rows = append(rows, row)
	}
	cur, ok := ms.rels[id]
	if !ok {
		sg, row, sealed, err := ms.sealedLocked(id)
		if err != nil {
			return nil, err
		}
		if sealed {
			if cur, err = sg.seg.Row(row); err != nil {
				return nil, err
			}
			ok = true
		}
	}
	if ok {
		rows = append(rows, indexpkg.ValidEndRowOfRel(cur, false))
	}
	return rows, nil
}

func (ms *Store) nodeValidEndRowsLocked(id types.NodeID) []indexpkg.ValidEndRow {
	var rows []indexpkg.ValidEndRow
	for v, n := range ms.nodeHistory[id] {
		row := indexpkg.ValidEndRowOfNode(n, true)
		row.Version = v
		rows = append(rows, row)
	}
	if n, ok := ms.nodes[id]; ok {
		rows = append(rows, indexpkg.ValidEndRowOfNode(n, false))
	}
	return rows
}

// buildRelValidEndsLocked builds the relationship index from every row.
// Caller holds ms.mu (write).
func (ms *Store) buildRelValidEndsLocked() error {
	ix := &ms.relValidEnds
	ix.BeginBuild()
	for id := range ms.relHistory {
		rows, err := ms.relValidEndRowsLocked(id)
		if err != nil {
			ix.Reset()
			return err
		}
		ix.Install(int64(id), rows)
	}
	if err := ms.forEachCurrentRelLocked(func(r *types.Relationship) bool {
		if _, hist := ms.relHistory[r.ID()]; !hist {
			ix.Install(int64(r.ID()), []indexpkg.ValidEndRow{indexpkg.ValidEndRowOfRel(r, false)})
		}
		return true
	}); err != nil {
		ix.Reset()
		return err
	}
	ix.EndBuild()
	return nil
}

func (ms *Store) buildNodeValidEndsLocked() {
	ix := &ms.nodeValidEnds
	ix.BeginBuild()
	for id := range ms.nodeHistory {
		ix.Install(int64(id), ms.nodeValidEndRowsLocked(id))
	}
	for id, n := range ms.nodes {
		if _, hist := ms.nodeHistory[id]; !hist {
			ix.Install(int64(id), []indexpkg.ValidEndRow{indexpkg.ValidEndRowOfNode(n, false)})
		}
	}
	ix.EndBuild()
}

// validEndCandidates answers ix under ms.mu.RLock once built (the re-read of
// unknown entities only reads rows; the index has its own lock); the first
// call builds under ms.mu (write), which holds every writer out.
func (ms *Store) validEndCandidates(ix *indexpkg.ValidEndIndex, build func() error,
	rows func(id int64) ([]indexpkg.ValidEndRow, error), answer func() []storecontract.ValidEndCandidate) ([]storecontract.ValidEndCandidate, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	read := func() ([]storecontract.ValidEndCandidate, error) {
		if err := ix.ResolveUnknown(rows); err != nil {
			return nil, err
		}
		return answer(), nil
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if ix.Built() {
		defer ms.mu.RUnlock()
		return read()
	}
	ms.mu.RUnlock()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if !ix.Built() {
		if err := build(); err != nil {
			return nil, err
		}
	}
	return read()
}

// RelValidEndCandidates implements store.RelValidEndCapability.
func (ms *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	return ms.validEndCandidates(&ms.relValidEnds, ms.buildRelValidEndsLocked,
		func(id int64) ([]indexpkg.ValidEndRow, error) { return ms.relValidEndRowsLocked(types.RelID(id)) },
		func() []storecontract.ValidEndCandidate {
			return ms.relValidEnds.Candidates([]uint16{typeToken}, pin, validFrom)
		})
}

// RefineRelValidEnd implements store.RelValidEndCapability.
func (ms *Store) RefineRelValidEnd(id types.RelID, gen uint64, end types.Instant) {
	if ms == nil {
		return
	}
	ms.relValidEnds.Refine(int64(id), gen, end)
}

// NodeValidEndCandidates implements store.NodeValidEndCapability.
func (ms *Store) NodeValidEndCandidates(labelTokens []uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	return ms.validEndCandidates(&ms.nodeValidEnds, func() error { ms.buildNodeValidEndsLocked(); return nil },
		func(id int64) ([]indexpkg.ValidEndRow, error) {
			return ms.nodeValidEndRowsLocked(types.NodeID(id)), nil
		},
		func() []storecontract.ValidEndCandidate {
			return ms.nodeValidEnds.Candidates(labelTokens, pin, validFrom)
		})
}

// RefineNodeValidEnd implements store.NodeValidEndCapability.
func (ms *Store) RefineNodeValidEnd(id types.NodeID, gen uint64, end types.Instant) {
	if ms == nil {
		return
	}
	ms.nodeValidEnds.Refine(int64(id), gen, end)
}

// validEndStats reports the sizes of the valid-end indexes (relationships,
// nodes).
func (ms *Store) validEndStats() (rels, nodes indexpkg.ValidEndStats) {
	return ms.relValidEnds.Stats(), ms.nodeValidEnds.Stats()
}
