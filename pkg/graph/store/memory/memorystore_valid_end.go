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
// Per queried token, RAM-only: the first candidates call for a type or label
// tracks it under ms.mu (write) — its ever-members from the K1 membership
// sidecar, each read once from its rows (sealed current rows included) — and
// later calls answer under ms.mu.RLock. Every door that writes a row notes it
// under ms.mu: a current row put or replaced (noteRelCurrentLocked /
// noteNodeCurrentLocked), a history row (putRelHistoryRowLocked /
// putNodeHistoryRowLocked, the one place history is written), a current row
// removed (deleteRelLocked and the node delete doors), and a history row
// removed (truncate, trim, compaction, retention purge, exact erasure: the
// entity is re-read on the next call). A note costs an atomic load while no
// token of the kind is tracked, and a map lookup under the index lock for an
// entity of an untracked token. A row sealed into or faulted in from a
// segment is the same logical row: no note. Clear drops both indexes.

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
	ms.relValidEnds.NoteRemoved(int64(id))
}

func (ms *Store) nodeHistoryRemovedLocked(id types.NodeID) {
	ms.nodeValidEnds.NoteRemoved(int64(id))
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

// validEndCandidates answers ix for toks under ms.mu.RLock when every token
// is tracked (the re-read of unknown entities only reads rows; the index has
// its own lock); a token queried for the first time is tracked under ms.mu
// (write, every writer held out) from the membership sidecar (ensureK1 builds
// it, members lists a token's ever-members) and its members re-read.
func (ms *Store) validEndCandidates(ix *indexpkg.ValidEndIndex, toks []uint16, ensureK1 func() error,
	members func(tok uint16, enlist func(id int64)), rows func(id int64) ([]indexpkg.ValidEndRow, error),
	pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	answer := func() ([]storecontract.ValidEndCandidate, error) {
		if err := ix.ResolveUnknown(rows); err != nil {
			return nil, err
		}
		return ix.Candidates(toks, pin, validFrom), nil
	}
	tracked := func() bool {
		for _, tok := range toks {
			if !ix.Tracked(tok) {
				return false
			}
		}
		return true
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return nil, err
	}
	if tracked() {
		defer ms.mu.RUnlock()
		return answer()
	}
	ms.mu.RUnlock()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := ensureK1(); err != nil {
		return nil, err
	}
	for _, tok := range toks {
		ix.Track(tok, func(enlist func(id int64)) { members(tok, enlist) })
	}
	return answer()
}

// RelValidEndCandidates implements store.RelValidEndCapability.
func (ms *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	return ms.validEndCandidates(&ms.relValidEnds, []uint16{typeToken}, ms.ensureRelTypeTxMembersBuiltLocked,
		func(tok uint16, enlist func(int64)) {
			for id := range ms.relTypeTxMembers[tok] {
				enlist(int64(id))
			}
		},
		func(id int64) ([]indexpkg.ValidEndRow, error) { return ms.relValidEndRowsLocked(types.RelID(id)) },
		pin, validFrom)
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
	return ms.validEndCandidates(&ms.nodeValidEnds, labelTokens,
		func() error { ms.ensureLabelTxMembersBuiltLocked(); return nil },
		func(tok uint16, enlist func(int64)) {
			for id := range ms.labelTxMembers[tok] {
				enlist(int64(id))
			}
		},
		func(id int64) ([]indexpkg.ValidEndRow, error) {
			return ms.nodeValidEndRowsLocked(types.NodeID(id)), nil
		},
		pin, validFrom)
}

// RefineNodeValidEnd implements store.NodeValidEndCapability.
func (ms *Store) RefineNodeValidEnd(id types.NodeID, gen uint64, end types.Instant) {
	if ms == nil {
		return
	}
	ms.nodeValidEnds.Refine(int64(id), gen, end)
}

// ValidEndStats reports the valid-end indexes (relationships, nodes) of
// backlog 51: a diagnostic, sizes and write notes, not a contract.
func (ms *Store) ValidEndStats() (rels, nodes storecontract.ValidEndStats) {
	if ms == nil {
		return rels, nodes
	}
	return ms.relValidEnds.Stats(), ms.nodeValidEnds.Stats()
}
