package badger

import (
	"errors"
	"fmt"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Valid-end indexes (backlog 51, store.RelValidEndCapability /
// NodeValidEndCapability; the fold and its soundness rules are
// index.ValidEndIndex's).
//
// RAM, rebuilt on the first bounded scan after an open — not persisted: the
// index is derived from rows that are already durable, and a persisted copy
// would have to be written atomically with every row write (a read-modify-write
// of the entity's index key per write) to stay true across a crash.
//
// Maintained where every row enters the write buffer: appendOps and
// appendOpsLoggedRouted pass each op to noteValidEndOp under wbMu (the funnel
// noteHistoryKey uses), so no door can write a current row (0x01 / 0x02) or a
// history row (0x07 / 0x08) without the index seeing it. A SET is decoded
// (temporal fields and tokens only, delta-aware) and folded; a DELETE of a
// current row is NoteCurrentDelete, of a history row MarkUnknown (the entity
// is re-read on the next call).
//
// The build is the history-stamps protocol: under wbMu the index starts
// building (every op noted from then on moves its entity to unknown), the
// write-buffer overlay is captured BEFORE the badger view (lesson 64) and
// every entity it touches is unknown, then the committed history rows and
// current rows are folded per entity, skipping unknown ones. Unknown entities
// are re-read by ID (current row and history, through the store's own doors)
// at the start of every candidates call; a write that lands meanwhile keeps
// its entity unknown, and unknown entities are listed by every call. Clear
// holds validEndBuildMu and drops both indexes.

var (
	_ storecontract.RelValidEndCapability  = (*Store)(nil)
	_ storecontract.NodeValidEndCapability = (*Store)(nil)
)

// noteValidEndOp feeds one buffered op to the valid-end index of its kind.
// Caller holds wbMu.
func (bs *Store) noteValidEndOp(op writeOp) {
	key := op.key
	if len(key) == 0 {
		return
	}
	var (
		ix   *indexpkg.ValidEndIndex
		node bool
		hist bool
	)
	switch {
	case key[0] == storepkg.KeyRel && len(key) == storepkg.SizeRelKey:
		ix = &bs.relValidEnds
	case key[0] == storepkg.KeyHistRel && len(key) == storepkg.SizeHistKey:
		ix, hist = &bs.relValidEnds, true
	case key[0] == storepkg.KeyNode && len(key) == storepkg.SizeNodeKey:
		ix, node = &bs.nodeValidEnds, true
	case key[0] == storepkg.KeyHistNode && len(key) == storepkg.SizeHistKey:
		ix, node, hist = &bs.nodeValidEnds, true, true
	default:
		return
	}
	if !ix.Active() {
		return
	}
	id := int64(storepkg.ParseIDFromKey(key, 1))
	if op.opType == writeOpDelete {
		if hist {
			ix.MarkUnknown(id)
		} else {
			ix.NoteCurrentDelete(id)
		}
		return
	}
	row, err := validEndRowOfValue(key, op.value, node, hist)
	if err != nil {
		ix.MarkUnknown(id) // the re-read reports the decode error
		return
	}
	ix.NoteRow(id, row)
}

// validEndRowOfValue decodes a stored row value under key.
func validEndRowOfValue(key, value []byte, node, hist bool) (indexpkg.ValidEndRow, error) {
	w, err := storepkg.DecodeValidEndValue(value, node)
	if err != nil {
		return indexpkg.ValidEndRow{}, err
	}
	row := indexpkg.ValidEndRow{Hist: hist, TxFrom: w.TxFrom, TxTo: w.TxTo, ValidTo: w.ValidTo, DeletedAt: w.DeletedAt, Tokens: w.Tokens}
	if hist {
		row.Version = uint32(min(historyVersionFromKey(key), uint64(^uint32(0)))) // #nosec G115 -- clamped to uint32
	}
	return row, nil
}

// buildValidEnds builds ix once (see the file comment). node selects the
// node keyspaces.
func (bs *Store) buildValidEnds(ix *indexpkg.ValidEndIndex, node bool) error {
	bs.validEndBuildMu.Lock()
	defer bs.validEndBuildMu.Unlock()
	if ix.Built() {
		return nil
	}
	current, history := storepkg.KeyRel, storepkg.KeyHistRel
	if node {
		current, history = storepkg.KeyNode, storepkg.KeyHistNode
	}
	bs.wbMu.Lock()
	ix.BeginBuild()
	bs.wbMu.Unlock()
	bs.rangePending(func(k string, _ writeOp) {
		if len(k) > 0 && (k[0] == current && len(k) == 1+8 || k[0] == history && len(k) == storepkg.SizeHistKey) {
			ix.MarkUnknown(int64(storepkg.ParseIDFromKey([]byte(k), 1)))
		}
	})
	if bs.validEndBuildHook != nil {
		bs.validEndBuildHook() // tests land writes and a flush here
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, prefix := range []byte{history, current} {
			hist := prefix == history
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			for it.Seek([]byte{prefix}); it.ValidForPrefix([]byte{prefix}); it.Next() {
				item := it.Item()
				key := item.Key()
				if (hist && len(key) != storepkg.SizeHistKey) || (!hist && len(key) != 1+8) {
					continue
				}
				id := int64(storepkg.ParseIDFromKey(key, 1))
				if err := item.Value(func(val []byte) error {
					row, err := validEndRowOfValue(key, val, node, hist)
					if err != nil {
						return err
					}
					ix.BuildRow(id, row)
					return nil
				}); err != nil {
					it.Close()
					return err
				}
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		ix.Reset()
		return fmt.Errorf("graph: build valid-end index: %w", err)
	}
	ix.EndBuild()
	return nil
}

// relValidEndRows reads every stored row of rel id through the store's doors
// (none: gone).
func (bs *Store) relValidEndRows(id int64) ([]indexpkg.ValidEndRow, error) {
	rid := types.RelID(id)
	hist, err := bs.GetRelHistory(rid)
	if err != nil && !errors.Is(err, storecontract.ErrRelNotFound) {
		return nil, err
	}
	rows := make([]indexpkg.ValidEndRow, 0, len(hist)+1)
	for _, r := range hist {
		rows = append(rows, indexpkg.ValidEndRowOfRel(r, true))
	}
	cur, err := bs.GetRelationship(rid)
	switch {
	case err == nil:
		rows = append(rows, indexpkg.ValidEndRowOfRel(cur, false))
	case !errors.Is(err, storecontract.ErrRelNotFound):
		return nil, err
	}
	return rows, nil
}

func (bs *Store) nodeValidEndRows(id int64) ([]indexpkg.ValidEndRow, error) {
	nid := types.NodeID(id)
	hist, err := bs.GetNodeHistory(nid)
	if err != nil && !errors.Is(err, storecontract.ErrNodeNotFound) {
		return nil, err
	}
	rows := make([]indexpkg.ValidEndRow, 0, len(hist)+1)
	for _, n := range hist {
		rows = append(rows, indexpkg.ValidEndRowOfNode(n, true))
	}
	cur, err := bs.GetNode(nid)
	switch {
	case err == nil:
		rows = append(rows, indexpkg.ValidEndRowOfNode(cur, false))
	case !errors.Is(err, storecontract.ErrNodeNotFound):
		return nil, err
	}
	return rows, nil
}

// RelValidEndCandidates implements store.RelValidEndCapability.
func (bs *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := bs.buildValidEnds(&bs.relValidEnds, false); err != nil {
		return nil, err
	}
	if err := bs.relValidEnds.ResolveUnknown(bs.relValidEndRows); err != nil {
		return nil, err
	}
	return bs.relValidEnds.Candidates([]uint16{typeToken}, pin, validFrom), nil
}

// RefineRelValidEnd implements store.RelValidEndCapability.
func (bs *Store) RefineRelValidEnd(id types.RelID, gen uint64, end types.Instant) {
	bs.relValidEnds.Refine(int64(id), gen, end)
}

// NodeValidEndCandidates implements store.NodeValidEndCapability.
func (bs *Store) NodeValidEndCandidates(labelTokens []uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := bs.buildValidEnds(&bs.nodeValidEnds, true); err != nil {
		return nil, err
	}
	if err := bs.nodeValidEnds.ResolveUnknown(bs.nodeValidEndRows); err != nil {
		return nil, err
	}
	return bs.nodeValidEnds.Candidates(labelTokens, pin, validFrom), nil
}

// RefineNodeValidEnd implements store.NodeValidEndCapability.
func (bs *Store) RefineNodeValidEnd(id types.NodeID, gen uint64, end types.Instant) {
	bs.nodeValidEnds.Refine(int64(id), gen, end)
}

// resetValidEndsLocked drops both indexes (Clear). Caller holds
// validEndBuildMu.
func (bs *Store) resetValidEndsLocked() {
	bs.relValidEnds.Reset()
	bs.nodeValidEnds.Reset()
}

// validEndStats reports the sizes of the valid-end indexes (relationships,
// nodes).
func (bs *Store) validEndStats() (rels, nodes indexpkg.ValidEndStats) {
	return bs.relValidEnds.Stats(), bs.nodeValidEnds.Stats()
}
