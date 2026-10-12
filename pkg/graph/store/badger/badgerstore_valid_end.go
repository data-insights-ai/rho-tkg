package badger

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
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
// Per queried token, RAM-only, not persisted: the first candidates call for a
// type or label after an open tracks it — under idxMu (no row write in
// flight) its ever-members are taken from the K1 membership sidecar (built on
// first use, one pass over the rows) and enlisted, then each is read once by
// ID through the store's own doors. Persisting the index would not make a
// restart flat: opening a badger store already walks every current row to
// rebuild its RAM indexes, linear in the history.
//
// Maintained where rows enter the write buffer: appendOps and
// appendOpsLoggedRouted pass each op to noteValidEndOp under wbMu (the funnel
// noteHistoryKey uses), so no door writes a current row (0x01 / 0x02) or a
// history row (0x07 / 0x08) of a held entity unseen. While no token of the
// kind is tracked a note is one atomic load; otherwise a map lookup, and only
// a held entity's SET is decoded (temporal block only). A row that makes an
// entity a member of a token (create, label add, history insert) reaches the
// index from the K1 membership doors (recordRelTypeMemberLocked /
// recordNodeLabelMembersLocked, NoteMember), which run before the row's op is
// buffered, so its SET is then folded with its kind. A DELETE of a current
// row is NoteCurrentDelete, of a history row NoteRemoved (the entity is
// re-read on the next call). Clear, holding idxMu, drops both indexes.

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
	switch {
	case op.opType == writeOpDelete && hist:
		ix.NoteRemoved(id)
	case op.opType == writeOpDelete:
		ix.NoteCurrentDelete(id)
	case ix.Has(id):
		tm, err := storepkg.DecodeValidEndValue(op.value, node)
		if err != nil {
			ix.NoteRemoved(id) // re-read: the store's doors report the decode error
			return
		}
		row := indexpkg.ValidEndRow{Hist: hist}
		if tm != nil {
			row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
		}
		if hist {
			row.Version = uint32(min(historyVersionFromKey(key), uint64(^uint32(0)))) // #nosec G115 -- clamped to uint32
		}
		ix.NoteRow(id, row)
	}
}

// noteValidEndRelMember / noteValidEndNodeMember feed a membership door's row
// to the index (index.ValidEndIndex.NoteMember). Caller holds idxMu (write).
func (bs *Store) noteValidEndRelMember(r *types.Relationship) {
	if bs.relValidEnds.Active() {
		bs.relValidEnds.NoteMember(int64(r.ID()), indexpkg.ValidEndRowOfRel(r, false))
	}
}

func (bs *Store) noteValidEndNodeMember(n *types.Node) {
	if bs.nodeValidEnds.Active() {
		bs.nodeValidEnds.NoteMember(int64(n.ID()), indexpkg.ValidEndRowOfNode(n, false))
	}
}

// trackValidEnds tracks every token of toks not tracked yet (see the file
// comment). ensureK1 builds the membership sidecar, members lists one token's
// ever-members (caller holds idxMu).
func (bs *Store) trackValidEnds(ix *indexpkg.ValidEndIndex, toks []uint16, ensureK1 func() error, members func(tok uint16, enlist func(int64))) error {
	missing := false
	for _, tok := range toks {
		missing = missing || !ix.Tracked(tok)
	}
	if !missing {
		return nil
	}
	if err := ensureK1(); err != nil {
		return err
	}
	bs.idxMu.Lock()
	for _, tok := range toks {
		ix.Track(tok, func(enlist func(int64)) { members(tok, enlist) })
	}
	bs.idxMu.Unlock()
	if bs.validEndTrackHook != nil {
		bs.validEndTrackHook() // tests land writes between the enlisting and the re-read
	}
	return nil
}

// validEndRowsBulk reads what the index folds of every entity in ids (the
// unknown entities of one kind): the history rows' and the current row's
// temporal blocks only, and the tracked tokens each belongs to from the K1
// membership sidecar (member, read under idxMu.RLock). The committed rows of
// all of them are read in one badger view by one history iterator and one
// current-row iterator walking forward in ID order (advanceTo); an entity with a buffered
// write when the read starts (overlay captured BEFORE the view, lesson 64) is
// read by validEndOverlayRows instead. An entity without rows is omitted.
func (bs *Store) validEndRowsBulk(ix *indexpkg.ValidEndIndex, ids []int64, node bool, member func(tok uint16, id int64) bool) (map[int64][]indexpkg.ValidEndRow, error) {
	current, history := storepkg.KeyRel, storepkg.KeyHistRel
	if node {
		current, history = storepkg.KeyNode, storepkg.KeyHistNode
	}
	buffered := make(map[int64]struct{})
	bs.rangePending(func(k string, _ writeOp) {
		if len(k) > 0 && (k[0] == current && len(k) == storepkg.SizeRelKey || k[0] == history && len(k) == storepkg.SizeHistKey) {
			buffered[int64(storepkg.ParseIDFromKey([]byte(k), 1))] = struct{}{}
		}
	})
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}
	slices.Sort(ids)
	out := make(map[int64][]indexpkg.ValidEndRow, len(ids))
	add := func(id int64, hist bool, version uint32, raw []byte) error {
		tm, err := storepkg.DecodeValidEndValue(raw, node)
		if err != nil {
			return fmt.Errorf("graph: decode row for the valid-end index: %w", err)
		}
		row := indexpkg.ValidEndRow{Hist: hist, Version: version}
		if tm != nil {
			row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
		}
		out[id] = append(out[id], row)
		return nil
	}
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		hit := txn.NewIterator(opts)
		defer hit.Close()
		hit.Seek([]byte{history})
		cit := txn.NewIterator(opts)
		defer cit.Close()
		cit.Seek([]byte{current})
		prefix := make([]byte, 1+8)
		for _, id := range ids {
			if _, ok := buffered[id]; ok {
				continue
			}
			prefix[0] = history
			storepkg.PutUint64(prefix, 1, id)
			advanceTo(hit, prefix)
			for ; hit.ValidForPrefix(prefix); hit.Next() {
				key := hit.Item().Key()
				if len(key) != storepkg.SizeHistKey {
					continue
				}
				version := uint32(min(historyVersionFromKey(key), uint64(^uint32(0)))) // #nosec G115 -- clamped to uint32
				if err := hit.Item().Value(func(val []byte) error { return add(id, true, version, val) }); err != nil {
					return err
				}
			}
			prefix[0] = current
			advanceTo(cit, prefix)
			if cit.Valid() && bytes.Equal(cit.Item().Key(), prefix) {
				if err := cit.Item().Value(func(val []byte) error { return add(id, false, 0, val) }); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for id := range buffered {
		if _, wanted := slices.BinarySearch(ids, id); !wanted {
			continue
		}
		rows, err := bs.validEndOverlayRows(id, node)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			out[id] = rows
		}
	}
	bs.idxMu.RLock()
	tracked := ix.TrackedTokens()
	for id, rows := range out {
		for _, tok := range tracked {
			if member(tok, id) {
				rows[0].Tokens = append(rows[0].Tokens, tok)
			}
		}
	}
	bs.idxMu.RUnlock()
	return out, nil
}

// advanceTo moves it to the first key at or after target: by Next while the
// target is a few keys ahead (members of a type are usually dense in the ID
// order, so the walk is sequential), else by Seek. it only moves forward;
// targets come in ascending order.
func advanceTo(it *badgerv4.Iterator, target []byte) {
	for steps := 0; it.Valid() && bytes.Compare(it.Item().Key(), target) < 0; steps++ {
		if steps == 16 {
			it.Seek(target)
			return
		}
		it.Next()
	}
}

// validEndOverlayRows reads one entity with buffered writes: its history
// rows' temporal skeletons (overlay first) and its current row's temporal
// block (the entity cache, else the committed key, as RelCurrentStamps reads
// it).
func (bs *Store) validEndOverlayRows(id int64, node bool) ([]indexpkg.ValidEndRow, error) {
	sid := snowflake.ID(id)
	var (
		cur    *types.TemporalMetadata
		found  = true
		prefix []byte
	)
	if node {
		prefix = storepkg.HistNodePrefix(sid)
		switch n, status := bs.nodeCache.GetNoPromote(sid); status {
		case indexpkg.CacheHit:
			cur = n.Temporal()
		case indexpkg.CacheDeleted:
			found = false
		default:
			bs.idxMu.RLock()
			_, found = bs.nodeIDs[types.NodeID(sid)]
			bs.idxMu.RUnlock()
			if found {
				var err error
				if cur, found, err = bs.committedTemporal(storepkg.KeyNode, sid); err != nil {
					return nil, err
				}
			}
		}
	} else {
		prefix = storepkg.HistRelPrefix(sid)
		switch r, status := bs.relCache.GetNoPromote(sid); status {
		case indexpkg.CacheHit:
			cur = r.Temporal()
		case indexpkg.CacheDeleted:
			found = false
		default:
			bs.idxMu.RLock()
			_, found = bs.relIDs[types.RelID(sid)]
			bs.idxMu.RUnlock()
			if found {
				var err error
				if cur, found, err = bs.committedTemporal(storepkg.KeyRel, sid); err != nil {
					return nil, err
				}
			}
		}
	}
	metas, err := bs.historyTemporalMetaByPrefix(prefix, node)
	if err != nil {
		return nil, err
	}
	rows := make([]indexpkg.ValidEndRow, 0, len(metas)+1)
	add := func(hist bool, version uint32, tm *types.TemporalMetadata) {
		row := indexpkg.ValidEndRow{Hist: hist, Version: version}
		if tm != nil {
			row.TxFrom, row.TxTo, row.ValidTo, row.DeletedAt = tm.TxFrom, tm.TxTo, tm.ValidTo, tm.DeletedAt
		}
		rows = append(rows, row)
	}
	for _, m := range metas {
		add(true, m.Version, m.Temporal)
	}
	if found {
		add(false, 0, cur)
	}
	return rows, nil
}

// committedTemporal reads the committed current row of id (prefix KeyNode or
// KeyRel) and decodes its temporal block; found false when the key holds no
// row.
func (bs *Store) committedTemporal(prefix byte, id snowflake.ID) (*types.TemporalMetadata, bool, error) {
	key := make([]byte, storepkg.SizeNodeKey)
	key[0] = prefix
	storepkg.PutUint64(key, 1, int64(id))
	var tm *types.TemporalMetadata
	found := true
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			found = false
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var derr error
			if _, tm, derr = storepkg.DecodeWireTemporalMeta(val); derr != nil {
				return fmt.Errorf("graph: decode current row temporal: %w", derr)
			}
			return nil
		})
	})
	return tm, found, err
}

// RelValidEndCandidates implements store.RelValidEndCapability.
func (bs *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	toks := []uint16{typeToken}
	if err := bs.trackValidEnds(&bs.relValidEnds, toks, bs.ensureRelTypeTxMembersBuilt, func(tok uint16, enlist func(int64)) {
		for id := range bs.relTypeTxMembers[tok] {
			enlist(int64(id))
		}
	}); err != nil {
		return nil, err
	}
	if err := bs.relValidEnds.ResolveUnknownBulk(func(ids []int64) (map[int64][]indexpkg.ValidEndRow, error) {
		return bs.validEndRowsBulk(&bs.relValidEnds, ids, false, func(tok uint16, id int64) bool {
			_, member := bs.relTypeTxMembers[tok][types.RelID(id)]
			return member
		})
	}); err != nil {
		return nil, err
	}
	return bs.relValidEnds.Candidates(toks, pin, validFrom), nil
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
	if err := bs.trackValidEnds(&bs.nodeValidEnds, labelTokens, bs.ensureLabelTxMembersBuilt, func(tok uint16, enlist func(int64)) {
		for id := range bs.labelTxMembers[tok] {
			enlist(int64(id))
		}
	}); err != nil {
		return nil, err
	}
	if err := bs.nodeValidEnds.ResolveUnknownBulk(func(ids []int64) (map[int64][]indexpkg.ValidEndRow, error) {
		return bs.validEndRowsBulk(&bs.nodeValidEnds, ids, true, func(tok uint16, id int64) bool {
			_, member := bs.labelTxMembers[tok][types.NodeID(id)]
			return member
		})
	}); err != nil {
		return nil, err
	}
	return bs.nodeValidEnds.Candidates(labelTokens, pin, validFrom), nil
}

// RefineNodeValidEnd implements store.NodeValidEndCapability.
func (bs *Store) RefineNodeValidEnd(id types.NodeID, gen uint64, end types.Instant) {
	bs.nodeValidEnds.Refine(int64(id), gen, end)
}

// resetValidEndsLocked drops both indexes (Clear). Caller holds idxMu.
func (bs *Store) resetValidEndsLocked() {
	bs.relValidEnds.Reset()
	bs.nodeValidEnds.Reset()
}

// ValidEndStats reports the valid-end indexes (relationships, nodes) of
// backlog 51: a diagnostic, sizes and write notes, not a contract.
func (bs *Store) ValidEndStats() (rels, nodes storecontract.ValidEndStats) {
	if bs == nil {
		return rels, nodes
	}
	return bs.relValidEnds.Stats(), bs.nodeValidEnds.Stats()
}
