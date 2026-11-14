package badger

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
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
// flight) its ever-members, the current members of the label or type index
// and the former ones of the K1 membership sidecar (built on first use, one
// pass over the history rows), are enlisted, then each is read once by
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
	_ storecontract.RelValidEndCapability         = (*Store)(nil)
	_ storecontract.NodeValidEndCapability        = (*Store)(nil)
	_ storecontract.RelValidEndMeteredCapability  = (*Store)(nil)
	_ storecontract.NodeValidEndMeteredCapability = (*Store)(nil)
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
		var version uint32
		if hist {
			version = uint32(min(historyVersionFromKey(key), uint64(^uint32(0)))) // #nosec G115 -- clamped to uint32
		}
		ix.NoteRow(id, indexpkg.ValidEndRowOf(hist, version, tm))
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

// tokenMembers is one token's ever-members: its current members (the label
// or type index) and its former ones (the K1 membership sidecar).
type tokenMembers struct {
	each iter.Seq[int64] // every member, once; the index may stop it (TrackUnder)
}

// nodeTokenMembersTalliedLocked is label tok's tokenMembers, counting, in t, the
// walk of a label map or keyspace not in RAM (R2 late counts:
// labelCurrentMembersTalliedLocked); a nil t counts nothing.
func (bs *Store) nodeTokenMembersTalliedLocked(tok uint16, t *storecontract.ReadTally) (tokenMembers, error) {
	current, err := bs.labelCurrentMembersTalliedLocked(tok, t)
	if err != nil {
		return tokenMembers{}, err
	}
	former := bs.labelTxMembers[tok]
	return tokenMembers{
		each: func(yield func(int64) bool) {
			for id := range current {
				if !yield(int64(id)) {
					return
				}
			}
			for id := range former {
				if _, dup := current[id]; !dup && !yield(int64(id)) {
					return
				}
			}
		},
	}, nil
}

// formerSet is one tracked token's former members, the ever-member sidecar in
// RAM: how many, each of them, and a lookup. Caller holds idxMu (R) while it
// is used.
type formerSet struct {
	n    int
	each iter.Seq[int64]
	has  func(id int64) bool
}

func formerSetOf[ID ~int64](m map[ID]types.Instant) formerSet {
	return formerSet{
		n: len(m),
		each: func(yield func(int64) bool) {
			for id := range m {
				if !yield(int64(id)) {
					return
				}
			}
		},
		has: func(id int64) bool {
			_, was := m[ID(id)]
			return was
		},
	}
}

// nodeFormerLocked is label tok's formerSet (labelTxMembers). Caller holds
// idxMu (R).
func (bs *Store) nodeFormerLocked(tok uint16) formerSet {
	return formerSetOf(bs.labelTxMembers[tok])
}

// relFormerLocked is type tok's formerSet (relTypeTxMembers). Caller holds
// idxMu (R).
func (bs *Store) relFormerLocked(tok uint16) formerSet {
	return formerSetOf(bs.relTypeTxMembers[tok])
}

// relTokenMembersLocked is type tok's tokenMembers. Caller holds idxMu.
func (bs *Store) relTokenMembersLocked(tok uint16) (tokenMembers, error) {
	current := bs.typeIdx[tok]
	former := bs.relTypeTxMembers[tok]
	isCurrent := func(id int64) bool { return current != nil && current.Has(id) }
	return tokenMembers{
		each: func(yield func(int64) bool) {
			if current != nil {
				for id := range current.All() {
					if !yield(id) {
						return
					}
				}
			}
			for id := range former {
				if !isCurrent(int64(id)) && !yield(int64(id)) {
					return
				}
			}
		},
	}, nil
}

// trackValidEnds tracks every token of toks not tracked yet (see the file
// comment). ensureK1 builds the membership sidecar, members gives one token's
// ever-members (caller holds idxMu). Under a read budget (t not nil) a token
// whose members the candidate room cannot hold is refused before any is
// enlisted (ValidEndIndex.TrackUnder); the tokens tracked before it stay.
func (bs *Store) trackValidEnds(ix *indexpkg.ValidEndIndex, toks []uint16, ensureK1 func() error, members func(tok uint16) (tokenMembers, error), t *storecontract.ReadTally) error {
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
		if ix.Tracked(tok) {
			continue
		}
		m, err := members(tok)
		if err != nil {
			bs.idxMu.Unlock()
			return err
		}
		if _, err := ix.TrackUnder(tok, m.each, t); err != nil {
			bs.idxMu.Unlock()
			return err
		}
	}
	bs.idxMu.Unlock()
	if bs.validEndTrackHook != nil {
		bs.validEndTrackHook() // tests land writes between the enlisting and the re-read
	}
	return nil
}

// validEndRowsBulk reads what the index folds of every entity in ids (the
// unknown entities of one kind): the history rows' and the current row's
// temporal blocks only, and the tracked tokens each belongs to. The committed
// rows of the entities without a buffered write are read in one badger view
// by one history iterator and one current-row iterator walking forward in ID
// order (advanceTo); an entity with a buffered write when the read starts
// (overlay captured BEFORE that view, lesson 64) is read afterwards by
// validEndOverlayRows, in views of its own. An entity without rows is omitted.
//
// The tokens (R2-2 VEATTRIB): an entity's current ones come off its current
// row, in the same decode (a node's labels, a relationship's type); the tracked
// tokens it carried before come from the ever-member sidecar in RAM (former),
// walked per tracked token on the smaller side, its former set or the entities
// read (attributeFormer). The attribution reads nothing from badger: a flush
// that commits once the rows are read cannot hide a token from it (H1).
//
// Under a read budget (t not nil, RH list R2) each entity counts one candidate
// before its rows are read, each history row one history version and the
// stored bytes of every row as decoded bytes before its value is fetched, and
// each attribution walk its lookups as candidates before it is made, so a
// refusal stops the read at the entity (the row, the walk) that passes the
// limit and the whole call returns the error: no entity is installed.
func (bs *Store) validEndRowsBulk(ix *indexpkg.ValidEndIndex, ids []int64, node bool, former func(tok uint16) formerSet, t *storecontract.ReadTally) (map[int64][]indexpkg.ValidEndRow, error) {
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
	decode := func(id int64, hist bool, version uint32, raw []byte) error {
		bs.validEndRowDecodes.Add(1)
		var (
			tm   *types.TemporalMetadata
			toks []uint16
			err  error
		)
		switch {
		case hist:
			tm, err = storepkg.DecodeValidEndValue(raw, node)
		case node:
			tm, toks, err = storepkg.DecodeValidEndNodeValue(raw) // the current labels, in the same decode
		default:
			var tok uint16
			if tm, tok, err = storepkg.DecodeValidEndRelValue(raw); err == nil {
				toks = []uint16{tok}
			}
		}
		if err != nil {
			return fmt.Errorf("graph: decode row for the valid-end index: %w", err)
		}
		row := indexpkg.ValidEndRowOf(hist, version, tm)
		row.Tokens = toks
		out[id] = append(out[id], row)
		return nil
	}
	// add charges a row before its value is fetched (it may sit in the value
	// log), then decodes it.
	add := func(id int64, hist bool, version uint32, item *badgerv4.Item) error {
		if hist {
			if err := t.History(1); err != nil {
				return err
			}
		}
		if err := t.Decoded(int(item.ValueSize())); err != nil {
			return err
		}
		return item.Value(func(raw []byte) error { return decode(id, hist, version, raw) })
	}
	err := bs.view(func(txn *badgerv4.Txn) error {
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
			if err := t.Candidates(1); err != nil {
				return err
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
				if err := add(id, true, version, hit.Item()); err != nil {
					return err
				}
			}
			prefix[0] = current
			advanceTo(cit, prefix)
			if cit.Valid() && bytes.Equal(cit.Item().Key(), prefix) {
				if err := add(id, false, 0, cit.Item()); err != nil {
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
		if err := t.Candidates(1); err != nil {
			return nil, err
		}
		// An entity with a buffered write is read from the write buffer and the
		// committed rows together, each history row counted before its decode
		// (and the current row's bytes, when it is read from badger).
		rows, err := bs.validEndOverlayRows(id, node, t)
		if err != nil {
			return nil, err
		}
		if len(rows) > 0 {
			out[id] = rows
		}
	}
	if len(out) == 0 {
		return out, nil
	}
	if bs.validEndAttribHook != nil {
		bs.validEndAttribHook() // tests land a flush or a writer after the rows' read
	}
	for _, tok := range ix.TrackedTokens() {
		if err := bs.attributeFormer(tok, former, out, t); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// attributeFormer adds tok to every entity of out that is a former member of
// it (former(tok), under idxMu.RLock, held for this one token so writers wait
// one token's walk at most). It walks the smaller side: the former set against
// out, or out against the former set, min(former, entities) lookups, counted
// as candidates in t before the walk (entries examined); a refusal walks
// nothing. A write between two tokens' walks re-marks its entity unknown, so
// its fold here is not installed (ResolveUnknownBulkUnder).
func (bs *Store) attributeFormer(tok uint16, former func(tok uint16) formerSet, out map[int64][]indexpkg.ValidEndRow, t *storecontract.ReadTally) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	f := former(tok)
	walk := min(f.n, len(out))
	if err := t.Candidates(walk); err != nil {
		return err
	}
	bs.validEndAttribLookups.Add(int64(walk))
	if f.n <= len(out) {
		for id := range f.each {
			if rows, ok := out[id]; ok {
				rows[0].Tokens = append(rows[0].Tokens, tok)
			}
		}
		return nil
	}
	for id, rows := range out {
		if f.has(id) {
			rows[0].Tokens = append(rows[0].Tokens, tok)
		}
	}
	return nil
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
// block and tokens (a node's labels, a relationship's type; the entity cache,
// else the committed key, as RelCurrentStamps reads it), in views of its own. Under a read budget (t not nil) each history row counts a version and
// its stored bytes before it is decoded, and so does the committed current
// row's value.
func (bs *Store) validEndOverlayRows(id int64, node bool, t *storecontract.ReadTally) ([]indexpkg.ValidEndRow, error) {
	sid := snowflake.ID(id)
	var (
		cur    *types.TemporalMetadata
		labels []uint16
		found  = true
		prefix []byte
	)
	if node {
		prefix = storepkg.HistNodePrefix(sid)
		switch n, status := bs.nodeCache.GetNoPromote(sid); status {
		case indexpkg.CacheHit:
			cur, labels = n.Temporal(), indexpkg.ValidEndRowOfNode(n, false).Tokens
		case indexpkg.CacheDeleted:
			found = false
		default:
			bs.idxMu.RLock()
			found = bs.nodeIDs.Has(int64(types.NodeID(sid)))
			bs.idxMu.RUnlock()
			if found {
				var err error
				if cur, labels, found, err = bs.committedTemporal(storepkg.KeyNode, sid, t); err != nil {
					return nil, err
				}
			}
		}
	} else {
		prefix = storepkg.HistRelPrefix(sid)
		switch r, status := bs.relCache.GetNoPromote(sid); status {
		case indexpkg.CacheHit:
			cur, labels = r.Temporal(), indexpkg.ValidEndRowOfRel(r, false).Tokens
		case indexpkg.CacheDeleted:
			found = false
		default:
			bs.idxMu.RLock()
			found = bs.relIDs.Has(int64(types.RelID(sid)))
			bs.idxMu.RUnlock()
			if found {
				var err error
				if cur, labels, found, err = bs.committedTemporal(storepkg.KeyRel, sid, t); err != nil {
					return nil, err
				}
			}
		}
	}
	metas, err := bs.historyTemporalMetaByPrefixTallied(prefix, node, t)
	if err != nil {
		return nil, err
	}
	rows := make([]indexpkg.ValidEndRow, 0, len(metas)+1)
	for _, m := range metas {
		rows = append(rows, indexpkg.ValidEndRowOf(true, m.Version, m.Temporal))
	}
	if found {
		row := indexpkg.ValidEndRowOf(false, 0, cur)
		row.Tokens = labels
		rows = append(rows, row)
	}
	return rows, nil
}

// committedTemporal reads the committed current row of id (prefix KeyNode or
// KeyRel) and decodes its temporal block and its tokens (a node's labels, a
// relationship's type; one decode); found false when the key holds no row.
// Under a read budget (t not nil) the value's stored bytes count before the
// value is fetched.
func (bs *Store) committedTemporal(prefix byte, id snowflake.ID, t *storecontract.ReadTally) (*types.TemporalMetadata, []uint16, bool, error) {
	key := make([]byte, storepkg.SizeNodeKey)
	key[0] = prefix
	storepkg.PutUint64(key, 1, int64(id))
	var (
		tm     *types.TemporalMetadata
		labels []uint16
	)
	found := true
	bs.validEndCommittedReads.Add(1)
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badgerv4.ErrKeyNotFound) {
			found = false
			return nil
		}
		if err != nil {
			return err
		}
		if err := t.Decoded(int(item.ValueSize())); err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var derr error
			if prefix == storepkg.KeyNode {
				_, tm, labels, derr = storepkg.DecodeWireNodeTemporalMetaLabels(val)
			} else {
				var tok uint16
				if _, tm, tok, derr = storepkg.DecodeWireRelTemporalMetaType(val); derr == nil {
					labels = []uint16{tok}
				}
			}
			if derr != nil {
				return fmt.Errorf("graph: decode current row temporal: %w", derr)
			}
			return nil
		})
	})
	return tm, labels, found, err
}

// RelValidEndCandidates implements store.RelValidEndCapability.
func (bs *Store) RelValidEndCandidates(typeToken uint16, pin, validFrom types.Instant) ([]storecontract.ValidEndCandidate, error) {
	return bs.RelValidEndCandidatesMetered(typeToken, pin, validFrom, nil)
}

// RelValidEndCandidatesMetered implements store.RelValidEndMeteredCapability:
// RelValidEndCandidates counting the first call's reads in t (validEndRowsBulk).
func (bs *Store) RelValidEndCandidatesMetered(typeToken uint16, pin, validFrom types.Instant, t *storecontract.ReadTally) ([]storecontract.ValidEndCandidate, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	toks := []uint16{typeToken}
	if err := bs.trackValidEnds(&bs.relValidEnds, toks, bs.ensureRelTypeTxMembersBuilt, bs.relTokenMembersLocked, t); err != nil {
		return nil, err
	}
	if err := bs.relValidEnds.ResolveUnknownBulkUnder(t, func(ids []int64) (map[int64][]indexpkg.ValidEndRow, error) {
		return bs.validEndRowsBulk(&bs.relValidEnds, ids, false, bs.relFormerLocked, t)
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
	return bs.NodeValidEndCandidatesMetered(labelTokens, pin, validFrom, nil)
}

// NodeValidEndCandidatesMetered implements store.NodeValidEndMeteredCapability
// (see RelValidEndCandidatesMetered).
func (bs *Store) NodeValidEndCandidatesMetered(labelTokens []uint16, pin, validFrom types.Instant, t *storecontract.ReadTally) ([]storecontract.ValidEndCandidate, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	members := func(tok uint16) (tokenMembers, error) { return bs.nodeTokenMembersTalliedLocked(tok, t) } // a label map read from its keys counts its walk (a lazy map, a label index on disk)
	if err := bs.trackValidEnds(&bs.nodeValidEnds, labelTokens, bs.ensureLabelTxMembersBuilt, members, t); err != nil {
		return nil, err
	}
	if err := bs.nodeValidEnds.ResolveUnknownBulkUnder(t, func(ids []int64) (map[int64][]indexpkg.ValidEndRow, error) {
		// The attribution (which tracked labels each entity read carries): the
		// current row's labels and the RAM ever-member sidecar; no label map or
		// keyspace of another label is read (attributeFormer).
		return bs.validEndRowsBulk(&bs.nodeValidEnds, ids, true, bs.nodeFormerLocked, t)
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
