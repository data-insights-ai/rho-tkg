package badger

import (
	"bytes"
	"errors"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Transaction-time label / rel-type membership sidecar (badger arm).
//
// A history-aware ByLabel/ByType scan (one carrying a temporal filter) must
// consider every node/rel whose PAST version could match the queried label/type
// at the pinned instant. Previously the core layer folded ALL node/rel history
// into the candidate set, so a pinned scan for a selective label cost
// O(everything that ever carried ANY label) rather than O(matches).
//
// labelTxMembers maps a label token to the FORMER members (TXMEMBERS): the
// node IDs a stored version (a history row) carried it in while their current
// row does not, tagged with a lower bound on the transaction time of the
// earliest acquisition. The current members are the label index's (labelIdx,
// or the persisted keyspace in LabelIndexOnDisk mode) and are not repeated
// here, so the sidecar grows with label removals and deletes, not with every
// node that ever carried the label (ai-soc 27 Oct: +127 MB in ten minutes on a
// reference shard whose entities never lose a label). relTypeTxMembers is the
// immutable-type rel mirror over typeIdx. Both are SOUND SUPERSETS (a stale
// entry, an ID that carries the token again, is harmless) and the core chain
// resolver remains the correctness authority. firstTxFrom is a lower bound (0
// = never prune); pruning is `pin < firstTxFrom → skip`. A history row written
// while the current row carried the token is not recorded, so its bound is
// unknown: the ID departs (label removal, delete) with 0, and a point lookup
// answers a current member with 0.
//
// LAZY, RAM-only: nil until the first pinned scan builds it (mirrors the
// relValidIdx precedent), rebuilt after a Close/reopen on next use. The build
// runs under idxMu.Lock (no writer can progress and no NEW flush can park —
// flush takes idxMu.RLock only for its snapshot+swap, so an already-parked flush
// still commits during the build), reading the write-buffer overlay (flushing ++
// pending) FIRST and then each committed history row's wire tokens and its
// entity's current row directly (no property decode, no registry resolve), so
// it captures a complete snapshot (lesson 64 ordering). Afterwards every door
// keeps it fresh incrementally: a history row records the tokens the entity's
// current row does not carry, a label-index removal (departLabelMemberLocked,
// removeTypeIdxLocked) records the departure.

// enqueueVersionAgainstLazyBuilds enqueues a history-version op for a door that
// does not hold idxMu across its write (PutNodeVersion / PutRelVersion), keeping
// the lazily built RAM structures that must see every version row — the K1
// membership sidecars and the belief watermarks — complete. built reports
// whether any of them is built; record updates them (caller-provided, run under
// idxMu.Lock); enqueue buffers the op.
//
// A lazy build holds idxMu.Lock from its overlay capture until it marks itself
// built. Reading the flag without idxMu let a row be enqueued behind a running
// build's overlay capture while the flag still read false: the build never saw
// it and the door never recorded it, so the member was lost for the life of the
// store. Checking the flag and enqueueing under idxMu.RLock closes that: the row
// is either enqueued before the build starts (its overlay capture or Badger scan
// sees it) or after it finished (record runs). The not-built path — import and
// replica bootstrap before any pinned scan — stays on the shared lock.
func (bs *Store) enqueueVersionAgainstLazyBuilds(built func() bool, record func(), enqueue func() error) error {
	bs.idxMu.RLock()
	if !built() {
		err := enqueue()
		bs.idxMu.RUnlock()
		return err
	}
	bs.idxMu.RUnlock()
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	record()
	return enqueue()
}

// recordLabelMemberLocked records node id as a former member of tok with the
// given acquisition transaction time, keeping the lowest firstTxFrom seen; 0 (an unstamped row, visible to a TxAt read at
// every pin, or an unknown bound) is never raised (index.MergeFirstTx).
// No-op until the sidecar is built. Caller holds idxMu (write).
func (bs *Store) recordLabelMemberLocked(tok uint16, id types.NodeID, txFrom types.Instant) {
	if tok == 0 || bs.labelTxMembers == nil {
		return
	}
	set := bs.labelTxMembers[tok]
	if set == nil {
		set = make(map[types.NodeID]types.Instant)
		bs.labelTxMembers[tok] = set
	}
	if prev, ok := set[id]; ok {
		set[id] = indexpkg.MergeFirstTx(prev, txFrom)
	} else {
		set[id] = txFrom
	}
}

// departLabelMemberLocked notes that nid's current row stops carrying tok (a
// label removal or a delete): the history rows that carried tok while the
// current row did too were not recorded, so nid becomes a former member with
// an unknown first acquisition. Caller holds idxMu (write).
func (bs *Store) departLabelMemberLocked(tok uint16, nid types.NodeID) {
	bs.recordLabelMemberLocked(tok, nid, 0)
}

// recordNodeLabelMembersLocked records n, a row a door writes: each token
// keep, the entity's current row after the door, does not carry, with n's
// transaction-time stamp. A current row passes itself (a member through the
// label index: nothing recorded). keep nil: the door does not hold it (a
// history-only version, a tombstone), so it is read; none, or unreadable,
// records every token. A token the current row carries is recorded at its
// departure (departLabelMemberLocked): a label changes only through the label
// doors. Caller holds idxMu (write).
func (bs *Store) recordNodeLabelMembersLocked(n, keep *types.Node) {
	if bs.labelTxMembers == nil || n == nil {
		return
	}
	bs.noteValidEndNodeMember(n) // backlog 51: a tracked label's new member
	if keep == nil {
		keep, _ = bs.getNodeLocked(n.ID())
	}
	var tx types.Instant
	if tm := n.Temporal(); tm != nil {
		tx = tm.TxFrom
	}
	id := n.ID()
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		if tok := n.LabelTokenRawAt(i); keep == nil || !keep.HasLabelTokenRaw(tok) {
			bs.recordLabelMemberLocked(tok, id, tx)
		}
	}
}

// recordRelTypeTxLocked records rel id as a former member of tok (see
// recordLabelMemberLocked). Caller holds idxMu (write).
func (bs *Store) recordRelTypeTxLocked(tok uint16, id types.RelID, txFrom types.Instant) {
	if tok == 0 || bs.relTypeTxMembers == nil {
		return
	}
	set := bs.relTypeTxMembers[tok]
	if set == nil {
		set = make(map[types.RelID]types.Instant)
		bs.relTypeTxMembers[tok] = set
	}
	if prev, ok := set[id]; ok {
		set[id] = indexpkg.MergeFirstTx(prev, txFrom)
	} else {
		set[id] = txFrom
	}
}

// recordRelTypeMemberLocked is recordNodeLabelMembersLocked for a rel row (a
// current one, or a history one): its type, unless the rel is a current
// member of it (typeIdx, which the doors fill before recording;
// removeTypeIdxLocked records the departure). Caller holds idxMu (write).
func (bs *Store) recordRelTypeMemberLocked(r *types.Relationship) {
	if bs.relTypeTxMembers == nil || r == nil {
		return
	}
	bs.noteValidEndRelMember(r) // backlog 51: a tracked type's new member
	tok := r.TypeToken().Value()
	if set := bs.typeIdx[tok]; set != nil && set.Has(int64(r.ID())) {
		return
	}
	var tx types.Instant
	if tm := r.Temporal(); tm != nil {
		tx = tm.TxFrom
	}
	bs.recordRelTypeTxLocked(tok, r.ID(), tx)
}

// nodeWireCarries reports whether a decoded node row carries tok.
func nodeWireCarries(w *storepkg.NodeWire, tok uint16) bool {
	if uint16(w.PrimaryLabel) == tok { // #nosec G115 -- wire label tokens are validated as uint16 on decode
		return true
	}
	for _, el := range w.ExtraLabels {
		if uint16(el) == tok { // #nosec G115 -- wire label tokens are validated as uint16 on decode
			return true
		}
	}
	return false
}

// recordNodeWireMembersLocked records the label tokens of a decoded history
// row (no full node reconstruction) that cur, its entity's current row (nil:
// none), does not carry. nid comes from the key. Caller holds idxMu (write).
func (bs *Store) recordNodeWireMembersLocked(nid types.NodeID, w, cur *storepkg.NodeWire) {
	tx := types.Instant(w.TxFrom)
	record := func(tok uint16) {
		if tok != 0 && (cur == nil || !nodeWireCarries(cur, tok)) {
			bs.recordLabelMemberLocked(tok, nid, tx)
		}
	}
	record(uint16(w.PrimaryLabel)) // #nosec G115 -- wire label tokens are validated as uint16 on decode
	for _, el := range w.ExtraLabels {
		record(uint16(el)) // #nosec G115 -- wire label tokens are validated as uint16 on decode
	}
}

// recordRelWireMembersLocked is the rel-type mirror of recordNodeWireMembersLocked.
func (bs *Store) recordRelWireMembersLocked(rid types.RelID, w, cur *storepkg.RelWire) {
	if cur != nil && cur.RelType == w.RelType {
		return
	}
	bs.recordRelTypeTxLocked(uint16(w.RelType), rid, types.Instant(w.TxFrom)) // #nosec G115 -- wire relationship token is validated as uint16 on decode
}

// decodeNodeWireForMembership decodes a node/node-history row's wire label
// tokens + TxFrom for the K1 sidecar build, delta-aware (BACKLOG 18d): a
// history row (0x07) can be a 'D'-tagged DELTA under HistoryDeltaEncoding, and
// a bare SafeUnmarshal into NodeWire fails on one (a delta's first byte is not
// a valid msgpack map header) — the old code silently skipped such rows ("skip
// an unreadable row; the fold fallback stays correct"), which is WRONG once
// labelTxMembersBuilt is set: after that point the sidecar is the ONLY
// candidate source a pinned scan consults, so a node whose ONLY label
// evidence is a delta row (the common case — up to HistoryAnchorInterval-1 out
// of every HistoryAnchorInterval versions) was never recorded. The sidecar's
// documented guarantee is a SOUND SUPERSET (over-inclusion is safe,
// under-inclusion is not), so silently dropping real members broke that
// contract. The fix needs no anchor read: DiffNodeHistory builds a delta's
// Meta as `target` with `Properties` cleared, so every NON-property field —
// including PrimaryLabel/ExtraLabels/TxFrom — is carried in Meta verbatim.
// Current rows (0x01) are always full (never delta-tagged), so this helper is
// safe to use uniformly for both keyspaces.
func decodeNodeWireForMembership(val []byte) (storepkg.NodeWire, bool) {
	if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return storepkg.NodeWire{}, false
		}
		return w, true
	}
	d, err := storepkg.DecodeNodeHistoryDelta(val)
	if err != nil {
		return storepkg.NodeWire{}, false
	}
	return d.Meta, true
}

// decodeRelWireForMembership mirrors decodeNodeWireForMembership for relationships.
func decodeRelWireForMembership(val []byte) (storepkg.RelWire, bool) {
	if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return storepkg.RelWire{}, false
		}
		return w, true
	}
	d, err := storepkg.DecodeRelHistoryDelta(val)
	if err != nil {
		return storepkg.RelWire{}, false
	}
	return d.Meta, true
}

// txMemberBuildRow is one buffered history row a sidecar build records.
type txMemberBuildRow[W any] struct {
	id  int64
	row W
}

// buildTxMembersLocked is the sidecar build of both kinds: the history rows of
// the write-buffer overlay (read FIRST) and of the committed history keyspace,
// each recorded against its entity's current row, the overlay's if buffered,
// else the committed one (lesson 64: a flush that already parked `pending`
// into `flushing` still commits while idxMu.Lock is held; read the other way
// round, a row it commits between the View and the overlay read is in
// neither). Deletes of history rows are ignored: a stale former member is
// harmless. Caller holds idxMu (write).
func buildTxMembersLocked[W any](bs *Store, currentTag, historyTag byte, currentSize int, decode func([]byte) (W, bool), record func(id int64, w, cur *W)) error {
	type overlayCurrent struct {
		row W
		ok  bool // false: buffered delete or an unreadable row
	}
	current := make(map[int64]overlayCurrent)
	var history []txMemberBuildRow[W]
	bs.rangePending(func(k string, op writeOp) {
		kb := []byte(k)
		switch {
		case len(kb) == currentSize && kb[0] == currentTag:
			var c overlayCurrent
			if op.opType != writeOpDelete && len(op.value) > 0 {
				c.row, c.ok = decode(op.value)
			}
			current[int64(storepkg.ParseIDFromKey(kb, 1))] = c
		case len(kb) == storepkg.SizeHistKey && kb[0] == historyTag:
			if op.opType == writeOpDelete || len(op.value) == 0 {
				return
			}
			if w, ok := decode(op.value); ok {
				history = append(history, txMemberBuildRow[W]{id: int64(storepkg.ParseIDFromKey(kb, 1)), row: w})
			}
		}
	})
	return bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // values are inline: no goroutine per row (PREFETCH)
		cit := txn.NewIterator(opts)
		defer cit.Close()
		cit.Seek([]byte{currentTag})
		key := make([]byte, 1+8)
		key[0] = currentTag
		// currentOf returns id's current row; ids come in ascending order.
		var lastID int64
		var last *W
		have := false
		currentOf := func(id int64) (*W, error) {
			if have && id == lastID {
				return last, nil
			}
			lastID, last, have = id, nil, true
			if c, buffered := current[id]; buffered {
				if c.ok {
					row := c.row
					last = &row
				}
				return last, nil
			}
			storepkg.PutUint64(key, 1, id)
			advanceTo(cit, key)
			if !cit.Valid() || !bytes.Equal(cit.Item().Key(), key) {
				return nil, nil
			}
			if err := cit.Item().Value(func(val []byte) error {
				if w, ok := decode(val); ok {
					last = &w
				}
				return nil
			}); err != nil {
				return nil, err
			}
			return last, nil
		}
		hit := txn.NewIterator(opts)
		defer hit.Close()
		prefix := []byte{historyTag}
		for hit.Seek(prefix); hit.ValidForPrefix(prefix); hit.Next() {
			item := hit.Item()
			if len(item.Key()) != storepkg.SizeHistKey {
				continue
			}
			id := int64(storepkg.ParseIDFromKey(item.Key(), 1))
			var w W
			ok := false
			if err := item.Value(func(val []byte) error {
				w, ok = decode(val)
				return nil
			}); err != nil {
				return err
			}
			if !ok {
				continue // skip a genuinely corrupt row; the fold fallback stays correct
			}
			cur, err := currentOf(id)
			if err != nil {
				return err
			}
			record(id, &w, cur)
		}
		// The buffered history rows, out of ID order: fresh current lookups.
		for _, h := range history {
			var cur *W
			if c, buffered := current[h.id]; buffered {
				if c.ok {
					cur = &c.row
				}
			} else {
				storepkg.PutUint64(key, 1, h.id)
				item, err := txn.Get(key)
				switch {
				case errors.Is(err, badgerv4.ErrKeyNotFound):
				case err != nil:
					return err
				default:
					if err := item.Value(func(val []byte) error {
						if w, ok := decode(val); ok {
							cur = &w
						}
						return nil
					}); err != nil {
						return err
					}
				}
			}
			record(h.id, &h.row, cur)
		}
		return nil
	})
}

// ensureLabelTxMembersBuilt lazily builds the label membership sidecar from the
// history rows of the write-buffer overlay and the committed keyspace.
func (bs *Store) ensureLabelTxMembersBuilt() error {
	if bs.labelTxMembersBuilt.Load() {
		return nil
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.labelTxMembers != nil {
		return nil // built by a racing caller while we waited for the lock
	}
	bs.labelTxMembers = make(map[uint16]map[types.NodeID]types.Instant)
	err := buildTxMembersLocked(bs, storepkg.KeyNode, storepkg.KeyHistNode, storepkg.SizeNodeKey, decodeNodeWireForMembership,
		func(id int64, w, cur *storepkg.NodeWire) { bs.recordNodeWireMembersLocked(types.NodeID(id), w, cur) })
	if err != nil {
		bs.labelTxMembers = nil // failed build — retry on next call
		return err
	}
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}
	bs.labelTxMembersBuilt.Store(true)
	return nil
}

// ensureRelTypeTxMembersBuilt lazily builds the rel-type membership sidecar.
func (bs *Store) ensureRelTypeTxMembersBuilt() error {
	if bs.relTypeMembersBuilt.Load() {
		return nil
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.relTypeTxMembers != nil {
		return nil
	}
	bs.relTypeTxMembers = make(map[uint16]map[types.RelID]types.Instant)
	err := buildTxMembersLocked(bs, storepkg.KeyRel, storepkg.KeyHistRel, storepkg.SizeRelKey, decodeRelWireForMembership,
		func(id int64, w, cur *storepkg.RelWire) { bs.recordRelWireMembersLocked(types.RelID(id), w, cur) })
	if err != nil {
		bs.relTypeTxMembers = nil
		return err
	}
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}
	bs.relTypeMembersBuilt.Store(true)
	return nil
}

// forgetTxMembersLocked drops a purged node and its purged relationships from
// the former members (the departure the purge's cascade recorded included):
// no row of them is left to resolve. Caller holds idxMu (write).
func (bs *Store) forgetTxMembersLocked(nid types.NodeID, rels []RelDeleteInfo) {
	for tok, members := range bs.labelTxMembers {
		delete(members, nid)
		if len(members) == 0 {
			delete(bs.labelTxMembers, tok)
		}
	}
	for _, rd := range rels {
		if members := bs.relTypeTxMembers[rd.RelType]; members != nil {
			delete(members, types.RelID(rd.ID))
			if len(members) == 0 {
				delete(bs.relTypeTxMembers, rd.RelType)
			}
		}
	}
}

// ForEachLabelTxMember implements store.LabelTxMembershipCapability.
func (bs *Store) ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := bs.ensureLabelTxMembersBuilt(); err != nil {
		return err
	}
	// Snapshot under idxMu.RLock so fn runs OUTSIDE the store lock (it re-enters
	// the store to resolve chains).
	bs.idxMu.RLock()
	set := bs.labelTxMembers[token]
	type member struct {
		id types.NodeID
		tx types.Instant
	}
	members := make([]member, 0, len(set))
	for id, tx := range set {
		members = append(members, member{id: id, tx: tx})
	}
	bs.idxMu.RUnlock()
	for _, m := range members {
		if !fn(m.id, m.tx) {
			return nil
		}
	}
	return nil
}

// ForEachRelTypeTxMember implements store.RelTypeTxMembershipCapability.
func (bs *Store) ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := bs.ensureRelTypeTxMembersBuilt(); err != nil {
		return err
	}
	bs.idxMu.RLock()
	set := bs.relTypeTxMembers[token]
	type member struct {
		id types.RelID
		tx types.Instant
	}
	members := make([]member, 0, len(set))
	for id, tx := range set {
		members = append(members, member{id: id, tx: tx})
	}
	bs.idxMu.RUnlock()
	for _, m := range members {
		if !fn(m.id, m.tx) {
			return nil
		}
	}
	return nil
}

// LabelTxMembersOf implements store.LabelTxMemberLookupCapability: each of ids
// that is a current member of token (firstTxFrom 0) or a former one (the
// sidecar, built on first use). fn runs outside the store lock.
func (bs *Store) LabelTxMembersOf(token uint16, ids []types.NodeID, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := bs.ensureLabelTxMembersBuilt(); err != nil {
		return err
	}
	bs.idxMu.RLock()
	current, err := bs.labelCurrentMembersLocked(token)
	var members []indexpkg.TxMember[types.NodeID]
	if err == nil {
		members = txMembersOf(bs.labelTxMembers[token], ids, func(id types.NodeID) bool {
			_, ok := current[id]
			return ok
		})
	}
	bs.idxMu.RUnlock()
	if err != nil {
		return err
	}
	for _, m := range members {
		if !fn(m.ID, m.FirstTx) {
			return nil
		}
	}
	return nil
}

// RelTypeTxMembersOf implements store.RelTypeTxMemberLookupCapability.
func (bs *Store) RelTypeTxMembersOf(token uint16, ids []types.RelID, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := bs.ensureRelTypeTxMembersBuilt(); err != nil {
		return err
	}
	bs.idxMu.RLock()
	current := bs.typeIdx[token]
	members := txMembersOf(bs.relTypeTxMembers[token], ids, func(id types.RelID) bool {
		return current != nil && current.Has(int64(id))
	})
	bs.idxMu.RUnlock()
	for _, m := range members {
		if !fn(m.ID, m.FirstTx) {
			return nil
		}
	}
	return nil
}

// labelCurrentMembersLocked returns token's current members: the label index,
// in LabelIndexOnDisk mode read from the persisted keyspace. The caller holds
// idxMu (R or W) and only reads the set.
func (bs *Store) labelCurrentMembersLocked(token uint16) (map[types.NodeID]struct{}, error) {
	if !bs.labelOnDisk {
		return bs.labelIdx[token], nil
	}
	ids, err := bs.labelNodeIDsSnapshotLocked(token)
	if err != nil {
		return nil, err
	}
	set := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set, nil
}

// txMembersOf returns the members of token among ids (point lookups): a
// current member (current) with an unknown first acquisition, else a former
// one from set.
func txMembersOf[ID comparable](set map[ID]types.Instant, ids []ID, current func(ID) bool) []indexpkg.TxMember[ID] {
	var out []indexpkg.TxMember[ID]
	for _, id := range ids {
		if current(id) {
			out = append(out, indexpkg.TxMember[ID]{ID: id})
		} else if tx, ok := set[id]; ok {
			out = append(out, indexpkg.TxMember[ID]{ID: id, FirstTx: tx})
		}
	}
	return out
}
