package badger

import (
	badgerv4 "github.com/dgraph-io/badger/v4"

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
// labelTxMembers maps a label token to the set of node IDs that EVER carried it
// (current OR any historical version), tagged with a lower bound on the
// transaction time of the earliest acquisition. relTypeTxMembers is the
// immutable-type rel mirror. Both are SOUND SUPERSETS (append-only — a removed/
// deleted member is retained so a pin before the removal still admits it) and
// the core chain resolver remains the correctness authority. firstTxFrom is a
// lower bound (0 = never prune); pruning is `pin < firstTxFrom → skip`.
//
// LAZY, RAM-only: nil until the first pinned scan builds it (mirrors the
// relValidIdx precedent), rebuilt after a Close/reopen on next use. The build
// runs under idxMu.Lock (no writer can progress and no NEW flush can park —
// flush takes idxMu.RLock only for its snapshot+swap, so an already-parked flush
// still commits during the build), reading the write-buffer overlay (flushing ++
// pending) FIRST and then each committed row's wire tokens directly (no property
// decode, no registry resolve), so it captures a complete current+history
// snapshot (lesson 64 ordering). Afterwards every
// label-acquiring door keeps it fresh incrementally; removal/delete doors are
// no-ops (append-only).

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

// recordLabelMemberLocked records node id as an ever-member of tok with the
// given acquisition transaction time, keeping the lowest firstTxFrom seen.
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
	if prev, ok := set[id]; !ok || (txFrom != 0 && (prev == 0 || txFrom < prev)) {
		set[id] = txFrom
	}
}

// recordNodeLabelMembersLocked records every label token n carries with n's
// transaction-time stamp. Caller holds idxMu (write).
func (bs *Store) recordNodeLabelMembersLocked(n *types.Node) {
	if bs.labelTxMembers == nil || n == nil {
		return
	}
	var tx types.Instant
	if tm := n.Temporal(); tm != nil {
		tx = tm.TxFrom
	}
	id := n.ID()
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		bs.recordLabelMemberLocked(n.LabelTokenRawAt(i), id, tx)
	}
}

// recordRelTypeMemberLocked records rel r as an ever-member of its type token.
// Caller holds idxMu (write).
func (bs *Store) recordRelTypeMemberLocked(r *types.Relationship) {
	if bs.relTypeTxMembers == nil || r == nil {
		return
	}
	tok := r.TypeToken().Value()
	if tok == 0 {
		return
	}
	var tx types.Instant
	if tm := r.Temporal(); tm != nil {
		tx = tm.TxFrom
	}
	id := r.ID()
	set := bs.relTypeTxMembers[tok]
	if set == nil {
		set = make(map[types.RelID]types.Instant)
		bs.relTypeTxMembers[tok] = set
	}
	if prev, ok := set[id]; !ok || (tx != 0 && (prev == 0 || tx < prev)) {
		set[id] = tx
	}
}

// recordNodeWireMembersLocked reads label tokens + TxFrom straight from a decoded
// NodeWire (no full node reconstruction) and records them. nid comes from the
// key. Caller holds idxMu (write).
func (bs *Store) recordNodeWireMembersLocked(nid types.NodeID, w *storepkg.NodeWire) {
	tx := types.Instant(w.TxFrom)
	if w.PrimaryLabel != 0 {
		bs.recordLabelMemberLocked(uint16(w.PrimaryLabel), nid, tx) // #nosec G115 -- wire label tokens are validated as uint16 on decode
	}
	for _, el := range w.ExtraLabels {
		if el != 0 {
			bs.recordLabelMemberLocked(uint16(el), nid, tx) // #nosec G115 -- wire label tokens are validated as uint16 on decode
		}
	}
}

// recordRelWireMembersLocked is the rel-type mirror of recordNodeWireMembersLocked.
func (bs *Store) recordRelWireMembersLocked(rid types.RelID, w *storepkg.RelWire) {
	if w.RelType == 0 || bs.relTypeTxMembers == nil {
		return
	}
	tok := uint16(w.RelType) // #nosec G115 -- wire relationship token is validated as uint16 on decode
	tx := types.Instant(w.TxFrom)
	set := bs.relTypeTxMembers[tok]
	if set == nil {
		set = make(map[types.RelID]types.Instant)
		bs.relTypeTxMembers[tok] = set
	}
	if p, has := set[rid]; !has || (tx != 0 && (p == 0 || tx < p)) {
		set[rid] = tx
	}
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

// ensureLabelTxMembersBuilt lazily builds the label membership sidecar from the
// write-buffer overlay (read first) + the committed node/history keyspaces.
func (bs *Store) ensureLabelTxMembersBuilt() error {
	if bs.labelTxMembersBuilt.Load() {
		return nil
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.labelTxMembers != nil {
		return nil // built by a racing caller while we waited for the lock
	}
	members := make(map[uint16]map[types.NodeID]types.Instant)
	bs.labelTxMembers = members

	// Write-buffer overlay FIRST, Badger View SECOND (lesson 64). idxMu.Lock
	// keeps writers out, but a flush that already parked `pending` into
	// `flushing` has released its idxMu.RLock and still commits to Badger and
	// clears `flushing` while this build runs. Read the other way round, a row
	// that flush commits between the View and the overlay read is in neither
	// and the member is lost for the life of the store (the build never
	// repeats). Overlay first: a row committed before the capture is in the
	// later View; a row committed after it was in the overlay. Deletes are
	// ignored — membership is append-only (a deleted node stays a historical
	// member).
	bs.rangePending(func(k string, op writeOp) {
		if op.opType == writeOpDelete || len(op.value) == 0 {
			return
		}
		kb := []byte(k)
		if len(kb) == 0 {
			return
		}
		switch kb[0] {
		case storepkg.KeyNode, storepkg.KeyHistNode:
		default:
			return
		}
		nid := types.NodeID(storepkg.ParseIDFromKey(kb, 1))
		w, ok := decodeNodeWireForMembership(op.value)
		if !ok {
			return
		}
		bs.recordNodeWireMembersLocked(nid, &w)
	})

	// Committed rows: scan the current-node (0x01) and node-history (0x07)
	// keyspaces, decoding only the wire label tokens + TxFrom.
	scanErr := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, prefix := range [][]byte{{storepkg.KeyNode}, {storepkg.KeyHistNode}} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				nid := types.NodeID(storepkg.ParseIDFromKey(item.KeyCopy(nil), 1))
				if verr := item.Value(func(val []byte) error {
					w, ok := decodeNodeWireForMembership(val)
					if !ok {
						return nil // skip a genuinely corrupt row; the fold fallback stays correct
					}
					bs.recordNodeWireMembersLocked(nid, &w)
					return nil
				}); verr != nil {
					it.Close()
					return verr
				}
			}
			it.Close()
		}
		return nil
	})
	if scanErr != nil {
		bs.labelTxMembers = nil // failed build — retry on next call
		return scanErr
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
	members := make(map[uint16]map[types.RelID]types.Instant)
	bs.relTypeTxMembers = members

	// Overlay first, View second — see ensureLabelTxMembersBuilt.
	bs.rangePending(func(k string, op writeOp) {
		if op.opType == writeOpDelete || len(op.value) == 0 {
			return
		}
		kb := []byte(k)
		if len(kb) == 0 {
			return
		}
		switch kb[0] {
		case storepkg.KeyRel, storepkg.KeyHistRel:
		default:
			return
		}
		rid := types.RelID(storepkg.ParseIDFromKey(kb, 1))
		w, ok := decodeRelWireForMembership(op.value)
		if !ok {
			return
		}
		bs.recordRelWireMembersLocked(rid, &w)
	})

	scanErr := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, prefix := range [][]byte{{storepkg.KeyRel}, {storepkg.KeyHistRel}} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				rid := types.RelID(storepkg.ParseIDFromKey(item.KeyCopy(nil), 1))
				if verr := item.Value(func(val []byte) error {
					w, ok := decodeRelWireForMembership(val)
					if !ok {
						return nil
					}
					bs.recordRelWireMembersLocked(rid, &w)
					return nil
				}); verr != nil {
					it.Close()
					return verr
				}
			}
			it.Close()
		}
		return nil
	})
	if scanErr != nil {
		bs.relTypeTxMembers = nil
		return scanErr
	}
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}

	bs.relTypeMembersBuilt.Store(true)
	return nil
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
