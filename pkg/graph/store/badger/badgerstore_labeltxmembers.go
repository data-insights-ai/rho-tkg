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
// runs under idxMu.Lock (no writer/flush can progress — flush takes idxMu.RLock,
// writers idxMu.Lock), reading each committed row's wire tokens directly (no
// property decode, no registry resolve) plus the pending write-buffer overlay,
// so it captures a consistent current+history snapshot. Afterwards every
// label-acquiring door keeps it fresh incrementally; removal/delete doors are
// no-ops (append-only).

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
		bs.recordLabelMemberLocked(uint16(w.PrimaryLabel), nid, tx)
	}
	for _, el := range w.ExtraLabels {
		if el != 0 {
			bs.recordLabelMemberLocked(uint16(el), nid, tx)
		}
	}
}

// recordRelWireMembersLocked is the rel-type mirror of recordNodeWireMembersLocked.
func (bs *Store) recordRelWireMembersLocked(rid types.RelID, w *storepkg.RelWire) {
	if w.RelType == 0 || bs.relTypeTxMembers == nil {
		return
	}
	tok := uint16(w.RelType)
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

// ensureLabelTxMembersBuilt lazily builds the label membership sidecar from the
// committed node/history keyspaces + the pending write-buffer overlay.
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
					var w storepkg.NodeWire
					if err := storepkg.SafeUnmarshal(val, &w); err != nil {
						return nil // skip an unreadable row; the fold fallback stays correct
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

	// Pending write-buffer overlay: unflushed node/history SET rows. Deletes are
	// ignored — membership is append-only (a deleted node stays a historical
	// member). Under idxMu.Lock no writer/flush can mutate these maps.
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
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(op.value, &w); err != nil {
			return
		}
		bs.recordNodeWireMembersLocked(nid, &w)
	})

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

	scanErr := bs.db.View(func(txn *badgerv4.Txn) error {
		for _, prefix := range [][]byte{{storepkg.KeyRel}, {storepkg.KeyHistRel}} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = true
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				rid := types.RelID(storepkg.ParseIDFromKey(item.KeyCopy(nil), 1))
				if verr := item.Value(func(val []byte) error {
					var w storepkg.RelWire
					if err := storepkg.SafeUnmarshal(val, &w); err != nil {
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
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(op.value, &w); err != nil {
			return
		}
		bs.recordRelWireMembersLocked(rid, &w)
	})

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
