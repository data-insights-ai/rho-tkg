package badger

import (
	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Belief-watermark sidecar (badger arm) — BACKLOG 10c.
//
// See store.NodeBeliefWatermarkCapability's doc comment for the full
// rationale: this restores a SAFE version of the current-row-only fast path
// BACKLOG 10b removed from nodeAtLockedTx/relAtLockedTx, gated on an explicit
// per-entity invariant instead of an assumption a bounded cascade correction
// could silently break.
//
// LAZY, RAM-only, built from the committed current-node (0x01) + node-history
// (0x07) keyspaces plus the pending write-buffer overlay (same shape as
// labelTxMembers/relTypeTxMembers in badgerstore_labeltxmembers.go — this
// file reuses decodeNodeWireForMembership/decodeRelWireForMembership since
// TxFrom is exposed the same way labels/rel-type are, delta-aware). Never
// decreases — bumping with an OLD, already-recorded TxFrom (e.g. a
// transaction rollback restoring prior state verbatim) is a safe no-op.
//
// No cleanup/drop helper: snowflake IDs are never reused, so a stale entry
// for a since-purged entity can never be misattributed later — see the
// memory-arm sidecar's identical note for the full rationale.

func nodeTxFrom(n *types.Node) types.Instant {
	if tm := n.Temporal(); tm != nil {
		return tm.TxFrom
	}
	return 0
}

func relTxFrom(r *types.Relationship) types.Instant {
	if tm := r.Temporal(); tm != nil {
		return tm.TxFrom
	}
	return 0
}

// bumpNodeBeliefWatermarkLocked records txFrom as a new lower bound for id's
// watermark if it exceeds what's already recorded. No-op until the sidecar
// is built. Caller holds idxMu (write).
func (bs *Store) bumpNodeBeliefWatermarkLocked(id types.NodeID, txFrom types.Instant) {
	if bs.nodeBeliefWatermark == nil {
		return
	}
	if prev, ok := bs.nodeBeliefWatermark[id]; !ok || txFrom > prev {
		bs.nodeBeliefWatermark[id] = txFrom
	}
}

// bumpRelBeliefWatermarkLocked mirrors bumpNodeBeliefWatermarkLocked for
// relationships. Caller holds idxMu (write).
func (bs *Store) bumpRelBeliefWatermarkLocked(id types.RelID, txFrom types.Instant) {
	if bs.relBeliefWatermark == nil {
		return
	}
	if prev, ok := bs.relBeliefWatermark[id]; !ok || txFrom > prev {
		bs.relBeliefWatermark[id] = txFrom
	}
}

// ensureNodeBeliefWatermarkBuilt lazily builds the node watermark sidecar
// from the committed current+history keyspaces + the pending overlay.
func (bs *Store) ensureNodeBeliefWatermarkBuilt() error {
	if bs.nodeBeliefWatermarkBuilt.Load() {
		return nil
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.nodeBeliefWatermark != nil {
		return nil // built by a racing caller while we waited for the lock
	}
	built := make(map[types.NodeID]types.Instant)
	bs.nodeBeliefWatermark = built

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
					bs.bumpNodeBeliefWatermarkLocked(nid, types.Instant(w.TxFrom))
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
		bs.nodeBeliefWatermark = nil // failed build — retry on next call
		return scanErr
	}

	// Pending write-buffer overlay: unflushed node/history rows. Under
	// idxMu.Lock no writer/flush can mutate these maps.
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
		bs.bumpNodeBeliefWatermarkLocked(nid, types.Instant(w.TxFrom))
	})

	bs.nodeBeliefWatermarkBuilt.Store(true)
	return nil
}

// ensureRelBeliefWatermarkBuilt lazily builds the rel watermark sidecar.
func (bs *Store) ensureRelBeliefWatermarkBuilt() error {
	if bs.relBeliefWatermarkBuilt.Load() {
		return nil
	}
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if bs.relBeliefWatermark != nil {
		return nil
	}
	built := make(map[types.RelID]types.Instant)
	bs.relBeliefWatermark = built

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
					bs.bumpRelBeliefWatermarkLocked(rid, types.Instant(w.TxFrom))
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
		bs.relBeliefWatermark = nil
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
		w, ok := decodeRelWireForMembership(op.value)
		if !ok {
			return
		}
		bs.bumpRelBeliefWatermarkLocked(rid, types.Instant(w.TxFrom))
	})

	bs.relBeliefWatermarkBuilt.Store(true)
	return nil
}

// NodeBeliefWatermark implements store.NodeBeliefWatermarkCapability.
func (bs *Store) NodeBeliefWatermark(id types.NodeID) (types.Instant, bool) {
	if err := bs.checkOpen(); err != nil {
		return 0, false
	}
	if err := bs.ensureNodeBeliefWatermarkBuilt(); err != nil {
		return 0, false
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	tx, ok := bs.nodeBeliefWatermark[id]
	return tx, ok
}

// RelBeliefWatermark implements store.RelBeliefWatermarkCapability.
func (bs *Store) RelBeliefWatermark(id types.RelID) (types.Instant, bool) {
	if err := bs.checkOpen(); err != nil {
		return 0, false
	}
	if err := bs.ensureRelBeliefWatermarkBuilt(); err != nil {
		return 0, false
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	tx, ok := bs.relBeliefWatermark[id]
	return tx, ok
}
