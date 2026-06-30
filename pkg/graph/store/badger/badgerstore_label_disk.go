package badger

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Disk-resident label index (enterprise-scale ceiling 1, opt-in via
// Config.LabelIndexOnDisk).
//
// The label keyspace (KeyLabel + token + nodeID, see storeutil.LabelIndexKey)
// has ALWAYS been written transactionally with node rows in the same flush
// batch — loadIndexes uses it as the persisted source of truth to rebuild
// the in-memory labelIdx map on open. Disk mode simply stops materializing
// that map (~50-100B per label entry of permanent RAM — THE ceiling at
// hundreds of millions of nodes) and answers label snapshots from the
// keyspace directly via prefix iteration, overlaying the unflushed pending
// ops so reads see writes immediately, exactly as the map did. Existing
// data directories need no migration: the keyspace is already complete.
//
// Orphan tolerance: a label key whose node row is gone (crash window,
// historical orphan) surfaces an ID whose fetch is skipped by every
// consumer (ErrNodeNotFound continue + HasLabelTokenRaw re-check) — the
// same tolerance the materializing paths already have.

// labelNodeIDsSnapshotLocked returns an unsorted snapshot of the node IDs
// carrying the label. The caller must hold idxMu (R or W) — matching the
// in-memory map read it replaces. Disk mode briefly takes wbMu inside
// (idxMu → wbMu is the flush cycle's established lock order).
func (bs *Store) labelNodeIDsSnapshotLocked(token uint16) ([]types.NodeID, error) {
	if !bs.labelOnDisk {
		set := bs.labelIdx[token]
		if len(set) == 0 {
			return nil, nil
		}
		nids := make([]types.NodeID, 0, len(set))
		for id := range set {
			nids = append(nids, id)
		}
		return nids, nil
	}

	// Pending overlay: unflushed label-index ops must be visible (adds)
	// and authoritative (deletes) over the committed keyspace.
	var adds, dels map[types.NodeID]struct{}
	// rangePending = flushing ++ pending, so a label add/remove a concurrent flush
	// is mid-committing is visible. Set/delete are kept symmetric (each removes the
	// node from the other set) so a node that appears in BOTH buffers during the
	// requeue window resolves to the newer (pending-visited-last) op.
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeLabelIdx || k[0] != storepkg.KeyLabel {
			return
		}
		if binary.BigEndian.Uint16([]byte(k[1:3])) != token {
			return
		}
		nid := types.NodeID(storepkg.ParseIDFromKey([]byte(k), 3))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.NodeID]struct{})
			}
			adds[nid] = struct{}{}
			delete(dels, nid)
		} else {
			if dels == nil {
				dels = make(map[types.NodeID]struct{})
			}
			dels[nid] = struct{}{}
			delete(adds, nid)
		}
	})

	var nids []types.NodeID
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := labelIndexPrefix(token)
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeLabelIdx {
				continue
			}
			nid := types.NodeID(storepkg.ParseIDFromKey(key, 3))
			if _, del := dels[nid]; del {
				continue
			}
			delete(adds, nid) // already committed; don't double-count
			nids = append(nids, nid)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: label index scan: %w", err)
	}
	for nid := range adds {
		nids = append(nids, nid)
	}
	return nids, nil
}

// labelIndexPrefix returns the 3-byte iteration prefix for a label token.
func labelIndexPrefix(token uint16) []byte {
	b := make([]byte, 3)
	b[0] = storepkg.KeyLabel
	binary.BigEndian.PutUint16(b[1:], token)
	return b
}

// nodeLabelTokensFromKeyspaceLocked returns the label tokens the persisted
// keyspace (plus pending overlay) holds for one node — the disk-mode arm of
// the delete-scrub fallback whose node row failed to decode (labels
// unknown). O(whole label keyspace); error-path only. Caller holds idxMu.
func (bs *Store) nodeLabelTokensFromKeyspaceLocked(nid types.NodeID) ([]uint16, error) {
	tokens := make(map[uint16]struct{})
	// rangePending = flushing ++ pending (pending visited last wins on the shared
	// `tokens` map), so a label op a concurrent flush is mid-committing is visible.
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != storepkg.SizeLabelIdx || k[0] != storepkg.KeyLabel {
			return
		}
		if types.NodeID(storepkg.ParseIDFromKey([]byte(k), 3)) != nid {
			return
		}
		tok := binary.BigEndian.Uint16([]byte(k[1:3]))
		if op.opType == writeOpSet {
			tokens[tok] = struct{}{}
		} else {
			delete(tokens, tok)
		}
	})

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyLabel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeLabelIdx {
				continue
			}
			if types.NodeID(storepkg.ParseIDFromKey(key, 3)) != nid {
				continue
			}
			tokens[binary.BigEndian.Uint16(key[1:3])] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: label keyspace scrub scan: %w", err)
	}
	out := make([]uint16, 0, len(tokens))
	for tok := range tokens {
		out = append(out, tok)
	}
	return out, nil
}
