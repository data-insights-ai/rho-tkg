package badger

import (
	"encoding/binary"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// CollectShardDropResidue supports the tiered whole-shard fast-drop (ADR-0008 R4). Under
// one read lock it reports whether this shard holds ONLY nodes carrying labelToken
// (onlyLabel) and, when it does, returns every such node ID plus every relationship
// touching those nodes — decoded from adjacency KEYS (both legs), deduped by rel ID —
// so the tiered layer can sweep any cross-shard residue on SURVIVOR shards before
// physically removing this shard. Mutates NOTHING (the physical removal is the tiered
// layer's os.RemoveAll). When onlyLabel is false the shard carries other labels and is
// NOT droppable by a single-label purge — nodeIDs/rels are empty and the caller falls
// back to the row-scan purge. Deciding onlyLabel is CONSERVATIVE: any foreign label
// token present forces false (a safe over-decline).
//
// BACKLOG 18p: genuinely read-only (checkOpen, idxMu.RLock), matching the doc claims
// above — a caller may run this against a read-only-opened Store (e.g. a
// transiently-read-opened cold shard). Its three constituent reads (
// hasForeignLabelTokensLocked, labelNodeIDsSnapshotLocked, purgedRelsForNodeLocked) are
// each independently safe under a shared idxMu hold: labelNodeIDsSnapshotLocked is
// already called under RLock elsewhere (e.g. ForEachNodeByLabel), and
// purgedRelsForNodeLocked reads only a Badger db.View snapshot plus rangePending (which
// guards itself with the separate wbMu) — RLock already excludes any idxMu.Lock-held
// writer for the duration, which is the only cross-consistency guarantee these reads
// need. Previously required checkWritable()+idxMu.Lock(), which worked for the current
// caller (always a writable-opened shard) but was an unnecessarily strict precondition
// for a function documented as mutating nothing.
func (bs *Store) CollectShardDropResidue(labelToken uint16) (onlyLabel bool, nodeIDs []types.NodeID, rels []storecontract.PurgedRel, err error) {
	if err := bs.checkOpen(); err != nil {
		return false, nil, nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()

	foreign, ferr := bs.hasForeignLabelTokensLocked(labelToken)
	if ferr != nil {
		return false, nil, nil, ferr
	}
	if foreign {
		return false, nil, nil, nil // other labels present — not droppable
	}

	ids, ierr := bs.labelNodeIDsSnapshotLocked(labelToken)
	if ierr != nil {
		return false, nil, nil, ierr
	}
	seen := make(map[types.RelID]struct{})
	var touched []storecontract.PurgedRel
	for _, nid := range ids {
		for _, pr := range bs.purgedRelsForNodeLocked(nid) {
			if _, dup := seen[pr.ID]; dup {
				continue // an internal rel appears from both its endpoints
			}
			seen[pr.ID] = struct{}{}
			touched = append(touched, pr)
		}
	}
	return true, ids, touched, nil
}

// hasForeignLabelTokensLocked reports whether the shard's label index holds any label
// token OTHER than keepToken (with a live entry). Caller holds idxMu. Handles both the
// RAM label map and the on-disk 0x03 keyspace (+ pending overlay).
func (bs *Store) hasForeignLabelTokensLocked(keepToken uint16) (bool, error) {
	if !bs.labelOnDisk {
		for tok, set := range bs.labelIdx {
			if tok != keepToken && len(set) > 0 {
				return true, nil
			}
		}
		return false, nil
	}

	foreignPending := false
	bs.rangePending(func(k string, op writeOp) {
		if foreignPending || len(k) != storepkg.SizeLabelIdx || k[0] != storepkg.KeyLabel {
			return
		}
		if binary.BigEndian.Uint16([]byte(k[1:3])) != keepToken && op.opType == writeOpSet {
			foreignPending = true
		}
	})
	if foreignPending {
		return true, nil
	}

	found := false
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
			if binary.BigEndian.Uint16(key[1:3]) != keepToken {
				found = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("graph: shard label scan: %w", err)
	}
	return found, nil
}
