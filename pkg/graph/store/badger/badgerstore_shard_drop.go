package badger

import (
	"encoding/binary"
	"fmt"
	"maps"
	"slices"

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
// token, or any row that is neither those nodes' nor those relationships'
// (hasForeignRowsLocked), forces false (a safe over-decline).
//
// Genuinely read-only (checkOpen, idxMu.RLock), matching the doc claims
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
	foreign, ferr = bs.hasForeignRowsLocked(ids, seen)
	if ferr != nil {
		return false, nil, nil, ferr
	}
	if foreign {
		return false, nil, nil, nil // rows that are not this label's: not droppable
	}
	return true, ids, touched, nil
}

// hasForeignRowsLocked reports whether the shard holds a relationship row,
// current or history, outside rels, or a node history row outside ids (a live
// node always has a label entry, which the label check reads). Such a row goes
// with the directory but is not this purge's: a RelsByMint shard holds
// relationships between reference nodes, and a deleted entity keeps its history
// with no label entry. Walks the three keyspaces' keys, stopping at the first;
// a drop is rare and already lists the label. Caller holds idxMu.
func (bs *Store) hasForeignRowsLocked(ids []types.NodeID, rels map[types.RelID]struct{}) (bool, error) {
	nodes := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		nodes[id] = struct{}{}
	}
	foreignKey := func(k []byte) bool {
		switch {
		case k[0] == storepkg.KeyHistNode && len(k) == storepkg.SizeHistKey:
			_, ok := nodes[types.NodeID(storepkg.ParseIDFromKey(k, 1))]
			return !ok
		case k[0] == storepkg.KeyRel && len(k) == storepkg.SizeRelKey,
			k[0] == storepkg.KeyHistRel && len(k) == storepkg.SizeHistKey:
			_, ok := rels[types.RelID(storepkg.ParseIDFromKey(k, 1))]
			return !ok
		}
		return false
	}
	foreign := false
	bs.rangePending(func(k string, op writeOp) {
		if !foreign && len(k) > 0 && op.opType == writeOpSet {
			foreign = foreignKey([]byte(k))
		}
	})
	if foreign {
		return true, nil
	}
	err := bs.view(func(txn *badgerv4.Txn) error {
		for _, tag := range [...]byte{storepkg.KeyRel, storepkg.KeyHistNode, storepkg.KeyHistRel} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte{tag}
			if done := func() bool {
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
					if foreignKey(it.Item().Key()) {
						return true
					}
				}
				return false
			}(); done {
				foreign = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("graph: shard row scan: %w", err)
	}
	return foreign, nil
}

// hasForeignLabelTokensLocked reports whether the shard's label index holds any label
// token OTHER than keepToken (with a live entry). Caller holds idxMu. Handles both the
// RAM label map and the on-disk 0x03 keyspace (+ pending overlay).
func (bs *Store) hasForeignLabelTokensLocked(keepToken uint16) (bool, error) {
	if !bs.labelOnDisk {
		return bs.labelOtherThanLocked(keepToken), nil
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
	err := bs.view(func(txn *badgerv4.Txn) error {
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

// CollectRelShardDropResidue supports the tiered whole-shard drop of a
// relationship retention purge (RHO-35). It reports whether this shard holds
// only relationships of typeTokens minted before `before` (droppable): no node
// row, node history row or label entry, and every relationship row, current or
// history, belongs to such a relationship. When it does, rels lists one
// routing descriptor per relationship and endpoint pair (relPurgeLegs), for
// the tiered layer's leg-residue sweep. Mutates nothing and decides
// conservatively: a pending delete of a committed key still counts the key.
// The ID collection reads under idxMu; the per-relationship row reads take
// their own locks, so the caller runs it on a shard no writer reaches (the
// drain protocol) for the authoritative answer.
func (bs *Store) CollectRelShardDropResidue(typeTokens []uint16, before types.Instant) (droppable bool, rels []storecontract.PurgedRel, err error) {
	if err := bs.checkOpen(); err != nil {
		return false, nil, err
	}
	if len(typeTokens) == 0 || before <= 0 {
		return false, nil, nil
	}
	ids, foreign, err := bs.relShardRowIDs()
	if err != nil || foreign {
		return false, nil, err
	}
	for _, id := range ids {
		if storepkg.SnowflakeInstant(id.SnowflakeID()) >= before {
			return false, nil, nil
		}
		legs, lerr := bs.relPurgeLegs(id)
		if lerr != nil {
			return false, nil, lerr
		}
		if len(legs) == 0 {
			return false, nil, nil // a key without a decodable row: not ours to drop
		}
		for _, l := range legs {
			if !slices.Contains(typeTokens, l.TypeToken) {
				return false, nil, nil
			}
		}
		rels = append(rels, legs...)
	}
	return true, rels, nil
}

// relShardRowIDs lists the distinct relationship IDs with a current or history
// row on this shard, ascending; foreign is true when the shard holds a node
// row, a node history row or a label entry. Committed keys and the pending
// overlay's sets are read under idxMu (the RAM label map needs it).
func (bs *Store) relShardRowIDs() (ids []types.RelID, foreign bool, err error) {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if foreign, err := bs.hasForeignLabelTokensLocked(0); err != nil || foreign {
		return nil, foreign, err // token 0 names no label: any entry is foreign
	}
	seen := make(map[types.RelID]struct{})
	visit := func(k []byte) bool {
		switch {
		case len(k) == 0:
		case k[0] == storepkg.KeyNode, k[0] == storepkg.KeyHistNode, k[0] == storepkg.KeyLabel:
			return true
		case k[0] == storepkg.KeyRel && len(k) == storepkg.SizeRelKey,
			k[0] == storepkg.KeyHistRel && len(k) == storepkg.SizeHistKey:
			seen[types.RelID(storepkg.ParseIDFromKey(k, 1))] = struct{}{}
		}
		return false
	}
	bs.rangePending(func(k string, op writeOp) {
		if !foreign && op.opType == writeOpSet {
			foreign = visit([]byte(k))
		}
	})
	if foreign {
		return nil, true, nil
	}
	err = bs.view(func(txn *badgerv4.Txn) error {
		for _, tag := range [...]byte{storepkg.KeyNode, storepkg.KeyLabel, storepkg.KeyHistNode, storepkg.KeyRel, storepkg.KeyHistRel} {
			opts := badgerv4.DefaultIteratorOptions
			opts.PrefetchValues = false
			opts.Prefix = []byte{tag}
			if func() bool {
				it := txn.NewIterator(opts)
				defer it.Close()
				for it.Seek(opts.Prefix); it.ValidForPrefix(opts.Prefix); it.Next() {
					if visit(it.Item().Key()) {
						return true
					}
				}
				return false
			}() {
				foreign = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("graph: shard row scan: %w", err)
	}
	if foreign {
		return nil, true, nil
	}
	ids = slices.Sorted(maps.Keys(seen))
	return ids, false, nil
}
