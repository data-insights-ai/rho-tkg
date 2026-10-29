package badger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Composite node indexes kept on disk (COMPDISK, ai-soc's request: on BA
// DerivedPage's (ov_namespace, ov_rel) composite held 253 MB of RAM, 287
// tuples over 31 M nodes).
//
// A composite of a label whose property indexes are kept on disk
// (PropertyIndexOnDisk, or the label in PropertyIndexOnDiskLabels) keeps one
// current entry per node and tuple in 0x0C (kind EntityIndexNodeComposite,
// storeutil.AppendEntityIndexCompositeHead), written in the batch of the row
// and deleted when the tuple changes or the node goes; its RAM index holds the
// declared keys only. Current reads (NodesByLabelAndProperties, the ID page)
// walk a tuple's entries from a cursor; pinned reads keep their path (the
// composite's current IDs and the label's history). Each definition has a
// built marker (storeutil.EntityIndexBuiltKey with the key tuple), set by its
// create or the writable open that builds it, deleted by a writable open that
// keeps it in RAM or by its drop; the first raises the format marker to 15.

// compositeDiskDef is one composite definition kept on disk
// (Store.compositeDiskDefs): head is its entries' head.
type compositeDiskDef struct {
	key  indexpkg.CompositeIndexKey
	idx  *indexpkg.CompositePropertyIndex
	head []byte
}

// compositeDiskHead is the head of label's composite on keys; ok=false
// without a property-key registry.
func (bs *Store) compositeDiskHead(label uint16, keys []string) ([]byte, bool) {
	tokens := make([]uint16, len(keys))
	for i, k := range keys {
		tok, ok := bs.propKeyTokenFor(k)
		if !ok {
			return nil, false
		}
		tokens[i] = tok
	}
	return storepkg.AppendEntityIndexCompositeHead(nil, label, tokens), true
}

// compositeDiskBuiltKey is the built marker of the composite key.
func compositeDiskBuiltKey(key indexpkg.CompositeIndexKey) []byte {
	return storepkg.EntityIndexBuiltKey(storepkg.EntityIndexNodeComposite, key.LabelToken, key.Keys)
}

// refreshCompositeDiskDefsLocked rebuilds compositeDiskDefs from
// compositeIndexes. Caller holds idxMu (write).
func (bs *Store) refreshCompositeDiskDefsLocked() {
	var defs []compositeDiskDef
	for key, idx := range bs.compositeIndexes {
		if !idx.OnDisk() {
			continue
		}
		head, _ := bs.compositeDiskHead(key.LabelToken, idx.Keys) // on disk only with a registry (placeComposite, the create)
		defs = append(defs, compositeDiskDef{key: key, idx: idx, head: head})
	}
	bs.compositeDiskDefs = defs
}

// compositeDiskEntry is the op of id's entry for the tuple tupleKey under head.
func compositeDiskEntry(head []byte, tupleKey string, id snowflake.ID, opType writeOpType) writeOp {
	k := append(append(make([]byte, 0, len(head)+len(tupleKey)+8), head...), tupleKey...)
	return writeOp{opType: opType, key: storepkg.AppendEntityIndexEntry(k, storepkg.EntityIndexNodeComposite, id, 0)}
}

// compositeDiskOpsLocked returns n's entries in the composite definitions kept
// on disk, of opType, and marks a definition being created as written for id.
// Caller holds idxMu (write).
func (bs *Store) compositeDiskOpsLocked(n *types.Node, id snowflake.ID, opType writeOpType) []writeOp {
	var ops []writeOp
	for _, def := range bs.compositeDiskDefs {
		if !n.HasLabelTokenRaw(def.key.LabelToken) {
			continue
		}
		if def.idx.Mutated != nil {
			def.idx.Mutated[id] = struct{}{}
		}
		if vk, ok := indexpkg.NodeCompositeValueKey(def.idx.Keys, n); ok {
			ops = append(ops, compositeDiskEntry(def.head, vk, id, opType))
		}
	}
	return ops
}

// compositeDiskBuildingLocked reports whether a composite kept on disk is
// being created. Caller holds idxMu.
func (bs *Store) compositeDiskBuildingLocked() bool {
	for _, def := range bs.compositeDiskDefs {
		if def.idx.Mutated != nil {
			return true
		}
	}
	return false
}

// compositeDiskResidueOpsLocked returns deletes of every composite entry of id
// under heads (nil: every composite entry), buffered and persisted: the purge
// of a node whose row is unreadable, and an exact erasure's residue. Caller
// holds idxMu.
func (bs *Store) compositeDiskResidueOpsLocked(id snowflake.ID, heads [][]byte) ([]writeOp, error) {
	if heads == nil {
		heads = [][]byte{{storepkg.KeyEntityIndex, storepkg.EntityIndexNodeComposite}}
	}
	var ops []writeOp
	for _, head := range heads {
		if err := bs.entityIndexRangeLocked(head, head, nil, func(key []byte) {
			if _, eid, _, ok := storepkg.EntityIndexEntryOf(key, storepkg.EntityIndexNodeComposite); ok && eid == id {
				ops = append(ops, writeOp{opType: writeOpDelete, key: bytes.Clone(key)})
			}
		}); err != nil {
			return nil, fmt.Errorf("graph: composite index entries on disk: %w", err)
		}
	}
	return ops, nil
}

// compositeDiskPostings reads a tuple's entries on disk as a sorted posting
// list (walkPostings, pagePostings): prefix is the tuple's value prefix, every
// key under it the prefix and an ID. A read error ends the list; err holds it.
type compositeDiskPostings struct {
	bs     *Store
	prefix []byte
	err    error
}

// AppendAfter appends at most limit IDs of the tuple's entries above after,
// ascending (every one when limit <= 0): a seek to after, the write buffer's
// entries merged. Caller holds idxMu (R or W).
func (p *compositeDiskPostings) AppendAfter(dst []snowflake.ID, after snowflake.ID, limit int) []snowflake.ID {
	if p.err != nil {
		return dst
	}
	idOf := func(key []byte) snowflake.ID {
		return snowflake.ID(binary.BigEndian.Uint64(key[len(p.prefix):])) // #nosec G115 -- the key's ID bits back to int64
	}
	ov := pendingKeyOverlay[struct{}]{}
	p.bs.rangePendingPrefixes([][]byte{p.prefix}, func(k string, op writeOp) { ov.note(k, op, struct{}{}) })
	var ids []snowflake.ID
	err := p.bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.AllVersions = true
		opts.Prefix = p.prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		var prev []byte
		lo := binary.BigEndian.AppendUint64(bytes.Clone(p.prefix), uint64(after)+1) // #nosec G115 -- a positive ID plus one
		for it.Seek(lo); it.ValidForPrefix(p.prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if bytes.Equal(key, prev) {
				continue // an older version of the key just decided
			}
			prev = append(prev[:0], key...)
			if item.IsDeletedOrExpired() || ov.overrides(key) {
				continue
			}
			ids = append(ids, idOf(key))
			if limit > 0 && len(ids) == limit {
				break // the buffered sets below are merged in after
			}
		}
		return nil
	})
	if err != nil {
		p.err = fmt.Errorf("graph: composite index scan: %w", err)
		return dst
	}
	for k := range ov.sets {
		if id := idOf([]byte(k)); id > after {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	return append(dst, ids...)
}

// compositeDiskPrefix is the value prefix of the tuple values asks for under
// def's head; ok=false when values give no tuple.
func compositeDiskPrefix(head []byte, keys []string, values map[string]any) ([]byte, bool) {
	vk, ok := indexpkg.QueryCompositeValueKey(keys, values)
	if !ok {
		return nil, false
	}
	return append(bytes.Clone(head), vk...), true
}

// compositeDiskDefLocked is the definition kept on disk for idx. Caller holds
// idxMu.
func (bs *Store) compositeDiskDefLocked(idx *indexpkg.CompositePropertyIndex) (compositeDiskDef, bool) {
	for _, def := range bs.compositeDiskDefs {
		if def.idx == idx {
			return def, true
		}
	}
	return compositeDiskDef{}, false
}

// createCompositePropertyIndexOnDiskLocked is CreateCompositePropertyIndex for
// a label kept on disk, entered under idxMu.Lock with key absent (released
// here). It installs the definition, so every door writes its entries from now
// on (Mutated records the nodes written or removed), deletes the entries an
// earlier definition left, reads the label's current rows without a lock and
// merges under the lock an entry for each node not written or removed
// meanwhile, with the built marker (format marker 15).
func (bs *Store) createCompositePropertyIndexOnDiskLocked(key indexpkg.CompositeIndexKey, keys []string) error {
	liveIdx := indexpkg.NewCompositePropertyIndexOnDisk(keys)
	liveIdx.Mutated = make(map[snowflake.ID]struct{})
	indexpkg.RegisterCompositeIndex(bs.compositeIndexes, bs.compositeIndexesByLabel, key, liveIdx)
	bs.refreshCompositeDiskDefsLocked()
	def, _ := bs.compositeDiskDefLocked(liveIdx)
	abortLocked := func(err error) error {
		deleteCompositeIndexIfCurrent(bs.compositeIndexes, bs.compositeIndexesByLabel, key, liveIdx)
		bs.refreshCompositeDiskDefsLocked()
		bs.idxMu.Unlock()
		return err
	}
	stale, err := bs.entityIndexKeysUnderLocked(def.head)
	if err != nil {
		return abortLocked(fmt.Errorf("graph: create composite property index: %w", err))
	}
	staleOps := make([]writeOp, 0, len(stale)+1)
	for _, k := range stale {
		staleOps = append(staleOps, writeOp{opType: writeOpDelete, key: k})
	}
	bs.appendOps(staleOps...)
	nids, err := bs.labelNodeIDsSnapshotLocked(key.LabelToken)
	if err != nil {
		return abortLocked(err)
	}
	bs.idxMu.Unlock()

	var ops []writeOp
	for _, nid := range nids {
		n, err := bs.prefetchNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			bs.idxMu.Lock()
			return abortLocked(fmt.Errorf("graph: create composite property index: %w", err))
		}
		if vk, found := indexpkg.NodeCompositeValueKey(keys, n); found {
			ops = append(ops, compositeDiskEntry(def.head, vk, nid.SnowflakeID(), writeOpSet))
		}
	}
	if bs.compositeDiskCreateTestHook != nil {
		bs.compositeDiskCreateTestHook()
	}

	bs.idxMu.Lock()
	if err := requireCompositeIndexCurrentForCreate(bs.compositeIndexes, key, liveIdx); err != nil {
		bs.refreshCompositeDiskDefsLocked()
		bs.idxMu.Unlock()
		return err
	}
	kept := ops[:0]
	for _, op := range ops {
		_, id, _, _ := storepkg.EntityIndexEntryOf(op.key, storepkg.EntityIndexNodeComposite)
		if _, mutated := liveIdx.Mutated[id]; mutated {
			continue // written, deleted, purged or erased during the scan: its door wrote the entry or none
		}
		kept = append(kept, op)
	}
	liveIdx.Mutated = nil
	bs.persistCompositeIndexDefs()
	kept = append(kept, writeOp{opType: writeOpSet, key: compositeDiskBuiltKey(key), value: []byte{1}})
	bs.appendOpsRaising(storepkg.CompositeDiskStoreFormatVersion, kept...)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// compositeDiskDropOpsLocked returns the deletes of a composite's entries kept
// on disk and of its built marker. Caller holds idxMu.Lock.
func (bs *Store) compositeDiskDropOpsLocked(def compositeDiskDef) ([]writeOp, error) {
	keys, err := bs.entityIndexKeysUnderLocked(def.head)
	if err != nil {
		return nil, fmt.Errorf("graph: drop composite index entries on disk: %w", err)
	}
	ops := make([]writeOp, 0, len(keys)+1)
	ops = append(ops, writeOp{opType: writeOpDelete, key: compositeDiskBuiltKey(def.key)})
	for _, k := range keys {
		ops = append(ops, writeOp{opType: writeOpDelete, key: k})
	}
	return ops, nil
}
