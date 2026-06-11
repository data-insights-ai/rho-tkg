package badger

import (
	"encoding/binary"
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Disk-resident adjacency index (enterprise-scale ceiling 1, opt-in via
// Config.AdjacencyIndexOnDisk) — the adjacency sibling of
// badgerstore_label_disk.go; see that file for the full design rationale.
//
// The adjacency keyspaces (KeyOut/KeyIn + nodeID(8B) + relType(2B) +
// otherID(8B) + relID(8B), see storeutil.OutKey/InKey) have ALWAYS been
// written transactionally with relationship rows in the same flush batch.
// Disk mode stops materializing the outIdx/inIdx maps (~2 map entries per
// relationship of permanent RAM — at 5M rels that is 10M entries, the
// largest of the index maps) and answers adjacency snapshots from the
// keyspace via prefix iteration: 9-byte prefix for all-types, 11-byte
// prefix (including the type token) for typed queries — the typed form
// REPLACES the in-memory mode's full typeIdx intersection. Unflushed
// pending ops are overlaid exactly as the label arm does. Existing data
// directories need no migration.

// adjacentRelIDsSnapshotLocked returns an unsorted snapshot of the rel IDs
// adjacent to nid in the given direction, optionally type-filtered
// (typeToken 0 = all types). Caller must hold idxMu (R or W) — matching
// the map reads it replaces. Disk mode briefly takes wbMu inside
// (idxMu → wbMu is the flush cycle's established lock order). The caller
// is responsible for the node-exists check, mirroring the map paths.
func (bs *Store) adjacentRelIDsSnapshotLocked(nid types.NodeID, typeToken uint16, incoming bool) ([]types.RelID, error) {
	if !bs.adjOnDisk {
		return bs.adjacentRelIDsFromMapsLocked(nid, typeToken, incoming), nil
	}

	keyTag := byte(storepkg.KeyOut)
	if incoming {
		keyTag = storepkg.KeyIn
	}
	prefix := make([]byte, 0, 11)
	prefix = append(prefix, keyTag)
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(nid.SnowflakeID())) // #nosec G115 — key encoding round-trips int64 bits
	if typeToken != 0 {
		prefix = binary.BigEndian.AppendUint16(prefix, typeToken)
	}

	// Pending overlay: unflushed adjacency ops must be visible (adds) and
	// authoritative (deletes) over the committed keyspace.
	var adds, dels map[types.RelID]struct{}
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) != storepkg.SizeAdjacency || k[0] != keyTag {
			continue
		}
		kb := []byte(k)
		if !hasPrefix(kb, prefix) {
			continue
		}
		rid := types.RelID(storepkg.ParseIDFromKey(kb, 19))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.RelID]struct{})
			}
			adds[rid] = struct{}{}
		} else {
			if dels == nil {
				dels = make(map[types.RelID]struct{})
			}
			dels[rid] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	var rids []types.RelID
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeAdjacency {
				continue
			}
			rid := types.RelID(storepkg.ParseIDFromKey(key, 19))
			if _, del := dels[rid]; del {
				continue
			}
			delete(adds, rid) // already committed; don't double-count
			rids = append(rids, rid)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: adjacency index scan: %w", err)
	}
	for rid := range adds {
		rids = append(rids, rid)
	}
	return rids, nil
}

// adjacentRelMetasSnapshotLocked is adjacentRelIDsSnapshotLocked plus the OTHER
// endpoint of each relationship, so the endpoint-streaming path never decodes a
// relationship row to learn its target. The other endpoint lives at adjacency
// key offset 11 (end node for OUT keys, start node for IN keys); RAM mode reads
// it from the adjacency map value. Caller holds idxMu.
func (bs *Store) adjacentRelMetasSnapshotLocked(nid types.NodeID, typeToken uint16, incoming bool) ([]adjMeta, error) {
	if !bs.adjOnDisk {
		return bs.adjacentRelMetasFromMapsLocked(nid, typeToken, incoming), nil
	}

	keyTag := byte(storepkg.KeyOut)
	if incoming {
		keyTag = storepkg.KeyIn
	}
	prefix := make([]byte, 0, 11)
	prefix = append(prefix, keyTag)
	prefix = binary.BigEndian.AppendUint64(prefix, uint64(nid.SnowflakeID())) // #nosec G115 — key encoding round-trips int64 bits
	if typeToken != 0 {
		prefix = binary.BigEndian.AppendUint16(prefix, typeToken)
	}

	var adds map[types.RelID]types.NodeID
	var dels map[types.RelID]struct{}
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) != storepkg.SizeAdjacency || k[0] != keyTag {
			continue
		}
		kb := []byte(k)
		if !hasPrefix(kb, prefix) {
			continue
		}
		rid := types.RelID(storepkg.ParseIDFromKey(kb, 19))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.RelID]types.NodeID)
			}
			adds[rid] = types.NodeID(storepkg.ParseIDFromKey(kb, 11))
		} else {
			if dels == nil {
				dels = make(map[types.RelID]struct{})
			}
			dels[rid] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	var metas []adjMeta
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeAdjacency {
				continue
			}
			rid := types.RelID(storepkg.ParseIDFromKey(key, 19))
			if _, del := dels[rid]; del {
				continue
			}
			delete(adds, rid)
			metas = append(metas, adjMeta{rel: rid, other: types.NodeID(storepkg.ParseIDFromKey(key, 11))})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: adjacency index scan: %w", err)
	}
	for rid, other := range adds {
		metas = append(metas, adjMeta{rel: rid, other: other})
	}
	return metas, nil
}

// inEdge is the in-memory incoming-adjacency value: for an end node, each
// relID maps to its START node and type token. Carrying the start node lets a
// traversal yield the source endpoint WITHOUT decoding the relationship row.
// (Outgoing adjacency stores the end node directly as the map value.)
type inEdge struct {
	start types.NodeID
	typ   uint16
}

// adjMeta is one adjacency entry from the endpoint-streaming path: the
// relationship and the OTHER endpoint (end node for outgoing, start node for
// incoming), both available without a relationship-row decode.
type adjMeta struct {
	rel   types.RelID
	other types.NodeID
}

// adjacentRelIDsFromMapsLocked is the in-memory arm: outIdx is a plain set
// (intersect typeIdx when filtered); inIdx carries the type token per rel.
func (bs *Store) adjacentRelIDsFromMapsLocked(nid types.NodeID, typeToken uint16, incoming bool) []types.RelID {
	if incoming {
		set := bs.inIdx[nid]
		rids := make([]types.RelID, 0, len(set))
		for id, ie := range set {
			if typeToken == 0 || ie.typ == typeToken {
				rids = append(rids, id)
			}
		}
		return rids
	}
	set := bs.outIdx[nid]
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = bs.typeIdx[typeToken]
		if len(typeSet) == 0 {
			return nil
		}
	}
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		if typeToken != 0 {
			if _, ok := typeSet[id]; !ok {
				continue
			}
		}
		rids = append(rids, id)
	}
	return rids
}

// adjacentRelMetasFromMapsLocked is the in-memory arm of the endpoint path: it
// returns (relID, otherEndpoint) pairs straight from the adjacency maps, so a
// traversal never decodes the relationship row to learn its target. Outgoing
// reads the end node from outIdx's value; incoming reads the start node from
// inIdx's value.
func (bs *Store) adjacentRelMetasFromMapsLocked(nid types.NodeID, typeToken uint16, incoming bool) []adjMeta {
	if incoming {
		set := bs.inIdx[nid]
		metas := make([]adjMeta, 0, len(set))
		for id, ie := range set {
			if typeToken == 0 || ie.typ == typeToken {
				metas = append(metas, adjMeta{rel: id, other: ie.start})
			}
		}
		return metas
	}
	set := bs.outIdx[nid]
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = bs.typeIdx[typeToken]
		if len(typeSet) == 0 {
			return nil
		}
	}
	metas := make([]adjMeta, 0, len(set))
	for id, end := range set {
		if typeToken != 0 {
			if _, ok := typeSet[id]; !ok {
				continue
			}
		}
		metas = append(metas, adjMeta{rel: id, other: end})
	}
	return metas
}

func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

// nodeHasAdjacentRelsLocked reports whether nid has ANY adjacency entry in
// either direction — the connected-relationships guard for non-cascade
// node deletes. In disk mode this MUST consult the keyspace: the empty RAM
// maps would otherwise let a connected node delete through, leaving
// dangling relationships. Early-stops on the first key found. Caller holds
// idxMu.
func (bs *Store) nodeHasAdjacentRelsLocked(nid types.NodeID) (bool, error) {
	if !bs.adjOnDisk {
		return len(bs.outIdx[nid]) != 0 || len(bs.inIdx[nid]) != 0, nil
	}
	for _, incoming := range []bool{false, true} {
		rids, err := bs.adjacentRelIDsSnapshotLocked(nid, 0, incoming)
		if err != nil {
			return false, err
		}
		if len(rids) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// incomingIndexEntriesFromKeyspaceLocked is the disk arm of
// IncomingIndexEntries (TieredStore repair surface): a full KeyIn keyspace
// walk with the pending overlay merged. Caller holds idxMu (R).
// Errors are logged, not returned — the repair contract is best-effort
// enumeration and the signature predates the disk arm.
func (bs *Store) incomingIndexEntriesFromKeyspaceLocked() []IncomingIndexEntry {
	type inKey struct {
		end, rel int64
		tok      uint16
	}
	merged := make(map[inKey]bool) // value: present (true) / deleted (false)

	decode := func(k []byte) (inKey, bool) {
		if len(k) != storepkg.SizeAdjacency || k[0] != storepkg.KeyIn {
			return inKey{}, false
		}
		return inKey{
			end: int64(storepkg.ParseIDFromKey(k, 1)),
			tok: binary.BigEndian.Uint16(k[9:11]),
			rel: int64(storepkg.ParseIDFromKey(k, 19)),
		}, true
	}

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefix := []byte{storepkg.KeyIn}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if ik, ok := decode(it.Item().Key()); ok {
				merged[ik] = true
			}
		}
		return nil
	})
	if err != nil {
		slog.Error("graph: incoming index keyspace scan failed", "err", err)
		return nil
	}

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if ik, ok := decode([]byte(k)); ok {
			merged[ik] = op.opType == writeOpSet
		}
	}
	bs.wbMu.Unlock()

	entries := make([]IncomingIndexEntry, 0, len(merged))
	for ik, present := range merged {
		if !present {
			continue
		}
		entries = append(entries, IncomingIndexEntry{
			EndID:   snowflake.ID(ik.end),
			RelID:   snowflake.ID(ik.rel),
			RelType: ik.tok,
		})
	}
	return entries
}
