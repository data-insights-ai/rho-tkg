package graph

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Partial relationship write/delete helpers for TieredStore ---
//
// A full PutRelationship writes 4 keys: entity (0x02), typeIdx (0x04),
// outIdx (0x05), inIdx (0x06). For cross-shard relationships, these keys
// are split across two BadgerStore instances:
//
//   Entity shard: entity (0x02) + typeIdx (0x04) + outIdx (0x05)
//   In shard:     inIdx (0x06)
//
// These unexported helpers perform the partial writes/deletes that
// TieredStore needs for cross-shard relationship routing.

// putRelEntityAndOut writes the relationship entity (0x02), type index (0x04),
// and outgoing adjacency (0x05) keys. Does NOT write the incoming adjacency
// key (0x06) — that belongs to the endpoint's shard for cross-shard rels.
// Does NOT verify endpoints exist (caller's responsibility).
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) putRelEntityAndOut(r *types.Relationship) error {
	rid := r.ID()
	startNID := r.StartNodeID()
	id := rid.SnowflakeID()
	startID := startNID.SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.relIDs[rid]; exists {
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, r.DeepCopy())
	bs.relIDs[rid] = struct{}{}

	// Type index.
	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[types.RelID]struct{})
	}
	bs.typeIdx[relType][rid] = struct{}{}

	// Outgoing adjacency only.
	if bs.outIdx[startNID] == nil {
		bs.outIdx[startNID] = make(map[types.RelID]struct{})
	}
	bs.outIdx[startNID][rid] = struct{}{}

	// NO inIdx update — the in/ key lives in the endpoint's shard.

	ops := []writeOp{
		{opType: writeOpSet, key: relKey(id), value: data},
		{opType: writeOpSet, key: relTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: outKey(startID, relType, endID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	return nil
}

// putRelIncoming writes only the incoming adjacency key (0x06) for a
// cross-shard relationship. The relationship entity is NOT stored in this
// shard — only the in/ index entry, so that IncomingRelationships queries
// on the endpoint node find the relationship.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) putRelIncoming(endID, startID snowflake.ID, relType uint16, relID snowflake.ID) error {
	endNID := types.NodeID(endID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if bs.inIdx[endNID] == nil {
		bs.inIdx[endNID] = make(map[types.RelID]uint16)
	}
	bs.inIdx[endNID][rid] = relType

	op := writeOp{
		opType: writeOpSet,
		key:    inKey(endID, relType, startID, relID),
	}
	bs.appendOps(op)
	return nil
}

// deleteRelEntityAndOut removes the relationship entity (0x02), type index
// (0x04), and outgoing adjacency (0x05) keys. Does NOT touch the incoming
// adjacency key (0x06). Returns relDeleteInfo so the caller can clean up
// the companion in-shard deletion.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) deleteRelEntityAndOut(id snowflake.ID) (relDeleteInfo, error) {
	rid := types.RelID(id)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	r, err := bs.getRelLocked(rid)
	if err != nil {
		return relDeleteInfo{}, err
	}

	info := relDeleteInfo{
		id:      id,
		relType: r.TypeToken().Value(),
		startID: r.StartNodeID().SnowflakeID(),
		endID:   r.EndNodeID().SnowflakeID(),
	}
	startNID := types.NodeID(info.startID)

	// Update in-memory state.
	bs.relCache.MarkDeleted(id)
	delete(bs.relIDs, rid)

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.relType]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, info.relType)
		}
	}

	// Outgoing adjacency cleanup only.
	if set, exists := bs.outIdx[startNID]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.outIdx, startNID)
		}
	}

	// NO inIdx cleanup — the in/ key lives in the endpoint's shard.

	ops := []writeOp{
		{opType: writeOpDelete, key: relKey(id)},
		{opType: writeOpDelete, key: relTypeIndexKey(info.relType, id)},
		{opType: writeOpDelete, key: outKey(info.startID, info.relType, info.endID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.relType).Add(-1)

	return info, nil
}

// deleteRelIncoming removes only the incoming adjacency key (0x06) for a
// cross-shard relationship.
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) deleteRelIncoming(info relDeleteInfo) error {
	endNID := types.NodeID(info.endID)
	rid := types.RelID(info.id)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if set, exists := bs.inIdx[endNID]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.inIdx, endNID)
		}
	}

	op := writeOp{
		opType: writeOpDelete,
		key:    inKey(info.endID, info.relType, info.startID, info.id),
	}
	bs.appendOps(op)
	return nil
}

// deleteIncomingByRelID removes a specific relationship from the inIdx of
// a given end node. Used by repair to clean up orphaned in/ entries where
// the relationship entity no longer exists.
// Since we don't know the relType/startID (the entity is gone), we scan
// the pending buffer for a matching Set op. If found, we replace it with a
// Delete op; otherwise the in-memory removal is sufficient (the orphaned
// Badger key will not be re-loaded because loadIndexes only re-adds keys
// whose relIDs are still in the pending buffer or DB, and the in-memory
// inIdx is authoritative during runtime).
// Acquires idxMu.Lock internally.
func (bs *BadgerStore) deleteIncomingByRelID(endNodeID snowflake.ID, relID snowflake.ID) error {
	endNID := types.NodeID(endNodeID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()

	set, exists := bs.inIdx[endNID]
	if !exists {
		bs.idxMu.Unlock()
		return nil // nothing to remove
	}
	if _, ok := set[rid]; !ok {
		bs.idxMu.Unlock()
		return nil // relID not in set
	}

	delete(set, rid)
	if len(set) == 0 {
		delete(bs.inIdx, endNID)
	}
	bs.idxMu.Unlock()

	// Scan pending buffer to find and convert the Set op to a Delete op.
	// The key format: 0x06 | endID(8B) | relType(2B) | startID(8B) | relID(8B) = 27B.
	// The relID is at offset 19.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) == sizeAdjacency && k[0] == keyIn && op.opType == writeOpSet {
			if parseRelIDFromAdjKey([]byte(k)) == relID {
				bs.pending[k] = writeOp{opType: writeOpDelete, key: op.key}
				bs.wbMu.Unlock()
				return nil
			}
		}
	}
	bs.wbMu.Unlock()

	// Not in pending buffer — scan Badger for the matching key.
	return bs.scanAndDeleteIncoming(endNodeID, relID)
}

// scanAndDeleteIncoming scans Badger for the 0x06 key matching (endNodeID, relID)
// and queues a delete op if found. Repair-only path; not performance critical.
func (bs *BadgerStore) scanAndDeleteIncoming(endNodeID, relID snowflake.ID) error {
	prefix := make([]byte, 1+8)
	prefix[0] = keyIn
	putUint64(prefix, 1, int64(endNodeID))

	return bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) == sizeAdjacency {
				if parseRelIDFromAdjKey(key) == relID {
					delKey := make([]byte, len(key))
					copy(delKey, key)
					bs.appendOps(writeOp{opType: writeOpDelete, key: delKey})
					return nil
				}
			}
		}
		return nil // not found — already cleaned up or was never persisted
	})
}

// --- Read helpers for TieredStore shard resolution ---

// hasNodeID checks whether the given node ID exists in this shard. O(1).
func (bs *BadgerStore) hasNodeID(id snowflake.ID) bool {
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[types.NodeID(id)]
	bs.idxMu.RUnlock()
	return exists
}

// hasRelID checks whether the given relationship ID exists in this shard. O(1).
func (bs *BadgerStore) hasRelID(id snowflake.ID) bool {
	bs.idxMu.RLock()
	_, exists := bs.relIDs[types.RelID(id)]
	bs.idxMu.RUnlock()
	return exists
}

// incomingRelIDs returns relationship IDs from the inIdx for the given node.
// typeToken 0 = all types. Returns a sorted slice. Snapshot under RLock.
func (bs *BadgerStore) incomingRelIDs(nodeID snowflake.ID, typeToken uint16) []snowflake.ID {
	bs.idxMu.RLock()
	set := bs.inIdx[types.NodeID(nodeID)]
	if len(set) == 0 {
		bs.idxMu.RUnlock()
		return nil
	}

	ids := make([]snowflake.ID, 0, len(set))
	for relID, tok := range set {
		if typeToken == 0 || tok == typeToken {
			ids = append(ids, relID.SnowflakeID())
		}
	}
	bs.idxMu.RUnlock()

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// outgoingRelIDs returns relationship IDs from the outIdx for the given node.
// Returns a sorted slice. Snapshot under RLock.
func (bs *BadgerStore) outgoingRelIDs(nodeID snowflake.ID) []snowflake.ID {
	bs.idxMu.RLock()
	set := bs.outIdx[types.NodeID(nodeID)]
	if len(set) == 0 {
		bs.idxMu.RUnlock()
		return nil
	}
	ids := make([]snowflake.ID, 0, len(set))
	for relID := range set {
		ids = append(ids, relID.SnowflakeID())
	}
	bs.idxMu.RUnlock()

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
