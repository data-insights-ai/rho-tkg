package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Partial relationship write/delete helpers for TieredStore ---
//
// A full PutRelationship writes 4 keys: entity (0x02), typeIdx (0x04),
// outIdx (0x05), inIdx (0x06). For cross-shard relationships, these keys
// are split across two Store instances:
//
//   Entity shard: entity (0x02) + typeIdx (0x04) + outIdx (0x05)
//   In shard:     inIdx (0x06)
//
// These unexported helpers perform the partial writes/deletes that
// TieredStore needs for cross-shard relationship routing.

// IncomingIndexEntry is a snapshot row from the incoming adjacency index.
type IncomingIndexEntry struct {
	EndID   snowflake.ID
	RelID   snowflake.ID
	RelType uint16
}

// PutRelEntityAndOut writes the relationship entity (0x02), type index (0x04),
// and outgoing adjacency (0x05) keys. Does NOT write the incoming adjacency
// key (0x06) — that belongs to the endpoint's shard for cross-shard rels.
// TieredStore verifies endpoint existence before invoking this split-write leg.
// Acquires idxMu.Lock internally.
func (bs *Store) PutRelEntityAndOut(r *types.Relationship) error {
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	rid := r.ID()
	startNID := r.StartNodeID()
	id := rid.SnowflakeID()
	startID := startNID.SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	data, err := storepkg.MarshalRelWire(r)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; exists {
		bs.idxMu.Unlock()
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
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: storepkg.RelTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: storepkg.OutKey(startID, relType, endID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// PutRelIncoming writes only the incoming adjacency key (0x06) for a
// cross-shard relationship. The relationship entity is NOT stored in this
// shard — only the in/ index entry, so that IncomingRelationships queries
// on the endpoint node find the relationship.
// Acquires idxMu.Lock internally.
func (bs *Store) PutRelIncoming(endID, startID snowflake.ID, relType uint16, relID snowflake.ID) error {
	if err := storecontract.ValidateRelationshipIndexEntry(types.NodeID(startID), types.NodeID(endID), relType, types.RelID(relID)); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	endNID := types.NodeID(endID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()

	if bs.inIdx[endNID] == nil {
		bs.inIdx[endNID] = make(map[types.RelID]uint16)
	}
	bs.inIdx[endNID][rid] = relType

	op := writeOp{
		opType: writeOpSet,
		key:    storepkg.InKey(endID, relType, startID, relID),
	}
	bs.appendOps(op)
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// DeleteRelEntityAndOut removes the relationship entity (0x02), type index
// (0x04), and outgoing adjacency (0x05) keys. Does NOT touch the incoming
// adjacency key (0x06). Returns RelDeleteInfo so the caller can clean up
// the companion in-shard deletion.
// Acquires idxMu.Lock internally.
func (bs *Store) DeleteRelEntityAndOut(id snowflake.ID) (RelDeleteInfo, error) {
	rid := types.RelID(id)
	if err := storecontract.ValidateRelID(rid); err != nil {
		return RelDeleteInfo{}, err
	}
	if err := bs.checkOpen(); err != nil {
		return RelDeleteInfo{}, err
	}

	bs.idxMu.Lock()

	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return RelDeleteInfo{}, err
	}

	info := RelDeleteInfo{
		ID:      id,
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
	startNID := types.NodeID(info.StartID)

	// Update in-memory state.
	bs.relCache.MarkDeleted(id)
	delete(bs.relIDs, rid)

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.RelType]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, info.RelType)
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
		{opType: writeOpDelete, key: storepkg.RelKey(id)},
		{opType: writeOpDelete, key: storepkg.RelTypeIndexKey(info.RelType, id)},
		{opType: writeOpDelete, key: storepkg.OutKey(info.StartID, info.RelType, info.EndID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.RelType).Add(-1)

	bs.idxMu.Unlock()
	return info, bs.flushIfSyncWrites()
}

// DeleteRelIncoming removes only the incoming adjacency key (0x06) for a
// cross-shard relationship.
// Acquires idxMu.Lock internally.
func (bs *Store) DeleteRelIncoming(info RelDeleteInfo) error {
	if err := storecontract.ValidateRelationshipIndexEntry(types.NodeID(info.StartID), types.NodeID(info.EndID), info.RelType, types.RelID(info.ID)); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	endNID := types.NodeID(info.EndID)
	rid := types.RelID(info.ID)

	bs.idxMu.Lock()

	if set, exists := bs.inIdx[endNID]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.inIdx, endNID)
		}
	}

	op := writeOp{
		opType: writeOpDelete,
		key:    storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID),
	}
	bs.appendOps(op)
	bs.idxMu.Unlock()
	return bs.flushIfSyncWrites()
}

// DeleteIncomingByRelID removes a specific relationship from the inIdx of
// a given end node. Used by repair to clean up orphaned in/ entries where
// the relationship entity no longer exists.
// Since we don't know the relType/startID (the entity is gone), we scan
// the pending buffer for a Set op matching this end node and relID. If found,
// we replace it with a Delete op; otherwise we scan persisted Badger keys by
// endpoint prefix and queue a delete for the matching in/ key.
// Acquires idxMu.Lock internally.
func (bs *Store) DeleteIncomingByRelID(endNodeID snowflake.ID, relID snowflake.ID) error {
	if err := validateIncomingDeleteTarget(endNodeID, relID); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if !bs.deleteIncomingMemoryEntry(endNodeID, relID) {
		return nil // nothing to remove
	}

	// Scan pending buffer to find and convert the Set op to a Delete op.
	// The key format: 0x06 | endID(8B) | relType(2B) | startID(8B) | relID(8B) = 27B.
	// The relID is at offset 19.
	if bs.deletePendingIncoming(endNodeID, relID) {
		return bs.flushIfSyncWrites()
	}

	// Not in pending buffer — scan Badger for the matching key.
	if err := bs.scanAndDeleteIncomingPersisted(endNodeID, relID); err != nil {
		return err
	}
	return bs.flushIfSyncWrites()
}

// ScanAndDeleteIncoming scans Badger for the 0x06 key matching (endNodeID, relID)
// and queues a delete op if found. Repair-only path; not performance critical.
func (bs *Store) ScanAndDeleteIncoming(endNodeID, relID snowflake.ID) error {
	if err := validateIncomingDeleteTarget(endNodeID, relID); err != nil {
		return err
	}
	if err := bs.checkOpen(); err != nil {
		return err
	}
	bs.deleteIncomingMemoryEntry(endNodeID, relID)
	if bs.deletePendingIncoming(endNodeID, relID) {
		return bs.flushIfSyncWrites()
	}
	if err := bs.scanAndDeleteIncomingPersisted(endNodeID, relID); err != nil {
		return err
	}
	return bs.flushIfSyncWrites()
}

func (bs *Store) scanAndDeleteIncomingPersisted(endNodeID, relID snowflake.ID) error {
	prefix := make([]byte, 1+8)
	prefix[0] = storepkg.KeyIn
	storepkg.PutUint64(prefix, 1, int64(endNodeID))

	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) == storepkg.SizeAdjacency {
				if storepkg.ParseRelIDFromAdjKey(key) == relID {
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

func (bs *Store) deleteIncomingMemoryEntry(endNodeID, relID snowflake.ID) bool {
	endNID := types.NodeID(endNodeID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	set, exists := bs.inIdx[endNID]
	if !exists {
		return false
	}
	if _, ok := set[rid]; !ok {
		return false
	}

	delete(set, rid)
	if len(set) == 0 {
		delete(bs.inIdx, endNID)
	}
	return true
}

func (bs *Store) deletePendingIncoming(endNodeID, relID snowflake.ID) bool {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()

	for k, op := range bs.pending {
		key := []byte(k)
		if len(key) != storepkg.SizeAdjacency || key[0] != storepkg.KeyIn || op.opType != writeOpSet {
			continue
		}
		if storepkg.ParseIDFromKey(key, 1) == endNodeID && storepkg.ParseRelIDFromAdjKey(key) == relID {
			bs.pending[k] = writeOp{opType: writeOpDelete, key: op.key}
			return true
		}
	}
	return false
}

func validateIncomingDeleteTarget(endNodeID, relID snowflake.ID) error {
	if err := storecontract.ValidateNodeID(types.NodeID(endNodeID)); err != nil {
		return fmt.Errorf("%w: invalid relationship end node ID %d", err, endNodeID)
	}
	if err := storecontract.ValidateRelID(types.RelID(relID)); err != nil {
		return err
	}
	return nil
}

// --- Read helpers for TieredStore shard resolution ---

// HasNodeID checks whether the given node ID exists in this shard. O(1).
func (bs *Store) HasNodeID(id snowflake.ID) bool {
	if bs.dbClosed.Load() {
		return false
	}
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[types.NodeID(id)]
	bs.idxMu.RUnlock()
	return exists
}

// HasRelID checks whether the given relationship ID exists in this shard. O(1).
func (bs *Store) HasRelID(id snowflake.ID) bool {
	if bs.dbClosed.Load() {
		return false
	}
	bs.idxMu.RLock()
	_, exists := bs.relIDs[types.RelID(id)]
	bs.idxMu.RUnlock()
	return exists
}

// IncomingRelIDs returns relationship IDs from the inIdx for the given node.
// typeToken 0 = all types. Returns a sorted slice. Snapshot under RLock.
func (bs *Store) IncomingRelIDs(nodeID snowflake.ID, typeToken uint16) []snowflake.ID {
	if bs.dbClosed.Load() {
		return nil
	}
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

// IncomingIndexEntries returns a sorted snapshot of all incoming adjacency
// entries in this shard, including entries whose end node no longer exists.
func (bs *Store) IncomingIndexEntries() []IncomingIndexEntry {
	if bs.dbClosed.Load() {
		return nil
	}
	bs.idxMu.RLock()
	entries := make([]IncomingIndexEntry, 0)
	for endID, set := range bs.inIdx {
		for relID, relType := range set {
			entries = append(entries, IncomingIndexEntry{
				EndID:   endID.SnowflakeID(),
				RelID:   relID.SnowflakeID(),
				RelType: relType,
			})
		}
	}
	bs.idxMu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].EndID != entries[j].EndID {
			return entries[i].EndID < entries[j].EndID
		}
		return entries[i].RelID < entries[j].RelID
	})
	return entries
}

// OutgoingRelIDs returns relationship IDs from the outIdx for the given node.
// Returns a sorted slice. Snapshot under RLock.
func (bs *Store) OutgoingRelIDs(nodeID snowflake.ID) []snowflake.ID {
	if bs.dbClosed.Load() {
		return nil
	}
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
