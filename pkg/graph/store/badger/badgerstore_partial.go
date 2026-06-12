package badger

import (
	"fmt"
	"log/slog"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
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
// These split helpers perform the partial writes/deletes that TieredStore needs
// for cross-shard relationship routing.

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
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	rid := r.ID()
	startNID := r.StartNodeID()
	id := rid.SnowflakeID()
	startID := startNID.SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	data, err := bs.marshalRelBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; exists {
		bs.idxMu.Unlock()
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, freezeRelCopy(r))
	bs.relIDs[rid] = struct{}{}

	// Type index.
	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[types.RelID]struct{})
	}
	bs.typeIdx[relType][rid] = struct{}{}

	// Outgoing adjacency only (RAM mirror; disk mode relies on the OutKey op).
	if !bs.adjOnDisk {
		if bs.outIdx[startNID] == nil {
			bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
		}
		bs.outIdx[startNID][rid] = types.NodeID(endID)
	}

	// NO inIdx update — the in/ key lives in the endpoint's shard.

	// OPT15: the rel entity lives on THIS shard, so its inline stamp is
	// maintainable here (the cross-shard incoming leg, PutRelIncoming, has no
	// entity and intentionally leaves no stamp — readers miss and decode).
	bs.setRelValidStampLocked(rid, r)

	ops := []writeOp{
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: storepkg.RelTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: storepkg.OutKey(startID, relType, endID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// PutRelIncoming writes only the incoming adjacency key (0x06) for a
// cross-shard relationship. The relationship entity is NOT stored in this
// shard — only the in/ index entry, so that IncomingRelationships queries
// on the endpoint node find the relationship.
// Acquires idxMu.Lock internally.
func (bs *Store) PutRelIncoming(endID, startID snowflake.ID, relType uint16, relID snowflake.ID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(types.NodeID(startID), types.NodeID(endID), relType, types.RelID(relID)); err != nil {
		return err
	}
	endNID := types.NodeID(endID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()

	if !bs.adjOnDisk {
		if bs.inIdx[endNID] == nil {
			bs.inIdx[endNID] = make(map[types.RelID]inEdge)
		}
		bs.inIdx[endNID][rid] = inEdge{start: types.NodeID(startID), typ: relType}
	}

	op := writeOp{
		opType: writeOpSet,
		key:    storepkg.InKey(endID, relType, startID, relID),
	}
	bs.appendOps(op)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DeleteRelEntityAndOut removes the relationship entity (0x02), type index
// (0x04), and outgoing adjacency (0x05) keys. Does NOT touch the incoming
// adjacency key (0x06). Returns RelDeleteInfo so the caller can clean up
// the companion in-shard deletion.
// Acquires idxMu.Lock internally.
func (bs *Store) DeleteRelEntityAndOut(id snowflake.ID) (RelDeleteInfo, error) {
	if err := bs.checkWritable(); err != nil {
		return RelDeleteInfo{}, err
	}
	rid := types.RelID(id)
	if err := storecontract.ValidateRelID(rid); err != nil {
		return RelDeleteInfo{}, err
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return RelDeleteInfo{}, ErrRelNotFound
	}

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
	delete(bs.relValidIdx, rid) // OPT15: drop the inline valid-time stamp

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.RelType]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, info.RelType)
		}
	}

	// Outgoing adjacency cleanup only (RAM mirror; disk mode relies on the
	// OutKey delete op).
	if !bs.adjOnDisk {
		if set, exists := bs.outIdx[startNID]; exists {
			delete(set, rid)
			if len(set) == 0 {
				delete(bs.outIdx, startNID)
			}
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
	return info, bs.flushIfNeeded()
}

// DeleteRelIncoming removes only the incoming adjacency key (0x06) for a
// cross-shard relationship.
// Acquires idxMu.Lock internally.
func (bs *Store) DeleteRelIncoming(info RelDeleteInfo) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(types.NodeID(info.StartID), types.NodeID(info.EndID), info.RelType, types.RelID(info.ID)); err != nil {
		return err
	}
	endNID := types.NodeID(info.EndID)
	rid := types.RelID(info.ID)

	bs.idxMu.Lock()

	if !bs.adjOnDisk {
		if set, exists := bs.inIdx[endNID]; exists {
			if tok, ok := set[rid]; ok {
				if tok.typ != info.RelType {
					bs.idxMu.Unlock()
					return ErrRelNotFound
				}
				delete(set, rid)
				if len(set) == 0 {
					delete(bs.inIdx, endNID)
				}
			}
		}
	}

	op := writeOp{
		opType: writeOpDelete,
		key:    storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID),
	}
	bs.appendOps(op)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
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
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := validateIncomingDeleteTarget(endNodeID, relID); err != nil {
		return err
	}
	bs.deleteIncomingMemoryEntry(endNodeID, relID)

	// Scan pending buffer to find and convert the Set op to a Delete op.
	// The key format: 0x06 | endID(8B) | relType(2B) | startID(8B) | relID(8B) = 27B.
	// The relID is at offset 19.
	bs.deletePendingIncoming(endNodeID, relID)
	if err := bs.scanAndDeleteIncomingPersisted(endNodeID, relID); err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

// ScanAndDeleteIncoming scans Badger for the 0x06 key matching (endNodeID, relID)
// and queues a delete op if found. Repair-only path; not performance critical.
func (bs *Store) ScanAndDeleteIncoming(endNodeID, relID snowflake.ID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := validateIncomingDeleteTarget(endNodeID, relID); err != nil {
		return err
	}
	bs.deleteIncomingMemoryEntry(endNodeID, relID)
	bs.deletePendingIncoming(endNodeID, relID)
	if err := bs.scanAndDeleteIncomingPersisted(endNodeID, relID); err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

func (bs *Store) scanAndDeleteIncomingPersisted(endNodeID, relID snowflake.ID) error {
	prefix := make([]byte, 1+8)
	prefix[0] = storepkg.KeyIn
	storepkg.PutUint64(prefix, 1, int64(endNodeID))

	var ops []writeOp
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
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
					ops = append(ops, writeOp{opType: writeOpDelete, key: delKey})
				}
			}
		}
		return nil // not found — already cleaned up or was never persisted
	}); err != nil {
		return err
	}
	if len(ops) > 0 {
		bs.appendOps(ops...)
	}
	return nil
}

func (bs *Store) deleteIncomingMemoryEntry(endNodeID, relID snowflake.ID) {
	endNID := types.NodeID(endNodeID)
	rid := types.RelID(relID)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if bs.adjOnDisk {
		return // no RAM mirror; the caller queued the persisted-key delete
	}
	set, exists := bs.inIdx[endNID]
	if !exists {
		return
	}
	if _, ok := set[rid]; !ok {
		return
	}

	delete(set, rid)
	if len(set) == 0 {
		delete(bs.inIdx, endNID)
	}
}

func (bs *Store) deletePendingIncoming(endNodeID, relID snowflake.ID) {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()

	for k, op := range bs.pending {
		key := []byte(k)
		if len(key) != storepkg.SizeAdjacency || key[0] != storepkg.KeyIn || op.opType != writeOpSet {
			continue
		}
		if storepkg.ParseIDFromKey(key, 1) == endNodeID && storepkg.ParseRelIDFromAdjKey(key) == relID {
			bs.pending[k] = writeOp{opType: writeOpDelete, key: op.key}
		}
	}
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
	if bs == nil || bs.dbClosed.Load() {
		return false
	}
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[types.NodeID(id)]
	bs.idxMu.RUnlock()
	return exists
}

// HasRelID checks whether the given relationship ID exists in this shard. O(1).
func (bs *Store) HasRelID(id snowflake.ID) bool {
	if bs == nil || bs.dbClosed.Load() {
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
	if bs == nil || bs.dbClosed.Load() {
		return nil
	}
	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(nodeID), typeToken, true)
	bs.idxMu.RUnlock()
	if err != nil {
		// Signature predates the disk arm and cannot surface errors; an
		// empty result makes the repair caller re-derive from entity rows
		// rather than trust a partial index view. Log loudly — silent nil
		// here would read as "no incoming entries".
		slog.Error("graph: IncomingRelIDs adjacency scan failed", "node", nodeID, "err", err)
		return nil
	}
	if len(rids) == 0 {
		return nil
	}

	ids := make([]snowflake.ID, 0, len(rids))
	for _, rid := range rids {
		ids = append(ids, rid.SnowflakeID())
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}

// IncomingIndexEntries returns a sorted snapshot of all incoming adjacency
// entries in this shard, including entries whose end node no longer exists.
func (bs *Store) IncomingIndexEntries() []IncomingIndexEntry {
	if bs == nil || bs.dbClosed.Load() {
		return nil
	}
	bs.idxMu.RLock()
	entries := make([]IncomingIndexEntry, 0)
	if bs.adjOnDisk {
		// Disk arm: full KeyIn keyspace scan (repair surface — the
		// materializing walk is the contract; pending in-keys are merged
		// so unflushed entries are visible like the map was).
		entries = bs.incomingIndexEntriesFromKeyspaceLocked()
	} else {
		for endID, set := range bs.inIdx {
			for relID, relType := range set {
				entries = append(entries, IncomingIndexEntry{
					EndID:   endID.SnowflakeID(),
					RelID:   relID.SnowflakeID(),
					RelType: relType.typ,
				})
			}
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
	if bs == nil || bs.dbClosed.Load() {
		return nil
	}
	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(nodeID), 0, false)
	bs.idxMu.RUnlock()
	if err != nil {
		// See IncomingRelIDs: error-free signature predates the disk arm.
		slog.Error("graph: OutgoingRelIDs adjacency scan failed", "node", nodeID, "err", err)
		return nil
	}
	if len(rids) == 0 {
		return nil
	}
	ids := make([]snowflake.ID, 0, len(rids))
	for _, rid := range rids {
		ids = append(ids, rid.SnowflakeID())
	}
	storepkg.SortSnowflakeIDs(ids)
	return ids
}
