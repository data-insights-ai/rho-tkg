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
// A full PutRelationship writes entity (0x02), typeIdx (0x04), outIdx (0x05) and
// inIdx (0x06). For cross-shard rels the keys split: the entity shard holds
// 0x02/0x04/0x05, the end node's shard holds 0x06. These helpers perform the
// partial writes/deletes TieredStore needs.

// IncomingIndexEntry is a snapshot row from the incoming adjacency index.
type IncomingIndexEntry struct {
	EndID   snowflake.ID
	RelID   snowflake.ID
	RelType uint16
}

// PutRelEntityAndOut writes the entity (0x02), type index (0x04) and outgoing
// adjacency (0x05) keys, not the incoming key (0x06). TieredStore verifies
// endpoint existence first. Acquires idxMu.Lock.
// Co-commits a ChangeRelPut record with the FULL relationship (BACKLOG 18c): the
// entity-bearing leg is the one place that can describe the whole rel, and a
// replica re-derives both legs by routing. PutRelIncoming emits nothing.
func (bs *Store) PutRelEntityAndOut(r *types.Relationship) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
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
	changePayload, err := bs.buildRelPutPayload(r, false)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	if bs.relIDs.Has(int64(rid)) {
		bs.idxMu.Unlock()
		return ErrRelExists
	}
	if err := bs.foldRecreatedRelHistoryLocked(r, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update in-memory state.
	bs.relCache.Put(id, bs.frozenRelRow(r))
	bs.relIDs.Add(int64(rid))
	bs.bumpRelRevLocked(rid)

	// Type index.
	bs.addTypeIdxLocked(relType, rid)

	// Outgoing adjacency only (RAM mirror; disk mode relies on the OutKey op).
	if !bs.adjOnDisk {
		if bs.outIdx[startNID] == nil {
			bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
		}
		bs.outIdx[startNID][rid] = types.NodeID(endID)
	}

	// NO inIdx update — the in/ key lives in the endpoint's shard.

	// The entity lives on THIS shard, so its inline stamp is maintainable here
	// (PutRelIncoming has no entity and leaves no stamp; readers miss and decode).
	bs.setRelValidStampLocked(rid, r)
	bs.recordRelTypeMemberLocked(r) // transaction-time rel-type membership

	ops := []writeOp{
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: storepkg.RelTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: storepkg.OutKey(startID, relType, endID, id)},
	}

	// BACKLOG 18l: the entity is fully resident here, so the property index / type
	// class-count contribution is computable; mirror PutRelationship's maintenance.
	bs.maintainRelPropertyIndexesAdd(r, id)
	bs.maintainRelTypeTemporalIndexesAdd(r, id) // backlog 10: a tiered shard's cross-shard rows
	bs.addRelPropertyTypeClassCounts(r)
	bs.addRelPropertyStatsCounts(r) // backlog 53: every door that makes a row current counts it, so a delete can take the row back out

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	bs.logChangeRaw(storecontract.ChangeRelPut, changePayload)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// PutRelIncoming writes only the incoming adjacency key (0x06) for a cross-shard
// relationship; the entity is not stored in this shard. Acquires idxMu.Lock.
// Emits NO change-log record (BACKLOG 18c): this leg carries only index bytes, and
// the companion PutRelEntityAndOut's ChangeRelPut already describes the relationship.
func (bs *Store) PutRelIncoming(endID, startID snowflake.ID, relType uint16, relID snowflake.ID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
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

// DeleteRelEntityAndOut removes the entity (0x02), type index (0x04) and outgoing
// adjacency (0x05) keys, not 0x06, and returns RelDeleteInfo for the companion
// in-shard deletion. Acquires idxMu.Lock.
// Co-commits a ChangeRelDelete record (BACKLOG 18c) like DeleteRelationship (ID
// only, hard delete); DeleteRelIncoming emits nothing (see PutRelIncoming). It
// records the dropped out-leg's ex-adjacency (backlog 50); removals that are not
// a relationship delete use RemoveRelEntityAndOutPlacement.
func (bs *Store) DeleteRelEntityAndOut(id snowflake.ID) (RelDeleteInfo, error) {
	return bs.deleteRelEntityAndOut(id, true)
}

// RemoveRelEntityAndOutPlacement is DeleteRelEntityAndOut for a placement that is
// not a relationship delete (cross-shard move or create rollback): the rel stays
// live elsewhere, so no ex-adjacency leg is recorded.
func (bs *Store) RemoveRelEntityAndOutPlacement(id snowflake.ID) (RelDeleteInfo, error) {
	return bs.deleteRelEntityAndOut(id, false)
}

func (bs *Store) deleteRelEntityAndOut(id snowflake.ID, exLeg bool) (RelDeleteInfo, error) {
	if err := bs.checkWritable(); err != nil {
		return RelDeleteInfo{}, err
	}
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
	rid := types.RelID(id)
	if err := storecontract.ValidateRelID(rid); err != nil {
		return RelDeleteInfo{}, err
	}
	// Build the change-log delete record up front, so a marshal error aborts
	// before any op is enqueued (mirrors DeleteRelationship / lesson 58).
	relDelPayload, err := bs.buildChangePayload(storepkg.RelDeleteBody{ID: int64(id)})
	if err != nil {
		return RelDeleteInfo{}, fmt.Errorf("graph: encode change-log: %w", err)
	}

	bs.idxMu.Lock()

	if !bs.relIDs.Has(int64(rid)) {
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
	bs.relIDs.Remove(int64(rid))
	bs.ords.dropRel(rid)
	bs.deleteRelRevLocked(rid)
	delete(bs.relValidIdx, rid) // drop the inline valid-time stamp

	// Type index cleanup.
	bs.removeTypeIdxLocked(info.RelType, rid)

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

	// BACKLOG 18l: mirror deleteRelByInfo's maintenance; r is available, so the
	// precise value-based removal is used instead of the brute-force purge.
	bs.maintainRelPropertyIndexesRemove(r, id)
	// The rel-type temporal envelope stays (append-only): the row's history
	// stays on this shard, and the tiered rollback re-puts the row without it.
	bs.removeRelPropertyTypeClassCounts(r)
	bs.removeRelPropertyStatsCounts(r)

	ops := []writeOp{
		{opType: writeOpDelete, key: storepkg.RelKey(id)},
		{opType: writeOpDelete, key: storepkg.RelTypeIndexKey(info.RelType, id)},
		{opType: writeOpDelete, key: storepkg.OutKey(info.StartID, info.RelType, info.EndID, id)},
	}
	if exLeg {
		ops = append(ops, exAdjLegOp(writeOpSet, info.StartID, false, id)) // backlog 50: the out-leg's ex-adjacency lives here
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.RelType).Add(-1)
	bs.logChangeRaw(storecontract.ChangeRelDelete, relDelPayload)

	bs.idxMu.Unlock()
	return info, bs.flushIfNeeded()
}

// DeleteRelIncoming removes only the incoming adjacency key (0x06) for a
// cross-shard relationship. Acquires idxMu.Lock. Emits NO change-log record (see
// PutRelIncoming, BACKLOG 18c). It records the dropped in-leg's ex-adjacency
// (backlog 50); non-delete removals use RemoveRelIncomingPlacement.
func (bs *Store) DeleteRelIncoming(info RelDeleteInfo) error {
	return bs.deleteRelIncoming(info, true)
}

// RemoveRelIncomingPlacement is DeleteRelIncoming for a placement that is not a
// relationship delete (see RemoveRelEntityAndOutPlacement): no ex-adjacency leg.
func (bs *Store) RemoveRelIncomingPlacement(info RelDeleteInfo) error {
	return bs.deleteRelIncoming(info, false)
}

func (bs *Store) deleteRelIncoming(info RelDeleteInfo, exLeg bool) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
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

	ops := []writeOp{{opType: writeOpDelete, key: storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID)}}
	if exLeg {
		ops = append(ops, exAdjLegOp(writeOpSet, info.EndID, true, info.ID)) // backlog 50: the in-leg's ex-adjacency
	}
	bs.appendOps(ops...)
	bs.idxMu.Unlock()
	return bs.flushIfNeeded()
}

// DeleteIncomingByRelID removes a relationship from the inIdx of an end node, for
// repair of orphaned in/ entries whose entity is gone. With relType/startID
// unknown, it converts a matching pending Set op to a Delete, otherwise scans
// persisted keys by endpoint prefix. Acquires idxMu.Lock.
func (bs *Store) DeleteIncomingByRelID(endNodeID snowflake.ID, relID snowflake.ID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
	if err := validateIncomingDeleteTarget(endNodeID, relID); err != nil {
		return err
	}
	bs.deleteIncomingMemoryEntry(endNodeID, relID)

	// Key format: 0x06 | endID(8B) | relType(2B) | startID(8B) | relID(8B) = 27B; relID is at offset 19.
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
	defer bs.bumpRelEpoch() // one half of a cross-shard relationship write
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
	if err := bs.view(func(txn *badgerv4.Txn) error {
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
			bs.putPendingLocked(writeOp{opType: writeOpDelete, key: op.key})
		}
	}
	// A matching SET parked in `flushing` is mid-commit and cannot be rewritten:
	// queue an explicit delete into pending, else the repair View scan misses the
	// not-yet-committed key and the incoming entry orphans.
	for k, op := range bs.flushing {
		key := []byte(k)
		if len(key) != storepkg.SizeAdjacency || key[0] != storepkg.KeyIn || op.opType != writeOpSet {
			continue
		}
		if storepkg.ParseIDFromKey(key, 1) == endNodeID && storepkg.ParseRelIDFromAdjKey(key) == relID {
			if _, has := bs.pending[k]; !has {
				bs.putPendingLocked(writeOp{opType: writeOpDelete, key: op.key})
			}
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
	exists := bs.nodeIDs.Has(int64(types.NodeID(id)))
	bs.idxMu.RUnlock()
	return exists
}

// HasRelID checks whether the given relationship ID exists in this shard. O(1).
func (bs *Store) HasRelID(id snowflake.ID) bool {
	if bs == nil || bs.dbClosed.Load() {
		return false
	}
	bs.idxMu.RLock()
	exists := bs.relIDs.Has(int64(types.RelID(id)))
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
		// The signature cannot surface errors; an empty result makes the repair caller
		// re-derive from entity rows. Log loudly: silent nil would read as "no incoming entries".
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
		// Disk arm: full KeyIn scan (repair surface; materializing is the contract);
		// pending in-keys are merged so unflushed entries are visible.
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
	return bs.OutgoingRelIDsOfType(nodeID, 0)
}

// OutgoingRelIDsOfType is OutgoingRelIDs filtered by type (typeToken 0 = all
// types), the outgoing mirror of IncomingRelIDs. No node check: a tiered
// mint shard holds legs of nodes that live elsewhere.
func (bs *Store) OutgoingRelIDsOfType(nodeID snowflake.ID, typeToken uint16) []snowflake.ID {
	if bs == nil || bs.dbClosed.Load() {
		return nil
	}
	bs.idxMu.RLock()
	rids, err := bs.adjacentRelIDsSnapshotLocked(types.NodeID(nodeID), typeToken, false)
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
