package graph

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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
	id := r.InternalID().SnowflakeID()
	intID := int64(id)
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.relIDs[id]; exists {
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, r.DeepCopy())
	bs.relIDs[id] = struct{}{}

	// Type index.
	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[snowflake.ID]struct{})
	}
	bs.typeIdx[relType][id] = struct{}{}

	// Outgoing adjacency only.
	if bs.outIdx[startID] == nil {
		bs.outIdx[startID] = make(map[snowflake.ID]struct{})
	}
	bs.outIdx[startID][id] = struct{}{}

	// NO inIdx update — the in/ key lives in the endpoint's shard.

	ops := []writeOp{
		{opType: writeOpSet, key: relKey(intID), value: data},
		{opType: writeOpSet, key: relTypeIndexKey(relType, intID)},
		{opType: writeOpSet, key: outKey(int64(startID), relType, int64(endID), intID)},
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
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if bs.inIdx[endID] == nil {
		bs.inIdx[endID] = make(map[snowflake.ID]struct{})
	}
	bs.inIdx[endID][relID] = struct{}{}

	op := writeOp{
		opType: writeOpSet,
		key:    inKey(int64(endID), relType, int64(startID), int64(relID)),
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
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	r, err := bs.getRelLocked(id)
	if err != nil {
		return relDeleteInfo{}, err
	}

	info := relDeleteInfo{
		id:      id,
		relType: r.TypeToken().Value(),
		startID: r.StartNodeID().SnowflakeID(),
		endID:   r.EndNodeID().SnowflakeID(),
	}

	intID := int64(id)

	// Update in-memory state.
	bs.relCache.MarkDeleted(id)
	delete(bs.relIDs, id)

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.relType]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.typeIdx, info.relType)
		}
	}

	// Outgoing adjacency cleanup only.
	if set, exists := bs.outIdx[info.startID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.outIdx, info.startID)
		}
	}

	// NO inIdx cleanup — the in/ key lives in the endpoint's shard.

	ops := []writeOp{
		{opType: writeOpDelete, key: relKey(intID)},
		{opType: writeOpDelete, key: relTypeIndexKey(info.relType, intID)},
		{opType: writeOpDelete, key: outKey(int64(info.startID), info.relType, int64(info.endID), intID)},
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
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if set, exists := bs.inIdx[info.endID]; exists {
		delete(set, info.id)
		if len(set) == 0 {
			delete(bs.inIdx, info.endID)
		}
	}

	op := writeOp{
		opType: writeOpDelete,
		key:    inKey(int64(info.endID), info.relType, int64(info.startID), int64(info.id)),
	}
	bs.appendOps(op)
	return nil
}

// --- Read helpers for TieredStore shard resolution ---

// hasNodeID checks whether the given node ID exists in this shard. O(1).
func (bs *BadgerStore) hasNodeID(id snowflake.ID) bool {
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[id]
	bs.idxMu.RUnlock()
	return exists
}

// hasRelID checks whether the given relationship ID exists in this shard. O(1).
func (bs *BadgerStore) hasRelID(id snowflake.ID) bool {
	bs.idxMu.RLock()
	_, exists := bs.relIDs[id]
	bs.idxMu.RUnlock()
	return exists
}

// incomingRelIDs returns relationship IDs from the inIdx for the given node.
// typeToken 0 = all types. Returns a sorted slice. Snapshot under RLock.
func (bs *BadgerStore) incomingRelIDs(nodeID snowflake.ID, typeToken uint16) []snowflake.ID {
	bs.idxMu.RLock()
	set := bs.inIdx[nodeID]
	if len(set) == 0 {
		bs.idxMu.RUnlock()
		return nil
	}

	var ids []snowflake.ID
	if typeToken == 0 {
		ids = make([]snowflake.ID, 0, len(set))
		for relID := range set {
			ids = append(ids, relID)
		}
	} else {
		// Filter by type: need to check each rel's type token.
		// Collect all relIDs first, then filter outside lock.
		ids = make([]snowflake.ID, 0, len(set))
		for relID := range set {
			ids = append(ids, relID)
		}
	}
	bs.idxMu.RUnlock()

	if typeToken != 0 {
		filtered := make([]snowflake.ID, 0, len(ids))
		for _, relID := range ids {
			r, err := bs.GetRelationship(relID)
			if err != nil {
				continue // orphan or cross-shard entity — skip
			}
			if r.TypeToken().Value() == typeToken {
				filtered = append(filtered, relID)
			}
		}
		ids = filtered
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// outgoingRelIDs returns relationship IDs from the outIdx for the given node.
// Returns a sorted slice. Snapshot under RLock.
func (bs *BadgerStore) outgoingRelIDs(nodeID snowflake.ID) []snowflake.ID {
	bs.idxMu.RLock()
	set := bs.outIdx[nodeID]
	if len(set) == 0 {
		bs.idxMu.RUnlock()
		return nil
	}
	ids := make([]snowflake.ID, 0, len(set))
	for relID := range set {
		ids = append(ids, relID)
	}
	bs.idxMu.RUnlock()

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
