// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"errors"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// PutRelationship stores a relationship with type index and adjacency entries.
// Returns ErrNodeNotFound if the start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (bs *Store) PutRelationship(r *types.Relationship) error {
	rid := r.InternalID()
	startNID := r.StartNodeID()
	endNID := r.EndNodeID()
	id := rid.SnowflakeID()
	startID := startNID.SnowflakeID()
	endID := endNID.SnowflakeID()
	relType := r.TypeToken().Value()

	w := storepkg.RelToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	// Verify endpoints exist.
	if _, exists := bs.nodeIDs[startNID]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if _, exists := bs.nodeIDs[endNID]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Check for duplicate via O(1) relIDs.
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

	// Adjacency indexes.
	if bs.outIdx[startNID] == nil {
		bs.outIdx[startNID] = make(map[types.RelID]struct{})
	}
	bs.outIdx[startNID][rid] = struct{}{}
	if bs.inIdx[endNID] == nil {
		bs.inIdx[endNID] = make(map[types.RelID]uint16)
	}
	bs.inIdx[endNID][rid] = relType

	// Build write ops.
	ops := []writeOp{
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: storepkg.RelTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: storepkg.OutKey(startID, relType, endID, id)},
		{opType: writeOpSet, key: storepkg.InKey(endID, relType, startID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Cache-first: checks LRU cache before falling through to Badger.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	// Check cache first.
	v, status := bs.relCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrRelNotFound
	}

	// Short-circuit: relIDs is the authoritative set of all relationship IDs.
	// Avoids opening a Badger transaction for non-existent relationships.
	bs.idxMu.RLock()
	_, exists := bs.relIDs[rid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrRelNotFound
	}

	// Cache miss, rel exists — read from Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.RelKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r = storepkg.WireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean.
	bs.relCache.LoadClean(id, r)
	return r.DeepCopy(), nil
}

// ReplaceRelationship overwrites an existing relationship's data in-place.
// Returns ErrRelNotFound if the relationship does not exist.
// No index changes — type and endpoints are immutable after creation.
func (bs *Store) ReplaceRelationship(r *types.Relationship) error {
	rid := r.InternalID()
	id := rid.SnowflakeID()

	w := storepkg.RelToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}

	bs.relCache.Put(id, r.DeepCopy())
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) DeleteRelationship(rid types.RelID) error {
	id := rid.SnowflakeID()
	bs.idxMu.Lock()
	err := bs.deleteRelLocked(id)
	bs.idxMu.Unlock()

	if err != nil {
		return err
	}
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// RelDeleteInfo holds pre-read relationship metadata for two-phase cascade delete.
type RelDeleteInfo struct {
	ID      snowflake.ID
	RelType uint16
	StartID snowflake.ID
	EndID   snowflake.ID
}

// deleteRelLocked removes a relationship and cleans up indexes.
// Caller must hold bs.idxMu write lock.
func (bs *Store) deleteRelLocked(id snowflake.ID) error {
	// Read phase.
	r, err := bs.getRelLocked(types.RelID(id))
	if err != nil {
		return err
	}

	// Mutation phase.
	bs.deleteRelByInfo(RelDeleteInfo{
		ID:      id,
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	})

	return nil
}

// deleteRelByInfo applies relationship deletion mutations using pre-read metadata.
// Caller must hold bs.idxMu write lock. This method performs no reads — it cannot fail.
func (bs *Store) deleteRelByInfo(info RelDeleteInfo) {
	// info.ID/startID/endID are raw snowflake.ID — RelDeleteInfo is shared with
	// off-limits TieredStore code paths and must keep raw fields. Convert at
	// each map access.
	rid := types.RelID(info.ID)
	startNID := types.NodeID(info.StartID)
	endNID := types.NodeID(info.EndID)

	// Update in-memory state.
	bs.relCache.MarkDeleted(info.ID)
	delete(bs.relIDs, rid)

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.RelType]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, info.RelType)
		}
	}

	// Adjacency cleanup.
	if set, exists := bs.outIdx[startNID]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.outIdx, startNID)
		}
	}
	if set, exists := bs.inIdx[endNID]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.inIdx, endNID)
		}
	}

	// Build delete ops.
	ops := []writeOp{
		{opType: writeOpDelete, key: storepkg.RelKey(info.ID)},
		{opType: writeOpDelete, key: storepkg.RelTypeIndexKey(info.RelType, info.ID)},
		{opType: writeOpDelete, key: storepkg.OutKey(info.StartID, info.RelType, info.EndID, info.ID)},
		{opType: writeOpDelete, key: storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.RelType).Add(-1)
}

// RelationshipsByType returns relationships with the given type token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.typeIdx[token]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)

	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(rids, opts)
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.outIdx[nid]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.GetRelationship(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			rels = append(rels, r)
		}
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots all relIDs under one idxMu.RLock;
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *Store) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.outIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]types.RelID, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		perNode[nid] = ids
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, type-filter, group by source node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
				rels = append(rels, r)
			}
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.inIdx[nid]
	rids := make([]types.RelID, 0, len(set))
	for id, tok := range set {
		if typeToken == 0 || tok == typeToken {
			rids = append(rids, id)
		}
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.GetRelationship(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		rels = append(rels, r)
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots relIDs from inIdx under one
// idxMu.RLock (with early type filtering since inIdx stores typeToken);
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	// inIdx stores relID -> typeToken, enabling early type filtering.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.inIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]types.RelID, 0, len(set))
		for id, tok := range set {
			if typeToken == 0 || tok == typeToken {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			perNode[nid] = ids
		}
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, group by target node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			rels = append(rels, r)
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// AllRelationships returns all stored relationships, with optional pagination
// and temporal filtering. Snapshot relIDs under lock, sort + paginate, then
// fetch via GetRelationship. Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)

	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(rids, opts)
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (bs *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		rels = append(rels, r)
	}

	if len(rels) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(rels)
	return rels, nil
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (bs *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if len(rels) == 0 {
		return nil
	}

	// Pre-serialize all relationships outside the lock.
	type relData struct {
		rid      types.RelID
		startNID types.NodeID
		endNID   types.NodeID
		id       snowflake.ID
		startID  snowflake.ID
		endID    snowflake.ID
		relType  uint16
		data     []byte
	}
	serialized := make([]relData, len(rels))
	for i, r := range rels {
		w := storepkg.RelToWire(r)
		data, err := msgpack.Marshal(w)
		if err != nil {
			return fmt.Errorf("graph: marshal relationship: %w", err)
		}
		rid := r.InternalID()
		startNID := r.StartNodeID()
		endNID := r.EndNodeID()
		serialized[i] = relData{
			rid:      rid,
			startNID: startNID,
			endNID:   endNID,
			id:       rid.SnowflakeID(),
			startID:  startNID.SnowflakeID(),
			endID:    endNID.SnowflakeID(),
			relType:  r.TypeToken().Value(),
			data:     data,
		}
	}

	bs.idxMu.Lock()

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, rd := range serialized {
		if _, exists := bs.nodeIDs[rd.startNID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if _, exists := bs.nodeIDs[rd.endNID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if _, exists := bs.relIDs[rd.rid]; exists {
			bs.idxMu.Unlock()
			return ErrRelExists
		}
		if _, exists := seen[rd.rid]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate relationship ID %d in batch", rd.id)
		}
		seen[rd.rid] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(rels)*4) // entity + type + out + in
	for i, r := range rels {
		rd := serialized[i]

		bs.relCache.Put(rd.id, r.DeepCopy())
		bs.relIDs[rd.rid] = struct{}{}

		if bs.typeIdx[rd.relType] == nil {
			bs.typeIdx[rd.relType] = make(map[types.RelID]struct{})
		}
		bs.typeIdx[rd.relType][rd.rid] = struct{}{}

		if bs.outIdx[rd.startNID] == nil {
			bs.outIdx[rd.startNID] = make(map[types.RelID]struct{})
		}
		bs.outIdx[rd.startNID][rd.rid] = struct{}{}

		if bs.inIdx[rd.endNID] == nil {
			bs.inIdx[rd.endNID] = make(map[types.RelID]uint16)
		}
		bs.inIdx[rd.endNID][rd.rid] = rd.relType

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelKey(rd.id), value: rd.data})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelTypeIndexKey(rd.relType, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.OutKey(rd.startID, rd.relType, rd.endID, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.InKey(rd.endID, rd.relType, rd.startID, rd.id)})
		bs.getOrCreateTypeCounter(rd.relType).Add(1)
	}

	bs.appendOps(ops...)
	bs.relCount.Add(int64(len(rels)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read relationship metadata.
// Phase 2: delete via deleteRelByInfo (mutation-only), clean up history.
// Missing ID → ErrRelNotFound, zero mutations. Nil/empty input → nil error.
func (bs *Store) DeleteRelationshipsBatch(typedIDs []types.RelID) error {
	if len(typedIDs) == 0 {
		return nil
	}

	bs.idxMu.Lock()

	// Phase 1: validate — all must exist + pre-read metadata.
	infos := make([]RelDeleteInfo, len(typedIDs))
	for i, rid := range typedIDs {
		if _, exists := bs.relIDs[rid]; !exists {
			bs.idxMu.Unlock()
			return ErrRelNotFound
		}
		r, err := bs.getRelLocked(rid)
		if err != nil {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: batch read relationship %d: %w", rid.SnowflakeID(), err)
		}
		infos[i] = RelDeleteInfo{
			ID:      rid.SnowflakeID(),
			RelType: r.TypeToken().Value(),
			StartID: r.StartNodeID().SnowflakeID(),
			EndID:   r.EndNodeID().SnowflakeID(),
		}
	}

	// Phase 2: apply — all validated, mutations cannot fail.
	for _, info := range infos {
		bs.deleteRelByInfo(info)
	}

	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy. O(N) in relIDs map size.
func (bs *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}
	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })
	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}
	return rids, nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *Store) ForEachRelID(fn func(types.RelID) bool) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for id := range bs.relIDs {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// getRelLocked retrieves a relationship from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *Store) getRelLocked(rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	v, status := bs.relCache.Get(id)
	if status == indexpkg.CacheHit {
		return v, nil
	}
	if status == indexpkg.CacheDeleted {
		return nil, ErrRelNotFound
	}

	// Cache miss — read from Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.RelKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r = storepkg.WireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.relCache.LoadClean(id, r)
	return r, nil
}
