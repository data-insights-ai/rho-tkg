// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

func (bs *Store) PutRelationship(r *types.Relationship) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	rid := r.InternalID()
	startNID := r.StartNodeID()
	endNID := r.EndNodeID()
	id := rid.SnowflakeID()
	startID := startNID.SnowflakeID()
	endID := endNID.SnowflakeID()
	relType := r.TypeToken().Value()

	data, err := bs.marshalRelBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}
	if err := bs.ensureRelationshipEndpointRowsLive(startNID, endNID); err != nil {
		return err
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
	bs.relCache.Put(id, freezeRelCopy(r))
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

	return bs.flushIfNeeded()
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Cache-first: checks LRU cache before falling through to Badger.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	// Check cache first.
	v, status := bs.relCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrRelNotFound
	}

	// Short-circuit: relIDs is rebuilt from relationship entity rows at open and
	// maintained with writes, so misses do not need a Badger transaction.
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
			decoded, err := bs.decodeRelWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode relationship: %w", err)
			}
			r = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean.
	r.Freeze() // shared between cache and caller
	bs.relCache.LoadClean(id, r)
	return r.DeepCopy(), nil
}

// ReplaceRelationship overwrites an existing relationship's data in-place.
// Returns ErrRelNotFound if the relationship does not exist.
// No index changes — type and endpoints are immutable after creation.
func (bs *Store) ReplaceRelationship(r *types.Relationship) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	rid := r.InternalID()
	id := rid.SnowflakeID()

	old, prefetchErr := bs.prefetchRel(rid)

	data, err := bs.marshalRelBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}
	if prefetchErr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read relationship before replace: %w", prefetchErr)
	}
	old, err = bs.currentRelForPrefetchLocked(rid, old)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read relationship before replace: %w", err)
	}
	if err := storecontract.ValidateRelationshipReplacement(old, r); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	bs.relCache.Put(id, freezeRelCopy(r))
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data})
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) DeleteRelationship(rid types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	// Pre-fetch the relationship before acquiring idxMu.Lock so a cache-miss
	// delete does not hold the global index write lock across Badger I/O.
	// The locked section re-reads from the cache-loaded current row below to
	// avoid cleaning indexes with stale type/endpoints if a direct Store caller
	// reused the same ID in the TOCTOU window.
	if _, err := bs.prefetchRel(rid); err != nil {
		return err
	}

	bs.idxMu.Lock()
	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}
	r, err := bs.getRelLocked(rid)
	if err == nil {
		bs.deleteRelByInfo(relDeleteInfoFromRelationship(r))
	}
	bs.idxMu.Unlock()

	if err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

// RelDeleteInfo holds pre-read relationship metadata for two-phase cascade delete.
type RelDeleteInfo struct {
	ID      snowflake.ID
	RelType uint16
	StartID snowflake.ID
	EndID   snowflake.ID
}

func relDeleteInfoFromRelationship(r *types.Relationship) RelDeleteInfo {
	return RelDeleteInfo{
		ID:      r.ID().SnowflakeID(),
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
}

func (bs *Store) currentRelForPrefetchLocked(rid types.RelID, prefetched *types.Relationship) (*types.Relationship, error) {
	if prefetched != nil {
		info := relDeleteInfoFromRelationship(prefetched)
		if types.RelID(info.ID) == rid && bs.relDeleteInfoStillIndexedLocked(info) {
			return prefetched, nil
		}
	}
	return bs.getRelLocked(rid)
}

func (bs *Store) prefetchRelDeleteInfo(rid types.RelID) (RelDeleteInfo, error) {
	r, err := bs.prefetchRel(rid)
	if err != nil {
		return RelDeleteInfo{}, err
	}
	return relDeleteInfoFromRelationship(r), nil
}

func (bs *Store) relDeleteInfoStillIndexedLocked(info RelDeleteInfo) bool {
	rid := types.RelID(info.ID)
	startNID := types.NodeID(info.StartID)
	endNID := types.NodeID(info.EndID)
	if _, exists := bs.relIDs[rid]; !exists {
		return false
	}
	if set, exists := bs.typeIdx[info.RelType]; !exists {
		return false
	} else if _, ok := set[rid]; !ok {
		return false
	}
	if set, exists := bs.outIdx[startNID]; !exists {
		return false
	} else if _, ok := set[rid]; !ok {
		return false
	}
	if set, exists := bs.inIdx[endNID]; !exists {
		return false
	} else if tok, ok := set[rid]; !ok || tok != info.RelType {
		return false
	}
	return true
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
			decoded, err := bs.decodeRelWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode relationship: %w", err)
			}
			r = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	r.Freeze() // shared between cache and caller
	bs.relCache.LoadClean(id, r)
	return r, nil
}

func (bs *Store) prefetchRel(rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	v, status := bs.relCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v, nil
	case indexpkg.CacheDeleted:
		return nil, ErrRelNotFound
	}

	bs.idxMu.RLock()
	_, exists := bs.relIDs[rid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrRelNotFound
	}

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
			decoded, err := bs.decodeRelWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode relationship: %w", err)
			}
			r = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	r.Freeze() // shared between cache and caller
	bs.relCache.LoadClean(id, r)
	return r, nil
}
