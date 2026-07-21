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
)

func (bs *Store) PutRelationship(r *types.Relationship) error {
	return bs.putRelationship(r, true, false, 0)
}

// PutRelationshipScoped mirrors PutRelationship but routes the change-log
// record into the store.ScopedTxChangeLog buffer named by token instead of
// the eager pending log. token == 0 is exactly PutRelationship. See
// badgerstore_changelog_scoped.go (BACKLOG 11f Batch A — foundation only).
func (bs *Store) PutRelationshipScoped(r *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.PutRelationship(r)
	}
	return bs.putRelationship(r, true, false, token)
}

// PutRelationshipCoLocated writes a relationship whose entity AND both adjacency
// legs (out + in) live on THIS shard, WITHOUT validating that its endpoint nodes
// exist on this shard — a sharded owner (ADR-0007: a rel + both legs live on the
// rel's shard, endpoints may be elsewhere) validates the endpoints on their own
// shards before calling. It emits the SAME co-committed ChangeRelPut record as
// PutRelationship, so a sharded rel create appears in the change-log feed (the
// partial doors PutRelEntityAndOut/PutRelIncoming are record-free split-write
// helpers and would leave a sharded rel create invisible to a tailing replica).
func (bs *Store) PutRelationshipCoLocated(r *types.Relationship) error {
	return bs.putRelationship(r, false, false, 0)
}

// PutRelationshipForeignIncoming writes a cross-machine incoming half-edge STUB
// (ADR-0010 Model A) co-located on THIS shard exactly like PutRelationshipCoLocated
// (entity + both adjacency legs, no endpoint validation), but co-commits a
// ChangeForeignIncoming record instead of ChangeRelPut. The stub's rel-ID belongs
// to a FOREIGN slot (the edge's real owner is another machine), so it is reachable
// only via this shard's adjacency fold, never a slot-routed GetRelationship — and
// the distinct record tag lets a replica route apply by the END-node slot rather
// than the (foreign) rel slot. Called by the sharded store's RecordForeignIncoming.
func (bs *Store) PutRelationshipForeignIncoming(r *types.Relationship) error {
	return bs.putRelationship(r, false, true, 0)
}

func (bs *Store) putRelationship(r *types.Relationship, validateEndpoints, foreignIncoming bool, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // expand path: adjacency view changed
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
	changePayload, err := bs.buildRelPutPayload(r, false)
	if err != nil {
		return err
	}
	changeTag := relPutTag(foreignIncoming)
	if validateEndpoints {
		if err := bs.ensureRelationshipEndpointRowsLive(startNID, endNID); err != nil {
			return err
		}
	}

	bs.idxMu.Lock()

	// Verify endpoints exist (skipped for a co-located sharded write — the sharded
	// owner validated the endpoints on their own shards before calling).
	if validateEndpoints {
		if _, exists := bs.nodeIDs[startNID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if _, exists := bs.nodeIDs[endNID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
	}

	// Check for duplicate via O(1) relIDs.
	if _, exists := bs.relIDs[rid]; exists {
		bs.idxMu.Unlock()
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, freezeRelCopy(r))
	bs.relIDs[rid] = struct{}{}
	bs.bumpRelRevLocked(rid)

	// Type index.
	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[types.RelID]struct{})
	}
	bs.typeIdx[relType][rid] = struct{}{}

	// Adjacency indexes (RAM mirror — the persisted OutKey/InKey ops below
	// are the durable truth; disk mode skips the mirror).
	if !bs.adjOnDisk {
		if bs.outIdx[startNID] == nil {
			bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
		}
		bs.outIdx[startNID][rid] = endNID
		if bs.inIdx[endNID] == nil {
			bs.inIdx[endNID] = make(map[types.RelID]inEdge)
		}
		bs.inIdx[endNID][rid] = inEdge{start: startNID, typ: relType}
	}
	bs.setRelValidStampLocked(rid, r) // inline valid-time stamp
	bs.recordRelTypeMemberLocked(r)   // transaction-time rel-type membership

	// Build write ops.
	ops := []writeOp{
		{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		{opType: writeOpSet, key: storepkg.RelTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: storepkg.OutKey(startID, relType, endID, id)},
		{opType: writeOpSet, key: storepkg.InKey(endID, relType, startID, id)},
	}

	bs.maintainRelPropertyIndexesAdd(r, id)
	bs.maintainRelTypeTemporalIndexesAdd(r, id) // BACKLOG 21c
	bs.addRelPropertyTypeClassCounts(r)
	bs.addRelPropertyStatsCounts(r)

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	logErr := bs.logChangeRoutedRaw(changeTag, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// bumpRelRevLocked mirrors bumpNodeRevLocked (BACKLOG 18b): called by every
// door that changes a live relationship's DATA (create + property-changing
// update), so relRevs[rid] changes on every such write. This is what lets
// currentRelForPrefetchLocked detect a concurrent property-changing write in
// the prefetch→lock window — relDeleteInfoStillIndexedLocked alone cannot,
// because endpoints/type are immutable, so a property-only update leaves
// relIDs/typeIdx/adjacency membership completely unchanged.
func (bs *Store) bumpRelRevLocked(rid types.RelID) {
	if bs.relRevs == nil {
		bs.relRevs = make(map[types.RelID]uint64)
	}
	bs.nextRelRev++
	if bs.nextRelRev == 0 {
		bs.nextRelRev = 1
	}
	bs.relRevs[rid] = bs.nextRelRev
}

// deleteRelRevLocked mirrors deleteNodeRevLocked. Called from the single
// shared delete mutator (deleteRelByInfo) so every rel delete path clears the
// rev entry in one place.
func (bs *Store) deleteRelRevLocked(rid types.RelID) {
	delete(bs.relRevs, rid)
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
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
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

// getRelInTxn is GetRelationship's body reading through an ALREADY-OPEN read
// transaction instead of opening its own — used by RelsAsOf's single-transaction
// bulk scan (BACKLOG 18k). Mirrors getNodeInTxn; see its doc comment. REQUIRES
// the caller to hold bs.idxMu (at least RLock) for the entire surrounding scan.
func (bs *Store) getRelInTxn(txn *badgerv4.Txn, rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	v, status := bs.relCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrRelNotFound
	}

	if _, exists := bs.relIDs[rid]; !exists {
		return nil, ErrRelNotFound
	}

	item, err := txn.Get(storepkg.RelKey(id))
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrRelNotFound
	}
	if err != nil {
		return nil, err
	}
	var r *types.Relationship
	if err := item.Value(func(val []byte) error {
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal relationship: %w", err)
		}
		decoded, err := bs.decodeRelWireForKey(w, id)
		if err != nil {
			return fmt.Errorf("graph: decode relationship: %w", err)
		}
		r = decoded
		return nil
	}); err != nil {
		return nil, err
	}

	r.Freeze()
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
	defer bs.bumpRelEpoch() // expand path: adjacency view changed
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	rid := r.InternalID()
	id := rid.SnowflakeID()

	// Snapshot the rev alongside the prefetched row (BACKLOG 18b): endpoints/
	// type are immutable, so relDeleteInfoStillIndexedLocked's structural check
	// alone cannot detect a concurrent property-only update racing this
	// prefetch — only relRevs (bumped on every rel data write) can.
	prefetched, prefetchErr := bs.prefetchRelWithRev(rid)

	data, err := bs.marshalRelBytes(r)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}
	changePayload, err := bs.buildRelPutPayload(r, false)
	if err != nil {
		return err
	}

	if bs.replaceRelPrefetchTestHook != nil {
		bs.replaceRelPrefetchTestHook()
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
	old, err := bs.currentRelForPrefetchLocked(rid, prefetched)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read relationship before replace: %w", err)
	}
	if err := storecontract.ValidateRelationshipReplacement(old, r); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Type/endpoints are immutable, but property values can change — refresh
	// the rel property index (remove old value, add new).
	bs.maintainRelPropertyIndexesRemove(old, id)
	bs.maintainRelTypeTemporalIndexesRemove(old, id)                     // BACKLOG 21c
	bs.removeRelPropertyTypeClassCountsByID(id, old.TypeToken().Value()) // decrement old (type immutable)
	bs.removeRelPropertyStatsCountsByID(id, old.TypeToken().Value())
	bs.relCache.Put(id, freezeRelCopy(r))
	bs.bumpRelRevLocked(rid)
	bs.maintainRelPropertyIndexesAdd(r, id)
	bs.maintainRelTypeTemporalIndexesAdd(r, id) // BACKLOG 21c
	bs.addRelPropertyTypeClassCounts(r)         // increment new
	bs.addRelPropertyStatsCounts(r)
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data})
	// A version update rewrites the row in place — endpoints/type are
	// immutable (no adjacency change) but valid_to may move, so the inline stamp
	// MUST be refreshed here or a temporal traversal reads a stale interval.
	bs.setRelValidStampLocked(rid, r)
	bs.logChangeRaw(storecontract.ChangeRelPut, changePayload)
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) DeleteRelationship(rid types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // expand path: adjacency view changed
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	// Build the change-log delete record up front (a standalone rel delete is a
	// hard delete with no tombstone), so a marshal error aborts before any op is
	// enqueued. deleteRelByInfo itself stays record-free — it is shared with the
	// node-cascade path, which emits a single ChangeNodeDelete instead.
	relDelPayload, err := bs.buildChangePayload(storepkg.RelDeleteBody{ID: int64(rid.SnowflakeID())})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
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
		bs.logChangeRaw(storecontract.ChangeRelDelete, relDelPayload)
	}
	bs.idxMu.Unlock()

	if err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

// DeleteRelationshipForeignIncoming removes a Model-A incoming half-edge stub
// (ADR-0010 §3.3) physically co-located on THIS (the END node's) shard. It is the
// delete counterpart of PutRelationshipForeignIncoming: the stub's row + both
// adjacency legs are removed exactly like DeleteRelationship, but a
// ChangeForeignIncomingDelete record is co-committed instead of ChangeRelDelete,
// carrying the END-node ID so a replica routes apply by the END slot (the rel's
// own slot is foreign). Returns ErrRelNotFound when the stub is absent — the
// caller (cascade / replica apply) tolerates it for idempotency.
func (bs *Store) DeleteRelationshipForeignIncoming(rid types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch()
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	if _, err := bs.prefetchRel(rid); err != nil {
		return err
	}

	bs.idxMu.Lock()
	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}
	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	// The END-node ID must be captured from the stub before deletion — a replica
	// routes the delete by it (the rel-ID's slot is foreign). Build the record
	// inside the lock but marshal errors surface after unlock, mirroring the
	// other delete doors.
	delPayload, perr := bs.buildChangePayload(storepkg.ForeignIncomingDeleteBody{
		RelID: int64(rid.SnowflakeID()),
		EndID: int64(r.EndNodeID().SnowflakeID()),
	})
	if perr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: encode change-log: %w", perr)
	}
	bs.deleteRelByInfo(relDeleteInfoFromRelationship(r))
	bs.logChangeRaw(storecontract.ChangeForeignIncomingDelete, delPayload)
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// RelDeleteInfo holds pre-read relationship metadata for two-phase cascade delete.
// Rev is populated ONLY by prefetchRelDeleteInfo (0 elsewhere) — see its doc comment.
type RelDeleteInfo struct {
	ID      snowflake.ID
	RelType uint16
	StartID snowflake.ID
	EndID   snowflake.ID
	Rev     uint64
}

func relDeleteInfoFromRelationship(r *types.Relationship) RelDeleteInfo {
	return RelDeleteInfo{
		ID:      r.ID().SnowflakeID(),
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
}

// relPrefetchSnapshot pairs a prefetched relationship with the relRevs value
// observed at prefetch time (BACKLOG 18b). Used by ReplaceRelationship to
// detect a concurrent property-changing write in the prefetch→lock window:
// relDeleteInfoStillIndexedLocked alone is insufficient because endpoints/
// type are immutable, so a concurrent property-only ReplaceRelationship
// leaves relIDs/typeIdx/adjacency membership completely unchanged — only the
// rev (bumped on every rel data write) detects it.
type relPrefetchSnapshot struct {
	rel *types.Relationship
	rev uint64
}

// prefetchRelWithRev is prefetchRel plus a rev snapshot taken under the SAME
// brief RLock the rev is later compared against — see relPrefetchSnapshot.
func (bs *Store) prefetchRelWithRev(rid types.RelID) (relPrefetchSnapshot, error) {
	bs.idxMu.RLock()
	rev := bs.relRevs[rid]
	bs.idxMu.RUnlock()
	r, err := bs.prefetchRel(rid)
	if err != nil {
		return relPrefetchSnapshot{}, err
	}
	return relPrefetchSnapshot{rel: r, rev: rev}, nil
}

func (bs *Store) currentRelForPrefetchLocked(rid types.RelID, prefetched relPrefetchSnapshot) (*types.Relationship, error) {
	if prefetched.rel != nil && prefetched.rev != 0 && bs.relRevs[rid] == prefetched.rev {
		info := relDeleteInfoFromRelationship(prefetched.rel)
		if types.RelID(info.ID) == rid && bs.relDeleteInfoStillIndexedLocked(info) {
			return prefetched.rel, nil
		}
	}
	return bs.getRelLocked(rid)
}

// prefetchRelDeleteInfo prefetches a relationship's delete metadata plus the
// relRevs value observed at prefetch time (BACKLOG 18g, mirroring
// prefetchRelWithRev/BACKLOG 18b). The rev lets a locked TOCTOU re-check
// detect ANY write to this specific rel ID in the prefetch→lock window —
// including a delete-then-recreate-with-the-same-ID-but-different-endpoints
// race (lesson 22), which relDeleteInfoStillIndexedLocked's relIDs/typeIdx
// membership check alone cannot catch in AdjacencyIndexOnDisk mode (no RAM
// adjacency mirror to verify endpoints against).
func (bs *Store) prefetchRelDeleteInfo(rid types.RelID) (RelDeleteInfo, error) {
	bs.idxMu.RLock()
	rev := bs.relRevs[rid]
	bs.idxMu.RUnlock()
	r, err := bs.prefetchRel(rid)
	if err != nil {
		return RelDeleteInfo{}, err
	}
	info := relDeleteInfoFromRelationship(r)
	info.Rev = rev
	return info, nil
}

// relDeleteInfoRevCurrentLocked reports whether info's prefetch-time rev
// still matches the live rev for its rel ID — i.e. nothing has written to
// (or deleted+recreated) that specific rel ID since prefetch. Caller must
// hold bs.idxMu (read or write). info.Rev == 0 (unset by construction, e.g.
// relDeleteInfoFromRelationship) always reports stale, forcing callers that
// didn't capture a rev to fall back to a locked re-read.
func (bs *Store) relDeleteInfoRevCurrentLocked(info RelDeleteInfo) bool {
	return info.Rev != 0 && bs.relRevs[types.RelID(info.ID)] == info.Rev
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
	// Disk mode keeps no adjacency RAM mirror to verify against; relIDs +
	// typeIdx membership above remain the TOCTOU currency check.
	if !bs.adjOnDisk {
		if set, exists := bs.outIdx[startNID]; !exists {
			return false
		} else if _, ok := set[rid]; !ok {
			return false
		}
		if set, exists := bs.inIdx[endNID]; !exists {
			return false
		} else if tok, ok := set[rid]; !ok || tok.typ != info.RelType {
			return false
		}
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
	bs.deleteRelRevLocked(rid)
	delete(bs.relValidIdx, rid)                                    // drop the inline valid-time stamp
	bs.maintainRelPropertyIndexesPurge(info.ID)                    // brute-force (RelDeleteInfo has no property values)
	bs.maintainRelTypeTemporalIndexesPurge(info.ID)                // BACKLOG 21c
	bs.removeRelPropertyTypeClassCountsByID(info.ID, info.RelType) // decrement via memoized contribution (the single delete seam)
	bs.removeRelPropertyStatsCountsByID(info.ID, info.RelType)     // same memoized-delete seam for NDV+min/max

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.RelType]; exists {
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, info.RelType)
		}
	}

	// Adjacency cleanup (RAM mirror only — the delete ops below remove the
	// persisted keys; disk mode has no mirror).
	if !bs.adjOnDisk {
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
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
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
	r, miss, err := bs.prefetchRelNoFill(rid, true) // point read: promote
	if err != nil || !miss {
		return r, err
	}
	bs.relCache.LoadClean(rid.SnowflakeID(), r)
	return r, nil
}

// prefetchRelScan is prefetchRel WITHOUT the cache fill on miss — the
// relationship mirror of prefetchNodeScan; see that comment for the LRU
// sequential-scan pathology it prevents. Used by full-cardinality scans
// (by-type, AllRelationships, temporal range); adjacency reads
// (Outgoing/Incoming) keep the filling prefetchRel because traversals
// revisit them.
func (bs *Store) prefetchRelScan(rid types.RelID) (*types.Relationship, error) {
	r, _, err := bs.prefetchRelNoFill(rid, false) // scan: no promote
	return r, err
}

// prefetchRelNoFill is the shared core: cache lookup, then badger decode.
// miss=true means r was decoded from badger (a cache-fill candidate). The
// decoded relationship is frozen before return because callers may publish
// it into the cache, and cache entries must never be mutable.
func (bs *Store) prefetchRelNoFill(rid types.RelID, promote bool) (r *types.Relationship, miss bool, err error) {
	id := rid.SnowflakeID()
	// Point reads promote (Get); scan reads do not (GetNoPromote) — see
	// prefetchNodeNoFill for the concurrent-scan serialization this avoids.
	var v *types.Relationship
	var status indexpkg.CacheStatus
	if promote {
		v, status = bs.relCache.Get(id)
	} else {
		v, status = bs.relCache.GetNoPromote(id)
	}
	switch status {
	case indexpkg.CacheHit:
		return v, false, nil
	case indexpkg.CacheDeleted:
		return nil, false, ErrRelNotFound
	}

	bs.idxMu.RLock()
	_, exists := bs.relIDs[rid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, false, ErrRelNotFound
	}

	err = bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.RelKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
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
		return nil, false, err
	}
	r.Freeze() // shared between cache and caller
	return r, true, nil
}
