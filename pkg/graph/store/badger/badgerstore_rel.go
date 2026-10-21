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

// PutRelationshipScoped is PutRelationship with the change-log record routed to
// the store.ScopedTxChangeLog buffer named by token (0 = PutRelationship). See
// badgerstore_changelog_scoped.go.
func (bs *Store) PutRelationshipScoped(r *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.PutRelationship(r)
	}
	return bs.putRelationship(r, true, false, token)
}

// PutRelationshipCoLocated writes a relationship whose entity and both adjacency
// legs live on THIS shard, without validating endpoint existence here (a sharded
// owner validates them on their own shards, ADR-0007). Emits the same ChangeRelPut
// record as PutRelationship; the partial split-write doors are record-free.
func (bs *Store) PutRelationshipCoLocated(r *types.Relationship) error {
	return bs.putRelationship(r, false, false, 0)
}

// PutRelationshipForeignIncoming writes a cross-machine incoming half-edge stub
// (ADR-0010 Model A), co-located like PutRelationshipCoLocated but co-committing
// ChangeForeignIncoming so a replica routes apply by the END-node slot (the rel-ID
// slot is foreign). Called by the sharded store's RecordForeignIncoming. The stub's
// rel-ID is in a foreign slot: reachable only via this shard's adjacency fold, never
// a slot-routed GetRelationship.
func (bs *Store) PutRelationshipForeignIncoming(r *types.Relationship) error {
	return bs.putRelationship(r, false, true, 0)
}

func (bs *Store) putRelationship(r *types.Relationship, validateEndpoints, foreignIncoming bool, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	// Per-type invalidation: the type is known here, so other types' columns stay valid.
	defer bs.bumpRelEpochAppend(r)
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
	if err := bs.foldRecreatedRelHistoryLocked(r, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update in-memory state.
	bs.relCache.Put(id, bs.frozenRelRow(r))
	bs.relIDs[rid] = struct{}{}
	bs.bumpRelRevLocked(rid)
	bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(r)) // BACKLOG 10c

	// Type index.
	bs.addTypeIdxLocked(relType, rid)

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

// bumpRelRevLocked mirrors bumpNodeRevLocked (BACKLOG 18b): called by every door
// that changes a live rel's data, so currentRelForPrefetchLocked can detect a
// property-only write in the prefetch-lock window (endpoints/type are immutable,
// so relDeleteInfoStillIndexedLocked cannot).
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

// seedRelRevAtOpenLocked gives a rel loaded at open a non-zero rev, so its
// first prefetch after open can reuse the prefetched row. AdjacencyIndexOnDisk
// skips it (26 B/rel): rev 0 makes every prefetch door re-read under the lock
// (currentRelForPrefetchLocked, relDeleteInfoRevCurrentLocked), and the rel's
// first write seeds it.
func (bs *Store) seedRelRevAtOpenLocked(rid types.RelID) {
	if !bs.adjOnDisk {
		bs.bumpRelRevLocked(rid)
	}
}

// deleteRelRevLocked mirrors deleteNodeRevLocked; called from deleteRelByInfo.
func (bs *Store) deleteRelRevLocked(rid types.RelID) {
	delete(bs.relRevs, rid)
}

// GetRelationship retrieves a relationship by ID (LRU cache first).
// Returns ErrRelNotFound if it does not exist.
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
	// Read before the snapshot opens — see LoadCleanAt.
	epoch := bs.relCache.FlushEpoch()
	err := bs.view(func(txn *badgerv4.Txn) error {
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
	bs.relCache.LoadCleanAt(id, r, epoch)
	return r.DeepCopy(), nil
}

// getRelInTxn is GetRelationship's body over an already-open read transaction
// (RelsAsOf bulk scan, BACKLOG 18k); mirrors getNodeInTxn. Caller must hold
// bs.idxMu (at least RLock) for the whole scan.
func (bs *Store) getRelInTxn(snap *scanSnapshot, rid types.RelID) (*types.Relationship, error) {
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

	item, err := snap.txnAfterMiss().Get(storepkg.RelKey(id))
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
	bs.relCache.LoadCleanAt(id, r, snap.pinned)
	return r.DeepCopy(), nil
}

// ReplaceRelationship overwrites a relationship's data in place; no index changes
// (type and endpoints are immutable). Returns ErrRelNotFound if absent.
func (bs *Store) ReplaceRelationship(r *types.Relationship) error {
	return bs.replaceRelationshipRouted(r, 0)
}

// ReplaceRelationshipScoped is ReplaceRelationship with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = ReplaceRelationship).
func (bs *Store) ReplaceRelationshipScoped(r *types.Relationship, token uint64) error {
	if token == 0 {
		return bs.ReplaceRelationship(r)
	}
	return bs.replaceRelationshipRouted(r, token)
}

// ReplaceRelationshipForeignIncoming overwrites a Model-A incoming stub
// (PutRelationshipForeignIncoming) in place, co-committing ChangeForeignIncoming
// so a replica routes the apply by the END-node slot (backlog 73: the untimed
// valid-start conversion stamps a stub of an untimed first version). Returns
// ErrRelNotFound if the stub is absent.
func (bs *Store) ReplaceRelationshipForeignIncoming(r *types.Relationship) error {
	return bs.replaceRelationshipTagged(r, 0, storecontract.ChangeForeignIncoming)
}

func (bs *Store) replaceRelationshipRouted(r *types.Relationship, token uint64) error {
	return bs.replaceRelationshipTagged(r, token, storecontract.ChangeRelPut)
}

func (bs *Store) replaceRelationshipTagged(r *types.Relationship, token uint64, tag storecontract.ChangeTag) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	// Per-type invalidation: the type is known here, so other types' columns stay valid.
	defer bs.bumpRelEpochForType(uint16(r.TypeToken())) // UPDATE: poisons, never records an append
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	rid := r.InternalID()
	id := rid.SnowflakeID()

	// Snapshot the rev with the prefetched row (BACKLOG 18b): only relRevs detects a
	// concurrent property-only update, the structural check cannot.
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
	bs.maintainRelTypeTemporalIndexesRemove(old, id) // BACKLOG 21c
	bs.removeRelPropertyTypeClassCounts(old)         // decrement old
	bs.removeRelPropertyStatsCounts(old)
	bs.relCache.Put(id, bs.frozenRelRow(r))
	bs.bumpRelRevLocked(rid)
	bs.bumpRelBeliefWatermarkLocked(rid, relTxFrom(r)) // BACKLOG 10c
	bs.maintainRelPropertyIndexesAdd(r, id)
	bs.maintainRelTypeTemporalIndexesAdd(r, id) // BACKLOG 21c
	bs.addRelPropertyTypeClassCounts(r)         // increment new
	bs.addRelPropertyStatsCounts(r)
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data})
	// A version update rewrites the row in place; valid_to may move, so the inline
	// stamp must be refreshed or a temporal traversal reads a stale interval.
	bs.setRelValidStampLocked(rid, r)
	logErr := bs.logChangeRoutedRaw(tag, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *Store) DeleteRelationship(rid types.RelID) error {
	return bs.deleteRelationshipRouted(rid, 0, true)
}

var (
	_ storecontract.RelCreateUndoCapability       = (*Store)(nil)
	_ storecontract.ScopedRelCreateUndoCapability = (*Store)(nil)
)

// UndoRelationshipCreate implements store.RelCreateUndoCapability:
// DeleteRelationship without the ex-adjacency legs.
func (bs *Store) UndoRelationshipCreate(rid types.RelID) error {
	return bs.deleteRelationshipRouted(rid, 0, false)
}

// UndoRelationshipCreateScoped implements store.ScopedRelCreateUndoCapability.
func (bs *Store) UndoRelationshipCreateScoped(rid types.RelID, token uint64) error {
	return bs.deleteRelationshipRouted(rid, token, false)
}

// DeleteRelationshipScoped is DeleteRelationship with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = DeleteRelationship).
func (bs *Store) DeleteRelationshipScoped(rid types.RelID, token uint64) error {
	if token == 0 {
		return bs.DeleteRelationship(rid)
	}
	return bs.deleteRelationshipRouted(rid, token, true)
}

// deleteRelationshipRouted removes rid; exLeg records its legs in the
// ex-adjacency (a delete; not an undone create).
func (bs *Store) deleteRelationshipRouted(rid types.RelID, token uint64, exLeg bool) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	// Coarse on purpose: only a RelID is known here; over-invalidating is the safe direction.
	defer bs.bumpRelEpoch()
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	// Build the change-log delete record up front so a marshal error aborts before
	// any op is enqueued; deleteRelByInfo stays record-free (shared with node cascade).
	relDelPayload, err := bs.buildChangePayload(storepkg.RelDeleteBody{ID: int64(rid.SnowflakeID())})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}

	// Prefetch before idxMu.Lock so a cache-miss delete does not hold the index lock
	// across Badger I/O; the locked section re-reads to avoid stale type/endpoints
	// if the ID was reused in the TOCTOU window.
	if _, err := bs.prefetchRel(rid); err != nil {
		return err
	}

	bs.idxMu.Lock()
	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}
	r, err := bs.getRelLocked(rid)
	var logErr error
	if err == nil {
		bs.removeRelByInfo(relDeleteInfoFromRelationship(r), r, exLeg)
		logErr = bs.logChangeRoutedRaw(storecontract.ChangeRelDelete, relDelPayload, token)
	}
	bs.idxMu.Unlock()

	if err != nil {
		return err
	}
	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
}

// DeleteRelationshipForeignIncoming removes a Model-A incoming half-edge stub
// (ADR-0010 section 3.3) on the END node's shard: the counterpart of
// PutRelationshipForeignIncoming, co-committing ChangeForeignIncomingDelete with
// the END-node ID. Returns ErrRelNotFound when absent (callers tolerate it).
// Callers (cascade / replica apply) tolerate ErrRelNotFound for idempotency.
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
	// Capture the END-node ID from the stub before deletion (replica routes by it);
	// marshal errors surface after unlock.
	delPayload, perr := bs.buildChangePayload(storepkg.ForeignIncomingDeleteBody{
		RelID: int64(rid.SnowflakeID()),
		EndID: int64(r.EndNodeID().SnowflakeID()),
	})
	if perr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: encode change-log: %w", perr)
	}
	bs.deleteRelByInfo(relDeleteInfoFromRelationship(r), r)
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

	// row carries the pre-read row of a deferred delete to deleteRelByInfo for the
	// planner-statistic decrement (backlog 53). Unset by relDeleteInfoFromRelationship.
	row *types.Relationship
}

func relDeleteInfoFromRelationship(r *types.Relationship) RelDeleteInfo {
	return RelDeleteInfo{
		ID:      r.ID().SnowflakeID(),
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
}

// relPrefetchSnapshot pairs a prefetched relationship with the relRevs value seen
// at prefetch (BACKLOG 18b), so ReplaceRelationship detects a concurrent
// property-only write; the structural check cannot (endpoints/type are immutable).
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

// prefetchRelDeleteInfo prefetches delete metadata plus the relRevs value
// (BACKLOG 18g). The rev lets the locked re-check detect any write to this rel ID,
// including delete-then-recreate with different endpoints (lesson 22), which the
// membership check cannot catch in AdjacencyIndexOnDisk mode.
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
	info.row = r
	return info, nil
}

// relDeleteInfoRevCurrentLocked reports whether info's prefetch-time rev still
// matches the live rev. Caller holds bs.idxMu (R or W). info.Rev == 0 (no rev
// captured) always reports stale, forcing a locked re-read.
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

// deleteRelByInfo applies relationship deletion using pre-read metadata. old is
// the row being deleted; the planner statistics take its contribution back out
// (backlog 53). old is nil only for unreadable rows (PurgeRelationshipByInfo
// residue cleanup). Caller holds bs.idxMu write lock; performs no reads.
func (bs *Store) deleteRelByInfo(info RelDeleteInfo, old *types.Relationship) {
	bs.removeRelByInfo(info, old, true)
}

// removeRelByInfo is deleteRelByInfo; exLeg false records no ex-adjacency (an
// undone create).
func (bs *Store) removeRelByInfo(info RelDeleteInfo, old *types.Relationship, exLeg bool) {
	// info.ID/startID/endID are raw snowflake.ID (RelDeleteInfo is shared with
	// TieredStore code); convert at each map access.
	rid := types.RelID(info.ID)
	startNID := types.NodeID(info.StartID)
	endNID := types.NodeID(info.EndID)

	// Update in-memory state.
	bs.relCache.MarkDeleted(info.ID)
	delete(bs.relIDs, rid)
	bs.ords.dropRel(rid)
	bs.deleteRelRevLocked(rid)
	delete(bs.relValidIdx, rid)                 // drop the inline valid-time stamp
	bs.maintainRelPropertyIndexesPurge(info.ID) // brute-force (RelDeleteInfo has no property values)
	// The rel-type temporal envelope is append-only (as memory's delete): history
	// stays, and a rolled-back delete does not re-add versions, so purging would drop
	// past versions from valid-time queries.
	bs.removeRelPropertyTypeClassCounts(old) // the deleted row's own contribution (the single delete seam)
	bs.removeRelPropertyStatsCounts(old)

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

	// Build delete ops. Both legs leave the live adjacency for the
	// ex-adjacency in the same batch (backlog 50), unless a create is undone.
	ops := []writeOp{
		{opType: writeOpDelete, key: storepkg.RelKey(info.ID)},
		{opType: writeOpDelete, key: storepkg.RelTypeIndexKey(info.RelType, info.ID)},
		{opType: writeOpDelete, key: storepkg.OutKey(info.StartID, info.RelType, info.EndID, info.ID)},
		{opType: writeOpDelete, key: storepkg.InKey(info.EndID, info.RelType, info.StartID, info.ID)},
	}
	if exLeg {
		ops = append(ops, exAdjLegOps(writeOpSet, info.ID, info.StartID, info.EndID)...)
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.RelType).Add(-1)
	bs.moveHook() // the relationship left its current slot (backlog 32 tests)
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
	// Read before the snapshot opens — see LoadCleanAt.
	epoch := bs.relCache.FlushEpoch()
	err := bs.view(func(txn *badgerv4.Txn) error {
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
	bs.relCache.LoadCleanAt(id, r, epoch)
	return r, nil
}

func (bs *Store) prefetchRel(rid types.RelID) (*types.Relationship, error) {
	// Read before the badger read opens — see LoadCleanAt.
	epoch := bs.relCache.FlushEpoch()
	r, miss, err := bs.prefetchRelNoFill(rid, true) // point read: promote
	if err != nil || !miss {
		return r, err
	}
	bs.relCache.LoadCleanAt(rid.SnowflakeID(), r, epoch)
	return r, nil
}

// prefetchRelScan is prefetchRel without the cache fill on miss (see
// prefetchNodeScan for the LRU scan pathology). Used by full-cardinality scans;
// adjacency reads keep the filling prefetchRel.
func (bs *Store) prefetchRelScan(rid types.RelID) (*types.Relationship, error) {
	r, _, err := bs.prefetchRelNoFill(rid, false) // scan: no promote
	return r, err
}

// prefetchRelNoFill is the shared core. miss=true means r was decoded from badger.
// The result is frozen because callers may publish it into the cache.
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

	err = bs.view(func(txn *badgerv4.Txn) error {
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
