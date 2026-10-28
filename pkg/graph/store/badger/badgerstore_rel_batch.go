package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship batch writes.

func (bs *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}
	if err := bs.checkWritable(); err != nil {
		return err
	}
	// Per-type invalidation over the batch's distinct types, recording each as an
	// append so the next read can extend rather than rebuild.
	defer bs.bumpRelEpochAppendBatch(rels)

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
		frozen   *types.Relationship
	}
	serialized := make([]relData, len(rels))
	// The frozen cache copy and change-log payload are pure functions of the finalized
	// rel: built outside idxMu (see PutNodesBatch).
	var putPayloads [][]byte
	if bs.logEnabled.Load() {
		putPayloads = make([][]byte, len(rels))
	}
	for i, r := range rels {
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		data, err := bs.marshalRelBytes(r)
		if err != nil {
			return fmt.Errorf("graph: marshal relationship: %w", err)
		}
		if bs.logEnabled.Load() {
			p, err := storepkg.RelPutPayload(r, false)
			if err != nil {
				return fmt.Errorf("graph: encode change-log: %w", err)
			}
			putPayloads[i] = p
			if err := bs.checkChangePayload(p); err != nil {
				return err
			}
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
			frozen:   bs.frozenRelRow(r),
		}
	}
	endpointIDs := make([]types.NodeID, 0, len(serialized)*2)
	for _, rd := range serialized {
		endpointIDs = append(endpointIDs, rd.startNID, rd.endNID)
	}
	if err := bs.ensureNodeRowsLive(endpointIDs); err != nil {
		return err
	}

	bs.idxMu.Lock()

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, rd := range serialized {
		if !bs.nodeIDs.Has(int64(rd.startNID)) {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if !bs.nodeIDs.Has(int64(rd.endNID)) {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if bs.relIDs.Has(int64(rd.rid)) {
			bs.idxMu.Unlock()
			return ErrRelExists
		}
		if _, exists := seen[rd.rid]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate relationship ID %d in batch", rd.id)
		}
		seen[rd.rid] = struct{}{}
	}
	for _, r := range rels {
		if err := bs.checkRelIndexKeysLocked(r); err != nil { // KEYSIZE, before any mutation
			bs.idxMu.Unlock()
			return err
		}
	}
	for i, r := range rels {
		if err := bs.foldRecreatedRelHistoryLocked(r, serialized[i].id); err != nil {
			bs.idxMu.Unlock()
			return err // a fold already done only widened an envelope: sound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(rels)*4) // entity + type + out + in
	for i, r := range rels {
		rd := serialized[i]

		bs.relCache.Put(rd.id, rd.frozen)
		bs.relIDs.Add(int64(rd.rid))
		bs.bumpRelRevLocked(rd.rid)
		bs.bumpRelBeliefWatermarkLocked(rd.rid, relTxFrom(r)) // BACKLOG 10c, as PutRelationship

		bs.addTypeIdxLocked(rd.relType, rd.rid)

		if !bs.adjOnDisk {
			if bs.outIdx[rd.startNID] == nil {
				bs.outIdx[rd.startNID] = make(map[types.RelID]types.NodeID)
			}
			bs.outIdx[rd.startNID][rd.rid] = rd.endNID

			if bs.inIdx[rd.endNID] == nil {
				bs.inIdx[rd.endNID] = make(map[types.RelID]inEdge)
			}
			bs.inIdx[rd.endNID][rd.rid] = inEdge{start: rd.startNID, typ: rd.relType}
		}
		bs.setRelValidStampLocked(rd.rid, r) // inline valid-time stamp
		bs.recordRelTypeMemberLocked(r)      // transaction-time rel-type membership
		ops = append(ops, bs.maintainRelPropertyIndexesAdd(r, rd.id)...)
		bs.maintainRelTypeTemporalIndexesAdd(r, rd.id) // BACKLOG 21c
		bs.addRelPropertyTypeClassCounts(r)
		bs.addRelPropertyStatsCounts(r)

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelKey(rd.id), value: rd.data})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelTypeIndexKey(rd.relType, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.OutKey(rd.startID, rd.relType, rd.endID, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.InKey(rd.endID, rd.relType, rd.startID, rd.id)})
		bs.getOrCreateTypeCounter(rd.relType).Add(1)
	}

	bs.appendOps(ops...)
	bs.relCount.Add(int64(len(rels)))
	// One ChangeRelPut per relationship (create => WithHistory false).
	for i := range putPayloads {
		bs.logChangeRaw(storecontract.ChangeRelPut, putPayloads[i])
	}
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// DeleteRelationshipsBatch deletes relationships atomically in two phases: all
// IDs exist (pre-read metadata), then deleteRelByInfo (history preserved). Missing
// ID returns ErrRelNotFound with zero mutations; duplicates are coalesced; nil/empty returns nil.
func (bs *Store) DeleteRelationshipsBatch(typedIDs []types.RelID) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(typedIDs) == 0 {
		return nil
	}
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpRelEpoch() // expand path: adjacency view changed
	for _, id := range typedIDs {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueRelIDs(typedIDs)

	// Pre-build the delete records (hard deletes) so a marshal error aborts before any
	// op is enqueued; indexed by typedIDs order, which infos preserves.
	delPayloads := make([][]byte, len(typedIDs))
	if bs.logEnabled.Load() {
		for i, rid := range typedIDs {
			p, err := storepkg.MarshalChangeBody(storepkg.RelDeleteBody{ID: int64(rid.SnowflakeID())})
			if err != nil {
				return fmt.Errorf("graph: encode change-log: %w", err)
			}
			delPayloads[i] = p
		}
	}

	// Phase 1a: validate existence under a read lock. Relationship row prefetch
	// may hit Badger, so keep that I/O outside idxMu.Lock().
	bs.idxMu.RLock()
	for _, rid := range typedIDs {
		if !bs.relIDs.Has(int64(rid)) {
			bs.idxMu.RUnlock()
			return ErrRelNotFound
		}
	}
	bs.idxMu.RUnlock()

	// Phase 1b: load relationship rows before acquiring the write lock. The
	// locked phase re-reads from the cache-loaded current row after TOCTOU checks.
	prefetched := make(map[types.RelID]RelDeleteInfo, len(typedIDs))
	for _, rid := range typedIDs {
		info, err := bs.prefetchRelDeleteInfo(rid)
		if err != nil {
			return fmt.Errorf("graph: batch read relationship %d: %w", rid.SnowflakeID(), err)
		}
		prefetched[rid] = info
	}
	if bs.relDeletePrefetchTestHook != nil {
		bs.relDeletePrefetchTestHook()
	}

	bs.idxMu.Lock()

	// Phase 1c: revalidate after the prefetch window and capture current metadata.
	infos := make([]RelDeleteInfo, len(typedIDs))
	for i, rid := range typedIDs {
		if !bs.relIDs.Has(int64(rid)) {
			bs.idxMu.Unlock()
			return ErrRelNotFound
		}
		if info, ok := prefetched[rid]; ok && bs.relDeleteInfoRevCurrentLocked(info) && bs.relDeleteInfoStillIndexedLocked(info) {
			infos[i] = info
			continue
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
			row:     r,
		}
	}

	// Phase 2: apply; cannot fail. One ChangeRelDelete per rel under the same lock
	// (deleteRelByInfo emits no record).
	for i, info := range infos {
		bs.deleteRelByInfo(info, info.row)
		bs.logChangeRaw(storecontract.ChangeRelDelete, delPayloads[i])
	}

	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

func uniqueRelIDs(ids []types.RelID) []types.RelID {
	seen := make(map[types.RelID]struct{}, len(ids))
	out := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
