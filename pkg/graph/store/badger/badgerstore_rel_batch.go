package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship batch writes (R5-F9 split out from badgerstore_rel.go).

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
	defer bs.bumpRelEpoch() // X5 expand path: adjacency view changed

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
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		data, err := bs.marshalRelBytes(r)
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

		bs.relCache.Put(rd.id, freezeRelCopy(r))
		bs.relIDs[rd.rid] = struct{}{}

		if bs.typeIdx[rd.relType] == nil {
			bs.typeIdx[rd.relType] = make(map[types.RelID]struct{})
		}
		bs.typeIdx[rd.relType][rd.rid] = struct{}{}

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
		bs.setRelValidStampLocked(rd.rid, r) // OPT15: inline valid-time stamp

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelKey(rd.id), value: rd.data})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.RelTypeIndexKey(rd.relType, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.OutKey(rd.startID, rd.relType, rd.endID, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.InKey(rd.endID, rd.relType, rd.startID, rd.id)})
		bs.getOrCreateTypeCounter(rd.relType).Add(1)
	}

	bs.appendOps(ops...)
	bs.relCount.Add(int64(len(rels)))
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read relationship metadata.
// Phase 2: delete via deleteRelByInfo (mutation-only), preserving history.
// Missing ID → ErrRelNotFound, zero mutations. Duplicate IDs are coalesced.
// Nil/empty input → nil error.
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
	defer bs.bumpRelEpoch() // X5 expand path: adjacency view changed
	for _, id := range typedIDs {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueRelIDs(typedIDs)

	// Phase 1a: validate existence under a read lock. Relationship row prefetch
	// may hit Badger, so keep that I/O outside idxMu.Lock().
	bs.idxMu.RLock()
	for _, rid := range typedIDs {
		if _, exists := bs.relIDs[rid]; !exists {
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

	bs.idxMu.Lock()

	// Phase 1c: revalidate after the prefetch window and capture current metadata.
	infos := make([]RelDeleteInfo, len(typedIDs))
	for i, rid := range typedIDs {
		if _, exists := bs.relIDs[rid]; !exists {
			bs.idxMu.Unlock()
			return ErrRelNotFound
		}
		if info, ok := prefetched[rid]; ok && bs.relDeleteInfoStillIndexedLocked(info) {
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
		}
	}

	// Phase 2: apply — all validated, mutations cannot fail.
	for _, info := range infos {
		bs.deleteRelByInfo(info)
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

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy. O(N) in relIDs map size.
