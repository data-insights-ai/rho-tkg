package badger

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Node batch writes + cascade-delete (R5-F9 split out from badgerstore_node.go).

func (bs *Store) DeleteNodeCascade(nid types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	prefetched, err := bs.prefetchCascadeDeleteRows(nid)
	if err != nil {
		return err
	}
	_, corruptErr, err := bs.cascadeDeleteLocked(nid, prefetched)
	if err != nil {
		return err
	}
	if corruptErr == nil && bs.syncWrites {
		return bs.flush()
	}
	return corruptErr
}

type cascadeDeletePrefetch struct {
	node nodeDeleteInfo
	rels map[types.RelID]RelDeleteInfo
}

func (bs *Store) prefetchCascadeDeleteRows(nid types.NodeID) (cascadeDeletePrefetch, error) {
	bs.idxMu.RLock()
	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.RUnlock()
		return cascadeDeletePrefetch{}, ErrNodeNotFound
	}
	relIDs := make([]types.RelID, 0, len(bs.outIdx[nid])+len(bs.inIdx[nid]))
	seen := make(map[types.RelID]struct{}, len(bs.outIdx[nid])+len(bs.inIdx[nid]))
	for relID := range bs.outIdx[nid] {
		seen[relID] = struct{}{}
		relIDs = append(relIDs, relID)
	}
	for relID := range bs.inIdx[nid] {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		relIDs = append(relIDs, relID)
	}
	bs.idxMu.RUnlock()

	// A corrupt or missing node row still uses cascadeDeleteInner's cleanup
	// path, which scrubs indexes and returns the corruption error.
	nodeInfo, _ := bs.prefetchNodeDeleteInfo(nid)
	infos := make(map[types.RelID]RelDeleteInfo, len(relIDs))
	for _, relID := range relIDs {
		info, err := bs.prefetchRelDeleteInfo(relID)
		if errors.Is(err, ErrRelNotFound) {
			continue
		}
		if err != nil {
			return cascadeDeletePrefetch{}, fmt.Errorf("graph: cascade read relationship: %w", err)
		}
		infos[relID] = info
	}
	return cascadeDeletePrefetch{node: nodeInfo, rels: infos}, nil
}

// cascadeDeleteInner performs Phases 1+2 of DeleteNodeCascade.
// Caller MUST hold bs.idxMu.Lock(). All ops are appended to pending under the same lock
// so that the caller can append additional ops (e.g. tombstone history) before releasing.
// Returns (toDelete, corruptErr, fatalErr):
//   - fatalErr != nil: aborted with no mutations applied.
//   - corruptErr != nil: cleanup completed but node data was unreadable (indexes brute-force purged).
//   - Otherwise: clean success.
func (bs *Store) cascadeDeleteInner(nid types.NodeID, prefetched cascadeDeletePrefetch) ([]RelDeleteInfo, error, error) {
	id := nid.SnowflakeID()
	if _, exists := bs.nodeIDs[nid]; !exists {
		return nil, nil, ErrNodeNotFound
	}

	// Collect all connected relIDs (dedup self-loops).
	relIDs := make(map[types.RelID]struct{})
	for relID := range bs.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range bs.inIdx[nid] {
		relIDs[relID] = struct{}{}
	}

	// Phase 1 — Preflight: read all relationship metadata before any mutations.
	// If any read fails (corruption), we abort without partial state changes.
	toDelete := make([]RelDeleteInfo, 0, len(relIDs))
	orphanRelIDs := make([]types.RelID, 0)
	for relID := range relIDs {
		if info, ok := prefetched.rels[relID]; ok && bs.relDeleteInfoStillIndexedLocked(info) {
			toDelete = append(toDelete, info)
			continue
		}
		r, err := bs.getRelLocked(relID)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				orphanRelIDs = append(orphanRelIDs, relID)
				continue
			}
			return nil, nil, fmt.Errorf("graph: cascade read relationship: %w", err)
		}
		toDelete = append(toDelete, RelDeleteInfo{
			ID:      relID.SnowflakeID(),
			RelType: r.TypeToken().Value(),
			StartID: r.StartNodeID().SnowflakeID(),
			EndID:   r.EndNodeID().SnowflakeID(),
		})
	}

	// Phase 2 — Apply: all mutations use pre-read data, no reads, cannot fail.
	for _, relID := range orphanRelIDs {
		if err := bs.purgeOrphanRelIDLocked(relID); err != nil {
			return nil, nil, fmt.Errorf("graph: cascade purge orphan relationship %d: %w", relID.SnowflakeID(), err)
		}
	}
	for _, info := range toDelete {
		bs.deleteRelByInfo(info)
	}

	// Get node data for label cleanup.
	var n *types.Node
	var err error
	if bs.nodeDeleteInfoStillCurrentLocked(nid, prefetched.node) {
		n = prefetched.node.node
	} else {
		n, err = bs.getNodeLocked(nid)
	}
	if err != nil {
		// Node was in nodeIDs but can't be loaded (data corruption or cache miss
		// with closed DB). Still proceed with cleanup — scrub labelIdx by scanning
		// ALL label sets to prevent orphaned index entries (perma-leak).
		// O(L) where L is total distinct labels — bounded, corruption-only path.
		ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}
		for tok, set := range bs.labelIdx {
			if _, exists := set[nid]; exists {
				delete(set, nid)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
				ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
				bs.getOrCreateLabelCounter(tok).Add(-1)
			}
		}
		// Property, temporal, and vector indexes: node data unavailable, brute-force purge.
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllHighFrequencyIndexes(bs.hfIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)

		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		delete(bs.nodeHashes, nid)
		bs.deleteNodeRevLocked(nid)
		bs.appendOps(ops...)
		bs.nodeCount.Add(-1)
		return toDelete, fmt.Errorf("graph: cascade completed with corrupt node data: %w", err), nil
	}

	// Build delete ops for node.
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, nid)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, n, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, nid)
	delete(bs.nodeHashes, nid)
	bs.deleteNodeRevLocked(nid)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)

	return toDelete, nil, nil
}

// cascadeDeleteLocked acquires idxMu.Lock() and delegates to cascadeDeleteInner.
// Used by DeleteNodeCascade — same contract as before the refactor.
func (bs *Store) cascadeDeleteLocked(nid types.NodeID, prefetched cascadeDeletePrefetch) ([]RelDeleteInfo, error, error) {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	return bs.cascadeDeleteInner(nid, prefetched)
}

// PurgeOrphanRelationshipIndexes removes type and adjacency index entries for
// rid only when the relationship row is already absent.
func (bs *Store) PurgeOrphanRelationshipIndexes(rid types.RelID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}

	bs.idxMu.Lock()
	if _, err := bs.getRelLocked(rid); err == nil {
		bs.idxMu.Unlock()
		return nil
	} else if !errors.Is(err, ErrRelNotFound) {
		bs.idxMu.Unlock()
		return err
	}
	err := bs.purgeOrphanRelIDLocked(rid)
	bs.idxMu.Unlock()
	if err != nil {
		return err
	}
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) purgeOrphanRelIDLocked(rid types.RelID) error {
	rawID := rid.SnowflakeID()
	typeTokens := make([]uint16, 0)
	outNodes := make([]types.NodeID, 0)
	inNodes := make([]types.NodeID, 0)

	for tok, set := range bs.typeIdx {
		if _, ok := set[rid]; ok {
			typeTokens = append(typeTokens, tok)
		}
	}
	for nid, set := range bs.outIdx {
		if _, ok := set[rid]; ok {
			outNodes = append(outNodes, nid)
		}
	}
	for nid, set := range bs.inIdx {
		if _, ok := set[rid]; ok {
			inNodes = append(inNodes, nid)
		}
	}

	indexKeys, err := bs.relationshipIndexKeysForRel(rawID)
	if err != nil {
		return err
	}
	ops := make([]writeOp, 0, len(indexKeys))
	for _, key := range indexKeys {
		ops = append(ops, writeOp{opType: writeOpDelete, key: key})
	}

	bs.wbMu.Lock()
	for k := range bs.pending {
		key := []byte(k)
		if relationshipIndexKeyMatchesRelID(key, rawID) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)
			bs.pending[k] = writeOp{opType: writeOpDelete, key: keyCopy}
			continue
		}
	}
	bs.wbMu.Unlock()

	for _, tok := range typeTokens {
		set := bs.typeIdx[tok]
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.typeIdx, tok)
		}
		bs.getOrCreateTypeCounter(tok).Store(int64(len(set)))
	}
	for _, nid := range outNodes {
		set := bs.outIdx[nid]
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.outIdx, nid)
		}
	}
	for _, nid := range inNodes {
		set := bs.inIdx[nid]
		delete(set, rid)
		if len(set) == 0 {
			delete(bs.inIdx, nid)
		}
	}
	if _, tracked := bs.relIDs[rid]; tracked {
		delete(bs.relIDs, rid)
		bs.relCount.Add(-1)
	}
	bs.relCache.MarkDeleted(rawID)
	if len(ops) > 0 {
		bs.appendOps(ops...)
	}
	return nil
}

func (bs *Store) relationshipIndexKeysForRel(relID snowflake.ID) ([][]byte, error) {
	keys := make([][]byte, 0)
	seen := make(map[string]struct{})

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false

		for _, prefix := range []byte{storepkg.KeyRelType, storepkg.KeyOut, storepkg.KeyIn} {
			prefixBytes := []byte{prefix}
			it := txn.NewIterator(opts)
			for it.Seek(prefixBytes); it.ValidForPrefix(prefixBytes); it.Next() {
				key := it.Item().Key()
				if !relationshipIndexKeyMatchesRelID(key, relID) {
					continue
				}
				keyCopy := make([]byte, len(key))
				copy(keyCopy, key)
				if _, ok := seen[string(keyCopy)]; ok {
					continue
				}
				seen[string(keyCopy)] = struct{}{}
				keys = append(keys, keyCopy)
			}
			it.Close()
		}
		return nil
	})
	return keys, err
}

func relationshipIndexKeyMatchesRelID(key []byte, relID snowflake.ID) bool {
	switch {
	case len(key) == storepkg.SizeRelTypeIdx && key[0] == storepkg.KeyRelType:
		return storepkg.ParseIDFromKey(key, 3) == relID
	case len(key) == storepkg.SizeAdjacency && (key[0] == storepkg.KeyOut || key[0] == storepkg.KeyIn):
		return storepkg.ParseRelIDFromAdjKey(key) == relID
	default:
		return false
	}
}

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (bs *Store) PutNodesBatch(nodes []*types.Node) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	if err := bs.checkWritable(); err != nil {
		return err
	}

	// Pre-serialize all nodes outside the lock.
	type nodeData struct {
		nid  types.NodeID
		id   snowflake.ID
		data []byte
	}
	serialized := make([]nodeData, len(nodes))
	for i, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		data, err := storepkg.MarshalNodeWire(n)
		if err != nil {
			return fmt.Errorf("graph: marshal node: %w", err)
		}
		nid := n.InternalID()
		serialized[i] = nodeData{nid: nid, id: nid.SnowflakeID(), data: data}
	}

	bs.idxMu.Lock()

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, nd := range serialized {
		if _, exists := bs.nodeIDs[nd.nid]; exists {
			bs.idxMu.Unlock()
			return ErrNodeExists
		}
		if _, exists := seen[nd.nid]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate node ID %d in batch", nd.id)
		}
		seen[nd.nid] = struct{}{}
	}
	vectorUpdates := make([][]indexpkg.NodeVectorIndexUpdate, len(nodes))
	for i, n := range nodes {
		updates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, n, serialized[i].id)
		if err != nil {
			bs.idxMu.Unlock()
			return err
		}
		vectorUpdates[i] = updates
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(nodes)*3) // entity + avg ~2 label indexes
	for i, n := range nodes {
		nd := serialized[i]

		bs.nodeCache.Put(nd.id, n.DeepCopy())
		bs.nodeIDs[nd.nid] = struct{}{}
		bs.nodeHashes[nd.nid] = badgerNodeIntegrityHash(n)
		bs.bumpNodeRevLocked(nd.nid)

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(nd.id), value: nd.data})
		labelCount := n.LabelTokenCount()
		for j := 0; j < labelCount; j++ {
			tok := n.LabelTokenRawAt(j)
			if bs.labelIdx[tok] == nil {
				bs.labelIdx[tok] = make(map[types.NodeID]struct{})
			}
			bs.labelIdx[tok][nd.nid] = struct{}{}
			ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, nd.id)})
			bs.getOrCreateLabelCounter(tok).Add(1)
		}
		indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, nd.id)
		indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, nd.id)
		indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, n, nd.id)
		if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates[i], nd.id); err != nil {
			bs.idxMu.Unlock()
			return err
		}
	}

	bs.appendOps(ops...)
	bs.nodeCount.Add(int64(len(nodes)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read node data for label cleanup.
// Phase 2: remove from cache, indexes, queue delete ops.
// Missing ID → ErrNodeNotFound, zero mutations. Duplicate IDs are coalesced.
// Nil/empty input → nil error.
func (bs *Store) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(typedIDs) == 0 {
		return nil
	}
	if err := bs.checkWritable(); err != nil {
		return err
	}
	for _, id := range typedIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueNodeIDs(typedIDs)

	// Phase 1a: validate cheap in-memory state under a read lock. Node row
	// prefetch below may hit Badger, so keep that I/O outside idxMu.Lock().
	bs.idxMu.RLock()
	for _, nid := range typedIDs {
		if _, exists := bs.nodeIDs[nid]; !exists {
			bs.idxMu.RUnlock()
			return ErrNodeNotFound
		}
		if len(bs.outIdx[nid]) != 0 || len(bs.inIdx[nid]) != 0 {
			bs.idxMu.RUnlock()
			return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
		}
	}
	bs.idxMu.RUnlock()

	// Phase 1b: load entity rows before acquiring the write lock. The locked
	// phase re-reads from the cache-loaded current row after TOCTOU checks.
	prefetchedNodes := make(map[types.NodeID]nodeDeleteInfo, len(typedIDs))
	for _, nid := range typedIDs {
		info, err := bs.prefetchNodeDeleteInfo(nid)
		if err != nil {
			return fmt.Errorf("graph: batch read node %d: %w", nid.SnowflakeID(), err)
		}
		prefetchedNodes[nid] = info
	}

	bs.idxMu.Lock()

	// Phase 1c: revalidate after the prefetch window and capture current rows.
	nodeData := make([]*types.Node, len(typedIDs))
	for i, nid := range typedIDs {
		if _, exists := bs.nodeIDs[nid]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if len(bs.outIdx[nid]) != 0 || len(bs.inIdx[nid]) != 0 {
			bs.idxMu.Unlock()
			return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
		}
		if info, ok := prefetchedNodes[nid]; ok && bs.nodeDeleteInfoStillCurrentLocked(nid, info) {
			nodeData[i] = info.node
			continue
		}
		n, err := bs.getNodeLocked(nid)
		if err != nil {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: batch read node %d: %w", nid.SnowflakeID(), err)
		}
		nodeData[i] = n
	}

	// Phase 2: apply — all validated, safe to mutate.
	for i, nid := range typedIDs {
		n := nodeData[i]
		id := nid.SnowflakeID()

		ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

		allTokens := collectNodeLabelTokens(n)
		for _, tok := range allTokens {
			ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
			if set, exists := bs.labelIdx[tok]; exists {
				delete(set, nid)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
			}
			bs.getOrCreateLabelCounter(tok).Add(-1)
		}

		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
		indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, n, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)
		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		delete(bs.nodeHashes, nid)
		bs.deleteNodeRevLocked(nid)
		bs.appendOps(ops...)
	}

	bs.nodeCount.Add(-int64(len(typedIDs)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func uniqueNodeIDs(ids []types.NodeID) []types.NodeID {
	seen := make(map[types.NodeID]struct{}, len(ids))
	out := make([]types.NodeID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// loadNodeFromBadger reads and unmarshals a node within an existing Badger transaction.
// Does not interact with the LRU cache. Used during loadIndexes where the cache is
// not yet populated and concurrent access has not started.
