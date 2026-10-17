package badger

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Node batch writes + cascade-delete.

func (bs *Store) DeleteNodeCascade(nid types.NodeID) error {
	return bs.deleteNodeCascadeRouted(nid, 0)
}

// DeleteNodeCascadeScoped is DeleteNodeCascade with the change-log record routed
// to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) DeleteNodeCascadeScoped(nid types.NodeID, token uint64) error {
	if token == 0 {
		return bs.DeleteNodeCascade(nid)
	}
	return bs.deleteNodeCascadeRouted(nid, token)
}

func (bs *Store) deleteNodeCascadeRouted(nid types.NodeID, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpNodeEpoch()
	defer bs.bumpRelEpoch() // the cascade deletes the node's relationships
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	prefetched, err := bs.prefetchCascadeDeleteRows(nid)
	if err != nil {
		return err
	}
	_, corruptErr, err := bs.cascadeDeleteRouted(nid, prefetched, token)
	if err != nil {
		return err
	}
	if corruptErr != nil {
		return corruptErr
	}
	return bs.flushIfNeeded()
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
	out, outErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, false)
	in, inErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, true)
	bs.idxMu.RUnlock()
	if outErr != nil {
		return cascadeDeletePrefetch{}, fmt.Errorf("graph: cascade adjacency snapshot: %w", outErr)
	}
	if inErr != nil {
		return cascadeDeletePrefetch{}, fmt.Errorf("graph: cascade adjacency snapshot: %w", inErr)
	}
	relIDs := make([]types.RelID, 0, len(out)+len(in))
	seen := make(map[types.RelID]struct{}, len(out)+len(in))
	for _, relID := range append(out, in...) {
		if _, ok := seen[relID]; ok {
			continue // dedup self-loops
		}
		seen[relID] = struct{}{}
		relIDs = append(relIDs, relID)
	}

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
	if bs.relDeletePrefetchTestHook != nil {
		bs.relDeletePrefetchTestHook()
	}
	return cascadeDeletePrefetch{node: nodeInfo, rels: infos}, nil
}

// cascadeDeleteInner performs Phases 1+2 of DeleteNodeCascade. Caller MUST hold
// bs.idxMu.Lock; all ops are appended under it. history is the with-history
// delete's tombstone batch (nil for a plain cascade), published via
// publishMoveLocked BEFORE Phase 2 removes the entities from the cache, so a
// lock-free reader never sees an entity in neither slot (backlog 32), raising
// the store format marker to marker (0: none) in the same batch.
// Returns (toDelete, corruptErr, fatalErr):
//   - fatalErr != nil: aborted with no mutations applied.
//   - corruptErr != nil: cleanup completed but node data was unreadable (indexes brute-force purged).
//   - otherwise: clean success.
func (bs *Store) cascadeDeleteInner(nid types.NodeID, prefetched cascadeDeletePrefetch, history []writeOp, marker uint16) ([]RelDeleteInfo, error, error) {
	if _, exists := bs.nodeIDs[nid]; !exists {
		return nil, nil, ErrNodeNotFound
	}

	// Collect all connected relIDs (dedup self-loops). Caller holds
	// idxMu.Lock, satisfying the snapshot helper's lock contract.
	relIDs := make(map[types.RelID]struct{})
	out, outErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, false)
	if outErr != nil {
		return nil, nil, fmt.Errorf("graph: cascade adjacency snapshot: %w", outErr)
	}
	in, inErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, true)
	if inErr != nil {
		return nil, nil, fmt.Errorf("graph: cascade adjacency snapshot: %w", inErr)
	}
	for _, relID := range append(out, in...) {
		relIDs[relID] = struct{}{}
	}

	// Phase 1 — Preflight: read all relationship metadata before any mutations.
	// If any read fails (corruption), we abort without partial state changes.
	toDelete := make([]RelDeleteInfo, 0, len(relIDs))
	orphanRelIDs := make([]types.RelID, 0)
	for relID := range relIDs {
		if info, ok := prefetched.rels[relID]; ok && bs.relDeleteInfoRevCurrentLocked(info) && bs.relDeleteInfoStillIndexedLocked(info) {
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
			row:     r,
		})
	}

	// The orphans' index keys are read here too, so Phase 2 starts only once
	// every fallible read has succeeded.
	orphanKeys := make([][][]byte, len(orphanRelIDs))
	for i, relID := range orphanRelIDs {
		keys, err := bs.relationshipIndexKeysForRel(relID.SnowflakeID())
		if err != nil {
			return nil, nil, fmt.Errorf("graph: cascade purge orphan relationship %d: %w", relID.SnowflakeID(), err)
		}
		orphanKeys[i] = keys
	}

	var corruptErr error
	apply := func() {
		corruptErr = bs.cascadeDeleteApply(nid, prefetched, toDelete, orphanRelIDs, orphanKeys)
		bs.moveHook() // the node left its current slot (backlog 32 tests)
	}
	if history == nil {
		apply()
	} else {
		bs.publishMoveLocked(marker, history, apply)
	}
	return toDelete, corruptErr, nil
}

// cascadeDeleteApply is Phase 2: every mutation from pre-read data under the
// caller's idxMu.Lock; cannot fail. Non-nil return = corrupt-node outcome.
func (bs *Store) cascadeDeleteApply(nid types.NodeID, prefetched cascadeDeletePrefetch, toDelete []RelDeleteInfo, orphanRelIDs []types.RelID, orphanKeys [][][]byte) error {
	id := nid.SnowflakeID()
	for i, relID := range orphanRelIDs {
		bs.purgeOrphanRelIDLockedWithIndexKeys(relID, orphanKeys[i])
	}
	for _, info := range toDelete {
		bs.deleteRelByInfo(info, info.row)
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
		// Node in nodeIDs but unloadable (corruption): still clean up by scanning ALL
		// label sets to avoid orphaned index entries. O(L), corruption-only path.
		ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}
		if bs.labelOnDisk {
			// Disk mode: derive the node's tokens from the persisted
			// keyspace (O(keyspace) — corruption-only path).
			toks, scanErr := bs.nodeLabelTokensFromKeyspaceLocked(nid)
			if scanErr != nil {
				return fmt.Errorf("graph: cascade scrub scan: %w (after: %w)", scanErr, err)
			}
			for _, tok := range toks {
				ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
				bs.getOrCreateLabelCounter(tok).Add(-1)
			}
		} else {
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
		}
		// Node data unavailable: brute-force purge. Property-key counters stay
		// conservatively positive (overcounts cost scans, undercounts could prune matches).
		ops = append(ops, bs.maintainPropertyIndexesPurge(id)...)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		ops = append(ops, bs.maintainTemporalIndexDiskPurge(id)...)
		indexpkg.PurgeNodeFromAllHighFrequencyIndexes(bs.hfIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)

		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		bs.ords.dropNode(nid)
		bs.deleteNodeRevLocked(nid)
		bs.appendOps(ops...)
		bs.nodeCount.Add(-1)
		// The label scrub touched an unknown set of labels, so bump the salt that
		// invalidates every per-label DocValues column (as purgeNodesByLabel does,
		// BACKLOG 4b/18h); otherwise a columnar query could still see this deleted row.
		bs.nodeEpochSalt.Add(1)
		bs.poisonAllLabels() // label-less event: no per-label append record can describe it (R3)
		return fmt.Errorf("graph: cascade completed with corrupt node data: %w", err)
	}

	// Build delete ops for node.
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
		if !bs.labelOnDisk {
			if set, exists := bs.labelIdx[tok]; exists {
				delete(set, nid)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	bs.removeNodePropertyKeyCounts(n)
	ops = append(ops, bs.maintainPropertyIndexesRemove(n, id)...)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(n, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, n, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, nid)
	bs.ords.dropNode(nid)
	bs.deleteNodeRevLocked(nid)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)

	return nil
}

// cascadeDeleteLocked takes idxMu.Lock and delegates to cascadeDeleteInner; the
// unscoped (token == 0) wrapper over cascadeDeleteRouted.
func (bs *Store) cascadeDeleteLocked(nid types.NodeID, prefetched cascadeDeletePrefetch) ([]RelDeleteInfo, error, error) {
	return bs.cascadeDeleteRouted(nid, prefetched, 0)
}

// cascadeDeleteRouted is cascadeDeleteLocked's token-aware sibling (BACKLOG
// 11f Batch F) — see logChangeRoutedRaw's routing rule.
func (bs *Store) cascadeDeleteRouted(nid types.NodeID, prefetched cascadeDeletePrefetch, token uint64) ([]RelDeleteInfo, error, error) {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	deleted, corruptErr, fatalErr := bs.cascadeDeleteInner(nid, prefetched, nil, 0)
	// Emit the single ChangeNodeDelete under the same lock as the cascade ops
	// (cascadeDeleteInner / deleteRelByInfo emit nothing). Skipped on fatalErr.
	if fatalErr == nil {
		if logErr := bs.logCascadeNodeDeleteRouted(nid.SnowflakeID(), deleted, token); logErr != nil {
			return deleted, corruptErr, logErr
		}
	}
	return deleted, corruptErr, fatalErr
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
	return bs.flushIfNeeded()
}

func (bs *Store) purgeOrphanRelIDLocked(rid types.RelID) error {
	indexKeys, err := bs.relationshipIndexKeysForRel(rid.SnowflakeID())
	if err != nil {
		return err
	}
	bs.purgeOrphanRelIDLockedWithIndexKeys(rid, indexKeys)
	return nil
}

// purgeOrphanRelIDLockedWithIndexKeys is the non-fallible apply half of
// purgeOrphanRelIDLocked: exact erasure precomputes indexKeys so a later Badger
// scan cannot split the destructive transition.
func (bs *Store) purgeOrphanRelIDLockedWithIndexKeys(rid types.RelID, indexKeys [][]byte) {
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
	// An index key parked in `flushing` is mid-commit (not in `pending`, not yet in
	// Badger): queue an explicit delete so the flush removes it once it lands, else it
	// orphans. Second guard for callers passing a precomputed key list.
	for k, op := range bs.flushing {
		if op.opType != writeOpSet || !relationshipIndexKeyMatchesRelID([]byte(k), rawID) {
			continue
		}
		if _, has := bs.pending[k]; !has {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			bs.pending[k] = writeOp{opType: writeOpDelete, key: keyCopy}
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
		bs.ords.dropRel(rid)
		bs.relCount.Add(-1)
	}
	delete(bs.relValidIdx, rid) // drop the inline valid-time stamp on node-cascade rel purge
	bs.relCache.MarkDeleted(rawID)
	if len(ops) > 0 {
		bs.appendOps(ops...)
	}
}

// relationshipIndexKeysForRel returns every type/adjacency index key of relID,
// buffered (flushing ++ pending) or committed. Buffer first, View second (lesson
// 64): otherwise a key committed between the two reads gets no delete and orphans.
func (bs *Store) relationshipIndexKeysForRel(relID snowflake.ID) ([][]byte, error) {
	if h := bs.relIndexKeysTestErr; h != nil {
		if err := h(); err != nil {
			return nil, err
		}
	}
	keys := make([][]byte, 0)
	seen := make(map[string]struct{})

	bs.rangePending(func(k string, op writeOp) {
		if op.opType != writeOpSet || !relationshipIndexKeyMatchesRelID([]byte(k), relID) {
			return
		}
		if _, ok := seen[k]; ok {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, []byte(k))
	})

	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false

		for _, prefix := range []byte{storepkg.KeyRelType, storepkg.KeyOut, storepkg.KeyIn, storepkg.KeyExAdj} {
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
	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}
	return keys, err
}

func relationshipIndexKeyMatchesRelID(key []byte, relID snowflake.ID) bool {
	switch {
	case len(key) == storepkg.SizeRelTypeIdx && key[0] == storepkg.KeyRelType:
		return storepkg.ParseIDFromKey(key, 3) == relID
	case len(key) == storepkg.SizeAdjacency && (key[0] == storepkg.KeyOut || key[0] == storepkg.KeyIn):
		return storepkg.ParseRelIDFromAdjKey(key) == relID
	case len(key) == storepkg.SizeExAdj && key[0] == storepkg.KeyExAdj:
		return storepkg.ParseIDFromKey(key, 10) == relID
	default:
		return false
	}
}

// PutNodesBatch stores nodes atomically in two phases: duplicate check (store and
// within batch), then serialize/cache/index/queue. Any duplicate errors with zero
// mutations. Nil/empty input returns nil.
func (bs *Store) PutNodesBatch(nodes []*types.Node) error {
	return bs.putNodesBatchInternal(nodes, nil, nil, false)
}

// PutNodesBatchPreEncoded persists nodes whose v2 entity-row wire is pre-encoded
// (tail patched by the applier), skipping the second msgpack pass
// (store.PreEncodedPutCapability, ADR-0006 section 4.5). wireBodies[i] == nil
// re-encodes node i, so persisted bytes are identical either way. The change-log
// put body is unchanged.
func (bs *Store) PutNodesBatchPreEncoded(nodes []*types.Node, wireBodies [][]byte) error {
	return bs.putNodesBatchInternal(nodes, wireBodies, nil, false)
}

// PutNodesBatchPreEncodedLog (store.PreEncodedPutLogCapability) additionally uses
// logBodies[i], when non-nil, verbatim as node i's ChangeNodePut body; nil
// elements are built at the door (byte-identical).
func (bs *Store) PutNodesBatchPreEncodedLog(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	return bs.putNodesBatchInternal(nodes, wireBodies, logBodies, false)
}

// PutNodesBatchOwnedPreEncoded is PutNodesBatchPreEncodedLog with an ownership
// transfer: the caller never reads or mutates the nodes again, so the store
// freezes and caches each in place instead of deep-copying
// (store.OwnedPreEncodedPutCapability). Undefined behavior if the caller touches
// a node afterward; only the ingest bulk path may use it.
func (bs *Store) PutNodesBatchOwnedPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error {
	return bs.putNodesBatchInternal(nodes, wireBodies, logBodies, true)
}

var _ storecontract.OwnedPreEncodedPutCapability = (*Store)(nil)

// putNodesBatchInternal is the shared body of PutNodesBatch (wireBodies nil) and
// PutNodesBatchPreEncoded: wireBodies[i] non-nil is the persisted entity-row
// bytes, otherwise the row is marshaled here. Everything else is identical.
func (bs *Store) putNodesBatchInternal(nodes []*types.Node, wireBodies, logBodies [][]byte, owned bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	if err := bs.checkWritable(); err != nil {
		return err
	}

	// Pre-serialize outside the lock: row bytes, the frozen cache copy and the
	// change-log payload are pure functions of the finalized node, and building them
	// under idxMu was the dominant contention cost. Only order-sensitive appends
	// (cache put, index maps, ops, record buffering) stay under the lock.
	type nodeData struct {
		nid    types.NodeID
		id     snowflake.ID
		data   []byte
		frozen *types.Node
	}
	serialized := make([]nodeData, len(nodes))
	var putPayloads [][]byte
	if bs.logEnabled.Load() {
		putPayloads = make([][]byte, len(nodes))
	}
	for i, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		var data []byte
		if i < len(wireBodies) && wireBodies[i] != nil {
			data = wireBodies[i]
		} else {
			d, err := bs.marshalNodeBytes(n)
			if err != nil {
				return fmt.Errorf("graph: marshal node: %w", err)
			}
			data = d
		}
		nid := n.InternalID()
		serialized[i] = nodeData{nid: nid, id: nid.SnowflakeID(), data: data, frozen: bs.frozenNodeRowForCache(n, owned)}
		if bs.logEnabled.Load() {
			if i < len(logBodies) && logBodies[i] != nil {
				putPayloads[i] = logBodies[i] // producer-encoded, applier-patched
			} else {
				p, err := storepkg.NodePutPayload(n, false)
				if err != nil {
					return fmt.Errorf("graph: encode change-log: %w", err)
				}
				putPayloads[i] = p
			}
		}
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
	for i, n := range nodes {
		if err := bs.foldRecreatedNodeHistoryLocked(n, serialized[i].id); err != nil {
			bs.idxMu.Unlock()
			return err // a fold already done only widened an envelope: sound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(nodes)*3) // entity + avg ~2 label indexes
	for i, n := range nodes {
		nd := serialized[i]

		// Apply this node's vector-index entries first (lesson 4, BACKLOG 18e): a failure
		// must not leave other indexes reflecting a node whose write was never queued.
		if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates[i], nd.id); err != nil {
			bs.idxMu.Unlock()
			return err
		}

		bs.nodeCache.Put(nd.id, nd.frozen)
		bs.nodeIDs[nd.nid] = struct{}{}
		bs.bumpNodeRevLocked(nd.nid)
		bs.bumpNodeBeliefWatermarkLocked(nd.nid, nodeTxFrom(n)) // BACKLOG 10c, as PutNode

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(nd.id), value: nd.data})
		labelCount := n.LabelTokenCount()
		for j := 0; j < labelCount; j++ {
			tok := n.LabelTokenRawAt(j)
			if !bs.labelOnDisk {
				bs.addLabelIdxLocked(tok, nd.nid)
			}
			ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, nd.id)})
			bs.getOrCreateLabelCounter(tok).Add(1)
		}
		bs.recordNodeLabelMembersLocked(n) // transaction-time label membership
		bs.addNodePropertyKeyCounts(n)
		ops = append(ops, bs.maintainPropertyIndexesAdd(n, nd.id)...)
		indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, n, nd.id)
		ops = append(ops, bs.maintainTemporalIndexDiskAdd(n, nd.id)...)
		indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, n, nd.id)
	}

	bs.appendOps(ops...)
	bs.nodeCount.Add(int64(len(nodes)))
	// One ChangeNodePut per node (create => WithHistory false), under the same
	// idxMu.Lock as the ops so the records and ops snapshot together.
	for i := range putPayloads {
		bs.logChangeRaw(storecontract.ChangeNodePut, putPayloads[i])
	}
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// DeleteNodesBatch deletes nodes atomically in two phases: all IDs exist (pre-read
// for label cleanup), then remove from cache/indexes and queue deletes. Missing ID
// returns ErrNodeNotFound with zero mutations; duplicates are coalesced; nil/empty returns nil.
func (bs *Store) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if len(typedIDs) == 0 {
		return nil
	}
	defer bs.bumpNodeEpoch()
	if err := bs.checkWritable(); err != nil {
		return err
	}
	for _, id := range typedIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueNodeIDs(typedIDs)

	// Pre-build the change-log delete records (DeleteNodesBatch is unconnected-only
	// — hard deletes, no tombstone), so a marshal error aborts before any op.
	delPayloads := make([][]byte, len(typedIDs))
	if bs.logEnabled.Load() {
		for i, nid := range typedIDs {
			p, err := storepkg.MarshalChangeBody(storepkg.NodeDeleteBody{ID: int64(nid.SnowflakeID())})
			if err != nil {
				return fmt.Errorf("graph: encode change-log: %w", err)
			}
			delPayloads[i] = p
		}
	}

	// Phase 1a: validate cheap in-memory state under a read lock. Node row
	// prefetch below may hit Badger, so keep that I/O outside idxMu.Lock().
	bs.idxMu.RLock()
	for _, nid := range typedIDs {
		if _, exists := bs.nodeIDs[nid]; !exists {
			bs.idxMu.RUnlock()
			return ErrNodeNotFound
		}
		if connected, cErr := bs.nodeHasAdjacentRelsLocked(nid); cErr != nil {
			bs.idxMu.RUnlock()
			return cErr
		} else if connected {
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
		if connected, cErr := bs.nodeHasAdjacentRelsLocked(nid); cErr != nil {
			bs.idxMu.Unlock()
			return cErr
		} else if connected {
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
			if !bs.labelOnDisk {
				if set, exists := bs.labelIdx[tok]; exists {
					delete(set, nid)
					if len(set) == 0 {
						delete(bs.labelIdx, tok)
					}
				}
			}
			bs.getOrCreateLabelCounter(tok).Add(-1)
		}

		bs.removeNodePropertyKeyCounts(n)
		ops = append(ops, bs.maintainPropertyIndexesRemove(n, id)...)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
		ops = append(ops, bs.maintainTemporalIndexDiskRemove(n, id)...)
		indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, n, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)
		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		bs.ords.dropNode(nid)
		bs.deleteNodeRevLocked(nid)
		bs.appendOps(ops...)
		bs.logChangeRaw(storecontract.ChangeNodeDelete, delPayloads[i])
	}

	bs.nodeCount.Add(-int64(len(typedIDs)))
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
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

// loadNodeFromBadger reads a node within an existing Badger transaction, bypassing
// the LRU cache (used by loadIndexes before concurrent access starts).
