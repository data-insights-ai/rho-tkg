// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"fmt"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

func (bs *Store) PutNode(n *types.Node) error {
	return bs.putNodeRouted(n, 0)
}

// PutNodeScoped is PutNode with the change-log record routed to the
// store.ScopedTxChangeLog buffer named by token (0 = PutNode). See
// badgerstore_changelog_scoped.go.
func (bs *Store) PutNodeScoped(n *types.Node, token uint64) error {
	if token == 0 {
		return bs.PutNode(n)
	}
	return bs.putNodeRouted(n, token)
}

func (bs *Store) putNodeRouted(n *types.Node, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.InternalID()
	id := nid.SnowflakeID() // LRU is keyed by raw snowflake.ID (Tier D — see lru.go).

	data, err := bs.marshalNodeBytes(n)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(n, false)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	// Check for duplicate.
	if bs.nodeIDs.Has(int64(nid)) {
		bs.idxMu.Unlock()
		return ErrNodeExists
	}
	if err := bs.checkNodeIndexKeysLocked(n, nid.SnowflakeID()); err != nil { // KEYSIZE, before any mutation
		bs.idxMu.Unlock()
		return err
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, n, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := bs.foldRecreatedNodeHistoryLocked(n, id); err != nil {
		bs.idxMu.Unlock()
		return err // only widened an envelope (sound), before the vector apply
	}
	// Apply the vector-index mutation right after its own prepare step, before other
	// RAM state commits (lesson 4, BACKLOG 18e): a failure must not leave other
	// indexes reflecting a node that was never queued for durable write.
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, bs.frozenNodeRow(n))
	bs.nodeIDs.Add(int64(nid))
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(n)) // BACKLOG 10c

	// Build write ops.
	labelCount := n.LabelTokenCount()
	ops := make([]writeOp, 0, 1+labelCount)
	ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data})
	for i := 0; i < labelCount; i++ {
		tok := n.LabelTokenRawAt(i)
		if !bs.labelOnDisk {
			bs.addLabelIdxLocked(tok, nid)
		}
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)})
		bs.getOrCreateLabelCounter(tok).Add(1)
	}
	bs.recordNodeLabelMembersLocked(n) // transaction-time label membership

	bs.addNodePropertyKeyCounts(n)
	ops = append(ops, bs.maintainPropertyIndexesAdd(n, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, n, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(n, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, n, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(1)
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// GetNode retrieves a node by ID: LRU cache, then the nodeIDs existence check,
// then Badger only if the node exists. Returns ErrNodeNotFound if it does not.
func (bs *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	// Check cache first.
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Short-circuit: nodeIDs is rebuilt from node entity rows at open and
	// maintained with writes, so misses do not need a Badger transaction.
	bs.idxMu.RLock()
	exists := bs.nodeIDs.Has(int64(nid))
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Cache miss, node exists — read from Badger.
	var n *types.Node
	// Read before the snapshot opens — see LoadCleanAt.
	epoch := bs.nodeCache.FlushEpoch()
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			decoded, err := bs.decodeNodeWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			n = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean (evictable).
	n.Freeze() // shared between cache and caller
	bs.nodeCache.LoadCleanAt(id, n, epoch)
	return n.DeepCopy(), nil
}

// getNodeInTxn is GetNode's body over an already-open read transaction
// (NodesAsOf bulk scan, BACKLOG 18k). Cache-first like GetNode.
// Caller must hold bs.idxMu (at least RLock) for the ENTIRE surrounding scan:
// the cache read is not covered by badger's MVCC snapshot, and every writer takes
// idxMu.Lock around cache.Put, so without it a write could tear the scan. This
// function does not take idxMu itself: RWMutex is not reentrant and a queued
// writer would self-deadlock it (lesson 9).
func (bs *Store) getNodeInTxn(snap *scanSnapshot, nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	if !bs.nodeIDs.Has(int64(nid)) {
		return nil, ErrNodeNotFound
	}

	item, err := snap.txnAfterMiss().Get(storepkg.NodeKey(id))
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var n *types.Node
	if err := item.Value(func(val []byte) error {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal node: %w", err)
		}
		decoded, err := bs.decodeNodeWireForKey(w, id)
		if err != nil {
			return fmt.Errorf("graph: decode node: %w", err)
		}
		n = decoded
		return nil
	}); err != nil {
		return nil, err
	}

	n.Freeze()
	bs.nodeCache.LoadCleanAt(id, n, snap.pinned)
	return n.DeepCopy(), nil
}

// NodeIntegrityHash returns a live node's integrity hash without exposing or
// defensive-copying the whole node value.
func (bs *Store) NodeIntegrityHash(nid types.NodeID) (string, error) {
	if err := bs.checkOpen(); err != nil {
		return "", err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return "", err
	}
	id := nid.SnowflakeID()
	bs.idxMu.RLock()
	exists := bs.nodeIDs.Has(int64(nid))
	hash, hasHash := bs.nodeHashes[nid]
	rev := bs.nodeRevs.get(int64(nid))
	bs.idxMu.RUnlock()
	if !exists {
		return "", ErrNodeNotFound
	}
	if hasHash {
		return hash, nil
	}

	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		hash = badgerNodeIntegrityHash(v)
		if h := bs.nodeHashFillTestHook; h != nil {
			h()
		}
		return bs.fillNodeHash(nid, rev, hash)
	case indexpkg.CacheDeleted:
		return "", ErrNodeNotFound
	}

	var n *types.Node
	// Read before the snapshot opens — see LoadCleanAt.
	epoch := bs.nodeCache.FlushEpoch()
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			decoded, err := bs.decodeNodeWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			n = decoded
			return nil
		})
	})
	if err != nil {
		return "", err
	}
	hash = badgerNodeIntegrityHash(n)
	n.Freeze() // shared between cache and caller
	bs.nodeCache.LoadCleanAt(id, n, epoch)
	if h := bs.nodeHashFillTestHook; h != nil {
		h()
	}
	return bs.fillNodeHash(nid, rev, hash)
}

// nodeHashCacheCap bounds the node integrity hash cache (EXISTMAP: ~99 B per
// entry, filled by every endpoint hash a rel write reads, so unbounded it grew
// with the nodes that have rels): 64 K entries, ~6.5 MB per store. A miss
// recomputes the hash from the cached or stored row.
const nodeHashCacheCap = 1 << 16

func (bs *Store) nodeHashLimit() int {
	if bs.nodeHashCap > 0 {
		return bs.nodeHashCap
	}
	return nodeHashCacheCap
}

// fillNodeHash caches a hash computed from a row read without the lock, at
// the node revision rev read before it. nodeHashes is filled only here (row
// 66: no write fills it, ~99 B per node), and every node-row write drops the
// entry under idxMu (bumpNodeRevLocked, deleteNodeRevLocked). A write between
// the row read and this fill changed the revision: the hash answers the read,
// which ran before that write, but is not cached over the newer row.
func (bs *Store) fillNodeHash(nid types.NodeID, rev uint64, hash string) (string, error) {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if !bs.nodeIDs.Has(int64(nid)) {
		return "", ErrNodeNotFound
	}
	if bs.nodeRevs.get(int64(nid)) != rev {
		return hash, nil
	}
	if cur, ok := bs.nodeHashes[nid]; ok {
		return cur, nil // a concurrent read filled it at the same revision
	}
	if len(bs.nodeHashes) >= bs.nodeHashLimit() {
		for k := range bs.nodeHashes { // full: drop one entry (map order: an arbitrary one)
			delete(bs.nodeHashes, k)
			break
		}
	}
	bs.nodeHashes[nid] = hash
	return hash, nil
}

// EndpointIntegrityHashes returns both endpoint hashes while avoiding the
// defensive copies required by GetNode.
func (bs *Store) EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error) {
	fromHash, err := bs.NodeIntegrityHash(startID)
	if err != nil {
		return "", "", err
	}
	if startID == endID {
		return fromHash, fromHash, nil
	}
	toHash, err := bs.NodeIntegrityHash(endID)
	if err != nil {
		return "", "", err
	}
	return fromHash, toHash, nil
}

func badgerNodeIntegrityHash(n *types.Node) string {
	if n == nil {
		return ""
	}
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

type nodeDeleteInfo struct {
	node *types.Node
	rev  uint64
}

func (bs *Store) bumpNodeRevLocked(nid types.NodeID) {
	bs.nodeRevs.bump(int64(nid))
	delete(bs.nodeHashes, nid) // the row changed: the next NodeIntegrityHash refills
	bs.nodeEpoch.Add(1)        // every non-delete node write invalidates cached columns
	bs.sharedEpochs.node()
}

// bumpNodeEpoch marks every cached DocValues column stale. The delete and
// version-write paths call it directly (bumpNodeRevLocked covers other writes).
// A spurious bump only forces a rebuild.
func (bs *Store) bumpNodeEpoch() {
	bs.nodeEpoch.Add(1)
	bs.sharedEpochs.node()
}

// bumpRelEpoch marks the adjacency view stale for the expand-aggregation column
// path. Called by every relationship mutation; a spurious bump only forces a Gate-2
// fall-back. It also advances relEpochCoarse, invalidating every rel-type column
// snapshot: sites without per-type precision (bumpRelEpochForType) must not leave
// a type's columns stale.
func (bs *Store) bumpRelEpoch() {
	bs.relEpoch.Add(1)
	bs.sharedEpochs.rel()
	bs.relEpochCoarse.Add(1)
	bs.poisonAllRelTypes() // the default door must void every append record too
}

func (bs *Store) deleteNodeRevLocked(nid types.NodeID) {
	bs.nodeRevs.bump(int64(nid))
	delete(bs.nodeHashes, nid)
}

func (bs *Store) prefetchNodeDeleteInfo(nid types.NodeID) (nodeDeleteInfo, error) {
	bs.idxMu.RLock()
	if !bs.nodeIDs.Has(int64(nid)) {
		bs.idxMu.RUnlock()
		return nodeDeleteInfo{}, ErrNodeNotFound
	}
	rev := bs.nodeRevs.get(int64(nid))
	bs.idxMu.RUnlock()

	n, err := bs.prefetchNode(nid)
	if err != nil {
		return nodeDeleteInfo{}, err
	}
	return nodeDeleteInfo{node: n, rev: rev}, nil
}

func (bs *Store) nodeDeleteInfoStillCurrentLocked(nid types.NodeID, info nodeDeleteInfo) bool {
	if info.node == nil || info.rev == 0 {
		return false
	}
	if !bs.nodeIDs.Has(int64(nid)) {
		return false
	}
	return bs.nodeRevs.get(int64(nid)) == info.rev
}

func (bs *Store) currentNodeForPrefetchLocked(nid types.NodeID, info nodeDeleteInfo) (*types.Node, error) {
	if bs.nodeDeleteInfoStillCurrentLocked(nid, info) {
		return info.node, nil
	}
	return bs.getNodeLocked(nid)
}

// DeleteNode removes a node and its label index entries. ErrInvalidStoreMutation
// if relationships are still attached; ErrNodeNotFound if absent.
func (bs *Store) DeleteNode(nid types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpNodeEpoch()
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	// Build the change-log delete record up front (unconnected-only, no tombstone) so
	// a marshal error aborts before any op is enqueued.
	delPayload, err := bs.buildChangePayload(storepkg.NodeDeleteBody{ID: int64(id)})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}

	// Prefetch before the write lock so cache misses do not hold idxMu.Lock across Badger I/O.
	if _, err := bs.prefetchNode(nid); err != nil {
		return err
	}

	bs.idxMu.Lock()

	// TOCTOU guard: re-verify existence after acquiring write lock.
	// A concurrent delete may have removed the node between prefetchNode and here.
	if !bs.nodeIDs.Has(int64(nid)) {
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

	// Re-read under the write lock: cleaning indexes from a stale prefetch would use
	// old labels if the ID was deleted and recreated in the TOCTOU window.
	n, err := bs.getNodeLocked(nid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Build delete ops using the current node (labels needed for index cleanup).
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
	bs.nodeIDs.Remove(int64(nid))
	bs.ords.dropNode(nid)
	bs.deleteNodeRevLocked(nid)
	bs.appendOps(ops...)
	bs.logChangeRaw(storecontract.ChangeNodeDelete, delPayload)
	bs.nodeCount.Add(-1)
	bs.idxMu.Unlock()

	return bs.flushIfNeeded()
}

// ReplaceNode overwrites a node's data in place. No label index changes (labels
// are immutable); property indexes follow property changes. ErrNodeNotFound if absent.
func (bs *Store) ReplaceNode(n *types.Node) error {
	return bs.replaceNodeRouted(n, 0)
}

// ReplaceNodeScoped is ReplaceNode with the change-log record routed to the
// ScopedTxChangeLog buffer named by token (0 = ReplaceNode).
func (bs *Store) ReplaceNodeScoped(n *types.Node, token uint64) error {
	if token == 0 {
		return bs.ReplaceNode(n)
	}
	return bs.replaceNodeRouted(n, token)
}

func (bs *Store) replaceNodeRouted(n *types.Node, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.InternalID()
	id := nid.SnowflakeID()

	data, err := bs.marshalNodeBytes(n)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(n, false)
	if err != nil {
		return err
	}

	// Prefetch before the write lock; if stale at lock time, use the locked current row.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if !bs.nodeIDs.Has(int64(nid)) {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if err := bs.checkNodeIndexKeysLocked(n, nid.SnowflakeID()); err != nil { // KEYSIZE, before any mutation
		bs.idxMu.Unlock()
		return err
	}
	if prefetchErr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before replace: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before replace: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before replace: missing prefetched state")
	}
	if err := storecontract.ValidateNodeReplacement(old, n); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, n, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update property, temporal, and vector indexes: remove old entries, add new.
	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	// Apply the new vector entries right after the old ones are purged, before other
	// index state commits (lesson 4, BACKLOG 18e), so a failure never leaves the
	// vector index with neither entry.
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	bs.nodeCache.Put(id, bs.frozenNodeRow(n))
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(n)) // BACKLOG 10c
	bs.addNodePropertyKeyCounts(n)
	ops = append(ops, bs.maintainPropertyIndexesAdd(n, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, n, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(n, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, n, id)
	ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data})
	bs.appendOps(ops...)
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// RemoveNodeLabelToken removes tok from the label index for id and persists
// updatedNode, which must already lack the label and have its version bumped. The
// caller writes version history first. ErrNodeNotFound if absent.
func (bs *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	return bs.removeNodeLabelTokenRouted(nid, tok, updatedNode, 0)
}

// RemoveNodeLabelTokenScoped is RemoveNodeLabelToken with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) RemoveNodeLabelTokenScoped(nid types.NodeID, tok uint16, updatedNode *types.Node, token uint64) error {
	if token == 0 {
		return bs.RemoveNodeLabelToken(nid, tok, updatedNode)
	}
	return bs.removeNodeLabelTokenRouted(nid, tok, updatedNode, token)
}

func (bs *Store) removeNodeLabelTokenRouted(nid types.NodeID, tok uint16, updatedNode *types.Node, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeBytes(updatedNode)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(updatedNode, false)
	if err != nil {
		return err
	}

	// Prefetch before the write lock; if stale at lock time, use the locked current row.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if !bs.nodeIDs.Has(int64(nid)) {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if err := bs.checkNodeIndexKeysLocked(updatedNode, nid.SnowflakeID()); err != nil { // KEYSIZE, before any mutation
		bs.idxMu.Unlock()
		return err
	}
	if prefetchErr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before remove label: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before remove label: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before remove label: missing prefetched state")
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, updatedNode, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update property, temporal, and vector indexes using the current old node state.
	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	// Apply the new vector-index entries right after the old ones are purged,
	// before any other new-state RAM mutation commits (lesson 4, BACKLOG 18e).
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Remove tok from the in-memory label index.
	if !bs.labelOnDisk {
		if set, ok := bs.labelIdx[tok]; ok {
			delete(set, nid)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
	}
	bs.getOrCreateLabelCounter(tok).Add(-1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, bs.frozenNodeRow(updatedNode))
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(updatedNode)) // BACKLOG 10c
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = append(ops, bs.maintainPropertyIndexesAdd(updatedNode, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, updatedNode, id)

	// Queue: set node data + delete label index entry.
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.appendOps(ops...)
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump or history entry (transaction rollback). ErrNodeNotFound if absent.
func (bs *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	return bs.addNodeLabelTokenRouted(nid, tok, updatedNode, 0)
}

// AddNodeLabelTokenScoped is AddNodeLabelToken with the change-log record routed
// to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) AddNodeLabelTokenScoped(nid types.NodeID, tok uint16, updatedNode *types.Node, token uint64) error {
	if token == 0 {
		return bs.AddNodeLabelToken(nid, tok, updatedNode)
	}
	return bs.addNodeLabelTokenRouted(nid, tok, updatedNode, token)
}

func (bs *Store) addNodeLabelTokenRouted(nid types.NodeID, tok uint16, updatedNode *types.Node, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeBytes(updatedNode)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(updatedNode, false)
	if err != nil {
		return err
	}

	// Prefetch before the write lock; if stale at lock time, use the locked current row.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if !bs.nodeIDs.Has(int64(nid)) {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if err := bs.checkNodeIndexKeysLocked(updatedNode, nid.SnowflakeID()); err != nil { // KEYSIZE, before any mutation
		bs.idxMu.Unlock()
		return err
	}
	if prefetchErr != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before add label: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before add label: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before add label: missing prefetched state")
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	// A label re-added to a live node: that label's index may not cover the
	// ID while its history holds rows with the label (backlog 56).
	if err := bs.foldRecreatedNodeHistoryLocked(updatedNode, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, updatedNode, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	// Apply the new vector-index entries right after the old ones are purged,
	// before any other new-state RAM mutation commits (lesson 4, BACKLOG 18e).
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	if !bs.labelOnDisk {
		bs.addLabelIdxLocked(tok, nid)
	}
	bs.getOrCreateLabelCounter(tok).Add(1)
	bs.recordNodeLabelMembersLocked(updatedNode) // transaction-time label membership (new token)

	bs.nodeCache.Put(id, bs.frozenNodeRow(updatedNode))
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(updatedNode)) // BACKLOG 10c
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = append(ops, bs.maintainPropertyIndexesAdd(updatedNode, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, updatedNode, id)

	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.appendOps(ops...)
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

// NodesByLabel returns nodes with the label token, with pagination and temporal
// filtering, sorted by snowflake.ID. Uses the temporal index when available.
func (bs *Store) loadNodeFromBadger(txn *badgerv4.Txn, id snowflake.ID) (*types.Node, error) {
	item, err := txn.Get(storepkg.NodeKey(id))
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var n *types.Node
	err = item.Value(func(val []byte) error {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal node: %w", err)
		}
		decoded, err := bs.decodeNodeWireForKey(w, id)
		if err != nil {
			return fmt.Errorf("graph: decode node: %w", err)
		}
		n = decoded
		return nil
	})
	return n, err
}

// loadRelFromBadger reads a relationship within an existing Badger transaction
// (rel-property-index rebuild at open).
func (bs *Store) loadRelFromBadger(txn *badgerv4.Txn, id snowflake.ID) (*types.Relationship, error) {
	item, err := txn.Get(storepkg.RelKey(id))
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrRelNotFound
	}
	if err != nil {
		return nil, err
	}
	var r *types.Relationship
	err = item.Value(func(val []byte) error {
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
	return r, err
}

// NodesByLabelAndProperty returns nodes matching label and property value, with
// temporal filtering, sorted by snowflake.ID. Uses the property index if present,
// else label scan + filter.
func (bs *Store) prefetchNode(nid types.NodeID) (*types.Node, error) {
	// Read before the badger read opens — see LoadCleanAt.
	epoch := bs.nodeCache.FlushEpoch()
	n, miss, err := bs.prefetchNodeNoFill(nid, true) // point read: promote
	if err != nil || !miss {
		return n, err
	}
	bs.nodeCache.LoadCleanAt(nid.SnowflakeID(), n, epoch)
	return n, nil
}

// prefetchNodeScan is prefetchNode without the cache fill on miss: scans larger
// than the LRU would evict exactly the entries the next pass needs and flush hot
// point reads. Hits are still served; the result is frozen either way.
func (bs *Store) prefetchNodeScan(nid types.NodeID) (*types.Node, error) {
	n, _, err := bs.prefetchNodeNoFill(nid, false) // scan: no promote
	return n, err
}

// prefetchNodeNoFill is the shared core. miss=true means n was decoded from
// badger. The result is frozen because callers may publish it into the cache.
func (bs *Store) prefetchNodeNoFill(nid types.NodeID, promote bool) (n *types.Node, miss bool, err error) {
	id := nid.SnowflakeID()
	// Point reads promote the hit to MRU (exclusive lock); scan reads do not
	// (GetNoPromote, read lock), so a scan neither serializes on the lock nor
	// evicts hot entries.
	var v *types.Node
	var status indexpkg.CacheStatus
	if promote {
		v, status = bs.nodeCache.Get(id)
	} else {
		v, status = bs.nodeCache.GetNoPromote(id)
	}
	switch status {
	case indexpkg.CacheHit:
		return v, false, nil
	case indexpkg.CacheDeleted:
		return nil, false, ErrNodeNotFound
	}

	// Cache miss — check existence before incurring Badger I/O.
	bs.idxMu.RLock()
	exists := bs.nodeIDs.Has(int64(nid))
	bs.idxMu.RUnlock()
	if !exists {
		return nil, false, ErrNodeNotFound
	}

	// Node exists in-memory but not in cache (flushed + evicted from LRU).
	// Read from Badger without holding any lock.
	err = bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			w := getPooledNodeWire()
			defer putPooledNodeWire(w)
			if err := storepkg.SafeUnmarshal(val, w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			decoded, err := bs.decodeNodeWireForKey(*w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			n = decoded
			return nil
		})
	})
	if err != nil {
		return nil, false, err
	}
	n.Freeze() // shared between cache and caller
	return n, true, nil
}

// getNodeLocked retrieves a node from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *Store) getNodeLocked(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	if status == indexpkg.CacheHit {
		return v, nil
	}
	if status == indexpkg.CacheDeleted {
		return nil, ErrNodeNotFound
	}

	// Cache miss — read from Badger.
	var n *types.Node
	// Read before the snapshot opens — see LoadCleanAt.
	epoch := bs.nodeCache.FlushEpoch()
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			decoded, err := bs.decodeNodeWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			n = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	n.Freeze() // shared between cache and caller
	bs.nodeCache.LoadCleanAt(id, n, epoch)
	return n, nil
}

// nodeWirePool keeps the property-entry backing array of a cold point read's
// NodeWire between reads (row 121): the entries are scratch, copied into the
// node's own property slice by the checked decode. Every other field is reset
// per read, so nothing of one row reaches the next or the decoded node.
var nodeWirePool = sync.Pool{New: func() any { return new(storepkg.NodeWire) }}

func getPooledNodeWire() *storepkg.NodeWire {
	w := nodeWirePool.Get().(*storepkg.NodeWire)
	props := w.Properties[:0]
	*w = storepkg.NodeWire{Properties: props}
	return w
}

func putPooledNodeWire(w *storepkg.NodeWire) {
	clear(w.Properties) // drop the keys and values: the pool must not retain them
	w.DropShare()
	if cap(w.Properties) > 256 {
		w.Properties = nil // do not pin a very wide row's array
	}
	nodeWirePool.Put(w)
}
