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

func (bs *Store) PutNode(n *types.Node) error {
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

	bs.idxMu.Lock()

	// Check for duplicate.
	if _, exists := bs.nodeIDs[nid]; exists {
		bs.idxMu.Unlock()
		return ErrNodeExists
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, n, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, n.DeepCopy())
	bs.nodeIDs[nid] = struct{}{}
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(n)
	bs.bumpNodeRevLocked(nid)

	// Build write ops.
	labelCount := n.LabelTokenCount()
	ops := make([]writeOp, 0, 1+labelCount)
	ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data})
	for i := 0; i < labelCount; i++ {
		tok := n.LabelTokenRawAt(i)
		if bs.labelIdx[tok] == nil {
			bs.labelIdx[tok] = make(map[types.NodeID]struct{})
		}
		bs.labelIdx[tok][nid] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)})
		bs.getOrCreateLabelCounter(tok).Add(1)
	}

	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, n, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	bs.appendOps(ops...)
	bs.nodeCount.Add(1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Cache-first: checks LRU cache, then nodeIDs (O(1) existence check),
// then falls through to Badger only if the node is confirmed to exist.
// Returns ErrNodeNotFound if the node does not exist.
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
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Cache miss, node exists — read from Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
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
	bs.nodeCache.LoadClean(id, n)
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
	_, exists := bs.nodeIDs[nid]
	hash, hasHash := bs.nodeHashes[nid]
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
		hash := badgerNodeIntegrityHash(v)
		bs.idxMu.Lock()
		if _, exists := bs.nodeIDs[nid]; exists {
			bs.nodeHashes[nid] = hash
			bs.idxMu.Unlock()
			return hash, nil
		}
		bs.idxMu.Unlock()
		return "", ErrNodeNotFound
	case indexpkg.CacheDeleted:
		return "", ErrNodeNotFound
	}

	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
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
	bs.nodeCache.LoadClean(id, n)
	bs.idxMu.Lock()
	if _, exists := bs.nodeIDs[nid]; exists {
		bs.nodeHashes[nid] = hash
		bs.idxMu.Unlock()
		return hash, nil
	}
	bs.idxMu.Unlock()
	return "", ErrNodeNotFound
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
	if bs.nodeRevs == nil {
		bs.nodeRevs = make(map[types.NodeID]uint64)
	}
	bs.nextNodeRev++
	if bs.nextNodeRev == 0 {
		bs.nextNodeRev = 1
	}
	bs.nodeRevs[nid] = bs.nextNodeRev
}

func (bs *Store) deleteNodeRevLocked(nid types.NodeID) {
	delete(bs.nodeRevs, nid)
}

func (bs *Store) prefetchNodeDeleteInfo(nid types.NodeID) (nodeDeleteInfo, error) {
	bs.idxMu.RLock()
	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.RUnlock()
		return nodeDeleteInfo{}, ErrNodeNotFound
	}
	rev := bs.nodeRevs[nid]
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
	if _, exists := bs.nodeIDs[nid]; !exists {
		return false
	}
	return bs.nodeRevs[nid] == info.rev
}

func (bs *Store) currentNodeForPrefetchLocked(nid types.NodeID, info nodeDeleteInfo) (*types.Node, error) {
	if bs.nodeDeleteInfoStillCurrentLocked(nid, info) {
		return info.node, nil
	}
	return bs.getNodeLocked(nid)
}

// DeleteNode removes a node and its label index entries.
// Returns ErrInvalidStoreMutation if the node still has connected relationships.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) DeleteNode(nid types.NodeID) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	// Pre-fetch node state before acquiring the write lock to avoid holding
	// idxMu.Lock() during Badger disk I/O on cache misses (B3: lock scope rule).
	// prefetchNode checks the cache and falls through to db.View without any lock.
	if _, err := bs.prefetchNode(nid); err != nil {
		return err
	}

	bs.idxMu.Lock()

	// TOCTOU guard: re-verify existence after acquiring write lock.
	// A concurrent delete may have removed the node between prefetchNode and here.
	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if len(bs.outIdx[nid]) != 0 || len(bs.inIdx[nid]) != 0 {
		bs.idxMu.Unlock()
		return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
	}

	// Re-read under the write lock. prefetchNode loaded the cache on the
	// normal cache-miss path, so this is a cache hit unless the entry was
	// evicted between prefetch and lock acquisition. Using the locked current
	// row avoids cleaning indexes with stale labels if a direct Store caller
	// deleted and recreated the same ID in the TOCTOU window.
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
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (bs *Store) ReplaceNode(n *types.Node) error {
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

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// If the prefetched row is stale by the time the lock is acquired, fall back
	// to the locked current row before cleaning secondary indexes.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
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
	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	bs.nodeCache.Put(id, n.DeepCopy())
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(n)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, n, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// RemoveNodeLabelToken removes tok from the label index for id and persists updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw) and have its
// version bumped. Version history must be written by the caller before this call.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// If the prefetched row is stale by the time the lock is acquired, fall back
	// to the locked current row before cleaning secondary indexes.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
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
	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)

	// Remove tok from the in-memory label index.
	if set, ok := bs.labelIdx[tok]; ok {
		delete(set, nid)
		if len(set) == 0 {
			delete(bs.labelIdx, tok)
		}
	}
	bs.getOrCreateLabelCounter(tok).Add(-1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Queue: set node data + delete label index entry.
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// If the prefetched row is stale by the time the lock is acquired, fall back
	// to the locked current row before cleaning secondary indexes.
	oldInfo, prefetchErr := bs.prefetchNodeDeleteInfo(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
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
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, updatedNode, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)

	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index fast path when available and a temporal filter is set.
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
		if err := msgpack.Unmarshal(val, &w); err != nil {
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

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional temporal filtering. Uses the property index if one exists;
// falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) prefetchNode(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v, nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Cache miss — check existence before incurring Badger I/O.
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Node exists in-memory but not in cache (flushed + evicted from LRU).
	// Read from Badger without holding any lock.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
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
	bs.nodeCache.LoadClean(id, n)
	return n, nil
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
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
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
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}
