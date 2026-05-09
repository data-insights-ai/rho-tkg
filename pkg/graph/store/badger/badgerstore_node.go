// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func (bs *Store) PutNode(n *types.Node) error {
	nid := n.InternalID()
	id := nid.SnowflakeID() // LRU is keyed by raw snowflake.ID (Tier D — see lru.go).

	w := storepkg.NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	bs.idxMu.Lock()

	// Check for duplicate.
	if _, exists := bs.nodeIDs[nid]; exists {
		bs.idxMu.Unlock()
		return ErrNodeExists
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, n.DeepCopy())
	bs.nodeIDs[nid] = struct{}{}

	// Build write ops.
	ops := []writeOp{{opType: writeOpSet, key: storepkg.NodeKey(id), value: data}}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if bs.labelIdx[tv] == nil {
			bs.labelIdx[tv] = make(map[types.NodeID]struct{})
		}
		bs.labelIdx[tv][nid] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tv, id)})
		bs.getOrCreateLabelCounter(tv).Add(1)
	}

	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, n, id)
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
	id := nid.SnowflakeID()
	// Check cache first.
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Short-circuit: nodeIDs is the authoritative set of all node IDs.
	// Avoids opening a Badger transaction for non-existent nodes.
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
			n = storepkg.WireToNode(w)
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

// DeleteNode removes a node and its label index entries.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) DeleteNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	// Pre-fetch node state before acquiring the write lock to avoid holding
	// idxMu.Lock() during Badger disk I/O on cache misses (B3: lock scope rule).
	// prefetchNode checks the cache and falls through to db.View without any lock.
	n, err := bs.prefetchNode(nid)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	// TOCTOU guard: re-verify existence after acquiring write lock.
	// A concurrent delete may have removed the node between prefetchNode and here.
	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Build delete ops using pre-fetched node (labels needed for index cleanup).
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
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, nid)
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
	nid := n.InternalID()
	id := nid.SnowflakeID()

	w := storepkg.NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes: remove old entries, add new.
	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		// Pre-fetch failed (concurrent delete between prefetch and write lock, or
		// cache miss on a just-opened store) — brute-force purge to avoid orphans.
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}
	bs.nodeCache.Put(id, n.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, n, id)
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
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes using pre-fetched old node state.
	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

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
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

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
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

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
		n = storepkg.WireToNode(w)
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
			n = storepkg.WireToNode(w)
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
			n = storepkg.WireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}
