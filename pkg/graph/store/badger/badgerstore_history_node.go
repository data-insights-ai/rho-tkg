package badger

import (
	"errors"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
)

// Node-history methods (R5-F9 split out from badgerstore_history.go).
// Helpers shared between node and rel paths stay in badgerstore_history.go;
// rel-history methods live in badgerstore_history_rel.go.

func (bs *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeToBytes(updatedNode)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	histData, err := bs.marshalNodeToBytes(prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
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
		return fmt.Errorf("graph: read node before remove label history: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before remove label history: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before remove label history: missing prefetched state")
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
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
	bs.nodeCache.Put(id, freezeNodeCopy(updatedNode))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — node data + history + label index delete — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeToBytes(updatedNode)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	histData, err := bs.marshalNodeToBytes(prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
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
		return fmt.Errorf("graph: read node before add label history: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before add label history: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before add label history: missing prefetched state")
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
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

	// Add tok to the in-memory label index.
	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, freezeNodeCopy(updatedNode))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — node data + history + label index set — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(current); err != nil {
		return err
	}
	nid := current.InternalID()
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	// Serialize current state.
	data, err := bs.marshalNodeToBytes(current)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Serialize history snapshot.
	histData, err := bs.marshalNodeToBytes(prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
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
		return fmt.Errorf("graph: read node before replace history: %w", prefetchErr)
	}
	old, err := bs.currentNodeForPrefetchLocked(nid, oldInfo)
	if err != nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before replace history: %w", err)
	}
	if old == nil {
		bs.idxMu.Unlock()
		return fmt.Errorf("graph: read node before replace history: missing prefetched state")
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, current); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(bs.vectorIndexes, current, id)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	bs.nodeCache.Put(id, freezeNodeCopy(current))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(current)
	bs.bumpNodeRevLocked(nid)
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, current, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, current, id)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, current, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevNodeVersion, nodeTombstone); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	// Serialize all tombstones OUTSIDE lock (B3).
	nodeData, err := bs.marshalNodeToBytes(nodeTombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal node tombstone: %w", err)
	}
	nodeHistKey := storepkg.HistNodeKey(id, uint64(prevNodeVersion))

	type histEntry struct{ key, data []byte }
	relEntries := make([]histEntry, 0, len(relTombstones))
	for _, rt := range relTombstones {
		if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rt.ID, rt.PrevVersion, rt.Tombstone); err != nil {
			return err
		}
		data, err := bs.marshalRelToBytes(rt.Tombstone)
		if err != nil {
			return fmt.Errorf("graph: marshal rel tombstone: %w", err)
		}
		relEntries = append(relEntries, histEntry{
			key:  storepkg.HistRelKey(rt.ID.SnowflakeID(), uint64(rt.PrevVersion)),
			data: data,
		})
	}
	prefetched, err := bs.prefetchCascadeDeleteRows(nid)
	if err != nil {
		return err
	}

	// Acquire lock ONCE — hold it across cascade + tombstone appends (B3 + lock ordering rule).
	bs.idxMu.Lock()
	var old *types.Node
	if bs.nodeDeleteInfoStillCurrentLocked(nid, prefetched.node) {
		old = prefetched.node.node
	} else {
		old, err = bs.getNodeLocked(nid)
	}
	if err == nil {
		err = storecontract.ValidateNodeLiveVersion(old, prevNodeVersion)
	}
	if err == nil {
		err = storecontract.ValidateNodeReplacement(old, nodeTombstone)
	}
	if err != nil && old != nil {
		bs.idxMu.Unlock()
		return err
	}
	if err := bs.validateDeleteNodeRelTombstonesLocked(nid, relTombstones, prefetched.rels); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	_, corruptErr, fatalErr := bs.cascadeDeleteInner(nid, prefetched)
	if fatalErr != nil {
		bs.idxMu.Unlock()
		return fatalErr
	}
	// Append tombstone history ops to SAME pending map before releasing lock.
	ops := make([]writeOp, 0, 1+len(relEntries))
	ops = append(ops, writeOp{opType: writeOpSet, key: nodeHistKey, value: nodeData})
	for _, e := range relEntries {
		ops = append(ops, writeOp{opType: writeOpSet, key: e.key, value: e.data})
	}
	bs.appendOps(ops...)
	bs.idxMu.Unlock()

	if corruptErr == nil && bs.syncWrites {
		return bs.flush()
	}
	return corruptErr
}

func (bs *Store) validateDeleteNodeRelTombstonesLocked(nid types.NodeID, relTombstones []RelTombstone, prefetched map[types.RelID]RelDeleteInfo) error {
	if _, exists := bs.nodeIDs[nid]; !exists {
		return ErrNodeNotFound
	}

	relIDs := make(map[types.RelID]struct{})
	for relID := range bs.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range bs.inIdx[nid] {
		relIDs[relID] = struct{}{}
	}

	tombed := make(map[types.RelID]struct{}, len(relTombstones))
	for _, rt := range relTombstones {
		if _, dup := tombed[rt.ID]; dup {
			return fmt.Errorf("%w: duplicate relationship tombstone %d", ErrInvalidStoreMutation, rt.ID)
		}
		tombed[rt.ID] = struct{}{}
		if _, connected := relIDs[rt.ID]; !connected {
			return fmt.Errorf("%w: relationship tombstone %d is not connected to node %d", ErrInvalidStoreMutation, rt.ID, nid)
		}
		old, err := bs.getRelLocked(rt.ID)
		if err != nil {
			return err
		}
		if err := storecontract.ValidateRelationshipLiveVersion(old, rt.PrevVersion); err != nil {
			return err
		}
		if err := storecontract.ValidateRelationshipReplacement(old, rt.Tombstone); err != nil {
			return err
		}
	}
	for relID := range relIDs {
		if info, ok := prefetched[relID]; ok && bs.relDeleteInfoStillIndexedLocked(info) {
			if _, ok := tombed[relID]; !ok {
				return fmt.Errorf("%w: missing relationship tombstone %d", ErrInvalidStoreMutation, relID)
			}
			continue
		}
		if _, err := bs.getRelLocked(relID); err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return err
		}
		if _, ok := tombed[relID]; !ok {
			return fmt.Errorf("%w: missing relationship tombstone %d", ErrInvalidStoreMutation, relID)
		}
	}
	return nil
}

func (bs *Store) marshalNodeToBytes(n *types.Node) ([]byte, error) {
	return bs.marshalNodeBytes(n)
}

func (bs *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, version, n); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeToBytes(n)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	key := storepkg.HistNodeKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

func (bs *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	key := storepkg.HistNodeKey(id, uint64(version))

	// Check pending buffer for unflushed writes.
	bs.wbMu.Lock()
	op, found := bs.pending[string(key)]
	bs.wbMu.Unlock()

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w storepkg.NodeWire
		if err := msgpack.Unmarshal(op.value, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, id, uint64(version))
		if err != nil {
			return nil, fmt.Errorf("graph: decode node version: %w", err)
		}
		return n.DeepCopy(), nil
	}

	// Fall through to Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node version: %w", err)
			}
			decoded, err := bs.decodeNodeHistoryWireForKey(w, id, uint64(version))
			if err != nil {
				return fmt.Errorf("graph: decode node version: %w", err)
			}
			n = decoded
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

func (bs *Store) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.getNodeHistoryByPrefix(prefix)
}

func (bs *Store) NodeHistoryVersionsFrom(nid types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.nodeHistoryVersionsFromPrefix(prefix, startVersion, limit)
}

func (bs *Store) getNodeHistoryByPrefix(prefix []byte) ([]*types.Node, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)
	prefixStr := string(prefix)

	// Collect from Badger.
	entries := make(map[string][]byte) // key string -> value bytes
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			k := string(key)
			err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Merge pending buffer entries (pending wins).
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) == storepkg.SizeHistKey && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(entries, k)
			} else {
				cp := make([]byte, len(op.value))
				copy(cp, op.value)
				entries[k] = cp
			}
		}
	}
	bs.wbMu.Unlock()

	if len(entries) == 0 {
		return nil, nil
	}

	// Sort by key (big-endian version in bytes 9-17 gives natural ascending order).
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]*types.Node, 0, len(keys))
	for _, k := range keys {
		var w storepkg.NodeWire
		if err := msgpack.Unmarshal(entries[k], &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, expectedID, historyVersionFromKey([]byte(k)))
		if err != nil {
			return nil, fmt.Errorf("graph: decode node version: %w", err)
		}
		result = append(result, n.DeepCopy())
	}
	return result, nil
}

func (bs *Store) nodeHistoryVersionsFromPrefix(prefix []byte, startVersion uint32, limit int) ([]*types.Node, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)
	pending, pendingDeletes := bs.pendingHistoryVersionOverlay(prefix, startVersion)
	pendingKeys := make([]string, 0, len(pending))
	for k := range pending {
		pendingKeys = append(pendingKeys, k)
	}
	sort.Strings(pendingKeys)

	result := make([]*types.Node, 0, capForLimit(limit))
	reachedLimit := func() bool {
		return limit > 0 && len(result) >= limit
	}
	emit := func(key string, data []byte) error {
		var w storepkg.NodeWire
		if err := msgpack.Unmarshal(data, &w); err != nil {
			return fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, expectedID, historyVersionFromKey([]byte(key)))
		if err != nil {
			return fmt.Errorf("graph: decode node version: %w", err)
		}
		result = append(result, n.DeepCopy())
		return nil
	}

	pendingIdx := 0
	drainPendingBefore := func(bound string) error {
		for pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] < bound {
			if err := emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]]); err != nil {
				return err
			}
			pendingIdx++
			if reachedLimit() {
				return nil
			}
		}
		return nil
	}

	seekKey := historyVersionSeekKey(prefix, startVersion)
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if historyVersionFromKey(key) < uint64(startVersion) {
				continue
			}
			k := string(key)
			if err := drainPendingBefore(k); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
			if pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] == k {
				if err := emit(k, pending[k]); err != nil {
					return err
				}
				pendingIdx++
				if reachedLimit() {
					return nil
				}
				continue
			}
			if _, deleted := pendingDeletes[k]; deleted {
				continue
			}
			if err := it.Item().Value(func(val []byte) error {
				return emit(k, val)
			}); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
		}
		for pendingIdx < len(pendingKeys) {
			if err := emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]]); err != nil {
				return err
			}
			pendingIdx++
			if reachedLimit() {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (bs *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// TrimNodeHistoryFrom removes node history entries at or after minVersion.
func (bs *Store) TrimNodeHistoryFrom(nid types.NodeID, minVersion uint32) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	prefix := storepkg.HistNodePrefix(nid.SnowflakeID())
	return bs.trimHistoryFromPrefix(prefix, minVersion)
}

func (bs *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistNode)
	if err != nil {
		return fmt.Errorf("graph: scan node history max ID: %w", err)
	}
	if maxID == 0 {
		return nil
	}
	var after types.NodeID
	for {
		ids, err := bs.AllNodeHistoryIDsFrom(after, 1024)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if id.SnowflakeID() > maxID {
				return nil
			}
			if !fn(id) {
				return nil
			}
		}
		after = ids[len(ids)-1]
		if after.SnowflakeID() >= maxID {
			return nil
		}
	}
}

// ForEachDeletedNodeID iterates IDs that have history rows but no current
// row. Same iteration shape as ForEachNodeHistoryID with an inline O(1)
// in-memory presence check via HasNodeID; the resulting cost is one extra
// map lookup per history ID, no additional Badger reads. Callbacks are
// invoked outside Badger transactions.
func (bs *Store) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistNode)
	if err != nil {
		return fmt.Errorf("graph: scan node history max ID: %w", err)
	}
	if maxID == 0 {
		return nil
	}
	var after types.NodeID
	for {
		ids, err := bs.AllNodeHistoryIDsFrom(after, 1024)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if id.SnowflakeID() > maxID {
				return nil
			}
			if bs.HasNodeID(id.SnowflakeID()) {
				continue
			}
			if !fn(id) {
				return nil
			}
		}
		after = ids[len(ids)-1]
		if after.SnowflakeID() >= maxID {
			return nil
		}
	}
}

// MaxNodeHistoryID returns the highest node ID with version history visible to
// this store. It includes pending buffered writes and the persisted Badger key
// space, and returns zero when no node history exists.
func (bs *Store) MaxNodeHistoryID() (types.NodeID, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	maxID, err := bs.maxHistoryID(storepkg.KeyHistNode)
	if err != nil {
		return 0, err
	}
	return types.NodeID(maxID), nil
}

func (bs *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	return bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
}

func (bs *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}
	afterRaw := snowflake.ID(after)

	// Phase 1: collect candidate IDs and tombstones from the pending buffer.
	// Pending deletes must mask Badger keys until the async buffer flushes.
	pending, pendingDeletes := bs.pendingHistoryIDOverlay(storepkg.KeyHistNode, afterRaw)

	pendingSorted := make([]snowflake.ID, 0, len(pending))
	for id := range pending {
		pendingSorted = append(pendingSorted, id)
	}
	storepkg.SortSnowflakeIDs(pendingSorted)

	// Phase 2: seek-based scan of Badger. Walk pending and Badger streams in
	// merge order, stopping at `limit`.
	seekKey := historyIDSeekKey(storepkg.KeyHistNode, afterRaw)
	if seekKey == nil {
		// after+1 overflow: after is already the max snowflake ID. The pending
		// set is filtered to id > afterRaw, so nothing can remain.
		return nil, nil
	}
	prefix := []byte{storepkg.KeyHistNode}

	out := make([]types.NodeID, 0, capForLimit(limit))
	pendingIdx := 0
	emitted := make(map[snowflake.ID]struct{}) // dedup across pending + Badger

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var lastBadger snowflake.ID
		var haveLastBadger bool

		for it.Seek(seekKey); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			if _, deleted := pendingDeletes[string(key)]; deleted {
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			if id <= afterRaw {
				continue // belt-and-braces; seekKey already excludes this.
			}
			if haveLastBadger && id == lastBadger {
				continue // skip same-node version-suffix repeats.
			}
			lastBadger = id
			haveLastBadger = true

			// Drain pending entries strictly less than the current Badger ID.
			for pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] < id {
				pid := pendingSorted[pendingIdx]
				pendingIdx++
				if _, dup := emitted[pid]; dup {
					continue
				}
				emitted[pid] = struct{}{}
				out = append(out, types.NodeID(pid))
				if limit > 0 && len(out) >= limit {
					return nil
				}
			}
			// Pending may also contain `id` (same id flushed and unflushed simultaneously).
			if pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] == id {
				pendingIdx++
			}

			if _, dup := emitted[id]; dup {
				continue
			}
			emitted[id] = struct{}{}
			out = append(out, types.NodeID(id))
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		// Drain any remaining pending entries past the end of Badger.
		for pendingIdx < len(pendingSorted) {
			pid := pendingSorted[pendingIdx]
			pendingIdx++
			if _, dup := emitted[pid]; dup {
				continue
			}
			emitted[pid] = struct{}{}
			out = append(out, types.NodeID(pid))
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan node history IDs: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
