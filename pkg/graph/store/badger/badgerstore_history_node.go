package badger

import (
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Node-history methods (R5-F9 split out from badgerstore_history.go).
// Helpers shared between node and rel paths stay in badgerstore_history.go;
// rel-history methods live in badgerstore_history_rel.go.

func (bs *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	hw := storepkg.NodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
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
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	hw := storepkg.NodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
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

	// Add tok to the in-memory label index.
	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

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
	nid := current.InternalID()
	id := nid.SnowflakeID()

	// Serialize current state.
	w := storepkg.NodeToWire(current)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Serialize history snapshot.
	hw := storepkg.NodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, prevState, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, prevState, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, prevState, id)
	bs.nodeCache.Put(id, current.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, current, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, current, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, current, id)

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
	id := nid.SnowflakeID()
	// Serialize all tombstones OUTSIDE lock (B3).
	nodeData, err := marshalNodeToBytes(nodeTombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal node tombstone: %w", err)
	}
	nodeHistKey := storepkg.HistNodeKey(id, uint64(prevNodeVersion))

	type histEntry struct{ key, data []byte }
	relEntries := make([]histEntry, 0, len(relTombstones))
	for _, rt := range relTombstones {
		data, err := marshalRelToBytes(rt.Tombstone)
		if err != nil {
			return fmt.Errorf("graph: marshal rel tombstone: %w", err)
		}
		relEntries = append(relEntries, histEntry{
			key:  storepkg.HistRelKey(rt.ID.SnowflakeID(), uint64(rt.PrevVersion)),
			data: data,
		})
	}

	// Acquire lock ONCE — hold it across cascade + tombstone appends (B3 + lock ordering rule).
	bs.idxMu.Lock()
	_, corruptErr, fatalErr := bs.cascadeDeleteInner(nid)
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

func marshalNodeToBytes(n *types.Node) ([]byte, error) {
	return msgpack.Marshal(storepkg.NodeToWire(n))
}

func (bs *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(n)
	data, err := msgpack.Marshal(w)
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
		n := storepkg.WireToNode(w)
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
			n = storepkg.WireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

func (bs *Store) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.getNodeHistoryByPrefix(prefix)
}

func (bs *Store) getNodeHistoryByPrefix(prefix []byte) ([]*types.Node, error) {
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
			k := string(item.KeyCopy(nil))
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
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
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
		n := storepkg.WireToNode(w)
		result = append(result, n.DeepCopy())
	}
	return result, nil
}

func (bs *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

func (bs *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: pending buffer.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistNode {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Emit pending IDs.
	for id := range seen {
		if !fn(types.NodeID(id)) {
			return nil
		}
	}

	// Phase 2: Badger prefix scan.
	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{storepkg.KeyHistNode}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= storepkg.SizeHistKey {
				id := storepkg.ParseIDFromKey(key, 1)
				if _, ok := seen[id]; ok {
					continue // already emitted
				}
				seen[id] = struct{}{}
				if !fn(types.NodeID(id)) {
					return nil
				}
			}
		}
		return nil
	})
}

func (bs *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	return bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
}

func (bs *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	afterRaw := snowflake.ID(after)

	// Phase 1: collect candidate IDs from pending buffer. We materialise the
	// full pending set once (typically small — bounded by FlushInterval), then
	// merge with the Badger scan in a sorted walk so we can stop at `limit`.
	pending := make(map[snowflake.ID]struct{})
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistNode {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			if id > afterRaw {
				pending[id] = struct{}{}
			}
		}
	}
	bs.wbMu.Unlock()

	pendingSorted := make([]snowflake.ID, 0, len(pending))
	for id := range pending {
		pendingSorted = append(pendingSorted, id)
	}
	sort.Slice(pendingSorted, func(i, j int) bool { return pendingSorted[i] < pendingSorted[j] })

	// Phase 2: seek-based scan of Badger. Walk pending and Badger streams in
	// merge order, stopping at `limit`.
	seekKey := historyIDSeekKey(storepkg.KeyHistNode, afterRaw)
	if seekKey == nil {
		// after+1 overflow — only the pending stream can contribute (shouldn't
		// happen in practice; defensive).
		return paginateRawNodeIDs(pendingSorted, limit), nil
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
			if len(key) < storepkg.SizeHistKey {
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

func paginateRawNodeIDs(ids []snowflake.ID, limit int) []types.NodeID {
	if len(ids) == 0 {
		return nil
	}
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out
}
