// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
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

// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
// writes a version history entry, and persists updatedNode via a single appendOps call.
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

// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
// writes a version history entry, and persists updatedNode via a single appendOps call.
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

// ReplaceNodeWithHistory atomically replaces a node and writes a version history entry.
// Both operations are queued in a single appendOps call — the flush loop cannot
// snapshot one without the other.
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

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both operations are queued in a single appendOps call.
func (bs *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	rid := current.InternalID()
	id := rid.SnowflakeID()

	// Serialize current state.
	w := storepkg.RelToWire(current)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	// Serialize history snapshot.
	hw := storepkg.RelToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[rid]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}

	bs.relCache.Put(id, current.DeepCopy())

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.RelKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in one batch flush.
//
// Serializes tombstone data outside the lock (B3), then holds idxMu.Lock() across
// both the live delete and the tombstone history append so both ops land in the
// same pending map before the next flush. Atomic within this shard.
func (bs *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	id := rid.SnowflakeID()
	// Serialize tombstone OUTSIDE lock (B3: no I/O under write lock).
	w := storepkg.RelToWire(tombstone)
	tombData, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal rel tombstone: %w", err)
	}
	histKey := storepkg.HistRelKey(id, uint64(prevVersion))

	bs.idxMu.Lock()
	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	info := RelDeleteInfo{
		ID:      id,
		RelType: r.TypeToken().Value(),
		StartID: r.StartNodeID().SnowflakeID(),
		EndID:   r.EndNodeID().SnowflakeID(),
	}
	bs.deleteRelByInfo(info) // appends delete ops to pending under lock
	bs.appendOps(writeOp{opType: writeOpSet, key: histKey, value: tombData})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteNodeWithHistory atomically combines PutRelVersion×N + PutNodeVersion +
// DeleteNodeCascade into a single batch flush.
//
// Serializes all tombstone data outside the lock (B3), then holds idxMu.Lock()
// across cascadeDeleteInner AND the tombstone history appends so all ops land in
// the same pending map. The background flush goroutine acquires idxMu.RLock() for
// its snapshot phase, so it is blocked until we release — guaranteeing all ops
// commit atomically.
//
// Cross-shard atomicity: per-shard only (same B7 limitation as DeleteNodeCascade).
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

// marshalNodeToBytes serializes a Node to msgpack bytes via the wire format.
func marshalNodeToBytes(n *types.Node) ([]byte, error) {
	return msgpack.Marshal(storepkg.NodeToWire(n))
}

// marshalRelToBytes serializes a Relationship to msgpack bytes via the wire format.
func marshalRelToBytes(r *types.Relationship) ([]byte, error) {
	return msgpack.Marshal(storepkg.RelToWire(r))
}

// PutNodeVersion stores a node snapshot at the given version.
// Serializes via nodeToWire (deep copy at serialization boundary).
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

// GetNodeVersion retrieves a node snapshot at the given version.
// Checks the pending buffer first (unflushed writes), then Badger.
// Returns ErrVersionNotFound if the version does not exist.
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

// GetNodeHistory returns all node version snapshots in ascending version order.
// Merges persisted Badger entries with unflushed pending buffer entries.
func (bs *Store) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.getNodeHistoryByPrefix(prefix)
}

// getNodeHistoryByPrefix scans Badger and the pending buffer for node history entries.
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

// TruncateNodeHistory removes all but the N most recent node versions.
// If keepVersions <= 0, all history is cleared.
func (bs *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// PutRelVersion stores a relationship snapshot at the given version.
// Serializes via relToWire (deep copy at serialization boundary).
func (bs *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	id := rid.SnowflakeID()
	w := storepkg.RelToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	key := storepkg.HistRelKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetRelVersion retrieves a relationship snapshot at the given version.
// Checks the pending buffer first, then Badger.
// Returns ErrVersionNotFound if the version does not exist.
func (bs *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	key := storepkg.HistRelKey(id, uint64(version))

	// Check pending buffer.
	bs.wbMu.Lock()
	op, found := bs.pending[string(key)]
	bs.wbMu.Unlock()

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(op.value, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r := storepkg.WireToRel(w)
		return r.DeepCopy(), nil
	}

	// Fall through to Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.RelWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal rel version: %w", err)
			}
			r = storepkg.WireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

// GetRelHistory returns all relationship version snapshots in ascending version order.
// Merges persisted Badger entries with unflushed pending buffer entries.
func (bs *Store) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.getRelHistoryByPrefix(prefix)
}

// getRelHistoryByPrefix scans Badger and the pending buffer for rel history entries.
func (bs *Store) getRelHistoryByPrefix(prefix []byte) ([]*types.Relationship, error) {
	prefixStr := string(prefix)

	entries := make(map[string][]byte)
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

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]*types.Relationship, 0, len(keys))
	for _, k := range keys {
		var w storepkg.RelWire
		if err := msgpack.Unmarshal(entries[k], &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r := storepkg.WireToRel(w)
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

// TruncateRelHistory removes all but the N most recent relationship versions.
// If keepVersions <= 0, all history is cleared.
func (bs *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// truncateHistoryByPrefix removes all but the N most recent history entries
// matching the given prefix. Scans both Badger and the pending buffer.
func (bs *Store) truncateHistoryByPrefix(prefix []byte, keepVersions int) error {
	prefixStr := string(prefix)

	// Collect all keys from Badger.
	var allKeys []string
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			allKeys = append(allKeys, string(it.Item().KeyCopy(nil)))
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Merge in pending buffer keys.
	keySet := make(map[string]struct{}, len(allKeys))
	for _, k := range allKeys {
		keySet[k] = struct{}{}
	}
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(keySet, k)
			} else {
				keySet[k] = struct{}{}
			}
		}
	}
	bs.wbMu.Unlock()

	if len(keySet) == 0 {
		return nil
	}

	sorted := make([]string, 0, len(keySet))
	for k := range keySet {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var toDelete []string
	if keepVersions <= 0 {
		toDelete = sorted
	} else if len(sorted) > keepVersions {
		toDelete = sorted[:len(sorted)-keepVersions]
	}

	if len(toDelete) == 0 {
		return nil
	}

	ops := make([]writeOp, len(toDelete))
	for i, k := range toDelete {
		ops[i] = writeOp{opType: writeOpDelete, key: []byte(k)}
	}
	bs.appendOps(ops...)
	return nil
}

// ForEachNodeHistoryID iterates over all node IDs with version history entries.
// Scans both the pending buffer and Badger for 0x07 prefix keys.
// Iteration stops early if fn returns false.
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

// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
// Scans both the pending buffer and Badger for 0x08 prefix keys.
// Iteration stops early if fn returns false.
func (bs *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: pending buffer.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistRel {
			id := storepkg.ParseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Emit pending IDs.
	for id := range seen {
		if !fn(types.RelID(id)) {
			return nil
		}
	}

	// Phase 2: Badger prefix scan.
	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{storepkg.KeyHistRel}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= storepkg.SizeHistKey {
				id := storepkg.ParseIDFromKey(key, 1)
				if _, ok := seen[id]; ok {
					continue // already emitted
				}
				seen[id] = struct{}{}
				if !fn(types.RelID(id)) {
					return nil
				}
			}
		}
		return nil
	})
}

// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
// Thin wrapper that delegates to AllNodeHistoryIDsFrom(0, 0).
func (bs *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	return bs.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
}

// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
// Thin wrapper that delegates to AllRelHistoryIDsFrom(0, 0).
func (bs *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	return bs.AllRelHistoryIDsFrom(types.RelID(0), 0)
}

// historyIDSeekKey returns the 9-byte Badger seek key for the first history
// record whose entity ID is strictly greater than `after`. The history-key
// layout is `prefix(1B) + entityID(8B BE) + version(8B BE)`. Seeking to
// `prefix + (after+1) BE` lands on the first key whose 8B ID portion is ≥
// after+1, i.e., strictly greater than after. If after+1 overflows int64,
// returns nil to signal "no remaining keys".
func historyIDSeekKey(prefix byte, after snowflake.ID) []byte {
	// Treat zero (unset cursor) as "from the very beginning".
	if after == 0 {
		return []byte{prefix}
	}
	next := int64(after) + 1
	if next < int64(after) {
		// Overflow guard: after is already MaxInt64, no successor exists.
		return nil
	}
	out := make([]byte, 9)
	out[0] = prefix
	storepkg.PutUint64(out, 1, next)
	return out
}

// AllNodeHistoryIDsFrom returns up to `limit` distinct node IDs (sorted
// ascending) with version-history entries whose ID is strictly greater than
// `after`. limit ≤ 0 means "all remaining".
//
// Implementation: scans the pending write-buffer once to surface unflushed
// history writes, then seeks the 0x07 prefix in Badger. Distinct IDs are
// dedup'd via a small `seen` set. The ID slice is bounded by `limit` (when
// limit > 0), so memory usage is O(limit), not O(total history).
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

// AllRelHistoryIDsFrom is the relationship analogue of AllNodeHistoryIDsFrom.
func (bs *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	afterRaw := snowflake.ID(after)

	pending := make(map[snowflake.ID]struct{})
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= storepkg.SizeHistKey && k[0] == storepkg.KeyHistRel {
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

	seekKey := historyIDSeekKey(storepkg.KeyHistRel, afterRaw)
	if seekKey == nil {
		return paginateRawRelIDs(pendingSorted, limit), nil
	}
	prefix := []byte{storepkg.KeyHistRel}

	out := make([]types.RelID, 0, capForLimit(limit))
	pendingIdx := 0
	emitted := make(map[snowflake.ID]struct{})

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
				continue
			}
			if haveLastBadger && id == lastBadger {
				continue
			}
			lastBadger = id
			haveLastBadger = true

			for pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] < id {
				pid := pendingSorted[pendingIdx]
				pendingIdx++
				if _, dup := emitted[pid]; dup {
					continue
				}
				emitted[pid] = struct{}{}
				out = append(out, types.RelID(pid))
				if limit > 0 && len(out) >= limit {
					return nil
				}
			}
			if pendingIdx < len(pendingSorted) && pendingSorted[pendingIdx] == id {
				pendingIdx++
			}
			if _, dup := emitted[id]; dup {
				continue
			}
			emitted[id] = struct{}{}
			out = append(out, types.RelID(id))
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		for pendingIdx < len(pendingSorted) {
			pid := pendingSorted[pendingIdx]
			pendingIdx++
			if _, dup := emitted[pid]; dup {
				continue
			}
			emitted[pid] = struct{}{}
			out = append(out, types.RelID(pid))
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan rel history IDs: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// capForLimit returns a sane initial capacity for the page result slice. Small
// limits get an exact preallocation; "all remaining" (limit ≤ 0) gets a 0
// capacity (let append grow it) since the caller may legitimately want
// hundreds of millions of IDs.
func capForLimit(limit int) int {
	if limit > 0 && limit <= 1<<20 {
		return limit
	}
	return 0
}

// paginateRawNodeIDs trims a sorted, dedup'd ID slice to `limit` and wraps to types.NodeID.
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

// paginateRawRelIDs trims a sorted, dedup'd ID slice to `limit` and wraps to types.RelID.
func paginateRawRelIDs(ids []snowflake.ID, limit int) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	if limit > 0 && limit < len(ids) {
		ids = ids[:limit]
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out
}
