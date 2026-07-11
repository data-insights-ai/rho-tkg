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
	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)

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
	bs.nodeCache.Put(id, freezeNodeCopy(updatedNode))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = append(ops, bs.maintainPropertyIndexesAdd(updatedNode, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — node data + history + label index delete — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.appendOps(ops...)
	logErr := bs.logNodePut(updatedNode, true)
	bs.idxMu.Unlock()

	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
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
	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)

	// Add tok to the in-memory label index.
	if !bs.labelOnDisk {
		set, ok := bs.labelIdx[tok]
		if !ok {
			set = make(map[types.NodeID]struct{})
			bs.labelIdx[tok] = set
		}
		set[nid] = struct{}{}
	}
	bs.getOrCreateLabelCounter(tok).Add(1)
	bs.recordNodeLabelMembersLocked(updatedNode) // K1: transaction-time label membership (new token)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, freezeNodeCopy(updatedNode))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(updatedNode)
	bs.bumpNodeRevLocked(nid)
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = append(ops, bs.maintainPropertyIndexesAdd(updatedNode, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, updatedNode, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — node data + history + label index set — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.appendOps(ops...)
	logErr := bs.logNodePut(updatedNode, true)
	bs.idxMu.Unlock()

	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
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

	bs.removeNodePropertyKeyCounts(old)
	ops := bs.maintainPropertyIndexesRemove(old, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
	ops = append(ops, bs.maintainTemporalIndexDiskRemove(old, id)...)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	bs.nodeCache.Put(id, freezeNodeCopy(current))
	bs.nodeHashes[nid] = badgerNodeIntegrityHash(current)
	bs.bumpNodeRevLocked(nid)
	bs.addNodePropertyKeyCounts(current)
	ops = append(ops, bs.maintainPropertyIndexesAdd(current, id)...)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, current, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(current, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.hfIndexes, current, id)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}

	// Single appendOps call — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.appendOps(ops...)
	logErr := bs.logNodePut(current, true)
	bs.idxMu.Unlock()

	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
}

func (bs *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpNodeEpoch()
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
	// Build the with-history delete record (node tombstone + rel tombstones +
	// cascaded rel IDs) before taking the lock, so a marshal error aborts before
	// any op is enqueued.
	delPayload, err := bs.nodeDeleteWithHistoryPayload(id, nodeTombstone, relTombstones)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
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
	bs.logChangeRaw(storecontract.ChangeNodeDelete, delPayload)
	bs.idxMu.Unlock()

	if corruptErr != nil {
		return corruptErr
	}
	return bs.flushIfNeeded()
}

func (bs *Store) validateDeleteNodeRelTombstonesLocked(nid types.NodeID, relTombstones []RelTombstone, prefetched map[types.RelID]RelDeleteInfo) error {
	if _, exists := bs.nodeIDs[nid]; !exists {
		return ErrNodeNotFound
	}

	relIDs := make(map[types.RelID]struct{})
	out, outErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, false)
	if outErr != nil {
		return fmt.Errorf("graph: tombstone adjacency snapshot: %w", outErr)
	}
	in, inErr := bs.adjacentRelIDsSnapshotLocked(nid, 0, true)
	if inErr != nil {
		return fmt.Errorf("graph: tombstone adjacency snapshot: %w", inErr)
	}
	for _, relID := range append(out, in...) {
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
	defer bs.bumpNodeEpoch()
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, version, n); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.marshalNodeToBytes(n)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	logPayload, err := bs.historyVersionNodePayload(version, n)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	key := storepkg.HistNodeKey(id, uint64(version))
	// K1: a historical version may carry a label the current row no longer has.
	// Only lock when the sidecar is already built (the import/replica bootstrap
	// common case runs before any pinned scan, so the flag is false and the lazy
	// build catches these history rows).
	if bs.labelTxMembersBuilt.Load() {
		bs.idxMu.Lock()
		bs.recordNodeLabelMembersLocked(n)
		bs.idxMu.Unlock()
	}
	// PutNodeVersion holds no idxMu, so enqueue the op and its record together
	// under one wbMu critical section (appendOpsLogged) for snapshot atomicity.
	bs.appendOpsLogged(storecontract.ChangeNodeHistoryVersion, logPayload, writeOp{opType: writeOpSet, key: key, value: data})
	return bs.flushIfNeeded()
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

	// Check the write buffer (pending + the snapshot a concurrent flush is
	// mid-committing) for an unflushed write. Consulting only `pending` would
	// miss a row parked in `flushing` during the commit window and wrongly
	// return ErrVersionNotFound for a version that was just written.
	op, found := bs.lookupPending(string(key))

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(op.value, &w); err != nil {
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
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
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

	// Snapshot the write-buffer overlay (flushing ++ pending) BEFORE opening the
	// Badger View. Ordering matters: a concurrent flush() commits a parked row to
	// Badger and THEN clears `flushing`, so a row is in `flushing` until strictly
	// after it lands in Badger. If we scanned Badger first (at snapshot time Ts)
	// and read the overlay second (at Tr > Ts), a flush that committed the row in
	// (Ts, Tr) would leave it in NEITHER view — invisible to the older Badger
	// snapshot AND to the already-cleared `flushing` — a dropped in-flight row
	// (load-dependent: a descheduled flush goroutine widens the window). Capturing
	// the overlay first (at Ta) and opening the View second (Tb >= Ta) closes the
	// window: any row committed after Ta was still in `flushing` at Ta and is in
	// the snapshot; any row committed before Ta is durable and in the View. See
	// lesson 64.
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

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

	if bs.historyScanTestHook != nil {
		bs.historyScanTestHook()
	}

	// Merge the pre-scan overlay snapshot (authoritative — it is strictly newer
	// than any committed Badger state): a delete masks a scanned row, a set
	// overwrites it. History version keys are append-only-immutable, so an
	// overlay set and a scanned row for the same key carry identical bytes.
	for k := range overlayDeletes {
		delete(entries, k)
	}
	for k, v := range overlay {
		entries[k] = v
	}

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
		if err := storepkg.SafeUnmarshal(entries[k], &w); err != nil {
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
		if err := storepkg.SafeUnmarshal(data, &w); err != nil {
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
	payload, err := bs.buildChangePayload(storepkg.HistoryTruncateBody{ID: int64(id), Bound: int64(keepVersions)})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	return bs.truncateHistoryByPrefix(prefix, keepVersions, storecontract.ChangeNodeHistoryTruncate, payload)
}

// TrimNodeHistoryFrom removes node history entries at or after minVersion.
func (bs *Store) TrimNodeHistoryFrom(nid types.NodeID, minVersion uint32) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	payload, err := bs.buildChangePayload(storepkg.HistoryTruncateBody{ID: int64(id), IsTrim: true, Bound: int64(minVersion)})
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	return bs.trimHistoryFromPrefix(prefix, minVersion, storecontract.ChangeNodeHistoryTruncate, payload)
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
