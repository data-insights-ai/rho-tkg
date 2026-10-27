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

// Node-history methods. Helpers shared with rels: badgerstore_history.go; rel
// history: badgerstore_history_rel.go.

func (bs *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	return bs.removeNodeLabelTokenWithHistoryRouted(nid, tok, updatedNode, prevVersion, prevState, 0)
}

// RemoveNodeLabelTokenWithHistoryScoped is RemoveNodeLabelTokenWithHistory with the
// change-log record routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) RemoveNodeLabelTokenWithHistoryScoped(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node, token uint64) error {
	if token == 0 {
		return bs.RemoveNodeLabelTokenWithHistory(nid, tok, updatedNode, prevVersion, prevState)
	}
	return bs.removeNodeLabelTokenWithHistoryRouted(nid, tok, updatedNode, prevVersion, prevState, token)
}

func (bs *Store) removeNodeLabelTokenWithHistoryRouted(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node, token uint64) error {
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

	histData, err := bs.historyNodeValue(id, uint64(prevVersion), prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(updatedNode, true)
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
	bs.departLabelMemberLocked(tok, nid) // TXMEMBERS: a former member from now on

	// Property/temporal/vector indexes for the new node state; the cache
	// (the new current row) is published last, by publishMoveLocked.
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(updatedNode)) // BACKLOG 10c
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(prevState))   // BACKLOG 10c — the demoted history row
	bs.recordNodeReplacedLocked(prevState, updatedNode)            // backlog 8 — and its property values
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = bs.maintainPropertyIndexesUpdate(ops, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, updatedNode, id)

	// Single appendOps call — node data + history + label index delete — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.publishMoveLocked(0, ops, func() { bs.putMovedNodeLocked(id, updatedNode) })
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

func (bs *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	return bs.addNodeLabelTokenWithHistoryRouted(nid, tok, updatedNode, prevVersion, prevState, 0)
}

// AddNodeLabelTokenWithHistoryScoped is AddNodeLabelTokenWithHistory with the
// change-log record routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) AddNodeLabelTokenWithHistoryScoped(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node, token uint64) error {
	if token == 0 {
		return bs.AddNodeLabelTokenWithHistory(nid, tok, updatedNode, prevVersion, prevState)
	}
	return bs.addNodeLabelTokenWithHistoryRouted(nid, tok, updatedNode, prevVersion, prevState, token)
}

func (bs *Store) addNodeLabelTokenWithHistoryRouted(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node, token uint64) error {
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

	histData, err := bs.historyNodeValue(id, uint64(prevVersion), prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(updatedNode, true)
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

	// Add tok to the in-memory label index.
	if !bs.labelOnDisk {
		bs.addLabelIdxLocked(tok, nid)
	}
	bs.getOrCreateLabelCounter(tok).Add(1)
	bs.recordNodeLabelMembersLocked(updatedNode, updatedNode) // transaction-time label membership (new token)

	// Property/temporal/vector indexes for the new node state; the cache
	// (the new current row) is published last, by publishMoveLocked.
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(updatedNode)) // BACKLOG 10c
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(prevState))   // BACKLOG 10c — the demoted history row
	bs.recordNodeReplacedLocked(prevState, updatedNode)            // backlog 8 — and its property values
	bs.addNodePropertyKeyCounts(updatedNode)
	ops = bs.maintainPropertyIndexesUpdate(ops, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, updatedNode, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(updatedNode, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, updatedNode, id)

	// Single appendOps call — node data + history + label index set — atomic in the pending buffer.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.publishMoveLocked(0, ops, func() { bs.putMovedNodeLocked(id, updatedNode) })
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

func (bs *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	return bs.replaceNodeWithHistoryRouted(current, prevVersion, prevState, 0)
}

// ReplaceNodeWithHistoryScoped is ReplaceNodeWithHistory with the change-log
// record routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) ReplaceNodeWithHistoryScoped(current *types.Node, prevVersion uint32, prevState *types.Node, token uint64) error {
	if token == 0 {
		return bs.ReplaceNodeWithHistory(current, prevVersion, prevState)
	}
	return bs.replaceNodeWithHistoryRouted(current, prevVersion, prevState, token)
}

func (bs *Store) replaceNodeWithHistoryRouted(current *types.Node, prevVersion uint32, prevState *types.Node, token uint64) error {
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
	histData, err := bs.historyNodeValue(id, uint64(prevVersion), prevState)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	changePayload, err := bs.buildNodePutPayload(current, true)
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
	if err := bs.checkNodeIndexKeysLocked(current, nid.SnowflakeID()); err != nil { // KEYSIZE, before any mutation
		bs.idxMu.Unlock()
		return err
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
	indexpkg.RemoveNodeFromHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, old, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	// Apply the new vector-index entries right after the old ones are purged,
	// before any other new-state RAM mutation commits (lesson 4, BACKLOG 18e).
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id); err != nil {
		bs.idxMu.Unlock()
		return err
	}
	bs.bumpNodeRevLocked(nid)
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(current))   // BACKLOG 10c
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(prevState)) // BACKLOG 10c — the demoted history row
	bs.recordNodeReplacedLocked(prevState, current)              // backlog 8 — and its property values
	bs.addNodePropertyKeyCounts(current)
	ops = bs.maintainPropertyIndexesUpdate(ops, current, id)
	indexpkg.AddNodeToTemporalIndexes(bs.validAxis.Get(), bs.temporalIndexes, current, id)
	ops = append(ops, bs.maintainTemporalIndexDiskAdd(current, id)...)
	indexpkg.AddNodeToHighFrequencyIndexes(bs.validAxis.Get(), bs.hfIndexes, current, id)

	// Single appendOps call — atomic in the pending buffer — then the cache.
	histKey := storepkg.HistNodeKey(id, uint64(prevVersion))
	ops = append(ops,
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.publishMoveLocked(0, ops, func() { bs.putMovedNodeLocked(id, current) })
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodePut, changePayload, token)
	bs.idxMu.Unlock()
	if logErr != nil {
		return logErr
	}

	return bs.flushIfNeeded()
}

func (bs *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	return bs.deleteNodeWithHistoryRouted(nid, prevNodeVersion, nodeTombstone, relTombstones, 0)
}

// DeleteNodeWithHistoryScoped is DeleteNodeWithHistory with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) DeleteNodeWithHistoryScoped(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone, token uint64) error {
	if token == 0 {
		return bs.DeleteNodeWithHistory(nid, prevNodeVersion, nodeTombstone, relTombstones)
	}
	return bs.deleteNodeWithHistoryRouted(nid, prevNodeVersion, nodeTombstone, relTombstones, token)
}

func (bs *Store) deleteNodeWithHistoryRouted(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpNodeEpoch()
	defer bs.bumpRelEpoch() // the delete removes the node's relationships with it
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevNodeVersion, nodeTombstone); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	// Serialize all tombstones OUTSIDE lock.
	nodeData, err := bs.historyNodeValue(id, uint64(prevNodeVersion), nodeTombstone)
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
	// Build the delete record (node + rel tombstones, cascaded rel IDs) before the
	// lock so a marshal error aborts before any op is enqueued.
	delPayload, err := bs.nodeDeleteWithHistoryPayload(id, nodeTombstone, relTombstones)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}

	prefetched, err := bs.prefetchCascadeDeleteRows(nid)
	if err != nil {
		return err
	}

	// Acquire lock ONCE — hold it across cascade + tombstone appends (lock ordering rule).
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
	// The tombstones (node + relationships) go to the SAME pending map, ahead
	// of the cascade's cache removals (publishMoveLocked, backlog 32).
	ops := make([]writeOp, 0, 1+len(relEntries))
	ops = append(ops, writeOp{opType: writeOpSet, key: nodeHistKey, value: nodeData})
	for _, e := range relEntries {
		ops = append(ops, writeOp{opType: writeOpSet, key: e.key, value: e.data})
	}
	marker := historyRowMarker(nodeTombstone.Temporal())
	for _, rt := range relTombstones {
		marker = max(marker, historyRowMarker(rt.Tombstone.Temporal()))
	}
	_, corruptErr, fatalErr := bs.cascadeDeleteInner(nid, prefetched, ops, marker)
	if fatalErr != nil {
		bs.idxMu.Unlock()
		return fatalErr
	}
	bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(nodeTombstone)) // BACKLOG 10c
	bs.recordNodeRowLocked(nodeTombstone, false)                     // backlog 8
	bs.forgetNodePropTxCurrentLocked(nodeTombstone)                  // ledger row 62: no current row any more
	for _, rt := range relTombstones {
		bs.bumpRelBeliefWatermarkLocked(rt.ID, relTxFrom(rt.Tombstone)) // BACKLOG 10c
		bs.recordRelRowLocked(rt.Tombstone, false)                      // backlog 8
		bs.forgetRelPropTxCurrentLocked(rt.Tombstone)                   // ledger row 62
	}
	logErr := bs.logChangeRoutedRaw(storecontract.ChangeNodeDelete, delPayload, token)
	bs.idxMu.Unlock()

	if corruptErr != nil {
		return corruptErr
	}
	if logErr != nil {
		return logErr
	}
	return bs.flushIfNeeded()
}

func (bs *Store) validateDeleteNodeRelTombstonesLocked(nid types.NodeID, relTombstones []RelTombstone, prefetched map[types.RelID]RelDeleteInfo) error {
	if !bs.nodeIDs.Has(int64(nid)) {
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
		if info, ok := prefetched[relID]; ok && bs.relDeleteInfoRevCurrentLocked(info) && bs.relDeleteInfoStillIndexedLocked(info) {
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
	return bs.putNodeVersionRouted(nid, version, n, 0)
}

// PutNodeVersionScoped is PutNodeVersion with the change-log record routed to the
// ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) PutNodeVersionScoped(nid types.NodeID, version uint32, n *types.Node, token uint64) error {
	if token == 0 {
		return bs.PutNodeVersion(nid, version, n)
	}
	return bs.putNodeVersionRouted(nid, version, n, token)
}

func (bs *Store) putNodeVersionRouted(nid types.NodeID, version uint32, n *types.Node, token uint64) error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	defer bs.bumpNodeEpoch()
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, version, n); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	data, err := bs.historyNodeValue(id, uint64(version), n)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	logPayload, err := bs.historyVersionNodePayload(version, n)
	if err != nil {
		return fmt.Errorf("graph: encode change-log: %w", err)
	}
	key := storepkg.HistNodeKey(id, uint64(version))
	// A historical version may carry a label the current row lacks and a TxFrom that
	// raises the belief watermark (BACKLOG 10c). Op and record are enqueued under one
	// wbMu section (appendOpsLoggedRouted) for snapshot atomicity.
	err = bs.enqueueVersionAgainstLazyBuilds(
		func() bool {
			return bs.labelTxMembersBuilt.Load() || bs.nodeBeliefWatermarkBuilt.Load() || len(bs.nodePropTx) > 0 || bs.nodeHasTemporalIndexLocked(n)
		},
		func() {
			bs.bumpNodeBeliefWatermarkLocked(nid, nodeTxFrom(n))
			bs.recordNodeRowLocked(n, false)          // backlog 8: tracking or built property sidecars
			bs.extendNodeVersionTemporalLocked(n, id) // backlog 56: a covered node's envelope holds every row
		},
		func() error {
			return bs.appendOpsLoggedRoutedRaising(historyRowMarker(n.Temporal()), storecontract.ChangeNodeHistoryVersion, logPayload, token, writeOp{opType: writeOpSet, key: key, value: data})
		},
	)
	if err != nil {
		return err
	}
	return bs.flushIfNeeded()
}

func (bs *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	return bs.GetNodeVersionSharing(nid, version, nil)
}

// GetNodeVersionSharing is GetNodeVersion decoded beside share
// (store.NodeVersionSharingCapability; nil shares nothing).
func (bs *Store) GetNodeVersionSharing(nid types.NodeID, version uint32, share *types.Node) (*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	key := storepkg.HistNodeKey(id, uint64(version))

	// Check pending and the snapshot a flush is mid-committing (`flushing`): pending
	// alone would miss a parked row and return ErrVersionNotFound for a just-written version.
	op, found := bs.lookupPending(string(key))

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		n, err := bs.decodeHistoryNodeValueSharing(id, uint64(version), op.value, share)
		if err != nil {
			return nil, err
		}
		return n, nil // a fresh decode: the caller's own row, no second copy
	}

	// A full row decodes inside the View; a delta row is copied out first,
	// because its reconstruction point-reads the anchor via a separate read txn.
	var raw []byte
	var n *types.Node
	err := bs.view(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
				var derr error
				n, derr = bs.decodeHistoryNodeValueSharing(id, uint64(version), val, share)
				return derr
			}
			raw = make([]byte, len(val))
			copy(raw, val)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if n != nil {
		return n, nil // a fresh decode: the caller's own row, no second copy
	}
	n, err = bs.decodeHistoryNodeValueSharing(id, uint64(version), raw, share)
	if err != nil {
		return nil, err
	}
	return n, nil // a fresh decode: the caller's own row, no second copy
}

// decodeHistoryNodeValue reconstructs (full-or-delta) then decodes one node
// history value to a *types.Node. Point-reads the interval anchor for a delta.
func (bs *Store) decodeHistoryNodeValue(id snowflake.ID, version uint64, raw []byte) (*types.Node, error) {
	return bs.decodeHistoryNodeValueSharing(id, version, raw, nil)
}

// decodeHistoryNodeValueSharing is decodeHistoryNodeValue whose full-row
// decode takes the string values equal to share's from share (the node's
// current row, lent beside it: R37); nil shares none.
func (bs *Store) decodeHistoryNodeValueSharing(id snowflake.ID, version uint64, raw []byte, share *types.Node) (*types.Node, error) {
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		// A full row needs no anchor: decode into a pooled wire (row 121).
		w := getPooledNodeWire()
		defer putPooledNodeWire(w)
		if err := storepkg.UnmarshalNodeWireSharing(raw, w, share); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(*w, id, version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode node version: %w", err)
		}
		return n, nil
	}
	w, err := bs.reconstructNodeHistoryWire(id, version, raw, nil)
	if err != nil {
		return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
	}
	n, err := bs.decodeNodeHistoryWireForKey(w, id, version)
	if err != nil {
		return nil, fmt.Errorf("graph: decode node version: %w", err)
	}
	return n, nil
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
	if top, ok := bs.historyTopBound(&bs.histNodePresence, id); ok && top < startVersion {
		return nil, nil // nothing at or above startVersion: no iterator
	}
	prefix := storepkg.HistNodePrefix(id)
	return bs.nodeHistoryVersionsFromPrefix(prefix, startVersion, limit)
}

func (bs *Store) getNodeHistoryByPrefix(prefix []byte) ([]*types.Node, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)

	// Snapshot the write-buffer overlay (flushing ++ pending) BEFORE opening the
	// Badger View. flush() commits a row to Badger and only then clears `flushing`;
	// overlay-after-View could drop a row committed in between from both views
	// (lesson 64).
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)

	// Collect from Badger.
	entries := make(map[string][]byte) // key string -> value bytes
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // values are inline: no goroutine per row (PREFETCH)
		opts.Prefix = prefix        // bound the scan and the value prefetch to this entity (backlog 20)
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

	// Merge the pre-scan overlay (authoritative, newer than committed state): a delete
	// masks a scanned row, a set overwrites it. History version keys are immutable, so
	// overlay and scan bytes agree for the same key.
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

	// The whole chain is in `entries`, so a delta's interval anchor is available
	// in-memory — no extra point read.
	localAnchor := func(anchorVer uint64) ([]byte, bool) {
		b, ok := entries[string(storepkg.HistNodeKey(expectedID, anchorVer))]
		return b, ok
	}
	result := make([]*types.Node, 0, len(keys))
	for _, k := range keys {
		version := historyVersionFromKey([]byte(k))
		w, err := bs.reconstructNodeHistoryWire(expectedID, version, entries[k], localAnchor)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, expectedID, version)
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

	collected := make([]historyRawRow, 0, capForLimit(limit))
	reachedLimit := func() bool {
		return limit > 0 && len(collected) >= limit
	}
	// No error return: appending to a slice cannot fail, and threading a
	// permanently-nil error through four call sites only obscures that.
	emit := func(key string, data []byte) {
		raw := make([]byte, len(data))
		copy(raw, data)
		collected = append(collected, historyRawRow{version: historyVersionFromKey([]byte(key)), raw: raw})
	}

	pendingIdx := 0
	// No error return, for the same reason as emit: with emit errorless this can
	// no longer fail either. Dropping it here is the cascade of that change.
	drainPendingBefore := func(bound string) {
		for pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] < bound {
			emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]])
			pendingIdx++
			if reachedLimit() {
				return
			}
		}
	}

	seekKey := historyVersionSeekKey(prefix, startVersion)
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // values are inline: no goroutine per row (PREFETCH)
		opts.Prefix = prefix        // bound the scan and the value prefetch to this entity (backlog 20)
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
			drainPendingBefore(k)
			if reachedLimit() {
				return nil
			}
			if pendingIdx < len(pendingKeys) && pendingKeys[pendingIdx] == k {
				emit(k, pending[k])
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
				emit(k, val)
				return nil
			}); err != nil {
				return err
			}
			if reachedLimit() {
				return nil
			}
		}
		for pendingIdx < len(pendingKeys) {
			emit(pendingKeys[pendingIdx], pending[pendingKeys[pendingIdx]])
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
	if len(collected) == 0 {
		return nil, nil
	}
	// Reconstruct after the scan closes: a delta whose anchor precedes startVersion is
	// point-read; anchors inside the window come from the collected rows.
	anchorCache := make(map[uint64][]byte, len(collected))
	for i := range collected {
		anchorCache[collected[i].version] = collected[i].raw
	}
	local := func(anchorVer uint64) ([]byte, bool) { b, ok := anchorCache[anchorVer]; return b, ok }
	result := make([]*types.Node, 0, len(collected))
	for i := range collected {
		w, err := bs.reconstructNodeHistoryWire(expectedID, collected[i].version, collected[i].raw, local)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, expectedID, collected[i].version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode node version: %w", err)
		}
		result = append(result, n.DeepCopy())
	}
	return result, nil
}

func (bs *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	return bs.truncateNodeHistoryRouted(nid, keepVersions, 0)
}

// TruncateNodeHistoryScoped is TruncateNodeHistory with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) TruncateNodeHistoryScoped(nid types.NodeID, keepVersions int, token uint64) error {
	if token == 0 {
		return bs.TruncateNodeHistory(nid, keepVersions)
	}
	return bs.truncateNodeHistoryRouted(nid, keepVersions, token)
}

func (bs *Store) truncateNodeHistoryRouted(nid types.NodeID, keepVersions int, token uint64) error {
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
	return bs.truncateHistoryByPrefixRouted(prefix, keepVersions, storecontract.ChangeNodeHistoryTruncate, payload, token)
}

// TrimNodeHistoryFrom removes node history entries at or after minVersion.
func (bs *Store) TrimNodeHistoryFrom(nid types.NodeID, minVersion uint32) error {
	return bs.trimNodeHistoryFromRouted(nid, minVersion, 0)
}

// TrimNodeHistoryFromScoped is TrimNodeHistoryFrom with the change-log record
// routed to the ScopedTxChangeLog buffer named by token (0 = unscoped).
func (bs *Store) TrimNodeHistoryFromScoped(nid types.NodeID, minVersion uint32, token uint64) error {
	if token == 0 {
		return bs.TrimNodeHistoryFrom(nid, minVersion)
	}
	return bs.trimNodeHistoryFromRouted(nid, minVersion, token)
}

func (bs *Store) trimNodeHistoryFromRouted(nid types.NodeID, minVersion uint32, token uint64) error {
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
	return bs.trimHistoryFromPrefixRouted(prefix, minVersion, storecontract.ChangeNodeHistoryTruncate, payload, token)
}

func (bs *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	// Once the RAM history-presence set is built, walk it instead of the
	// keyspace (round 5 requests 7 and 8; forEachPresentHistoryID).
	if ok, err := bs.forEachPresentHistoryID(&bs.histNodePresence, storepkg.KeyHistNode, nil, func(id snowflake.ID) bool {
		return fn(types.NodeID(id))
	}); ok {
		return err
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

// ForEachDeletedNodeID iterates IDs that have history rows but no current row
// (inline HasNodeID check, no extra Badger reads). Once the RAM history-presence
// set is built (forEachPresentHistoryID) IDs come from it in ascending order.
// Callbacks run outside Badger transactions.
func (bs *Store) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if ok, err := bs.forEachPresentHistoryID(&bs.histNodePresence, storepkg.KeyHistNode, bs.HasNodeID, func(id snowflake.ID) bool {
		return fn(types.NodeID(id))
	}); ok {
		return err
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

// MaxNodeHistoryID returns the highest node ID with version history, including
// pending buffered writes; zero when there is none.
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

	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(seekKey); it.ValidForPrefix(prefix); {
			key := it.Item().Key()
			if len(key) != storepkg.SizeHistKey {
				it.Next()
				continue
			}
			if _, deleted := pendingDeletes[string(key)]; deleted {
				// Only this version row is masked; the id may still be emitted via another row, so step to the next row.
				it.Next()
				continue
			}
			id := storepkg.ParseIDFromKey(key, 1)
			if id <= afterRaw {
				it.Next()
				continue // belt-and-braces; seekKey already excludes this.
			}

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

			if _, dup := emitted[id]; !dup {
				emitted[id] = struct{}{}
				out = append(out, types.NodeID(id))
				if limit > 0 && len(out) >= limit {
					return nil
				}
			}
			// Id decided: Seek past its remaining version rows so the scan is O(distinct IDs),
			// not O(version rows).
			next := historyIDSeekKey(storepkg.KeyHistNode, id)
			if next == nil {
				break // id is the max snowflake ID — no successor exists.
			}
			it.Seek(next)
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

var _ storecontract.NodeVersionSharingCapability = (*Store)(nil)
