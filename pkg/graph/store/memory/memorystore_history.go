// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"sort"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, updatedNode, rawID)
	if err != nil {
		return err
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	// Remove only the specified token from the label index.
	if set, ok := ms.labelIdx[tok]; ok {
		delete(set, nid)
		if len(set) == 0 {
			delete(ms.labelIdx, tok)
		}
	}

	// Update property, temporal, and vector indexes.
	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	ms.removeNodeFromCompositeIndexesLocked(old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(updatedNode)
	ms.addNodePropertyKeyCounts(updatedNode)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	ms.addNodeToCompositeIndexesLocked(updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return ms.logNodePutLocked(updatedNode, true)
}

// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, updatedNode, rawID)
	if err != nil {
		return err
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	// Add tok to the label index.
	set, ok := ms.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		ms.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	ms.recordNodeLabelMembersLocked(updatedNode) // transaction-time label membership (new token)

	// Update property, temporal, and vector indexes.
	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	ms.removeNodeFromCompositeIndexesLocked(old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(updatedNode)
	ms.addNodePropertyKeyCounts(updatedNode)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	ms.addNodeToCompositeIndexesLocked(updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return ms.logNodePutLocked(updatedNode, true)
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in a single locked operation.
// All under one lock: atomic with respect to concurrent readers.
func (ms *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpRelEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, prevVersion, tombstone); err != nil {
		return err
	}

	old, ok := ms.rels[rid]
	if !ok {
		return ErrRelNotFound
	}
	if err := storecontract.ValidateRelationshipLiveVersion(old, prevVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, tombstone); err != nil {
		return err
	}
	// Write tombstone to history before deleting live entity.
	inner, ok := ms.relHistory[rid]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[rid] = inner
	}
	inner[prevVersion] = tombstone.DeepCopy()

	if err := ms.deleteRelLocked(rid); err != nil {
		return err
	}
	return ms.logRelDeleteWithHistoryLocked(rid.SnowflakeID(), tombstone)
}

// DeleteNodeWithHistory atomically writes tombstone history entries for the node
// and all connected relationships, then performs the cascade delete.
// All under one lock: atomic with respect to concurrent readers.
func (ms *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()
	// The cascade removes every connected relationship's tombstoned row too
	// (adjacency changes) — bumpRelEpoch, matching DeleteNodeCascade
	// (BACKLOG 17a).
	defer ms.bumpRelEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevNodeVersion, nodeTombstone); err != nil {
		return err
	}
	for _, rt := range relTombstones {
		if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rt.ID, rt.PrevVersion, rt.Tombstone); err != nil {
			return err
		}
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLiveVersion(n, prevNodeVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(n, nodeTombstone); err != nil {
		return err
	}

	relIDs := make(map[types.RelID]struct{})
	for relID := range ms.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range ms.inIdx[nid] {
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
		old, exists := ms.rels[rt.ID]
		if !exists {
			return ErrRelNotFound
		}
		if err := storecontract.ValidateRelationshipLiveVersion(old, rt.PrevVersion); err != nil {
			return err
		}
		if err := storecontract.ValidateRelationshipReplacement(old, rt.Tombstone); err != nil {
			return err
		}
	}
	for relID := range relIDs {
		if _, exists := ms.rels[relID]; !exists {
			continue
		}
		if _, ok := tombed[relID]; !ok {
			return fmt.Errorf("%w: missing relationship tombstone %d", ErrInvalidStoreMutation, relID)
		}
	}

	// Write node tombstone to history.
	nodeInner, ok := ms.nodeHistory[nid]
	if !ok {
		nodeInner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = nodeInner
	}
	nodeInner[prevNodeVersion] = nodeTombstone.DeepCopy()

	// Write rel tombstones to history.
	for _, rt := range relTombstones {
		relInner, ok := ms.relHistory[rt.ID]
		if !ok {
			relInner = make(map[uint32]*types.Relationship)
			ms.relHistory[rt.ID] = relInner
		}
		relInner[rt.PrevVersion] = rt.Tombstone.DeepCopy()
	}

	// Inline cascade: delete all connected relationships then the node.
	for relID := range relIDs {
		if err := ms.deleteRelOrPurgeOrphanLocked(relID); err != nil {
			return err
		}
	}

	// Remove label index entries.
	for i := 0; i < n.LabelTokenCount(); i++ {
		tok := n.LabelTokenRawAt(i)
		if set, exists := ms.labelIdx[tok]; exists {
			delete(set, nid)
			if len(set) == 0 {
				delete(ms.labelIdx, tok)
			}
		}
	}

	rawID := nid.SnowflakeID()
	ms.removeNodePropertyKeyCounts(n)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
	ms.removeNodeFromCompositeIndexesLocked(n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return ms.logNodeDeleteWithHistoryLocked(nid.SnowflakeID(), nodeTombstone, relTombstones)
}

// --- Version history ---

// PutNodeVersion stores a node snapshot at the given version.
// Deep-copies the node at the store boundary.
func (ms *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, version, n); err != nil {
		return err
	}

	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[version] = n.DeepCopy()
	ms.recordNodeLabelMembersLocked(n) // a historical version may carry labels the current row dropped
	return ms.logNodeHistoryVersionLocked(version, n)
}

// GetNodeVersion retrieves a node snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	inner, ok := ms.nodeHistory[nid]
	if !ok {
		return nil, ErrVersionNotFound
	}
	n, ok := inner[version]
	if !ok {
		return nil, ErrVersionNotFound
	}
	return n.DeepCopy(), nil
}

// GetNodeHistory returns all node version snapshots in ascending version order.
// Returns an empty slice if no history exists.
func (ms *Store) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	inner := ms.nodeHistory[nid]
	if len(inner) == 0 {
		return nil, nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	result := make([]*types.Node, len(versions))
	for i, v := range versions {
		result[i] = inner[v].DeepCopy()
	}
	return result, nil
}

// NodeHistoryVersionsFrom returns node history snapshots with Version() >=
// startVersion in ascending version order. limit 0 returns all remaining.
func (ms *Store) NodeHistoryVersionsFrom(nid types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}

	inner := ms.nodeHistory[nid]
	if len(inner) == 0 {
		return nil, nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		if v >= startVersion {
			versions = append(versions, v)
		}
	}
	if len(versions) == 0 {
		return nil, nil
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	if limit > 0 && len(versions) > limit {
		versions = versions[:limit]
	}

	result := make([]*types.Node, len(versions))
	for i, v := range versions {
		result[i] = inner[v].DeepCopy()
	}
	return result, nil
}

// TruncateNodeHistory removes all but the N most recent node versions.
// If keepVersions == 0, all history is cleared.
func (ms *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}

	inner := ms.nodeHistory[nid]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions == 0 {
		delete(ms.nodeHistory, nid)
		return ms.logHistoryTruncateLocked(storecontract.ChangeNodeHistoryTruncate, nid.SnowflakeID(), false, 0)
	}

	if len(inner) <= keepVersions {
		return nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	// Delete all but the most recent keepVersions.
	for _, v := range versions[:len(versions)-keepVersions] {
		delete(inner, v)
	}
	return ms.logHistoryTruncateLocked(storecontract.ChangeNodeHistoryTruncate, nid.SnowflakeID(), false, int64(keepVersions))
}

// PutRelVersion stores a relationship snapshot at the given version.
// Deep-copies the relationship at the store boundary.
func (ms *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, version, r); err != nil {
		return err
	}

	inner, ok := ms.relHistory[rid]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[rid] = inner
	}
	inner[version] = r.DeepCopy()
	ms.recordRelTypeMemberLocked(r) // transaction-time rel-type membership (history version)
	return ms.logRelHistoryVersionLocked(version, r)
}

// GetRelVersion retrieves a relationship snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}

	inner, ok := ms.relHistory[rid]
	if !ok {
		return nil, ErrVersionNotFound
	}
	r, ok := inner[version]
	if !ok {
		return nil, ErrVersionNotFound
	}
	return r.DeepCopy(), nil
}

// GetRelHistory returns all relationship version snapshots in ascending version order.
// Returns an empty slice if no history exists.
func (ms *Store) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}

	inner := ms.relHistory[rid]
	if len(inner) == 0 {
		return nil, nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	result := make([]*types.Relationship, len(versions))
	for i, v := range versions {
		result[i] = inner[v].DeepCopy()
	}
	return result, nil
}

// RelHistoryVersionsFrom returns relationship history snapshots with Version()
// >= startVersion in ascending version order. limit 0 returns all remaining.
func (ms *Store) RelHistoryVersionsFrom(rid types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}

	inner := ms.relHistory[rid]
	if len(inner) == 0 {
		return nil, nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		if v >= startVersion {
			versions = append(versions, v)
		}
	}
	if len(versions) == 0 {
		return nil, nil
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })
	if limit > 0 && len(versions) > limit {
		versions = versions[:limit]
	}

	result := make([]*types.Relationship, len(versions))
	for i, v := range versions {
		result[i] = inner[v].DeepCopy()
	}
	return result, nil
}

// TruncateRelHistory removes all but the N most recent relationship versions.
// If keepVersions == 0, all history is cleared.
func (ms *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}

	inner := ms.relHistory[rid]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions == 0 {
		delete(ms.relHistory, rid)
		return ms.logHistoryTruncateLocked(storecontract.ChangeRelHistoryTruncate, rid.SnowflakeID(), false, 0)
	}

	if len(inner) <= keepVersions {
		return nil
	}

	versions := make([]uint32, 0, len(inner))
	for v := range inner {
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	for _, v := range versions[:len(versions)-keepVersions] {
		delete(inner, v)
	}
	return ms.logHistoryTruncateLocked(storecontract.ChangeRelHistoryTruncate, rid.SnowflakeID(), false, int64(keepVersions))
}

// ReplaceNodeWithHistory atomically replaces a node and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(current); err != nil {
		return err
	}
	nid := current.ID()
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, current, rawID)
	if err != nil {
		return err
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	ms.removeNodeFromCompositeIndexesLocked(old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(current)
	ms.addNodePropertyKeyCounts(current)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, current, rawID)
	ms.addNodeToCompositeIndexesLocked(current, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, current, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, current, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return ms.logNodePutLocked(current, true)
}

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	id := current.ID()
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(id, prevVersion, prevState); err != nil {
		return err
	}

	old, exists := ms.rels[id]
	if !exists {
		return ErrRelNotFound
	}
	if err := storecontract.ValidateRelationshipLiveVersion(old, prevVersion); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, current); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipReplacement(old, prevState); err != nil {
		return err
	}

	// Write history entry.
	inner, ok := ms.relHistory[id]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[id] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	// K3b: refresh the rel property index (property values may have changed).
	indexpkg.RemoveRelFromPropertyIndexes(ms.relPropertyIndexes, old, id.SnowflakeID())
	ms.adjustRelPropertyTypeClassCounts(old, -1)
	ms.adjustRelPropertyKeyCounts(old, -1)
	// Replace current entity.
	ms.rels[id] = freezeRelCopy(current)
	indexpkg.AddRelToPropertyIndexes(ms.relPropertyIndexes, current, id.SnowflakeID())
	ms.adjustRelPropertyTypeClassCounts(current, 1)
	ms.adjustRelPropertyKeyCounts(current, 1)
	return ms.logRelPutLocked(current, true)
}

// NodeAsOf returns the node version visible at txTime without materializing
// the full history slice. Returns ErrVersionNotFound when no version matches.
func (ms *Store) NodeAsOf(nid types.NodeID, txTime types.Instant) (*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	best := nodeAsOfLocked(ms.nodes[nid], ms.nodeHistory[nid], txTime)
	if best == nil {
		return nil, ErrVersionNotFound
	}
	return nodeCopyVisibleAtTxTime(best, txTime), nil
}

// RelAsOf returns the relationship version visible at txTime without
// materializing the full history slice.
func (ms *Store) RelAsOf(rid types.RelID, txTime types.Instant) (*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	best := relAsOfLocked(ms.rels[rid], ms.relHistory[rid], txTime)
	if best == nil {
		return nil, ErrVersionNotFound
	}
	return relCopyVisibleAtTxTime(best, txTime), nil
}

// NodesAsOf returns all node versions visible at txTime. It inspects current
// rows and history under one read lock and deep-copies only the selected
// version for each entity.
func (ms *Store) NodesAsOf(txTime types.Instant) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}

	result := make([]*types.Node, 0, len(ms.nodes))
	for id, current := range ms.nodes {
		if best := nodeAsOfLocked(current, ms.nodeHistory[id], txTime); best != nil {
			result = append(result, nodeCopyVisibleAtTxTime(best, txTime))
		}
	}
	for id, history := range ms.nodeHistory {
		if _, live := ms.nodes[id]; live {
			continue
		}
		if best := nodeAsOfLocked(nil, history, txTime); best != nil {
			result = append(result, nodeCopyVisibleAtTxTime(best, txTime))
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	storeutil.SortNodesByID(result)
	return result, nil
}

// RelsAsOf returns all relationship versions visible at txTime. It mirrors
// NodesAsOf for relationship history.
func (ms *Store) RelsAsOf(txTime types.Instant) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}

	result := make([]*types.Relationship, 0, len(ms.rels))
	for id, current := range ms.rels {
		if best := relAsOfLocked(current, ms.relHistory[id], txTime); best != nil {
			result = append(result, relCopyVisibleAtTxTime(best, txTime))
		}
	}
	for id, history := range ms.relHistory {
		if _, live := ms.rels[id]; live {
			continue
		}
		if best := relAsOfLocked(nil, history, txTime); best != nil {
			result = append(result, relCopyVisibleAtTxTime(best, txTime))
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	storeutil.SortRelsByID(result)
	return result, nil
}

func nodeAsOfLocked(current *types.Node, history map[uint32]*types.Node, txTime types.Instant) *types.Node {
	if nodeMatchesTxTime(current, txTime) {
		return current
	}
	// History arm: the shared as-of selection rule (newest belief by version with
	// TxFrom<=txTime, absent if that decisive belief was retracted/deleted by the
	// pin — lesson 62). The current fast path above already returned the live row
	// when it is the answer, so history alone is the candidate set here.
	best, ok := storeutil.SelectAsOf(nodeHistoryVersionSlice(history), txTime)
	if !ok {
		return nil
	}
	return best
}

func relAsOfLocked(current *types.Relationship, history map[uint32]*types.Relationship, txTime types.Instant) *types.Relationship {
	if relMatchesTxTime(current, txTime) {
		return current
	}
	best, ok := storeutil.SelectAsOf(relHistoryVersionSlice(history), txTime)
	if !ok {
		return nil
	}
	return best
}

// nodeHistoryVersionSlice flattens a version-keyed history map into the slice
// storeutil.SelectAsOf consumes. Map iteration order is irrelevant — SelectAsOf
// selects by version ordinal, not slice position.
func nodeHistoryVersionSlice(history map[uint32]*types.Node) []*types.Node {
	if len(history) == 0 {
		return nil
	}
	out := make([]*types.Node, 0, len(history))
	for _, v := range history {
		out = append(out, v)
	}
	return out
}

func relHistoryVersionSlice(history map[uint32]*types.Relationship) []*types.Relationship {
	if len(history) == 0 {
		return nil
	}
	out := make([]*types.Relationship, 0, len(history))
	for _, v := range history {
		out = append(out, v)
	}
	return out
}

func nodeMatchesTxTime(n *types.Node, txTime types.Instant) bool {
	if n == nil {
		return false
	}
	tm := n.Temporal()
	return tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0
}

func relMatchesTxTime(r *types.Relationship, txTime types.Instant) bool {
	if r == nil {
		return false
	}
	tm := r.Temporal()
	return tm != nil && tm.TxFrom > 0 && tm.TxFrom <= txTime && tm.TxTo == 0
}

func nodeCopyVisibleAtTxTime(n *types.Node, txTime types.Instant) *types.Node {
	if n == nil {
		return nil
	}
	out := n.DeepCopy()
	normalizeTemporalVisibleAtTxTime(out.Temporal(), txTime)
	return out
}

func relCopyVisibleAtTxTime(r *types.Relationship, txTime types.Instant) *types.Relationship {
	if r == nil {
		return nil
	}
	out := r.DeepCopy()
	normalizeTemporalVisibleAtTxTime(out.Temporal(), txTime)
	return out
}

func normalizeTemporalVisibleAtTxTime(tm *types.TemporalMetadata, txTime types.Instant) {
	if tm == nil {
		return
	}
	if tm.TxTo > txTime {
		tm.TxTo = 0
	}
	if tm.DeletedAt > txTime {
		if tm.ValidTo == tm.DeletedAt {
			tm.ValidTo = 0
		}
		tm.DeletedAt = 0
	}
}

// TrimNodeHistoryFrom removes all node history entries at or after minVersion.
// Transaction rollback uses this after restoring the pre-transaction current
// row, because graph mutation paths append history at the previous Version().
func (ms *Store) TrimNodeHistoryFrom(nid types.NodeID, minVersion uint32) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	inner := ms.nodeHistory[nid]
	deleted := false
	for version := range inner {
		if version >= minVersion {
			delete(inner, version)
			deleted = true
		}
	}
	if len(inner) == 0 {
		delete(ms.nodeHistory, nid)
	}
	if !deleted {
		return nil
	}
	return ms.logHistoryTruncateLocked(storecontract.ChangeNodeHistoryTruncate, nid.SnowflakeID(), true, int64(minVersion))
}

// TrimRelHistoryFrom removes all relationship history entries at or after
// minVersion. See TrimNodeHistoryFrom for the rollback invariant.
func (ms *Store) TrimRelHistoryFrom(rid types.RelID, minVersion uint32) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	inner := ms.relHistory[rid]
	deleted := false
	for version := range inner {
		if version >= minVersion {
			delete(inner, version)
			deleted = true
		}
	}
	if len(inner) == 0 {
		delete(ms.relHistory, rid)
	}
	if !deleted {
		return nil
	}
	return ms.logHistoryTruncateLocked(storecontract.ChangeRelHistoryTruncate, rid.SnowflakeID(), true, int64(minVersion))
}
