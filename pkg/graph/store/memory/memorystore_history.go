// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"sort"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, prevState); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
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
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = updatedNode.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
		return err
	}
	return nil
}

// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, prevState); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
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

	// Update property, temporal, and vector indexes.
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = updatedNode.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
		return err
	}
	return nil
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in a single locked operation.
// All under one lock: atomic with respect to concurrent readers.
func (ms *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, tombstone); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, ok := ms.rels[rid]
	if !ok {
		return ErrRelNotFound
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

	return ms.deleteRelLocked(rid)
}

// DeleteNodeWithHistory atomically writes tombstone history entries for the node
// and all connected relationships, then performs the cascade delete.
// All under one lock: atomic with respect to concurrent readers.
func (ms *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, nodeTombstone); err != nil {
		return err
	}
	for _, rt := range relTombstones {
		if err := storecontract.ValidateRelationshipHistorySnapshot(rt.ID, rt.Tombstone); err != nil {
			return err
		}
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
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
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if set, exists := ms.labelIdx[tv]; exists {
			delete(set, nid)
			if len(set) == 0 {
				delete(ms.labelIdx, tv)
			}
		}
	}

	rawID := nid.SnowflakeID()
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return nil
}

// --- Version history ---

// PutNodeVersion stores a node snapshot at the given version.
// Deep-copies the node at the store boundary.
func (ms *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, n); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[version] = n.DeepCopy()
	return nil
}

// GetNodeVersion retrieves a node snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
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

// TruncateNodeHistory removes all but the N most recent node versions.
// If keepVersions == 0, all history is cleared.
func (ms *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
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
		return nil
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
	return nil
}

// PutRelVersion stores a relationship snapshot at the given version.
// Deep-copies the relationship at the store boundary.
func (ms *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, r); err != nil {
		return err
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	inner, ok := ms.relHistory[rid]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[rid] = inner
	}
	inner[version] = r.DeepCopy()
	return nil
}

// GetRelVersion retrieves a relationship snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
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

// TruncateRelHistory removes all but the N most recent relationship versions.
// If keepVersions == 0, all history is cleared.
func (ms *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
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
		return nil
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
	return nil
}

// ReplaceNodeWithHistory atomically replaces a node and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if err := storecontract.ValidateNodeWrite(current); err != nil {
		return err
	}
	nid := current.ID()
	if err := storecontract.ValidateNodeHistorySnapshot(nid, prevState); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeReplacement(old, current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, current, rawID); err != nil {
		return err
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = current.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, current, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, current, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, current, rawID)
	if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, current, rawID); err != nil {
		return err
	}
	return nil
}

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	id := current.ID()
	if err := storecontract.ValidateRelationshipHistorySnapshot(id, prevState); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, exists := ms.rels[id]
	if !exists {
		return ErrRelNotFound
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

	// Replace current entity.
	ms.rels[id] = current.DeepCopy()
	return nil
}
