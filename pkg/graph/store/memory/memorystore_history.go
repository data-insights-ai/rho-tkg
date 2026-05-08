// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"sort"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
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

	rawID := nid.SnowflakeID()
	// Update property, temporal, and vector indexes.
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = updatedNode.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, updatedNode, rawID)
	return nil
}

// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
// writes a version history entry, and persists updatedNode under a single lock.
func (ms *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
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

	rawID := nid.SnowflakeID()
	// Update property, temporal, and vector indexes.
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = updatedNode.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, updatedNode, rawID)
	return nil
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in a single locked operation.
// All under one lock: atomic with respect to concurrent readers.
func (ms *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, ok := ms.rels[rid]; !ok {
		return ErrRelNotFound
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
	ms.mu.Lock()
	defer ms.mu.Unlock()

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
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
	relIDs := make(map[types.RelID]struct{})
	for relID := range ms.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range ms.inIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range relIDs {
		_ = ms.deleteRelLocked(relID) // Ignore ErrRelNotFound — defensive only.
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
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return nil
}

// --- Version history ---

// PutNodeVersion stores a node snapshot at the given version.
// Deep-copies the node at the store boundary.
func (ms *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

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
// If keepVersions <= 0, all history is cleared.
func (ms *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner := ms.nodeHistory[nid]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions <= 0 {
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
	ms.mu.Lock()
	defer ms.mu.Unlock()

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
// If keepVersions <= 0, all history is cleared.
func (ms *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner := ms.relHistory[rid]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions <= 0 {
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
	nid := current.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[nid]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[nid] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	rawID := nid.SnowflakeID()
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = current.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, current, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, current, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, current, rawID)
	return nil
}

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	id := current.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.rels[id]; !exists {
		return ErrRelNotFound
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
