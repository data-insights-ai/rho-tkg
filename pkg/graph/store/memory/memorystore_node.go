// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// PutNode stores a node and indexes all its label tokens.
// Returns ErrNodeExists if a node with the same ID already exists.
func (ms *Store) PutNode(n *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.ID()

	if _, exists := ms.nodes[nid]; exists {
		return ErrNodeExists
	}

	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, n, rawID)
	if err != nil {
		return err
	}

	ms.nodes[nid] = freezeNodeCopy(n)

	ms.addNodeLabelIndexes(nid, n)
	ms.addNodePropertyKeyCounts(n)

	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if nid <= 0 {
		return nil, storecontract.ValidateNodeID(nid)
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return n.DeepCopy(), nil
}

// NodeIntegrityHash returns the live node's integrity hash without exposing the
// stored node pointer or copying the whole entity.
func (ms *Store) NodeIntegrityHash(nid types.NodeID) (string, error) {
	if ms == nil {
		return "", ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return "", err
	}
	if nid <= 0 {
		return "", storecontract.ValidateNodeID(nid)
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return "", ErrNodeNotFound
	}
	if ig := n.Integrity(); ig != nil {
		return ig.Hash, nil
	}
	return "", nil
}

// EndpointIntegrityHashes returns both live endpoint integrity hashes under one
// store read lock.
func (ms *Store) EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error) {
	if ms == nil {
		return "", "", ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return "", "", err
	}
	if startID <= 0 {
		return "", "", storecontract.ValidateNodeID(startID)
	}
	if endID <= 0 {
		return "", "", storecontract.ValidateNodeID(endID)
	}

	start, ok := ms.nodes[startID]
	if !ok {
		return "", "", ErrNodeNotFound
	}
	fromHash := nodeIntegrityHash(start)
	if startID == endID {
		return fromHash, fromHash, nil
	}
	end, ok := ms.nodes[endID]
	if !ok {
		return "", "", ErrNodeNotFound
	}
	return fromHash, nodeIntegrityHash(end), nil
}

func nodeIntegrityHash(n *types.Node) string {
	if ig := n.Integrity(); ig != nil {
		return ig.Hash
	}
	return ""
}

// DeleteNode removes a node and its label index entries.
// Returns ErrInvalidStoreMutation if the node still has connected relationships.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) DeleteNode(nid types.NodeID) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
	}
	if len(ms.outIdx[nid]) != 0 || len(ms.inIdx[nid]) != 0 {
		return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
	}

	ms.removeNodeLabelIndexes(nid, n)
	ms.removeNodePropertyKeyCounts(n)

	rawID := nid.SnowflakeID()
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return nil
}

// RemoveNodeLabelToken removes tok from the label index for id and stores updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw).
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		return err
	}

	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, updatedNode, rawID)
	if err != nil {
		return err
	}

	// Remove only the specified token from the label index.
	if set, ok := ms.labelIdx[tok]; ok {
		delete(set, nid)
		if len(set) == 0 {
			delete(ms.labelIdx, tok)
		}
	}

	// Update property, temporal, and vector indexes (properties may have changed due to hash update).
	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(updatedNode)
	ms.addNodePropertyKeyCounts(updatedNode)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return nil
}

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		return err
	}

	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, updatedNode, rawID)
	if err != nil {
		return err
	}

	set, ok := ms.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		ms.labelIdx[tok] = set
	}
	set[nid] = struct{}{}

	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(updatedNode)
	ms.addNodePropertyKeyCounts(updatedNode)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, updatedNode, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (ms *Store) ReplaceNode(n *types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.ID()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeReplacement(old, n); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, n, rawID)
	if err != nil {
		return err
	}
	ms.removeNodePropertyKeyCounts(old)
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(n)
	ms.addNodePropertyKeyCounts(n)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return nil
}

// DeleteNodeCascade atomically removes a node and all connected relationships.
// Holds the write lock for the entire operation — no TOCTOU window.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) DeleteNodeCascade(nid types.NodeID) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
	}

	// Collect all connected relIDs from adjacency indexes.
	// Use a map for dedup (self-loops appear in both outgoing and incoming).
	relIDs := make(map[types.RelID]struct{})
	for relID := range ms.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range ms.inIdx[nid] {
		relIDs[relID] = struct{}{}
	}

	// Delete each relationship (lock-free inner call).
	for relID := range relIDs {
		if err := ms.deleteRelOrPurgeOrphanLocked(relID); err != nil {
			return err
		}
	}

	ms.removeNodeLabelIndexes(nid, n)
	ms.removeNodePropertyKeyCounts(n)

	rawID := nid.SnowflakeID()
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return nil
}

// --- Batch operations ---

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: deep-copy each, store, and update label indexes.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (ms *Store) PutNodesBatch(nodes []*types.Node) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		id := n.ID()
		if _, exists := ms.nodes[id]; exists {
			return ErrNodeExists
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("graph: duplicate node ID %d in batch", id)
		}
		seen[id] = struct{}{}
	}
	vectorUpdates := make([][]indexpkg.NodeVectorIndexUpdate, len(nodes))
	for i, n := range nodes {
		updates, err := indexpkg.PrepareNodeVectorIndexUpdates(ms.vectorIndexes, n, n.ID().SnowflakeID())
		if err != nil {
			return err
		}
		vectorUpdates[i] = updates
	}

	// Phase 2: apply — all validated, safe to mutate.
	for i, n := range nodes {
		id := n.ID()
		ms.nodes[id] = freezeNodeCopy(n)

		ms.addNodeLabelIndexes(id, n)
		ms.addNodePropertyKeyCounts(n)
		rawID := id.SnowflakeID()
		indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
		indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates[i], rawID); err != nil {
			return err
		}
	}

	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: remove each from store and clean label indexes.
// Missing ID → ErrNodeNotFound, zero mutations. Duplicate IDs are coalesced.
// Nil/empty input → nil error.
func (ms *Store) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	defer ms.bumpNodeEpoch()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if len(typedIDs) == 0 {
		return nil
	}
	for _, id := range typedIDs {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueNodeIDs(typedIDs)

	// Phase 1: validate — all must exist and be unconnected.
	for _, id := range typedIDs {
		if _, exists := ms.nodes[id]; !exists {
			return ErrNodeNotFound
		}
		if len(ms.outIdx[id]) != 0 || len(ms.inIdx[id]) != 0 {
			return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, id)
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, id := range typedIDs {
		n := ms.nodes[id]
		ms.removeNodeLabelIndexes(id, n)
		ms.removeNodePropertyKeyCounts(n)
		rawID := id.SnowflakeID()
		indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, n, rawID)
		indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
		delete(ms.nodes, id)
	}

	return nil
}

func uniqueNodeIDs(ids []types.NodeID) []types.NodeID {
	seen := make(map[types.NodeID]struct{}, len(ids))
	out := make([]types.NodeID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (ms *Store) addNodeLabelIndexes(id types.NodeID, n *types.Node) {
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		ms.addNodeLabelIndex(id, n.LabelTokenRawAt(i))
	}
}

func (ms *Store) addNodeLabelIndex(id types.NodeID, tok uint16) {
	if ms.labelIdx[tok] == nil {
		ms.labelIdx[tok] = make(map[types.NodeID]struct{})
	}
	ms.labelIdx[tok][id] = struct{}{}
}

func (ms *Store) removeNodeLabelIndexes(id types.NodeID, n *types.Node) {
	count := n.LabelTokenCount()
	for i := 0; i < count; i++ {
		ms.removeNodeLabelIndex(id, n.LabelTokenRawAt(i))
	}
}

func (ms *Store) removeNodeLabelIndex(id types.NodeID, tok uint16) {
	if set, exists := ms.labelIdx[tok]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.labelIdx, tok)
		}
	}
}
