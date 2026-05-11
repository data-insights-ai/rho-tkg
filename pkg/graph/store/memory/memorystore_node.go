// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// PutNode stores a node and indexes all its label tokens.
// Returns ErrNodeExists if a node with the same ID already exists.
func (ms *Store) PutNode(n *types.Node) error {
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	if _, exists := ms.nodes[nid]; exists {
		return ErrNodeExists
	}

	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, n, rawID); err != nil {
		return err
	}

	ms.nodes[nid] = n.DeepCopy()

	// Index all label tokens.
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if ms.labelIdx[tv] == nil {
			ms.labelIdx[tv] = make(map[types.NodeID]struct{})
		}
		ms.labelIdx[tv][nid] = struct{}{}
	}

	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID); err != nil {
		return err
	}
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}

	n, ok := ms.nodes[nid]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return n.DeepCopy(), nil
}

// DeleteNode removes a node and its label index entries.
// Returns ErrInvalidStoreMutation if the node still has connected relationships.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) DeleteNode(nid types.NodeID) error {
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
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
	if len(ms.outIdx[nid]) != 0 || len(ms.inIdx[nid]) != 0 {
		return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
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

// RemoveNodeLabelToken removes tok from the label index for id and stores updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw).
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
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

	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
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

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
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

	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, updatedNode, rawID); err != nil {
		return err
	}

	set, ok := ms.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		ms.labelIdx[tok] = set
	}
	set[nid] = struct{}{}

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

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (ms *Store) ReplaceNode(n *types.Node) error {
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	nid := n.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	if err := storecontract.ValidateNodeReplacement(old, n); err != nil {
		return err
	}
	rawID := nid.SnowflakeID()
	if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, n, rawID); err != nil {
		return err
	}
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = n.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID); err != nil {
		return err
	}
	return nil
}

// DeleteNodeCascade atomically removes a node and all connected relationships.
// Holds the write lock for the entire operation — no TOCTOU window.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) DeleteNodeCascade(nid types.NodeID) error {
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
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

// --- Batch operations ---

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: deep-copy each, store, and update label indexes.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (ms *Store) PutNodesBatch(nodes []*types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

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
	for _, n := range nodes {
		if err := indexpkg.ValidateNodeVectorIndexes(ms.vectorIndexes, n, n.ID().SnowflakeID()); err != nil {
			return err
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, n := range nodes {
		id := n.ID()
		ms.nodes[id] = n.DeepCopy()

		for _, tok := range n.AllLabelTokens() {
			tv := tok.Value()
			if ms.labelIdx[tv] == nil {
				ms.labelIdx[tv] = make(map[types.NodeID]struct{})
			}
			ms.labelIdx[tv][id] = struct{}{}
		}
		rawID := id.SnowflakeID()
		indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
		indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		if err := indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID); err != nil {
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
	ms.mu.Lock()
	defer ms.mu.Unlock()

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
		for _, tok := range n.AllLabelTokens() {
			tv := tok.Value()
			if set, exists := ms.labelIdx[tv]; exists {
				delete(set, id)
				if len(set) == 0 {
					delete(ms.labelIdx, tv)
				}
			}
		}
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
