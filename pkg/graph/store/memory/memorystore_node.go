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
	return ms.putNodeRouted(n, 0)
}

// PutNodeScoped mirrors PutNode but routes the change-log record into the
// store.ScopedTxChangeLog buffer named by token instead of the eager pending
// log. token == 0 is exactly PutNode. See memorystore_changelog_scoped.go
// (BACKLOG 11f Batch A — foundation only).
func (ms *Store) PutNodeScoped(n *types.Node, token uint64) error {
	if token == 0 {
		return ms.PutNode(n)
	}
	return ms.putNodeRouted(n, token)
}

func (ms *Store) putNodeRouted(n *types.Node, token uint64) error {
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
	ms.recordNodeLabelMembersLocked(n) // transaction-time label membership
	ms.addNodePropertyKeyCounts(n)

	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	ms.addNodeToCompositeIndexesLocked(n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return ms.logNodePutRoutedLocked(n, false, token)
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
	ms.removeNodeFromCompositeIndexesLocked(n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)
	return ms.logNodeHardDeleteLocked(nid.SnowflakeID(), nil)
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
	return ms.logNodePutLocked(updatedNode, false)
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
	ms.recordNodeLabelMembersLocked(updatedNode) // transaction-time label membership (new token)

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
	return ms.logNodePutLocked(updatedNode, false)
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (ms *Store) ReplaceNode(n *types.Node) error {
	return ms.replaceNodeRouted(n, 0)
}

// ReplaceNodeScoped mirrors ReplaceNode but routes the change-log record
// into the store.ScopedTxChangeLog buffer named by token instead of the
// eager pending log. token == 0 is exactly ReplaceNode. See
// memorystore_changelog_scoped.go (BACKLOG 11f Batch E — foundation only).
func (ms *Store) ReplaceNodeScoped(n *types.Node, token uint64) error {
	if token == 0 {
		return ms.ReplaceNode(n)
	}
	return ms.replaceNodeRouted(n, token)
}

func (ms *Store) replaceNodeRouted(n *types.Node, token uint64) error {
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
	ms.removeNodeFromCompositeIndexesLocked(old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = freezeNodeCopy(n)
	ms.addNodePropertyKeyCounts(n)
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	ms.addNodeToCompositeIndexesLocked(n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, rawID); err != nil {
		return err
	}
	return ms.logNodePutRoutedLocked(n, false, token)
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
	// The cascade removes every connected relationship (adjacency changes),
	// not just the node — bumpRelEpoch too, or a concurrent X5
	// expand-aggregation column reader's staleness gate would pass despite
	// adjacency having changed mid-scan (BACKLOG 17a).
	defer ms.bumpRelEpoch()

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

	// Capture the rel IDs whose CURRENT ROW actually exists BEFORE deleting:
	// CascadedRelIDs is defined as the rel rows the cascade removes (orphan-only
	// adjacency entries are purged but not "removed rows"), matching the badger
	// backend so the change-log record is identical across backends.
	cascaded := make([]int64, 0, len(relIDs))
	for relID := range relIDs {
		if _, ok := ms.rels[relID]; ok {
			cascaded = append(cascaded, int64(relID.SnowflakeID()))
		}
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
	ms.removeNodeFromCompositeIndexesLocked(n, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
	delete(ms.nodes, nid)

	return ms.logNodeHardDeleteLocked(nid.SnowflakeID(), cascaded)
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
		ms.recordNodeLabelMembersLocked(n) // transaction-time label membership
		ms.addNodePropertyKeyCounts(n)
		rawID := id.SnowflakeID()
		indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
		ms.addNodeToCompositeIndexesLocked(n, rawID)
		indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.AddNodeToHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates[i], rawID); err != nil {
			return err
		}
		if err := ms.logNodePutLocked(n, false); err != nil {
			return err
		}
	}

	return nil
}

// PutNodesBatchPreEncoded satisfies store.PreEncodedPutCapability. The memory
// backend holds live *types.Node objects and never serializes an entity row, so
// the pre-encoded buffers are irrelevant here — it delegates to PutNodesBatch.
// Byte-identity is trivially preserved: memory produces no persisted entity
// bytes, and its change feed uses the same UNTOKENIZED encode on both paths. The
// method exists so the ingest applier can exercise the capability path uniformly
// across the native backends (the badger arm is where the encode is actually
// skipped).
func (ms *Store) PutNodesBatchPreEncoded(nodes []*types.Node, _ [][]byte) error {
	return ms.PutNodesBatch(nodes)
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
		ms.removeNodeFromCompositeIndexesLocked(n, rawID)
		indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, n, rawID)
		indexpkg.RemoveNodeFromHighFrequencyIndexes(ms.hfIndexes, n, rawID)
		indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
		delete(ms.nodes, id)
		if err := ms.logNodeHardDeleteLocked(id.SnowflakeID(), nil); err != nil {
			return err
		}
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
