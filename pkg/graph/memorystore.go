package graph

import (
	"fmt"
	"sort"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// MemoryStore is a thread-safe in-memory Store implementation.
// Uses maps for O(1) entity lookup and nested hash-sets for O(1) index maintenance.
type MemoryStore struct {
	mu    sync.RWMutex
	nodes map[snowflake.ID]*types.Node
	rels  map[snowflake.ID]*types.Relationship

	// Label index: labelToken → set of node IDs.
	labelIdx map[uint16]map[snowflake.ID]struct{}

	// RelType index: relTypeToken → set of rel IDs.
	typeIdx map[uint16]map[snowflake.ID]struct{}

	// Adjacency indexes — nested hash sets for O(1) insert/delete.
	outIdx map[snowflake.ID]map[snowflake.ID]struct{} // startNodeID → set(relID)
	inIdx  map[snowflake.ID]map[snowflake.ID]struct{} // endNodeID → set(relID)

	// Version history — pre-mutation snapshots keyed by entity ID and version.
	nodeHistory map[snowflake.ID]map[uint32]*types.Node
	relHistory  map[snowflake.ID]map[uint32]*types.Relationship

	// Property indexes — label+property → value → set of node IDs.
	propertyIndexes map[propertyIndexKey]*propertyIndex

	// Temporal indexes — labelToken → interval index for temporal push-down.
	temporalIndexes map[uint16]*temporalIndex

	// High-frequency indexes — labelToken → time-bucketed index for O(1) insertion.
	// Separate map from temporalIndexes; only one type can exist per label at a time.
	hfIndexes map[uint16]*highFrequencyIndex

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	vectorIndexes map[vectorIndexKey]*vectorIndex
}

// NewMemoryStore creates an empty MemoryStore with all indexes initialized.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		nodes:           make(map[snowflake.ID]*types.Node),
		rels:            make(map[snowflake.ID]*types.Relationship),
		labelIdx:        make(map[uint16]map[snowflake.ID]struct{}),
		typeIdx:         make(map[uint16]map[snowflake.ID]struct{}),
		outIdx:          make(map[snowflake.ID]map[snowflake.ID]struct{}),
		inIdx:           make(map[snowflake.ID]map[snowflake.ID]struct{}),
		nodeHistory:     make(map[snowflake.ID]map[uint32]*types.Node),
		relHistory:      make(map[snowflake.ID]map[uint32]*types.Relationship),
		propertyIndexes: make(map[propertyIndexKey]*propertyIndex),
		temporalIndexes: make(map[uint16]*temporalIndex),
		hfIndexes:       make(map[uint16]*highFrequencyIndex),
		vectorIndexes:   make(map[vectorIndexKey]*vectorIndex),
	}
}

// PutNode stores a node and indexes all its label tokens.
// Returns ErrNodeExists if a node with the same ID already exists.
func (ms *MemoryStore) PutNode(n *types.Node) error {
	id := n.InternalID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.nodes[id]; exists {
		return ErrNodeExists
	}

	ms.nodes[id] = n.DeepCopy()

	// Index all label tokens.
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if ms.labelIdx[tv] == nil {
			ms.labelIdx[tv] = make(map[snowflake.ID]struct{})
		}
		ms.labelIdx[tv][id] = struct{}{}
	}

	addNodeToPropertyIndexes(ms.propertyIndexes, n, id)
	addNodeToTemporalIndexes(ms.temporalIndexes, n, id)
	addNodeToVectorIndexes(ms.vectorIndexes, n, id)
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *MemoryStore) GetNode(id snowflake.ID) (*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	n, ok := ms.nodes[id]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return n.DeepCopy(), nil
}

// DeleteNode removes a node and its label index entries.
// The Graph layer is responsible for cascade-deleting relationships first.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *MemoryStore) DeleteNode(id snowflake.ID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	n, ok := ms.nodes[id]
	if !ok {
		return ErrNodeNotFound
	}

	// Remove label index entries.
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if set, exists := ms.labelIdx[tv]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(ms.labelIdx, tv)
			}
		}
	}

	removeNodeFromPropertyIndexes(ms.propertyIndexes, n, id)
	removeNodeFromTemporalIndexes(ms.temporalIndexes, n, id)
	removeNodeFromVectorIndexes(ms.vectorIndexes, n, id)
	delete(ms.nodes, id)
	return nil
}

// RemoveNodeLabelToken removes tok from the label index for id and stores updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw).
// Returns ErrNodeNotFound if the node does not exist.
func (ms *MemoryStore) RemoveNodeLabelToken(id snowflake.ID, tok uint16, updatedNode *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}

	// Remove only the specified token from the label index.
	if set, ok := ms.labelIdx[tok]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.labelIdx, tok)
		}
	}

	// Update property, temporal, and vector indexes (properties may have changed due to hash update).
	removeNodeFromPropertyIndexes(ms.propertyIndexes, old, id)
	removeNodeFromTemporalIndexes(ms.temporalIndexes, old, id)
	removeNodeFromVectorIndexes(ms.vectorIndexes, old, id)
	ms.nodes[id] = updatedNode.DeepCopy()
	addNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, id)
	addNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, id)
	addNodeToVectorIndexes(ms.vectorIndexes, updatedNode, id)
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (ms *MemoryStore) ReplaceNode(n *types.Node) error {
	id := n.InternalID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}
	removeNodeFromPropertyIndexes(ms.propertyIndexes, old, id)
	removeNodeFromTemporalIndexes(ms.temporalIndexes, old, id)
	removeNodeFromVectorIndexes(ms.vectorIndexes, old, id)
	ms.nodes[id] = n.DeepCopy()
	addNodeToPropertyIndexes(ms.propertyIndexes, n, id)
	addNodeToTemporalIndexes(ms.temporalIndexes, n, id)
	addNodeToVectorIndexes(ms.vectorIndexes, n, id)
	return nil
}

// PutRelationship stores a relationship and indexes its type and adjacency.
// Returns ErrNodeNotFound if start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (ms *MemoryStore) PutRelationship(r *types.Relationship) error {
	id := r.InternalID().SnowflakeID()
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Verify endpoints exist.
	if _, ok := ms.nodes[startID]; !ok {
		return ErrNodeNotFound
	}
	if _, ok := ms.nodes[endID]; !ok {
		return ErrNodeNotFound
	}

	if _, exists := ms.rels[id]; exists {
		return ErrRelExists
	}

	ms.rels[id] = r.DeepCopy()

	// Type index.
	tv := r.TypeToken().Value()
	if ms.typeIdx[tv] == nil {
		ms.typeIdx[tv] = make(map[snowflake.ID]struct{})
	}
	ms.typeIdx[tv][id] = struct{}{}

	// Adjacency: outgoing.
	if ms.outIdx[startID] == nil {
		ms.outIdx[startID] = make(map[snowflake.ID]struct{})
	}
	ms.outIdx[startID][id] = struct{}{}

	// Adjacency: incoming.
	if ms.inIdx[endID] == nil {
		ms.inIdx[endID] = make(map[snowflake.ID]struct{})
	}
	ms.inIdx[endID][id] = struct{}{}

	return nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *MemoryStore) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	r, ok := ms.rels[id]
	if !ok {
		return nil, ErrRelNotFound
	}
	return r.DeepCopy(), nil
}

// ReplaceRelationship overwrites an existing relationship's data in-place.
// Returns ErrRelNotFound if the relationship does not exist.
// No index changes — type and endpoints are immutable after creation.
func (ms *MemoryStore) ReplaceRelationship(r *types.Relationship) error {
	id := r.InternalID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.rels[id]; !exists {
		return ErrRelNotFound
	}
	ms.rels[id] = r.DeepCopy()
	return nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *MemoryStore) DeleteRelationship(id snowflake.ID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.deleteRelLocked(id)
}

// deleteRelLocked removes a relationship and cleans up indexes.
// Caller must hold ms.mu write lock.
func (ms *MemoryStore) deleteRelLocked(id snowflake.ID) error {
	r, ok := ms.rels[id]
	if !ok {
		return ErrRelNotFound
	}

	// Type index cleanup.
	tv := r.TypeToken().Value()
	if set, exists := ms.typeIdx[tv]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.typeIdx, tv)
		}
	}

	// Adjacency cleanup — O(1) delete from hash sets.
	startID := r.StartNodeID().SnowflakeID()
	if set, exists := ms.outIdx[startID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.outIdx, startID)
		}
	}

	endID := r.EndNodeID().SnowflakeID()
	if set, exists := ms.inIdx[endID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.inIdx, endID)
		}
	}

	delete(ms.rels, id)
	return nil
}

// DeleteNodeCascade atomically removes a node and all connected relationships.
// Holds the write lock for the entire operation — no TOCTOU window.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *MemoryStore) DeleteNodeCascade(id snowflake.ID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	n, ok := ms.nodes[id]
	if !ok {
		return ErrNodeNotFound
	}

	// Collect all connected relIDs from adjacency indexes.
	// Use a map for dedup (self-loops appear in both outgoing and incoming).
	relIDs := make(map[snowflake.ID]struct{})
	for relID := range ms.outIdx[id] {
		relIDs[relID] = struct{}{}
	}
	for relID := range ms.inIdx[id] {
		relIDs[relID] = struct{}{}
	}

	// Delete each relationship (lock-free inner call).
	for relID := range relIDs {
		// Ignore ErrRelNotFound — can't happen with dedup, but defensive.
		_ = ms.deleteRelLocked(relID)
	}

	// Remove label index entries.
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if set, exists := ms.labelIdx[tv]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(ms.labelIdx, tv)
			}
		}
	}

	removeNodeFromPropertyIndexes(ms.propertyIndexes, n, id)
	removeNodeFromTemporalIndexes(ms.temporalIndexes, n, id)
	delete(ms.nodes, id)
	return nil
}

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index for fast filtering when one exists and a temporal filter is set.
// MemoryStore never returns an error.
func (ms *MemoryStore) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.labelIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	// Temporal index fast path: use interval index if one exists for this label.
	if ti, ok := ms.temporalIndexes[token]; ok {
		var ids []snowflake.ID
		if opts.ValidAt != 0 {
			ids = ti.queryAt(opts.ValidAt)
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			ids = ti.queryOverlap(opts.ValidStart, opts.ValidEnd)
		}
		if ids != nil {
			ids = paginateIDs(ids, opts.After, opts.Limit)
			if len(ids) == 0 {
				return nil, nil
			}
			result := make([]*types.Node, 0, len(ids))
			for _, id := range ids {
				if n, ok := ms.nodes[id]; ok {
					result = append(result, n.DeepCopy())
				}
			}
			return result, nil
		}
	}

	// Standard path: collect and sort IDs before fetching entities.
	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter: read in-memory entity pointer (no deep-copy).
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	return result, nil
}

// RelationshipsByType returns relationships with the given type token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// MemoryStore never returns an error.
func (ms *MemoryStore) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	return result, nil
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
// MemoryStore never returns an error.
func (ms *MemoryStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.outIdx[nodeID]
	if len(set) == 0 {
		return nil, nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			result = append(result, r.DeepCopy())
		}
	}
	sortRelsByID(result)
	return result, nil
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
// MemoryStore never returns an error.
func (ms *MemoryStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.inIdx[nodeID]
	if len(set) == 0 {
		return nil, nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			result = append(result, r.DeepCopy())
		}
	}
	sortRelsByID(result)
	return result, nil
}

// NodeCount returns the number of stored nodes.
// MemoryStore never returns an error.
func (ms *MemoryStore) NodeCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.nodes), nil
}

// RelationshipCount returns the number of stored relationships.
// MemoryStore never returns an error.
func (ms *MemoryStore) RelationshipCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.rels), nil
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
// MemoryStore never returns an error.
func (ms *MemoryStore) NodeCountByLabel(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.labelIdx[token]), nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
// MemoryStore never returns an error.
func (ms *MemoryStore) RelCountByType(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.typeIdx[token]), nil
}

// --- Version history ---

// PutNodeVersion stores a node snapshot at the given version.
// Deep-copies the node at the store boundary.
func (ms *MemoryStore) PutNodeVersion(id snowflake.ID, version uint32, n *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner, ok := ms.nodeHistory[id]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[id] = inner
	}
	inner[version] = n.DeepCopy()
	return nil
}

// GetNodeVersion retrieves a node snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *MemoryStore) GetNodeVersion(id snowflake.ID, version uint32) (*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	inner, ok := ms.nodeHistory[id]
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
func (ms *MemoryStore) GetNodeHistory(id snowflake.ID) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	inner := ms.nodeHistory[id]
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
func (ms *MemoryStore) TruncateNodeHistory(id snowflake.ID, keepVersions int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner := ms.nodeHistory[id]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions <= 0 {
		delete(ms.nodeHistory, id)
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
func (ms *MemoryStore) PutRelVersion(id snowflake.ID, version uint32, r *types.Relationship) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner, ok := ms.relHistory[id]
	if !ok {
		inner = make(map[uint32]*types.Relationship)
		ms.relHistory[id] = inner
	}
	inner[version] = r.DeepCopy()
	return nil
}

// GetRelVersion retrieves a relationship snapshot at the given version.
// Returns ErrVersionNotFound if the version does not exist.
func (ms *MemoryStore) GetRelVersion(id snowflake.ID, version uint32) (*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	inner, ok := ms.relHistory[id]
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
func (ms *MemoryStore) GetRelHistory(id snowflake.ID) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	inner := ms.relHistory[id]
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
func (ms *MemoryStore) TruncateRelHistory(id snowflake.ID, keepVersions int) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	inner := ms.relHistory[id]
	if len(inner) == 0 {
		return nil
	}

	if keepVersions <= 0 {
		delete(ms.relHistory, id)
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
func (ms *MemoryStore) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	id := current.InternalID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}

	// Write history entry.
	inner, ok := ms.nodeHistory[id]
	if !ok {
		inner = make(map[uint32]*types.Node)
		ms.nodeHistory[id] = inner
	}
	inner[prevVersion] = prevState.DeepCopy()

	// Replace current entity with property and temporal index updates.
	removeNodeFromPropertyIndexes(ms.propertyIndexes, old, id)
	removeNodeFromTemporalIndexes(ms.temporalIndexes, old, id)
	ms.nodes[id] = current.DeepCopy()
	addNodeToPropertyIndexes(ms.propertyIndexes, current, id)
	addNodeToTemporalIndexes(ms.temporalIndexes, current, id)
	return nil
}

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both writes happen under a single lock acquisition — no interleaving possible.
func (ms *MemoryStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	id := current.InternalID().SnowflakeID()

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

// --- Property indexes ---

// CreatePropertyIndex creates a property index for the given label token and property key.
// Scans all existing nodes with that label to populate the index.
// Returns ErrIndexExists if the index already exists.
func (ms *MemoryStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := propertyIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; exists {
		return ErrIndexExists
	}

	idx := newPropertyIndex()

	// Populate from existing nodes with this label.
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			if val, found := n.GetProperty(propertyKey); found {
				idx.add(nodeID, val)
			}
		}
	}

	ms.propertyIndexes[key] = idx
	return nil
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (ms *MemoryStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := propertyIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(ms.propertyIndexes, key)
	return nil
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal interval index on nodes with the given label token.
// Scans existing nodes with that label to populate the index.
// Returns ErrTemporalIndexExists if an index already exists for this label.
func (ms *MemoryStore) CreateTemporalIndex(labelToken uint16) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	ti := newTemporalIndex()
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			from, to := nodeTemporalBounds(nodeID, n.Temporal())
			ti.add(nodeID, from, to)
		}
	}

	ms.temporalIndexes[labelToken] = ti
	return nil
}

// DropTemporalIndex removes a temporal index for the given label token.
// Returns ErrTemporalIndexNotFound if no index exists.
func (ms *MemoryStore) DropTemporalIndex(labelToken uint16) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.temporalIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.temporalIndexes, labelToken)
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
func (ms *MemoryStore) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	ms.hfIndexes[labelToken] = newHighFrequencyIndex(bucketSize, 0)
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (ms *MemoryStore) DropHighFrequencyIndex(labelToken uint16) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.hfIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(ms.hfIndexes, labelToken)
	return nil
}

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims.
// Scans existing nodes to populate the index. Returns ErrVectorIndexExists on duplicate.
func (ms *MemoryStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; exists {
		return ErrVectorIndexExists
	}
	vi := &vectorIndex{dims: dims, metric: metric}
	ms.vectorIndexes[key] = vi

	// Populate from existing nodes.
	for id, n := range ms.nodes {
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		val, ok := n.GetProperty(propertyKey)
		if !ok {
			continue
		}
		vec, ok := toFloat32Slice(val)
		if !ok {
			continue
		}
		_ = vi.add(id, vec)
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ms *MemoryStore) DropVectorIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(ms.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
func (ms *MemoryStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	ids, err := vi.searchNearest(query, k)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination and temporal filtering. Uses the property index if
// one exists; falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *MemoryStore) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	key := propertyIndexKey{labelToken: labelToken, propertyKey: propKey}
	if idx, ok := ms.propertyIndexes[key]; ok {
		// Indexed path: collect matching IDs, sort, temporal filter, paginate, then fetch.
		matchSet := idx.lookup(value)
		if len(matchSet) == 0 {
			return nil, nil
		}
		ids := make([]snowflake.ID, 0, len(matchSet))
		for id := range matchSet {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		ids = ms.filterNodeIDsByTemporal(ids, opts)

		ids = paginateIDs(ids, opts.After, opts.Limit)
		if len(ids) == 0 {
			return nil, nil
		}

		result := make([]*types.Node, 0, len(ids))
		for _, id := range ids {
			if n, ok := ms.nodes[id]; ok {
				result = append(result, n.DeepCopy())
			}
		}
		return result, nil
	}

	// Fallback: label scan + property filter.
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}

	targetKey := propertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	// Collect matching IDs from label scan.
	var matchIDs []snowflake.ID
	for id := range labelIDs {
		n, ok := ms.nodes[id]
		if !ok {
			continue
		}
		if v, found := n.GetProperty(propKey); found {
			if propertyValueKey(v) == targetKey {
				matchIDs = append(matchIDs, id)
			}
		}
	}

	if len(matchIDs) == 0 {
		return nil, nil
	}
	sort.Slice(matchIDs, func(i, j int) bool { return matchIDs[i] < matchIDs[j] })

	// Temporal pre-filter.
	matchIDs = ms.filterNodeIDsByTemporal(matchIDs, opts)

	matchIDs = paginateIDs(matchIDs, opts.After, opts.Limit)
	if len(matchIDs) == 0 {
		return nil, nil
	}

	result := make([]*types.Node, 0, len(matchIDs))
	for _, id := range matchIDs {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	return result, nil
}

// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy.
func (ms *MemoryStore) AllNodeIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy.
func (ms *MemoryStore) AllRelIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *MemoryStore) ForEachNodeID(fn func(snowflake.ID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.nodes {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *MemoryStore) ForEachRelID(fn func(snowflake.ID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.rels {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachNodeHistoryID iterates over all node IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *MemoryStore) ForEachNodeHistoryID(fn func(snowflake.ID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.nodeHistory {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *MemoryStore) ForEachRelHistoryID(fn func(snowflake.ID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.relHistory {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
func (ms *MemoryStore) AllNodeHistoryIDs() ([]snowflake.ID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodeHistory) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
func (ms *MemoryStore) AllRelHistoryIDs() ([]snowflake.ID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.relHistory) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

// Clear removes all entities, indexes, history, and property indexes.
// After Clear(), the MemoryStore is empty (same state as NewMemoryStore()).
func (ms *MemoryStore) Clear() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.nodes = make(map[snowflake.ID]*types.Node)
	ms.rels = make(map[snowflake.ID]*types.Relationship)
	ms.labelIdx = make(map[uint16]map[snowflake.ID]struct{})
	ms.typeIdx = make(map[uint16]map[snowflake.ID]struct{})
	ms.outIdx = make(map[snowflake.ID]map[snowflake.ID]struct{})
	ms.inIdx = make(map[snowflake.ID]map[snowflake.ID]struct{})
	ms.nodeHistory = make(map[snowflake.ID]map[uint32]*types.Node)
	ms.relHistory = make(map[snowflake.ID]map[uint32]*types.Relationship)
	ms.propertyIndexes = make(map[propertyIndexKey]*propertyIndex)
	ms.temporalIndexes = make(map[uint16]*temporalIndex)
	ms.vectorIndexes = make(map[vectorIndexKey]*vectorIndex)
	return nil
}

// Close is a no-op for MemoryStore. Satisfies the Store interface.
func (ms *MemoryStore) Close() error { return nil }

// --- Batch operations ---

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: deep-copy each, store, and update label indexes.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (ms *MemoryStore) PutNodesBatch(nodes []*types.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[snowflake.ID]struct{}, len(nodes))
	for _, n := range nodes {
		id := n.InternalID().SnowflakeID()
		if _, exists := ms.nodes[id]; exists {
			return ErrNodeExists
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("graph: duplicate node ID %d in batch", id)
		}
		seen[id] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, n := range nodes {
		id := n.InternalID().SnowflakeID()
		ms.nodes[id] = n.DeepCopy()

		for _, tok := range n.AllLabelTokens() {
			tv := tok.Value()
			if ms.labelIdx[tv] == nil {
				ms.labelIdx[tv] = make(map[snowflake.ID]struct{})
			}
			ms.labelIdx[tv][id] = struct{}{}
		}
		addNodeToPropertyIndexes(ms.propertyIndexes, n, id)
	}

	return nil
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: deep-copy each, store, update type + adjacency indexes.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (ms *MemoryStore) PutRelationshipsBatch(rels []*types.Relationship) error {
	if len(rels) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[snowflake.ID]struct{}, len(rels))
	for _, r := range rels {
		id := r.InternalID().SnowflakeID()
		startID := r.StartNodeID().SnowflakeID()
		endID := r.EndNodeID().SnowflakeID()

		if _, exists := ms.nodes[startID]; !exists {
			return ErrNodeNotFound
		}
		if _, exists := ms.nodes[endID]; !exists {
			return ErrNodeNotFound
		}
		if _, exists := ms.rels[id]; exists {
			return ErrRelExists
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("graph: duplicate relationship ID %d in batch", id)
		}
		seen[id] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, r := range rels {
		id := r.InternalID().SnowflakeID()
		startID := r.StartNodeID().SnowflakeID()
		endID := r.EndNodeID().SnowflakeID()

		ms.rels[id] = r.DeepCopy()

		tv := r.TypeToken().Value()
		if ms.typeIdx[tv] == nil {
			ms.typeIdx[tv] = make(map[snowflake.ID]struct{})
		}
		ms.typeIdx[tv][id] = struct{}{}

		if ms.outIdx[startID] == nil {
			ms.outIdx[startID] = make(map[snowflake.ID]struct{})
		}
		ms.outIdx[startID][id] = struct{}{}

		if ms.inIdx[endID] == nil {
			ms.inIdx[endID] = make(map[snowflake.ID]struct{})
		}
		ms.inIdx[endID][id] = struct{}{}
	}

	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: remove each from store and clean label indexes.
// Missing ID → ErrNodeNotFound, zero mutations. Nil/empty input → nil error.
func (ms *MemoryStore) DeleteNodesBatch(ids []snowflake.ID) error {
	if len(ids) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — all must exist.
	for _, id := range ids {
		if _, exists := ms.nodes[id]; !exists {
			return ErrNodeNotFound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, id := range ids {
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
		removeNodeFromPropertyIndexes(ms.propertyIndexes, n, id)
		delete(ms.nodes, id)
	}

	return nil
}

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: delete each via deleteRelLocked (handles type/adjacency/history cleanup).
// Missing ID → ErrRelNotFound, zero mutations. Nil/empty input → nil error.
func (ms *MemoryStore) DeleteRelationshipsBatch(ids []snowflake.ID) error {
	if len(ids) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — all must exist.
	for _, id := range ids {
		if _, exists := ms.rels[id]; !exists {
			return ErrRelNotFound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, id := range ids {
		// deleteRelLocked can't fail here (verified existence above, holding write lock).
		_ = ms.deleteRelLocked(id)
	}

	return nil
}

// --- Bulk queries ---

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *MemoryStore) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}

	ids := make([]snowflake.ID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	return result, nil
}

// AllRelationships returns all stored relationships, with optional pagination and temporal filtering.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *MemoryStore) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}

	ids := make([]snowflake.ID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	return result, nil
}

// GetNodesByIDs returns nodes matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (ms *MemoryStore) GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	sortNodesByID(result)
	return result, nil
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (ms *MemoryStore) GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	sortRelsByID(result)
	return result, nil
}

// --- Temporal filtering helpers ---

// filterNodeIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy, no allocation per entity.
// Caller must hold ms.mu (read or write).
func (ms *MemoryStore) filterNodeIDsByTemporal(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		n, ok := ms.nodes[id]
		if !ok {
			continue
		}
		if matchesTemporalFilter(id, n.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// filterRelIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy.
// Caller must hold ms.mu (read or write).
func (ms *MemoryStore) filterRelIDsByTemporal(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		r, ok := ms.rels[id]
		if !ok {
			continue
		}
		if matchesTemporalFilter(id, r.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// sortNodesByID sorts nodes by snowflake.ID for deterministic output.
// Order is time-dominant (ms timestamp in high bits) with nodeField and step as tiebreakers.
func sortNodesByID(nodes []*types.Node) {
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].InternalID().SnowflakeID() < nodes[j].InternalID().SnowflakeID()
	})
}

// sortRelsByID sorts relationships by snowflake.ID for deterministic output.
// Order is time-dominant (ms timestamp in high bits) with nodeField and step as tiebreakers.
func sortRelsByID(rels []*types.Relationship) {
	sort.Slice(rels, func(i, j int) bool {
		return rels[i].InternalID().SnowflakeID() < rels[j].InternalID().SnowflakeID()
	})
}
