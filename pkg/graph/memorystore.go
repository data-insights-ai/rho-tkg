package graph

import (
	"fmt"
	"sort"
	"sync"

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
}

// NewMemoryStore creates an empty MemoryStore with all indexes initialized.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		nodes:       make(map[snowflake.ID]*types.Node),
		rels:        make(map[snowflake.ID]*types.Relationship),
		labelIdx:    make(map[uint16]map[snowflake.ID]struct{}),
		typeIdx:     make(map[uint16]map[snowflake.ID]struct{}),
		outIdx:      make(map[snowflake.ID]map[snowflake.ID]struct{}),
		inIdx:       make(map[snowflake.ID]map[snowflake.ID]struct{}),
		nodeHistory: make(map[snowflake.ID]map[uint32]*types.Node),
		relHistory:  make(map[snowflake.ID]map[uint32]*types.Relationship),
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

	delete(ms.nodes, id)
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No index changes — labels are immutable after creation.
func (ms *MemoryStore) ReplaceNode(n *types.Node) error {
	id := n.InternalID().SnowflakeID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.nodes[id]; !exists {
		return ErrNodeNotFound
	}
	ms.nodes[id] = n.DeepCopy()
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
	delete(ms.relHistory, id)
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

	delete(ms.nodes, id)
	delete(ms.nodeHistory, id)
	return nil
}

// NodesByLabel returns all nodes with the given label token.
// Results are sorted by snowflake.ID for deterministic output.
// MemoryStore never returns an error.
func (ms *MemoryStore) NodesByLabel(token uint16) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.labelIdx[token]
	if len(set) == 0 {
		return nil, nil
	}
	result := make([]*types.Node, 0, len(set))
	for id := range set {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	sortNodesByID(result)
	return result, nil
}

// RelationshipsByType returns all relationships with the given type token.
// Results are sorted by snowflake.ID for deterministic output.
// MemoryStore never returns an error.
func (ms *MemoryStore) RelationshipsByType(token uint16) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil, nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for id := range set {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	sortRelsByID(result)
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

// AllNodes returns all stored nodes.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *MemoryStore) AllNodes() ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}
	result := make([]*types.Node, 0, len(ms.nodes))
	for _, n := range ms.nodes {
		result = append(result, n.DeepCopy())
	}
	sortNodesByID(result)
	return result, nil
}

// AllRelationships returns all stored relationships.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *MemoryStore) AllRelationships() ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}
	result := make([]*types.Relationship, 0, len(ms.rels))
	for _, r := range ms.rels {
		result = append(result, r.DeepCopy())
	}
	sortRelsByID(result)
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
