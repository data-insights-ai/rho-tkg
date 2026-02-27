package graph

import (
	"sync"

	snowflake "gitlab2024.bds421-cloud.com/bds421/rho/snowflake-2026"
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
}

// NewMemoryStore creates an empty MemoryStore with all indexes initialized.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		nodes:    make(map[snowflake.ID]*types.Node),
		rels:     make(map[snowflake.ID]*types.Relationship),
		labelIdx: make(map[uint16]map[snowflake.ID]struct{}),
		typeIdx:  make(map[uint16]map[snowflake.ID]struct{}),
		outIdx:   make(map[snowflake.ID]map[snowflake.ID]struct{}),
		inIdx:    make(map[snowflake.ID]map[snowflake.ID]struct{}),
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

	ms.nodes[id] = n

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
	return n, nil
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

	ms.rels[id] = r

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
	return r, nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *MemoryStore) DeleteRelationship(id snowflake.ID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

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

// NodesByLabel returns all nodes with the given label token.
func (ms *MemoryStore) NodesByLabel(token uint16) []*types.Node {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.labelIdx[token]
	if len(set) == 0 {
		return nil
	}
	result := make([]*types.Node, 0, len(set))
	for id := range set {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n)
		}
	}
	return result
}

// RelationshipsByType returns all relationships with the given type token.
func (ms *MemoryStore) RelationshipsByType(token uint16) []*types.Relationship {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for id := range set {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r)
		}
	}
	return result
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
func (ms *MemoryStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.outIdx[nodeID]
	if len(set) == 0 {
		return nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			result = append(result, r)
		}
	}
	return result
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
func (ms *MemoryStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) []*types.Relationship {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.inIdx[nodeID]
	if len(set) == 0 {
		return nil
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			result = append(result, r)
		}
	}
	return result
}

// NodeCount returns the number of stored nodes.
func (ms *MemoryStore) NodeCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.nodes)
}

// RelationshipCount returns the number of stored relationships.
func (ms *MemoryStore) RelationshipCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.rels)
}
