// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Store-contract sentinel error aliases. Re-exporting them as package-local
// names keeps the moved file readable. The canonical sentinel-error
// declarations live in pkg/graph/store (public contract).
var (
	ErrNodeExists            = storecontract.ErrNodeExists
	ErrNodeNotFound          = storecontract.ErrNodeNotFound
	ErrRelExists             = storecontract.ErrRelExists
	ErrRelNotFound           = storecontract.ErrRelNotFound
	ErrVersionNotFound       = storecontract.ErrVersionNotFound
	ErrIndexExists           = storecontract.ErrIndexExists
	ErrIndexNotFound         = storecontract.ErrIndexNotFound
	ErrTemporalIndexExists   = storecontract.ErrTemporalIndexExists
	ErrTemporalIndexNotFound = storecontract.ErrTemporalIndexNotFound
	ErrVectorIndexExists     = indexpkg.ErrVectorIndexExists
	ErrVectorIndexNotFound   = indexpkg.ErrVectorIndexNotFound
	ErrDimensionMismatch     = indexpkg.ErrDimensionMismatch
)

// QueryOpts is a Store-contract alias; canonical declaration lives in
// pkg/graph/store (the public contract).
type QueryOpts = storecontract.QueryOpts

// DistanceMetric is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type DistanceMetric = storecontract.DistanceMetric

// RelTombstone is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type RelTombstone = storecontract.RelTombstone

// Store is a thread-safe in-memory Store implementation.
// Uses maps for O(1) entity lookup and nested hash-sets for O(1) index maintenance.
type Store struct {
	mu    sync.RWMutex
	nodes map[types.NodeID]*types.Node
	rels  map[types.RelID]*types.Relationship

	// Label index: labelToken → set of node IDs.
	labelIdx map[uint16]map[types.NodeID]struct{}

	// RelType index: relTypeToken → set of rel IDs.
	typeIdx map[uint16]map[types.RelID]struct{}

	// Adjacency indexes — nested hash sets for O(1) insert/delete.
	outIdx map[types.NodeID]map[types.RelID]struct{} // startNodeID → set(relID)
	inIdx  map[types.NodeID]map[types.RelID]struct{} // endNodeID → set(relID)

	// Version history — pre-mutation snapshots keyed by entity ID and version.
	nodeHistory map[types.NodeID]map[uint32]*types.Node
	relHistory  map[types.RelID]map[uint32]*types.Relationship

	// Property indexes — label+property → value → set of node IDs.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Temporal indexes — labelToken → interval index for temporal push-down.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// High-frequency indexes — labelToken → time-bucketed index for O(1) insertion.
	// Separate map from temporalIndexes; only one type can exist per label at a time.
	hfIndexes map[uint16]*indexpkg.HighFrequencyIndex

	// Vector indexes — in-memory brute-force k-NN index on node properties.
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex
}

// New creates an empty Store with all indexes initialized.
func New() *Store {
	return &Store{
		nodes:           make(map[types.NodeID]*types.Node),
		rels:            make(map[types.RelID]*types.Relationship),
		labelIdx:        make(map[uint16]map[types.NodeID]struct{}),
		typeIdx:         make(map[uint16]map[types.RelID]struct{}),
		outIdx:          make(map[types.NodeID]map[types.RelID]struct{}),
		inIdx:           make(map[types.NodeID]map[types.RelID]struct{}),
		nodeHistory:     make(map[types.NodeID]map[uint32]*types.Node),
		relHistory:      make(map[types.RelID]map[uint32]*types.Relationship),
		propertyIndexes: make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex),
		temporalIndexes: make(map[uint16]*indexpkg.TemporalIndex),
		hfIndexes:       make(map[uint16]*indexpkg.HighFrequencyIndex),
		vectorIndexes:   make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
	}
}

// PutNode stores a node and indexes all its label tokens.
// Returns ErrNodeExists if a node with the same ID already exists.
func (ms *Store) PutNode(n *types.Node) error {
	nid := n.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.nodes[nid]; exists {
		return ErrNodeExists
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

	rawID := nid.SnowflakeID()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID)
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	n, ok := ms.nodes[nid]
	if !ok {
		return nil, ErrNodeNotFound
	}
	return n.DeepCopy(), nil
}

// DeleteNode removes a node and its label index entries.
// The Graph layer is responsible for cascade-deleting relationships first.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) DeleteNode(nid types.NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	n, ok := ms.nodes[nid]
	if !ok {
		return ErrNodeNotFound
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

// RemoveNodeLabelToken removes tok from the label index for id and stores updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw).
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}

	// Remove only the specified token from the label index.
	if set, ok := ms.labelIdx[tok]; ok {
		delete(set, nid)
		if len(set) == 0 {
			delete(ms.labelIdx, tok)
		}
	}

	rawID := nid.SnowflakeID()
	// Update property, temporal, and vector indexes (properties may have changed due to hash update).
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = updatedNode.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, updatedNode, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, updatedNode, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, updatedNode, rawID)
	return nil
}

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

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (ms *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}

	set, ok := ms.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		ms.labelIdx[tok] = set
	}
	set[nid] = struct{}{}

	rawID := nid.SnowflakeID()
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

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (ms *Store) ReplaceNode(n *types.Node) error {
	nid := n.ID()

	ms.mu.Lock()
	defer ms.mu.Unlock()

	old, exists := ms.nodes[nid]
	if !exists {
		return ErrNodeNotFound
	}
	rawID := nid.SnowflakeID()
	indexpkg.RemoveNodeFromPropertyIndexes(ms.propertyIndexes, old, rawID)
	indexpkg.RemoveNodeFromTemporalIndexes(ms.temporalIndexes, old, rawID)
	indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, old, rawID)
	ms.nodes[nid] = n.DeepCopy()
	indexpkg.AddNodeToPropertyIndexes(ms.propertyIndexes, n, rawID)
	indexpkg.AddNodeToTemporalIndexes(ms.temporalIndexes, n, rawID)
	indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID)
	return nil
}

// PutRelationship stores a relationship and indexes its type and adjacency.
// Returns ErrNodeNotFound if start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (ms *Store) PutRelationship(r *types.Relationship) error {
	id := r.ID()
	startID := r.StartNodeID()
	endID := r.EndNodeID()

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
		ms.typeIdx[tv] = make(map[types.RelID]struct{})
	}
	ms.typeIdx[tv][id] = struct{}{}

	// Adjacency: outgoing.
	if ms.outIdx[startID] == nil {
		ms.outIdx[startID] = make(map[types.RelID]struct{})
	}
	ms.outIdx[startID][id] = struct{}{}

	// Adjacency: incoming.
	if ms.inIdx[endID] == nil {
		ms.inIdx[endID] = make(map[types.RelID]struct{})
	}
	ms.inIdx[endID][id] = struct{}{}

	return nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	r, ok := ms.rels[rid]
	if !ok {
		return nil, ErrRelNotFound
	}
	return r.DeepCopy(), nil
}

// ReplaceRelationship overwrites an existing relationship's data in-place.
// Returns ErrRelNotFound if the relationship does not exist.
// No index changes — type and endpoints are immutable after creation.
func (ms *Store) ReplaceRelationship(r *types.Relationship) error {
	id := r.ID()

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
func (ms *Store) DeleteRelationship(rid types.RelID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	return ms.deleteRelLocked(rid)
}

// deleteRelLocked removes a relationship and cleans up indexes.
// Caller must hold ms.mu write lock.
func (ms *Store) deleteRelLocked(id types.RelID) error {
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
	startID := r.StartNodeID()
	if set, exists := ms.outIdx[startID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.outIdx, startID)
		}
	}

	endID := r.EndNodeID()
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
func (ms *Store) DeleteNodeCascade(nid types.NodeID) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

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
		// Ignore ErrRelNotFound — can't happen with dedup, but defensive.
		_ = ms.deleteRelLocked(relID)
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

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index for fast filtering when one exists and a temporal filter is set.
// Store never returns an error.
func (ms *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.labelIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	// Temporal index fast path: use interval index if one exists for this label.
	// When a temporal query is requested (ValidAt or interval), the index result
	// is always authoritative — nil means 0 matches, not "index not consulted."
	// We must not fall through to the full label scan in that case.
	if ti, ok := ms.temporalIndexes[token]; ok {
		// temporalIndex returns raw snowflake.IDs (Tier D handoff).
		var rawIDs []snowflake.ID
		temporalQuery := false
		if opts.ValidAt != 0 {
			rawIDs = ti.QueryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			rawIDs = ti.QueryOverlap(opts.ValidStart, opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			rawIDs = storepkg.PaginateIDs(rawIDs, opts.After, opts.Limit)
			if len(rawIDs) == 0 {
				return nil, nil
			}
			result := make([]*types.Node, 0, len(rawIDs))
			for _, id := range rawIDs {
				if n, ok := ms.nodes[types.NodeID(id)]; ok {
					result = append(result, n.DeepCopy())
				}
			}
			return result, nil
		}
	}

	// Standard path: collect and sort IDs before fetching entities.
	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter: read in-memory entity pointer (no deep-copy).
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
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
// Store never returns an error.
func (ms *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
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
// Store never returns an error.
func (ms *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.outIdx[nid]
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
	storepkg.SortRelsByID(result)
	return result, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation under one read lock.
func (ms *Store) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make(map[types.NodeID][]*types.Relationship, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := result[nid]; done {
			continue // deduplicate input
		}
		set := ms.outIdx[nid]
		if len(set) == 0 {
			continue
		}
		rels := make([]*types.Relationship, 0, len(set))
		for relID := range set {
			r, ok := ms.rels[relID]
			if !ok {
				continue
			}
			if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
				rels = append(rels, r.DeepCopy())
			}
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
// Store never returns an error.
func (ms *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.inIdx[nid]
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
	storepkg.SortRelsByID(result)
	return result, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation under one read lock.
func (ms *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make(map[types.NodeID][]*types.Relationship, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := result[nid]; done {
			continue // deduplicate input
		}
		set := ms.inIdx[nid]
		if len(set) == 0 {
			continue
		}
		rels := make([]*types.Relationship, 0, len(set))
		for relID := range set {
			r, ok := ms.rels[relID]
			if !ok {
				continue
			}
			if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
				rels = append(rels, r.DeepCopy())
			}
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// NodeCount returns the number of stored nodes.
// Store never returns an error.
func (ms *Store) NodeCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.nodes), nil
}

// RelationshipCount returns the number of stored relationships.
// Store never returns an error.
func (ms *Store) RelationshipCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.rels), nil
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
// Store never returns an error.
func (ms *Store) NodeCountByLabel(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.labelIdx[token]), nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
// Store never returns an error.
func (ms *Store) RelCountByType(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.typeIdx[token]), nil
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

// --- Property indexes ---

// CreatePropertyIndex creates a property index for the given label token and property key.
// Scans all existing nodes with that label to populate the index.
// Returns ErrIndexExists if the index already exists.
func (ms *Store) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.propertyIndexes[key]; exists {
		return ErrIndexExists
	}

	idx := indexpkg.NewPropertyIndex()

	// Populate from existing nodes with this label.
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			if val, found := n.GetProperty(propertyKey); found {
				idx.Add(nodeID.SnowflakeID(), val)
			}
		}
	}

	ms.propertyIndexes[key] = idx
	return nil
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (ms *Store) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
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
func (ms *Store) CreateTemporalIndex(labelToken uint16) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	ti := indexpkg.NewTemporalIndex()
	if nodeIDs, ok := ms.labelIdx[labelToken]; ok {
		for nodeID := range nodeIDs {
			n := ms.nodes[nodeID]
			if n == nil {
				continue
			}
			rawID := nodeID.SnowflakeID()
			from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
			ti.Add(rawID, from, to)
		}
	}

	ms.temporalIndexes[labelToken] = ti
	return nil
}

// DropTemporalIndex removes a temporal index for the given label token.
// Returns ErrTemporalIndexNotFound if no index exists.
func (ms *Store) DropTemporalIndex(labelToken uint16) error {
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
func (ms *Store) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if _, exists := ms.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := ms.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	ms.hfIndexes[labelToken] = indexpkg.NewHighFrequencyIndex(bucketSize, 0)
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (ms *Store) DropHighFrequencyIndex(labelToken uint16) error {
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
func (ms *Store) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if _, exists := ms.vectorIndexes[key]; exists {
		return ErrVectorIndexExists
	}
	vi := &indexpkg.VectorIndex{Dims: dims, Metric: metric}
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
		vec, ok := indexpkg.ToFloat32Slice(val)
		if !ok {
			continue
		}
		_ = vi.Add(id.SnowflakeID(), vec) // dimension mismatch: skip entry, index is still usable
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ms *Store) DropVectorIndex(labelToken uint16, propertyKey string) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
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
//
// The opts parameter is intentionally unused: Store is single-tier,
// so Depth has no meaning, and temporal filtering is applied by the Graph
// layer via searchNearestFiltered before this path is taken. The parameter
// is required by the Store interface contract.
func (ms *Store) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, _ QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	ids, err := vi.SearchNearest(query, k, nil)
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
		if n, ok := ms.nodes[types.NodeID(id)]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// SearchNearestFiltered is the package-internal entry point used by the
// Graph layer to perform vector search with an eligibility filter applied
// BEFORE the k-cut. The filter is invoked under the vector index read lock,
// so it must NOT call back into the store (deadlock).
//
// Returns raw snowflake.IDs in ascending distance order; the caller is
// responsible for resolving entities (current or historical version).
func (ms *Store) SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(snowflake.ID) bool) ([]snowflake.ID, error) {
	ms.mu.RLock()
	key := indexpkg.VectorIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	vi, exists := ms.vectorIndexes[key]
	ms.mu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}
	return vi.SearchNearest(query, k, filter)
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional pagination and temporal filtering. Uses the property index if
// one exists; falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}
	if idx, ok := ms.propertyIndexes[key]; ok {
		// Indexed path: collect matching IDs, sort, temporal filter, paginate, then fetch.
		// propertyIndex returns raw snowflake.IDs (Tier D handoff).
		matchSet := idx.Lookup(value)
		if len(matchSet) == 0 {
			return nil, nil
		}
		ids := make([]types.NodeID, 0, len(matchSet))
		for id := range matchSet {
			ids = append(ids, types.NodeID(id))
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		ids = ms.filterNodeIDsByTemporal(ids, opts)

		ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
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
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	labelIDs := ms.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		return nil, nil
	}

	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	// Collect matching IDs from label scan.
	var matchIDs []types.NodeID
	for id := range labelIDs {
		n, ok := ms.nodes[id]
		if !ok {
			continue
		}
		if v, found := n.GetProperty(propKey); found {
			if indexpkg.PropertyValueKey(v) == targetKey {
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

	matchIDs = storepkg.PaginateNodeIDs(matchIDs, opts.After, opts.Limit)
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
func (ms *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}
	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy.
func (ms *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}
	ids := make([]types.RelID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
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
func (ms *Store) ForEachRelID(fn func(types.RelID) bool) error {
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
func (ms *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
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
func (ms *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
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
// Thin wrapper that delegates to AllNodeHistoryIDsFrom(0, 0).
func (ms *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	return ms.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
}

// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
// Thin wrapper that delegates to AllRelHistoryIDsFrom(0, 0).
func (ms *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	return ms.AllRelHistoryIDsFrom(types.RelID(0), 0)
}

// AllNodeHistoryIDsFrom returns the IDs of nodes with version history, sorted
// ascending, starting strictly after `after`. limit ≤ 0 returns all remaining.
func (ms *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodeHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.NodeID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateNodeIDs(ids, types.EntityID(after), limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelHistoryIDsFrom returns the IDs of relationships with version history,
// sorted ascending, starting strictly after `after`. limit ≤ 0 returns all remaining.
func (ms *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.relHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.RelID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateRelIDs(ids, types.EntityID(after), limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// Clear removes all entities, indexes, history, and property indexes.
// After Clear(), the Store is empty (same state as New()).
func (ms *Store) Clear() error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.nodes = make(map[types.NodeID]*types.Node)
	ms.rels = make(map[types.RelID]*types.Relationship)
	ms.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	ms.typeIdx = make(map[uint16]map[types.RelID]struct{})
	ms.outIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.inIdx = make(map[types.NodeID]map[types.RelID]struct{})
	ms.nodeHistory = make(map[types.NodeID]map[uint32]*types.Node)
	ms.relHistory = make(map[types.RelID]map[uint32]*types.Relationship)
	ms.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	ms.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	ms.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	ms.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)
	return nil
}

// Close is a no-op for Store. Satisfies the Store interface.
func (ms *Store) Close() error { return nil }

// --- Batch operations ---

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: deep-copy each, store, and update label indexes.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (ms *Store) PutNodesBatch(nodes []*types.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		id := n.ID()
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
		indexpkg.AddNodeToVectorIndexes(ms.vectorIndexes, n, rawID)
	}

	return nil
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: deep-copy each, store, update type + adjacency indexes.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (ms *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if len(rels) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		id := r.ID()
		startID := r.StartNodeID()
		endID := r.EndNodeID()

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
		id := r.ID()
		startID := r.StartNodeID()
		endID := r.EndNodeID()

		ms.rels[id] = r.DeepCopy()

		tv := r.TypeToken().Value()
		if ms.typeIdx[tv] == nil {
			ms.typeIdx[tv] = make(map[types.RelID]struct{})
		}
		ms.typeIdx[tv][id] = struct{}{}

		if ms.outIdx[startID] == nil {
			ms.outIdx[startID] = make(map[types.RelID]struct{})
		}
		ms.outIdx[startID][id] = struct{}{}

		if ms.inIdx[endID] == nil {
			ms.inIdx[endID] = make(map[types.RelID]struct{})
		}
		ms.inIdx[endID][id] = struct{}{}
	}

	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: remove each from store and clean label indexes.
// Missing ID → ErrNodeNotFound, zero mutations. Nil/empty input → nil error.
func (ms *Store) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if len(typedIDs) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — all must exist.
	for _, id := range typedIDs {
		if _, exists := ms.nodes[id]; !exists {
			return ErrNodeNotFound
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
		indexpkg.RemoveNodeFromVectorIndexes(ms.vectorIndexes, n, rawID)
		delete(ms.nodes, id)
	}

	return nil
}

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: delete each via deleteRelLocked (handles type/adjacency/history cleanup).
// Missing ID → ErrRelNotFound, zero mutations. Nil/empty input → nil error.
func (ms *Store) DeleteRelationshipsBatch(typedIDs []types.RelID) error {
	if len(typedIDs) == 0 {
		return nil
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Phase 1: validate — all must exist.
	for _, id := range typedIDs {
		if _, exists := ms.rels[id]; !exists {
			return ErrRelNotFound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, id := range typedIDs {
		// deleteRelLocked can't fail here (verified existence above, holding write lock).
		_ = ms.deleteRelLocked(id)
	}

	return nil
}

// --- Bulk queries ---

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}

	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
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
func (ms *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}

	ids := make([]types.RelID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
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
func (ms *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
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
	storepkg.SortNodesByID(result)
	return result, nil
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (ms *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
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
	storepkg.SortRelsByID(result)
	return result, nil
}

// --- Temporal filtering helpers ---

// filterNodeIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy, no allocation per entity.
// Caller must hold ms.mu (read or write).
func (ms *Store) filterNodeIDsByTemporal(ids []types.NodeID, opts QueryOpts) []types.NodeID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		n, ok := ms.nodes[id]
		if !ok {
			continue
		}
		if storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// filterRelIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy.
// Caller must hold ms.mu (read or write).
func (ms *Store) filterRelIDsByTemporal(ids []types.RelID, opts QueryOpts) []types.RelID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		r, ok := ms.rels[id]
		if !ok {
			continue
		}
		if storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}
