// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"errors"
	"fmt"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// PutRelationship stores a relationship and indexes its type and adjacency.
// Returns ErrNodeNotFound if start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (ms *Store) PutRelationship(r *types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	id := r.ID()
	startID := r.StartNodeID()
	endID := r.EndNodeID()

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

// PutRelationshipGeneratedIDWithEndpointHashes stores a graph-generated
// relationship while capturing live endpoint hashes under the same store lock.
func (ms *Store) PutRelationshipGeneratedIDWithEndpointHashes(r *types.Relationship, proof generatedcreate.Proof) (string, string, error) {
	if !proof.Valid() {
		if err := ms.PutRelationship(r); err != nil {
			return "", "", err
		}
		if ig := r.Integrity(); ig != nil {
			return ig.FromNodeHash, ig.ToNodeHash, nil
		}
		return "", "", nil
	}
	if ms == nil {
		return "", "", ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return "", "", err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return "", "", err
	}
	id := r.ID()
	startID := r.StartNodeID()
	endID := r.EndNodeID()

	start, ok := ms.nodes[startID]
	if !ok {
		return "", "", ErrNodeNotFound
	}
	end, ok := ms.nodes[endID]
	if !ok {
		return "", "", ErrNodeNotFound
	}
	if _, exists := ms.rels[id]; exists {
		return "", "", ErrRelExists
	}

	fromHash := nodeIntegrityHash(start)
	toHash := fromHash
	if startID != endID {
		toHash = nodeIntegrityHash(end)
	}

	ig := r.Integrity()
	if ig == nil {
		ig = &types.RelIntegrity{}
		r.SetIntegrity(ig)
	}
	ig.FromNodeHash = fromHash
	ig.ToNodeHash = toHash

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

	return fromHash, toHash, nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if rid <= 0 {
		return nil, storecontract.ValidateRelID(rid)
	}

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
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(r); err != nil {
		return err
	}
	id := r.ID()

	old, exists := ms.rels[id]
	if !exists {
		return ErrRelNotFound
	}
	if err := storecontract.ValidateRelationshipReplacement(old, r); err != nil {
		return err
	}
	ms.rels[id] = r.DeepCopy()
	return nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (ms *Store) DeleteRelationship(rid types.RelID) error {
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

func (ms *Store) deleteRelOrPurgeOrphanLocked(id types.RelID) error {
	if err := ms.deleteRelLocked(id); err != nil {
		if errors.Is(err, ErrRelNotFound) {
			ms.purgeRelIDFromIndexesLocked(id)
			return nil
		}
		return err
	}
	return nil
}

func (ms *Store) purgeRelIDFromIndexesLocked(id types.RelID) {
	for tok, set := range ms.typeIdx {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.typeIdx, tok)
		}
	}
	for nid, set := range ms.outIdx {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.outIdx, nid)
		}
	}
	for nid, set := range ms.inIdx {
		delete(set, id)
		if len(set) == 0 {
			delete(ms.inIdx, nid)
		}
	}
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
// Returns ErrNodeNotFound if the requested node does not exist.
func (ms *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
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
	if _, ok := ms.nodes[nid]; !ok {
		return nil, ErrNodeNotFound
	}

	set := ms.outIdx[nid]
	if len(set) == 0 {
		return nil, nil
	}
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
		if len(typeSet) == 0 {
			return nil, nil
		}
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		if typeToken != 0 {
			if _, ok := typeSet[relID]; !ok {
				continue
			}
		}
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if relationshipMatchesOutgoing(r, nid, typeToken) {
			result = append(result, r.DeepCopy())
		}
	}
	storepkg.SortRelsByID(result)
	return result, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation under one read lock. Every requested node must
// exist; missing IDs return ErrNodeNotFound instead of partial results.
func (ms *Store) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	result := make(map[types.NodeID][]*types.Relationship, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if err := storecontract.ValidateNodeID(nid); err != nil {
			return nil, err
		}
		if _, done := result[nid]; done {
			continue
		}
		if _, ok := ms.nodes[nid]; !ok {
			return nil, ErrNodeNotFound
		}
		result[nid] = nil
	}

	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
	}
	for nid := range result {
		set := ms.outIdx[nid]
		if len(set) == 0 {
			delete(result, nid)
			continue
		}
		if typeToken != 0 && len(typeSet) == 0 {
			delete(result, nid)
			continue
		}
		rels := make([]*types.Relationship, 0, len(set))
		for relID := range set {
			if typeToken != 0 {
				if _, ok := typeSet[relID]; !ok {
					continue
				}
			}
			r, ok := ms.rels[relID]
			if !ok {
				continue
			}
			if relationshipMatchesOutgoing(r, nid, typeToken) {
				rels = append(rels, r.DeepCopy())
			}
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		} else {
			delete(result, nid)
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
// Returns ErrNodeNotFound if the requested node does not exist.
func (ms *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
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
	if _, ok := ms.nodes[nid]; !ok {
		return nil, ErrNodeNotFound
	}

	set := ms.inIdx[nid]
	if len(set) == 0 {
		return nil, nil
	}
	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
		if len(typeSet) == 0 {
			return nil, nil
		}
	}
	result := make([]*types.Relationship, 0, len(set))
	for relID := range set {
		if typeToken != 0 {
			if _, ok := typeSet[relID]; !ok {
				continue
			}
		}
		r, ok := ms.rels[relID]
		if !ok {
			continue
		}
		if relationshipMatchesIncoming(r, nid, typeToken) {
			result = append(result, r.DeepCopy())
		}
	}
	storepkg.SortRelsByID(result)
	return result, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation under one read lock. Every requested node must
// exist; missing IDs return ErrNodeNotFound instead of partial results.
func (ms *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	result := make(map[types.NodeID][]*types.Relationship, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if err := storecontract.ValidateNodeID(nid); err != nil {
			return nil, err
		}
		if _, done := result[nid]; done {
			continue
		}
		if _, ok := ms.nodes[nid]; !ok {
			return nil, ErrNodeNotFound
		}
		result[nid] = nil
	}

	var typeSet map[types.RelID]struct{}
	if typeToken != 0 {
		typeSet = ms.typeIdx[typeToken]
		if len(typeSet) == 0 {
			return nil, nil
		}
	}
	for nid := range result {
		set := ms.inIdx[nid]
		if len(set) == 0 {
			delete(result, nid)
			continue
		}
		rels := make([]*types.Relationship, 0, len(set))
		for relID := range set {
			if typeToken != 0 {
				if _, ok := typeSet[relID]; !ok {
					continue
				}
			}
			r, ok := ms.rels[relID]
			if !ok {
				continue
			}
			if relationshipMatchesIncoming(r, nid, typeToken) {
				rels = append(rels, r.DeepCopy())
			}
		}
		if len(rels) > 0 {
			storepkg.SortRelsByID(rels)
			result[nid] = rels
		} else {
			delete(result, nid)
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func relationshipMatchesOutgoing(r *types.Relationship, nid types.NodeID, typeToken uint16) bool {
	if r == nil || r.StartNodeID() != nid {
		return false
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}

func relationshipMatchesIncoming(r *types.Relationship, nid types.NodeID, typeToken uint16) bool {
	if r == nil || r.EndNodeID() != nid {
		return false
	}
	return typeToken == 0 || r.HasTypeTokenRaw(typeToken)
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: deep-copy each, store, update type + adjacency indexes.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (ms *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
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

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist.
// Phase 2: delete each via deleteRelLocked (handles type/adjacency cleanup; history is preserved).
// Missing ID → ErrRelNotFound, zero mutations. Duplicate IDs are coalesced.
// Nil/empty input → nil error.
func (ms *Store) DeleteRelationshipsBatch(typedIDs []types.RelID) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()

	if err := ms.checkOpenLocked(); err != nil {
		return err
	}
	if len(typedIDs) == 0 {
		return nil
	}
	for _, id := range typedIDs {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
	}
	typedIDs = uniqueRelIDs(typedIDs)

	// Phase 1: validate — all must exist.
	for _, id := range typedIDs {
		if _, exists := ms.rels[id]; !exists {
			return ErrRelNotFound
		}
	}

	// Phase 2: apply — all validated, safe to mutate.
	for _, id := range typedIDs {
		if err := ms.deleteRelLocked(id); err != nil {
			return err
		}
	}

	return nil
}

func uniqueRelIDs(ids []types.RelID) []types.RelID {
	seen := make(map[types.RelID]struct{}, len(ids))
	out := make([]types.RelID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
