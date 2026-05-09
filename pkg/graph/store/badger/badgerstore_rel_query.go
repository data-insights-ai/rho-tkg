package badger

import (
	"errors"
	"fmt"
	"sort"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Relationship-side query methods (R5-F9 split out from badgerstore_rel.go).
// Single-rel CRUD and helpers stay in badgerstore_rel.go; batch writes
// live in badgerstore_rel_batch.go.

func (bs *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.typeIdx[token]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)

	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(rids, opts)
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.outIdx[nid]
	rids := make([]types.RelID, 0, len(set))
	for id := range set {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.GetRelationship(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			rels = append(rels, r)
		}
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots all relIDs under one idxMu.RLock;
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *Store) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.outIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]types.RelID, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		perNode[nid] = ids
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, type-filter, group by source node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
				rels = append(rels, r)
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
func (bs *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.inIdx[nid]
	rids := make([]types.RelID, 0, len(set))
	for id, tok := range set {
		if typeToken == 0 || tok == typeToken {
			rids = append(rids, id)
		}
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.GetRelationship(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		rels = append(rels, r)
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots relIDs from inIdx under one
// idxMu.RLock (with early type filtering since inIdx stores typeToken);
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	// inIdx stores relID -> typeToken, enabling early type filtering.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.inIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]types.RelID, 0, len(set))
		for id, tok := range set {
			if typeToken == 0 || tok == typeToken {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			perNode[nid] = ids
		}
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, group by target node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			rels = append(rels, r)
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

// AllRelationships returns all stored relationships, with optional pagination
// and temporal filtering. Snapshot relIDs under lock, sort + paginate, then
// fetch via GetRelationship. Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)

	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(rids, opts)
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (bs *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		rels = append(rels, r)
	}

	if len(rels) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(rels)
	return rels, nil
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (bs *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}
	sort.Slice(rids, func(i, j int) bool { return rids[i].SnowflakeID() < rids[j].SnowflakeID() })
	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}
	return rids, nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *Store) ForEachRelID(fn func(types.RelID) bool) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for id := range bs.relIDs {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// getRelLocked retrieves a relationship from cache or Badger.
