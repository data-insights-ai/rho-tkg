package badger

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-side query methods.
// Single-rel CRUD and helpers stay in badgerstore_rel.go; batch writes
// live in badgerstore_rel_batch.go.

func (bs *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
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

	storepkg.SortRelIDs(rids)

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)
	return bs.fetchRelationshipsByTypeIDs(token, rids, opts)
}

func (bs *Store) fetchRelationshipsByTypeIDs(token uint16, ids []types.RelID, opts QueryOpts) ([]*types.Relationship, error) {
	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	rels := make([]*types.Relationship, 0, capForLimit(opts.Limit))
	for _, rid := range ids {
		id := rid.SnowflakeID()
		r, err := bs.prefetchRelScan(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", id, err)
		}
		if !r.HasTypeTokenRaw(token) {
			continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id, r.Temporal(), opts) {
			continue
		}
		rels = append(rels, r)
		if opts.Limit > 0 && len(rels) >= opts.Limit {
			break
		}
	}
	if len(rels) == 0 {
		return nil, nil
	}
	return rels, nil
}

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		bs.idxMu.RUnlock()
		return nil, ErrNodeNotFound
	}
	rids, idErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, false)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.prefetchRel(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		if relationshipMatchesOutgoing(r, nid, typeToken) {
			rels = append(rels, r)
		}
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots all relIDs under one idxMu.RLock;
// phase 2 fetches entities outside the lock via the LRU cache. Every requested
// node must exist; missing IDs return ErrNodeNotFound instead of partial results.
func (bs *Store) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}
	for _, nid := range typedNodeIDs {
		if err := storecontract.ValidateNodeID(nid); err != nil {
			return nil, err
		}
	}
	if err := bs.ensureNodeRowsLive(typedNodeIDs); err != nil {
		return nil, err
	}

	// Phase 1: snapshot matching relIDs per node under single read lock.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	seen := make(map[types.NodeID]struct{}, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := seen[nid]; done {
			continue
		}
		seen[nid] = struct{}{}
		if _, ok := bs.nodeIDs[nid]; !ok {
			bs.idxMu.RUnlock()
			return nil, ErrNodeNotFound
		}
		ids, idErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, false)
		if idErr != nil {
			bs.idxMu.RUnlock()
			return nil, idErr
		}
		if len(ids) > 0 {
			perNode[nid] = ids
		}
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities and group by source node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.prefetchRel(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			if relationshipMatchesOutgoing(r, nid, typeToken) {
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

// OutgoingDegree returns the number of outgoing relationships from nid
// (type-filtered) by counting adjacency-index entries — no entity resolution,
// no DeepCopy. See store.DegreeCapability for the exact/orphan semantics.
func (bs *Store) OutgoingDegree(nid types.NodeID, typeToken uint16) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return 0, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		return 0, ErrNodeNotFound
	}
	if bs.adjOnDisk {
		rids, err := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, false)
		if err != nil {
			return 0, err
		}
		return len(rids), nil
	}
	set := bs.outIdx[nid]
	if typeToken == 0 {
		return len(set), nil
	}
	typeSet := bs.typeIdx[typeToken]
	if len(typeSet) == 0 {
		return 0, nil
	}
	n := 0
	for id := range set {
		if _, ok := typeSet[id]; ok {
			n++
		}
	}
	return n, nil
}

// IncomingDegree returns the number of incoming relationships to nid
// (type-filtered) by counting adjacency-index entries — no entity resolution,
// no DeepCopy. See store.DegreeCapability for the exact/orphan semantics.
func (bs *Store) IncomingDegree(nid types.NodeID, typeToken uint16) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return 0, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		return 0, ErrNodeNotFound
	}
	if bs.adjOnDisk {
		rids, err := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, true)
		if err != nil {
			return 0, err
		}
		return len(rids), nil
	}
	set := bs.inIdx[nid]
	if typeToken == 0 {
		return len(set), nil
	}
	n := 0
	for _, ie := range set {
		if ie.typ == typeToken {
			n++
		}
	}
	return n, nil
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := bs.ensureNodeRowLive(nid); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	if _, ok := bs.nodeIDs[nid]; !ok {
		bs.idxMu.RUnlock()
		return nil, ErrNodeNotFound
	}
	rids, idErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, true)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}

	if len(rids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(rids))
	for _, rid := range rids {
		r, err := bs.prefetchRel(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
		}
		if relationshipMatchesIncoming(r, nid, typeToken) {
			rels = append(rels, r)
		}
	}

	storepkg.SortRelsByID(rels)
	return rels, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots relIDs from inIdx under one
// idxMu.RLock (with early type filtering since inIdx stores typeToken);
// phase 2 fetches entities outside the lock via the LRU cache. Every requested
// node must exist; missing IDs return ErrNodeNotFound instead of partial results.
func (bs *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}
	for _, nid := range typedNodeIDs {
		if err := storecontract.ValidateNodeID(nid); err != nil {
			return nil, err
		}
	}
	if err := bs.ensureNodeRowsLive(typedNodeIDs); err != nil {
		return nil, err
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	// inIdx stores relID -> typeToken, enabling early type filtering.
	bs.idxMu.RLock()
	perNode := make(map[types.NodeID][]types.RelID, len(typedNodeIDs))
	seen := make(map[types.NodeID]struct{}, len(typedNodeIDs))
	for _, nid := range typedNodeIDs {
		if _, done := seen[nid]; done {
			continue
		}
		seen[nid] = struct{}{}
		if _, ok := bs.nodeIDs[nid]; !ok {
			bs.idxMu.RUnlock()
			return nil, ErrNodeNotFound
		}
		ids, idErr := bs.adjacentRelIDsSnapshotLocked(nid, typeToken, true)
		if idErr != nil {
			bs.idxMu.RUnlock()
			return nil, idErr
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
			r, err := bs.prefetchRel(rid)
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid.SnowflakeID(), err)
			}
			if relationshipMatchesIncoming(r, nid, typeToken) {
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

func (bs *Store) ensureNodeRowLive(nid types.NodeID) error {
	_, err := bs.prefetchNode(nid)
	return err
}

func (bs *Store) ensureRelationshipEndpointRowsLive(start, end types.NodeID) error {
	if err := bs.ensureNodeRowLive(start); err != nil {
		return err
	}
	if end == start {
		return nil
	}
	return bs.ensureNodeRowLive(end)
}

func (bs *Store) ensureNodeRowsLive(ids []types.NodeID) error {
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		if err := bs.ensureNodeRowLive(id); err != nil {
			return err
		}
	}
	return nil
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

// AllRelationships returns all stored relationships, with optional pagination
// and temporal filtering. Snapshot relIDs under lock, sort + paginate, then
// fetch via GetRelationship. Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}

	storepkg.SortRelIDs(rids)

	// Temporal pre-filter via Peek.
	rids = bs.filterRelIDsByTemporalPeek(rids, opts)

	if storepkg.HasTemporalFilter(opts) {
		return bs.fetchRelsWithTemporalFilterPage(rids, opts)
	}
	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(rids, opts)
}

// GetRelationshipsByIDs returns relationships for every requested ID.
// Missing IDs return ErrRelNotFound. Results are sorted by snowflake.ID.
func (bs *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return nil, err
		}
	}

	if unique, ok := uniqueRelIDsPreserveOrderIfDuplicate(ids); ok {
		return bs.getRelationshipsByIDsWithDuplicates(ids, unique)
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.prefetchRel(id)
		if err != nil {
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

func (bs *Store) getRelationshipsByIDsWithDuplicates(ids, unique []types.RelID) ([]*types.Relationship, error) {
	found := make(map[types.RelID]*types.Relationship, len(unique))
	for _, id := range unique {
		r, err := bs.prefetchRel(id)
		if err != nil {
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		found[id] = r
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r := found[id]
		if r == nil {
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, ErrRelNotFound)
		}
		rels = append(rels, r)
	}
	if len(rels) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(rels)
	return rels, nil
}

func uniqueRelIDsPreserveOrderIfDuplicate(ids []types.RelID) ([]types.RelID, bool) {
	if len(ids) < 2 {
		return nil, false
	}
	if len(ids) <= 32 {
		for i, id := range ids {
			for _, prev := range ids[:i] {
				if id == prev {
					return uniqueRelIDsPreserveOrder(ids), true
				}
			}
		}
		return nil, false
	}

	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return uniqueRelIDsPreserveOrder(ids), true
		}
		seen[id] = struct{}{}
	}
	return nil, false
}

func uniqueRelIDsPreserveOrder(ids []types.RelID) []types.RelID {
	unique := make([]types.RelID, 0, len(ids))
	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

// AllRelIDs returns relationship IDs matching the temporal/depth query options,
// sorted in ascending ID order after pagination is applied.
func (bs *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	rids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		rids = append(rids, id)
	}
	bs.idxMu.RUnlock()

	if len(rids) == 0 {
		return nil, nil
	}
	storepkg.SortRelIDs(rids)
	if storepkg.HasTemporalFilter(opts) {
		var err error
		rids, err = bs.filterRelIDsByTemporalFetch(rids, opts)
		if err != nil {
			return nil, err
		}
	}
	rids = storepkg.PaginateRelIDs(rids, opts.After, opts.Limit)
	if len(rids) == 0 {
		return nil, nil
	}
	return rids, nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *Store) ForEachRelID(fn func(types.RelID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	bs.idxMu.RLock()
	ids := make([]types.RelID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// getRelLocked retrieves a relationship from cache or Badger.
