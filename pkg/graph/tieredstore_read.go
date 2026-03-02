package graph

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// --- Entity reads ---
// O(1) shard resolution: ref probe + timestamp extraction.

func (ts *TieredStore) GetNode(id snowflake.ID) (*types.Node, error) {
	shard := ts.shardForNodeID(id)
	return shard.GetNode(id)
}

func (ts *TieredStore) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	shard := ts.shardForRelID(id)
	return shard.GetRelationship(id)
}

func (ts *TieredStore) GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error) {
	var result []*types.Node
	for _, id := range ids {
		n, err := ts.GetNode(id)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, nil
}

func (ts *TieredStore) GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error) {
	var result []*types.Relationship
	for _, id := range ids {
		r, err := ts.GetRelationship(id)
		if errors.Is(err, ErrRelNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

// --- Label/type queries ---

func (ts *TieredStore) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	// Single-shard by label classification.
	shard := ts.shardForNode(token)
	return shard.NodesByLabel(token, opts)
}

func (ts *TieredStore) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	// Merge from ref + depth-filtered event shards.
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refRels, err := ts.refShard.RelationshipsByType(token, QueryOpts{
		ValidAt:    opts.ValidAt,
		ValidStart: opts.ValidStart,
		ValidEnd:   opts.ValidEnd,
	})
	if err != nil {
		return nil, err
	}

	var slices [][]*types.Relationship
	if len(refRels) > 0 {
		slices = append(slices, refRels)
	}

	for _, store := range eventStores {
		evtRels, err := store.RelationshipsByType(token, QueryOpts{
			ValidAt:    opts.ValidAt,
			ValidStart: opts.ValidStart,
			ValidEnd:   opts.ValidEnd,
		})
		if err != nil {
			return nil, err
		}
		if len(evtRels) > 0 {
			slices = append(slices, evtRels)
		}
	}

	merged := mergeRelSlices(slices)
	return applyRelPagination(merged, opts), nil
}

// --- Bulk queries ---

func (ts *TieredStore) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refNodes, err := ts.refShard.AllNodes(QueryOpts{
		ValidAt:    opts.ValidAt,
		ValidStart: opts.ValidStart,
		ValidEnd:   opts.ValidEnd,
	})
	if err != nil {
		return nil, err
	}

	var slices [][]*types.Node
	if len(refNodes) > 0 {
		slices = append(slices, refNodes)
	}

	for _, store := range eventStores {
		evtNodes, err := store.AllNodes(QueryOpts{
			ValidAt:    opts.ValidAt,
			ValidStart: opts.ValidStart,
			ValidEnd:   opts.ValidEnd,
		})
		if err != nil {
			return nil, err
		}
		if len(evtNodes) > 0 {
			slices = append(slices, evtNodes)
		}
	}

	merged := mergeNodeSlices(slices)
	return applyNodePagination(merged, opts), nil
}

func (ts *TieredStore) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refRels, err := ts.refShard.AllRelationships(QueryOpts{
		ValidAt:    opts.ValidAt,
		ValidStart: opts.ValidStart,
		ValidEnd:   opts.ValidEnd,
	})
	if err != nil {
		return nil, err
	}

	var slices [][]*types.Relationship
	if len(refRels) > 0 {
		slices = append(slices, refRels)
	}

	for _, store := range eventStores {
		evtRels, err := store.AllRelationships(QueryOpts{
			ValidAt:    opts.ValidAt,
			ValidStart: opts.ValidStart,
			ValidEnd:   opts.ValidEnd,
		})
		if err != nil {
			return nil, err
		}
		if len(evtRels) > 0 {
			slices = append(slices, evtRels)
		}
	}

	merged := mergeRelSlices(slices)
	return applyRelPagination(merged, opts), nil
}

// --- Adjacency queries ---

func (ts *TieredStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	// Entity + out/ are co-located in the node's shard.
	shard := ts.shardForNodeID(nodeID)
	return shard.OutgoingRelationships(nodeID, typeToken)
}

func (ts *TieredStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	// Get relIDs from the node's shard inIdx.
	shard := ts.shardForNodeID(nodeID)
	relIDs := shard.incomingRelIDs(nodeID, typeToken)

	if len(relIDs) == 0 {
		return nil, nil
	}

	// Fetch each rel entity via shard resolution (relID timestamp -> O(1) per entity).
	result := make([]*types.Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		relShard := ts.shardForRelID(relID)
		r, err := relShard.GetRelationship(relID)
		if errors.Is(err, ErrRelNotFound) {
			continue // orphan from partial failure
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
	})
	return result, nil
}

// --- Counts ---

func (ts *TieredStore) NodeCount() (int, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.NodeCount()
	if err != nil {
		return 0, err
	}
	total += n

	for _, store := range eventStores {
		n, err := store.NodeCount()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (ts *TieredStore) RelationshipCount() (int, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.RelationshipCount()
	if err != nil {
		return 0, err
	}
	total += n

	for _, store := range eventStores {
		n, err := store.RelationshipCount()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (ts *TieredStore) NodeCountByLabel(token uint16) (int, error) {
	// Single-shard by label classification.
	shard := ts.shardForNode(token)
	return shard.NodeCountByLabel(token)
}

func (ts *TieredStore) RelCountByType(token uint16) (int, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.RelCountByType(token)
	if err != nil {
		return 0, err
	}
	total += n

	for _, store := range eventStores {
		n, err := store.RelCountByType(token)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// --- Property indexes ---

func (ts *TieredStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	shard := ts.shardForNode(labelToken)
	return shard.NodesByLabelAndProperty(labelToken, key, value, opts)
}

// --- ID enumeration ---

func (ts *TieredStore) AllNodeIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllNodeIDs(QueryOpts{
		ValidAt:    opts.ValidAt,
		ValidStart: opts.ValidStart,
		ValidEnd:   opts.ValidEnd,
	})
	if err != nil {
		return nil, err
	}

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}

	for _, store := range eventStores {
		evtIDs, err := store.AllNodeIDs(QueryOpts{
			ValidAt:    opts.ValidAt,
			ValidStart: opts.ValidStart,
			ValidEnd:   opts.ValidEnd,
		})
		if err != nil {
			return nil, err
		}
		if len(evtIDs) > 0 {
			slices = append(slices, evtIDs)
		}
	}

	merged := mergeIDSlices(slices)
	return applyIDPagination(merged, opts), nil
}

func (ts *TieredStore) AllRelIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllRelIDs(QueryOpts{
		ValidAt:    opts.ValidAt,
		ValidStart: opts.ValidStart,
		ValidEnd:   opts.ValidEnd,
	})
	if err != nil {
		return nil, err
	}

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}

	for _, store := range eventStores {
		evtIDs, err := store.AllRelIDs(QueryOpts{
			ValidAt:    opts.ValidAt,
			ValidStart: opts.ValidStart,
			ValidEnd:   opts.ValidEnd,
		})
		if err != nil {
			return nil, err
		}
		if len(evtIDs) > 0 {
			slices = append(slices, evtIDs)
		}
	}

	merged := mergeIDSlices(slices)
	return applyIDPagination(merged, opts), nil
}

// --- History reads ---

func (ts *TieredStore) GetNodeVersion(id snowflake.ID, version uint32) (*types.Node, error) {
	shard := ts.shardForNodeID(id)
	return shard.GetNodeVersion(id, version)
}

func (ts *TieredStore) GetNodeHistory(id snowflake.ID) ([]*types.Node, error) {
	shard := ts.shardForNodeID(id)
	return shard.GetNodeHistory(id)
}

func (ts *TieredStore) GetRelVersion(id snowflake.ID, version uint32) (*types.Relationship, error) {
	shard := ts.shardForRelID(id)
	return shard.GetRelVersion(id, version)
}

func (ts *TieredStore) GetRelHistory(id snowflake.ID) ([]*types.Relationship, error) {
	shard := ts.shardForRelID(id)
	return shard.GetRelHistory(id)
}

func (ts *TieredStore) AllNodeHistoryIDs() ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllNodeHistoryIDs()
	if err != nil {
		return nil, err
	}

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}

	for _, store := range eventStores {
		evtIDs, err := store.AllNodeHistoryIDs()
		if err != nil {
			return nil, err
		}
		if len(evtIDs) > 0 {
			slices = append(slices, evtIDs)
		}
	}

	return mergeIDSlices(slices), nil
}

func (ts *TieredStore) AllRelHistoryIDs() ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventStores := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllRelHistoryIDs()
	if err != nil {
		return nil, err
	}

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}

	for _, store := range eventStores {
		evtIDs, err := store.AllRelHistoryIDs()
		if err != nil {
			return nil, err
		}
		if len(evtIDs) > 0 {
			slices = append(slices, evtIDs)
		}
	}

	return mergeIDSlices(slices), nil
}

// --- Merge helpers ---
// Standard k-way merge of pre-sorted slices. For Phase 3a with 2 shards,
// this is a simple 2-way merge.

func mergeNodeSlices(slices [][]*types.Node) []*types.Node {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	// 2-way merge for the common case.
	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Node, 0, total)

	// Flatten and sort (simple approach for small shard counts).
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
	})
	return result
}

func mergeRelSlices(slices [][]*types.Relationship) []*types.Relationship {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Relationship, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
	})
	return result
}

func mergeIDSlices(slices [][]snowflake.ID) []snowflake.ID {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]snowflake.ID, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

// --- Pagination helpers ---
// Apply After/Limit from QueryOpts to already-merged, sorted slices.

func applyNodePagination(nodes []*types.Node, opts QueryOpts) []*types.Node {
	if opts.After != 0 {
		i := sort.Search(len(nodes), func(i int) bool {
			return nodes[i].InternalID().SnowflakeID() > opts.After
		})
		nodes = nodes[i:]
	}
	if opts.Limit > 0 && len(nodes) > opts.Limit {
		nodes = nodes[:opts.Limit]
	}
	if len(nodes) == 0 {
		return nil
	}
	return nodes
}

func applyRelPagination(rels []*types.Relationship, opts QueryOpts) []*types.Relationship {
	if opts.After != 0 {
		i := sort.Search(len(rels), func(i int) bool {
			return rels[i].InternalID().SnowflakeID() > opts.After
		})
		rels = rels[i:]
	}
	if opts.Limit > 0 && len(rels) > opts.Limit {
		rels = rels[:opts.Limit]
	}
	if len(rels) == 0 {
		return nil
	}
	return rels
}

func applyIDPagination(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.After != 0 {
		i := sort.Search(len(ids), func(i int) bool {
			return ids[i] > opts.After
		})
		ids = ids[i:]
	}
	if opts.Limit > 0 && len(ids) > opts.Limit {
		ids = ids[:opts.Limit]
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}
