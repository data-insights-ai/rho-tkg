package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Bulk read (parallel folds across all shards) ---

func (s *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Node, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		nodes, e := shard.NodesByLabel(token, stripPagination(opts))
		per[idx] = nodes
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateNodes(mergeSortNodes(per), opts), nil
}

func (s *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Relationship, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		rels, e := shard.RelationshipsByType(token, stripPagination(opts))
		per[idx] = rels
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateRels(mergeSortRels(per), opts), nil
}

func (s *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Node, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		nodes, e := shard.AllNodes(stripPagination(opts))
		per[idx] = nodes
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateNodes(mergeSortNodes(per), opts), nil
}

func (s *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]*types.Relationship, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		rels, e := shard.AllRelationships(stripPagination(opts))
		per[idx] = rels
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateRels(mergeSortRels(per), opts), nil
}

// GetNodesByIDs routes each requested ID to its shard, resolves per shard (all
// missing → ErrNodeNotFound), and returns the union sorted by ID.
func (s *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	buckets := make(map[int][]types.NodeID)
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
		idx, err := s.shardIndexForNode(id)
		if err != nil {
			return nil, err
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var slices [][]*types.Node
	for idx, bucket := range buckets {
		nodes, err := s.shards[idx].GetNodesByIDs(bucket)
		if err != nil {
			return nil, err
		}
		slices = append(slices, nodes)
	}
	merged := mergeSortNodes(slices)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

func (s *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	buckets := make(map[int][]types.RelID)
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return nil, err
		}
		idx, err := s.shardIndexForRel(id)
		if err != nil {
			return nil, err
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var slices [][]*types.Relationship
	for idx, bucket := range buckets {
		rels, err := s.shards[idx].GetRelationshipsByIDs(bucket)
		if err != nil {
			return nil, err
		}
		slices = append(slices, rels)
	}
	merged := mergeSortRels(slices)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// shardIndexForNode / shardIndexForRel return the shard index owning the ID, or
// ErrSlotNotLocal.
func (s *Store) shardIndexForNode(id types.NodeID) (int, error) {
	idx, ok := s.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
	if !ok {
		return 0, ErrSlotNotLocal
	}
	return idx, nil
}

func (s *Store) shardIndexForRel(id types.RelID) (int, error) {
	idx, ok := s.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
	if !ok {
		return 0, ErrSlotNotLocal
	}
	return idx, nil
}
