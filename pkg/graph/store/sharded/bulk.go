package sharded

import (
	"errors"

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
// missing → ErrNodeNotFound), and returns the union sorted by ID. Shard
// buckets are read in PARALLEL (BACKLOG 20l) through the same bounded worker
// pool every other multi-shard read in this file uses (forEachShardErr) —
// this was the one multi-shard read left applying its buckets sequentially.
func (s *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	return s.GetNodesByIDsMetered(ids, nil)
}

var _ storecontract.NodesByIDsMeteredCapability = (*Store)(nil)

// GetNodesByIDsMetered is GetNodesByIDs counting in t (nil: nothing) the bytes
// of each row a slot decodes on a cache miss, before the decode (RH list R2).
func (s *Store) GetNodesByIDsMetered(ids []types.NodeID, t *storecontract.ReadTally) ([]*types.Node, error) {
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
	bucketIdx := make([]int, 0, len(buckets))
	for idx := range buckets {
		bucketIdx = append(bucketIdx, idx)
	}
	slices := make([][]*types.Node, len(bucketIdx))
	errs := make([]error, len(bucketIdx))
	runShardPool(len(bucketIdx), func(i int) {
		idx := bucketIdx[i]
		slices[i], errs[i] = s.shards[idx].GetNodesByIDsMetered(buckets[idx], t)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	merged := mergeSortNodes(slices)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// GetRelationshipsByIDs mirrors GetNodesByIDs's parallel per-shard-bucket read
// (BACKLOG 20l).
func (s *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	return s.GetRelationshipsByIDsMetered(ids, nil)
}

var _ storecontract.RelsByIDsMeteredCapability = (*Store)(nil)

// GetRelationshipsByIDsMetered is GetRelationshipsByIDs counting in t (nil:
// nothing) the bytes of each row a slot decodes on a cache miss, before the
// decode (RH list R2).
func (s *Store) GetRelationshipsByIDsMetered(ids []types.RelID, t *storecontract.ReadTally) ([]*types.Relationship, error) {
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
	bucketIdx := make([]int, 0, len(buckets))
	for idx := range buckets {
		bucketIdx = append(bucketIdx, idx)
	}
	slices := make([][]*types.Relationship, len(bucketIdx))
	errs := make([]error, len(bucketIdx))
	runShardPool(len(bucketIdx), func(i int) {
		idx := bucketIdx[i]
		slices[i], errs[i] = s.shards[idx].GetRelationshipsByIDsMetered(buckets[idx], t)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	merged := mergeSortRels(slices)
	if len(merged) == 0 {
		return nil, nil
	}
	return merged, nil
}

// shardIndexForNode / shardIndexForRel return the shard index owning the ID,
// ErrInvalidStoreMutation for a zero/negative ID (checked before routing), or
// ErrSlotNotLocal.
func (s *Store) shardIndexForNode(id types.NodeID) (int, error) {
	if err := validRoutingID(id.SnowflakeID()); err != nil {
		return 0, err
	}
	idx, ok := s.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
	if !ok {
		return 0, ErrSlotNotLocal
	}
	return idx, nil
}

func (s *Store) shardIndexForRel(id types.RelID) (int, error) {
	if err := validRoutingID(id.SnowflakeID()); err != nil {
		return 0, err
	}
	idx, ok := s.catalog.shardIndexForSlot(slotOf(id.SnowflakeID()))
	if !ok {
		return 0, ErrSlotNotLocal
	}
	return idx, nil
}
