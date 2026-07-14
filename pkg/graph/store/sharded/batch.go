package sharded

import (
	"errors"
	"fmt"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Batch mutations ---

// PutNodesBatch partitions nodes by shard and submits one batch per shard. On a
// shard failure it rolls back the already-committed shards (their node IDs are
// deleted) so the operation is all-or-nothing.
func (s *Store) PutNodesBatch(nodes []*types.Node) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	buckets := make(map[int][]*types.Node)
	for _, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		idx, err := s.shardIndexForNode(n.InternalID())
		if err != nil {
			return err
		}
		buckets[idx] = append(buckets[idx], n)
	}

	var committed []int
	rollback := func() {
		for _, idx := range committed {
			ids := make([]types.NodeID, 0, len(buckets[idx]))
			for _, n := range buckets[idx] {
				ids = append(ids, n.InternalID())
			}
			_ = s.shards[idx].DeleteNodesBatch(ids)
		}
	}
	for idx, bucket := range buckets {
		if err := s.shards[idx].PutNodesBatch(bucket); err != nil {
			rollback()
			return fmt.Errorf("graph: sharded: put nodes batch shard %d: %w", idx, err)
		}
		committed = append(committed, idx)
	}
	return nil
}

// PutRelationshipsBatch validates endpoints (cross-shard) and in-batch duplicate
// IDs, then writes each relationship to its own shard, rolling back on failure.
func (s *Store) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(rels) == 0 {
		return nil
	}
	seen := make(map[types.RelID]struct{}, len(rels))
	for _, r := range rels {
		if err := storecontract.ValidateRelationshipWrite(r); err != nil {
			return err
		}
		if _, dup := seen[r.ID()]; dup {
			return ErrRelExists
		}
		seen[r.ID()] = struct{}{}
	}

	var committed []types.RelID
	rollback := func() {
		for i := len(committed) - 1; i >= 0; i-- {
			_ = s.DeleteRelationship(committed[i])
		}
	}
	for _, r := range rels {
		if err := s.PutRelationship(r); err != nil {
			rollback()
			return err
		}
		committed = append(committed, r.ID())
	}
	return nil
}

// DeleteNodesBatch removes unconnected node rows. Connectivity is checked across
// all shards; a node with any cross-shard edge is rejected before any delete.
func (s *Store) DeleteNodesBatch(ids []types.NodeID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	buckets := make(map[int][]types.NodeID)
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
		if _, dup := seen[id]; dup {
			continue // coalesce duplicate requested deletes
		}
		seen[id] = struct{}{}
		idx, err := s.shardIndexForNode(id)
		if err != nil {
			return err
		}
		connected, cerr := s.nodeConnectedAnyShard(id)
		if cerr != nil {
			return cerr
		}
		if connected {
			return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, id)
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var errs []error
	for idx, bucket := range buckets {
		if err := s.shards[idx].DeleteNodesBatch(bucket); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: delete nodes batch shard %d: %w", idx, err))
		}
	}
	return errors.Join(errs...)
}

// DeleteRelationshipsBatch partitions rel IDs by their (co-located) shard and
// submits one batch per shard.
func (s *Store) DeleteRelationshipsBatch(ids []types.RelID) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	buckets := make(map[int][]types.RelID)
	seen := make(map[types.RelID]struct{}, len(ids))
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return err
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		idx, err := s.shardIndexForRel(id)
		if err != nil {
			return err
		}
		buckets[idx] = append(buckets[idx], id)
	}
	var errs []error
	for idx, bucket := range buckets {
		if err := s.shards[idx].DeleteRelationshipsBatch(bucket); err != nil {
			errs = append(errs, fmt.Errorf("graph: sharded: delete rels batch shard %d: %w", idx, err))
		}
	}
	return errors.Join(errs...)
}
