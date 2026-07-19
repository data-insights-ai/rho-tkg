package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Stats (sum across shards) ---

func (s *Store) NodeCount() (int, error) {
	return s.sumCount(func(shard *badgerShard) (int, error) { return shard.NodeCount() })
}

func (s *Store) RelationshipCount() (int, error) {
	return s.sumCount(func(shard *badgerShard) (int, error) { return shard.RelationshipCount() })
}

func (s *Store) NodeCountByLabel(token uint16) (int, error) {
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, err
	}
	return s.sumCount(func(shard *badgerShard) (int, error) { return shard.NodeCountByLabel(token) })
}

func (s *Store) RelCountByType(token uint16) (int, error) {
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return 0, err
	}
	return s.sumCount(func(shard *badgerShard) (int, error) { return shard.RelCountByType(token) })
}

func (s *Store) sumCount(fn func(shard *badgerShard) (int, error)) (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	counts := make([]int, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		n, e := fn(shard)
		counts[idx] = n
		return e
	})
	if err != nil {
		return 0, err
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	return total, nil
}

// --- Iteration ---

func (s *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]types.NodeID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllNodeIDs(stripPagination(opts))
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateNodeIDs(mergeSortNodeIDs(per), opts), nil
}

func (s *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	per := make([][]types.RelID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllRelIDs(stripPagination(opts))
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	return paginateRelIDs(mergeSortRelIDs(per), opts), nil
}

func (s *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	per := make([][]types.NodeID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllNodeHistoryIDs()
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	return mergeSortNodeIDs(per), nil
}

func (s *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	per := make([][]types.RelID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllRelHistoryIDs()
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	return mergeSortRelIDs(per), nil
}

func (s *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	per := make([][]types.NodeID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllNodeHistoryIDsFrom(after, limit)
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	merged := mergeSortNodeIDs(per)
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}

func (s *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	per := make([][]types.RelID, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, e := shard.AllRelHistoryIDsFrom(after, limit)
		per[idx] = ids
		return e
	})
	if err != nil {
		return nil, err
	}
	merged := mergeSortRelIDs(per)
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged, nil
}

func (s *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	return s.forEachID(fn, func(shard *badgerShard, cb func(types.NodeID) bool) error {
		return shard.ForEachNodeID(cb)
	})
}

func (s *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	return s.forEachID(fn, func(shard *badgerShard, cb func(types.NodeID) bool) error {
		return shard.ForEachNodeHistoryID(cb)
	})
}

func (s *Store) ForEachRelID(fn func(types.RelID) bool) error {
	return s.forEachRelID(fn, func(shard *badgerShard, cb func(types.RelID) bool) error {
		return shard.ForEachRelID(cb)
	})
}

func (s *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	return s.forEachRelID(fn, func(shard *badgerShard, cb func(types.RelID) bool) error {
		return shard.ForEachRelHistoryID(cb)
	})
}

// ForEachDeletedNodeID and ForEachDeletedRelID implement
// storecontract.DeletedIterationCapability by fanning out to every shard's
// own DeletedIterationCapability (badger.Store already implements it
// natively). Unlike tiered.Store, sharded.Store routes an entity to exactly
// ONE shard by ID (never by time window), so no cross-shard dedup is needed
// — each shard's deleted-ID set is already disjoint from every other
// shard's, the same reasoning ForEachNodeID/ForEachNodeHistoryID above
// already rely on.
func (s *Store) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	return s.forEachID(fn, func(shard *badgerShard, cb func(types.NodeID) bool) error {
		return shard.ForEachDeletedNodeID(cb)
	})
}

func (s *Store) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	return s.forEachRelID(fn, func(shard *badgerShard, cb func(types.RelID) bool) error {
		return shard.ForEachDeletedRelID(cb)
	})
}

// forEachID iterates node IDs across shards sequentially, stopping early if fn
// returns false. Sequential (not parallel) so the callback contract — invoked
// outside backend locks, may call back into Store — holds without interleaving.
func (s *Store) forEachID(fn func(types.NodeID) bool, iter func(*badgerShard, func(types.NodeID) bool) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	stopped := false
	wrapped := func(id types.NodeID) bool {
		if !fn(id) {
			stopped = true
			return false
		}
		return true
	}
	for _, shard := range s.shards {
		if stopped {
			break
		}
		if err := iter(shard, wrapped); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) forEachRelID(fn func(types.RelID) bool, iter func(*badgerShard, func(types.RelID) bool) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	stopped := false
	wrapped := func(id types.RelID) bool {
		if !fn(id) {
			stopped = true
			return false
		}
		return true
	}
	for _, shard := range s.shards {
		if stopped {
			break
		}
		if err := iter(shard, wrapped); err != nil {
			return err
		}
	}
	return nil
}
