package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// NodeEqualCount sums the slots' NodeEqualCount (round 7 request 4): a node's
// row and its property index entry live on its own ID's slot only, so the
// sum counts each node once. Exact only when every slot answered exactly.
func (s *Store) NodeEqualCount(token uint16, propKey string, value any) (int64, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, false, err
	}
	return s.sumRangeCardinality(func(shard *badgerShard) (int64, bool, error) {
		return shard.NodeEqualCount(token, propKey, value)
	})
}

// RelEqualCount is the relationship mirror: a relationship's row lives on its
// own ID's slot only.
func (s *Store) RelEqualCount(relTypeToken uint16, propKey string, value any) (int64, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return 0, false, err
	}
	return s.sumRangeCardinality(func(shard *badgerShard) (int64, bool, error) {
		return shard.RelEqualCount(relTypeToken, propKey, value)
	})
}
