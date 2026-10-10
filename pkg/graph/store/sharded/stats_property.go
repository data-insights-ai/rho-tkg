package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Property-key statistics (NodePropertyKeyStatsCapability +
// NodePropertyTypeClassCountsCapability) and range cardinality — ADR-0007.
//
// These counters are maintained per shard on every node mutation (the store's
// adjustNodePropertyKeyCounts choke point), independent of any property index,
// so each shard holds an exact partition of ITS local nodes. The global answer
// is the field-wise sum across shards — no index required. Missing is a
// graph-layer computation and is always 0 at the store boundary.
//
// Each fold writes its result into its OWN indexed slot (never a shared
// accumulator) because forEachShardErr runs the shards in PARALLEL; the
// reduction happens sequentially after the barrier.

var (
	_ storecontract.NodePropertyKeyStatsCapability        = (*Store)(nil)
	_ storecontract.NodePropertyTypeClassCountsCapability = (*Store)(nil)
)

// NodeCountByLabelAndPropertyKey sums each shard's presence count (current
// nodes carrying labelToken with an indexable scalar propertyKey value).
func (s *Store) NodeCountByLabelAndPropertyKey(labelToken uint16, propertyKey string) (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return 0, err
	}
	per := make([]int, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		c, e := shard.NodeCountByLabelAndPropertyKey(labelToken, propertyKey)
		per[idx] = c
		return e
	})
	if err != nil {
		return 0, err
	}
	total := 0
	for _, c := range per {
		total += c
	}
	return total, nil
}

// NodePropertyTypeClassCounts sums the per-shard exact type-class partitions.
// Missing stays 0 at the store boundary (graph-layer computed).
func (s *Store) NodePropertyTypeClassCounts(labelToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	var sum storecontract.PropertyTypeClassCounts
	if err := s.checkOpen(); err != nil {
		return sum, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return sum, err
	}
	per := make([]storecontract.PropertyTypeClassCounts, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		c, e := shard.NodePropertyTypeClassCounts(labelToken, propertyKey)
		per[idx] = c
		return e
	})
	if err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	for _, c := range per {
		sum.Numeric += c.Numeric
		sum.NaN += c.NaN
		sum.String += c.String
		sum.Bool += c.Bool
		sum.Other += c.Other
	}
	return sum, nil
}

// perShardCardinality carries one shard's range-cardinality result.
type perShardCardinality struct {
	count int64
	exact bool
}

// NodeRangeCardinality sums each shard's prefix-sum range count. The total is
// EXACT only if every shard reported exact — a single inexact shard (missing or
// poisoned index) makes the whole sum inexact, so the caller falls back to a
// scan. Because a property index is fanned out to EVERY shard, the common case
// is "all shards have the index" -> all exact -> exact total.
func (s *Store) NodeRangeCardinality(token uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, false, err
	}
	return s.sumRangeCardinality(func(shard *badgerShard) (int64, bool, error) {
		return shard.NodeRangeCardinality(token, propKey, min, max, inclMin, inclMax)
	})
}

// RelRangeCardinality is the relationship mirror (round 4 R2): each slot's
// rel property index counts the relationships whose rows it holds, and a
// relationship's row lives on its own ID's slot only, so the sum counts each
// once. Exact only when every slot answered exactly.
func (s *Store) RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, false, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return 0, false, err
	}
	return s.sumRangeCardinality(func(shard *badgerShard) (int64, bool, error) {
		return shard.RelRangeCardinality(relTypeToken, propKey, min, max, inclMin, inclMax)
	})
}

// sumRangeCardinality sums one range count per shard; inexact if any shard is.
func (s *Store) sumRangeCardinality(count func(*badgerShard) (int64, bool, error)) (int64, bool, error) {
	per := make([]perShardCardinality, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		c, exact, e := count(shard)
		per[idx] = perShardCardinality{count: c, exact: exact}
		return e
	})
	if err != nil {
		return 0, false, err
	}
	var total int64
	for _, r := range per {
		if !r.exact {
			return 0, false, nil
		}
		total += r.count
	}
	return total, true, nil
}

var _ storecontract.RelPropertyTypeClassCountsCapability = (*Store)(nil)

// RelPropertyTypeClassCounts sums the slots' exact per-(type, key) partitions.
// A relationship's row lives only on its own ID's slot (both adjacency legs
// with it; a Model-A incoming stub is not a row), so the sum counts every
// current relationship of the type exactly once. Missing stays 0 at the store
// boundary (graph-layer computed).
func (s *Store) RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	var sum storecontract.PropertyTypeClassCounts
	if err := s.checkOpen(); err != nil {
		return sum, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return sum, err
	}
	per := make([]storecontract.PropertyTypeClassCounts, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		c, e := shard.RelPropertyTypeClassCounts(relTypeToken, propertyKey)
		per[idx] = c
		return e
	})
	if err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	for _, c := range per {
		sum.Numeric += c.Numeric
		sum.NaN += c.NaN
		sum.String += c.String
		sum.Bool += c.Bool
		sum.Other += c.Other
	}
	return sum, nil
}
