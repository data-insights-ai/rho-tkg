package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Property-key statistics (NodePropertyKeyStatsCapability +
// NodePropertyTypeClassCountsCapability) and range cardinality — ADR-0007 S5.
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

// NodeRangeCardinality sums each shard's O(bitmap) range count. The total is
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
	per := make([]perShardCardinality, len(s.shards))
	err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		c, exact, e := shard.NodeRangeCardinality(token, propKey, min, max, inclMin, inclMax)
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
