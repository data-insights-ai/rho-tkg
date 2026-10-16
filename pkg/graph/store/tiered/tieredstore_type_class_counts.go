package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// NodePropertyTypeClassCounts satisfies store.NodePropertyTypeClassCountsCapability
// by folding the exact per-shard counters over the same shards as
// NodeCountByLabelAndPropertyKey, so both count the same rows (foldShards).
func (ts *Store) NodePropertyTypeClassCounts(token uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	return sumClassCounts(foldShards(ts, func(store *BadgerStore) (storecontract.PropertyTypeClassCounts, error) {
		return store.NodePropertyTypeClassCounts(token, propertyKey)
	}))
}
