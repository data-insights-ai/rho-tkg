package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// NodePropertyTypeClassCounts satisfies the optional
// store.NodePropertyTypeClassCountsCapability by folding the exact per-shard
// counters across the reference shard, the archive (when open), and every
// event shard — the same shard walk as NodeCountByLabelAndPropertyKey, so the
// two capabilities agree on which rows are counted. Composite-index
// introspection is NOT implemented (tiered declines composite indexes
// entirely).
func (ts *Store) NodePropertyTypeClassCounts(token uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	var total storecontract.PropertyTypeClassCounts
	if err := ts.checkOpen(); err != nil {
		return total, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return total, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return total, err
	}

	add := func(c storecontract.PropertyTypeClassCounts) {
		total.Numeric += c.Numeric
		total.NaN += c.NaN
		total.String += c.String
		total.Bool += c.Bool
		total.Other += c.Other
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	n, err := ref.NodePropertyTypeClassCounts(token, propertyKey)
	refCheckin()
	if err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	add(n)

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return storecontract.PropertyTypeClassCounts{}, archiveErr
	}
	if archive != nil {
		an, err := archive.NodePropertyTypeClassCounts(token, propertyKey)
		archiveCheckin()
		if err != nil {
			return storecontract.PropertyTypeClassCounts{}, err
		}
		add(an)
	}

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	type result struct {
		counts storecontract.PropertyTypeClassCounts
		err    error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].counts, results[i].err = store.NodePropertyTypeClassCounts(token, propertyKey)
	})
	for _, r := range results {
		if r.err != nil {
			return storecontract.PropertyTypeClassCounts{}, r.err
		}
		add(r.counts)
	}
	return total, nil
}
