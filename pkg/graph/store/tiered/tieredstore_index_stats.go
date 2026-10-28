package tiered

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.PropertyIndexStatsCapability = (*Store)(nil)

// PropertyIndexStats implements store.PropertyIndexStatsCapability (IDXSTATS):
// each open shard's definitions, Shard set to its name. A closed shard holds
// none in RAM and is not opened.
func (ts *Store) PropertyIndexStats() ([]storecontract.PropertyIndexStat, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.PropertyIndexStat
	err := ts.forEachOpenShardNamed(func(name string, s *BadgerStore) error {
		stats, err := s.PropertyIndexStats()
		if err != nil {
			return err
		}
		for i := range stats {
			stats[i].Shard = name
		}
		out = append(out, stats...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storepkg.SortPropertyIndexStats(out)
	return out, nil
}
