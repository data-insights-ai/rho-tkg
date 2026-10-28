package sharded

import (
	"strconv"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.PropertyIndexStatsCapability = (*Store)(nil)

// PropertyIndexStats implements store.PropertyIndexStatsCapability (IDXSTATS):
// each shard's definitions in slot order, Shard set to its slot number.
func (s *Store) PropertyIndexStats() ([]storecontract.PropertyIndexStat, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	var out []storecontract.PropertyIndexStat
	for i, shard := range s.shards {
		stats, err := shard.PropertyIndexStats()
		if err != nil {
			return nil, err
		}
		for j := range stats {
			stats[j].Shard = strconv.Itoa(i)
		}
		out = append(out, stats...)
	}
	return out, nil
}
