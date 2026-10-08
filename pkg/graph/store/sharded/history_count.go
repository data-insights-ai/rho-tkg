package sharded

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.HistoryCountCapability = (*Store)(nil)

// NodeHistoryCount sums the slots' exact counts of node IDs with history rows
// (store.HistoryCountCapability): an entity's rows, history included, live on
// the one slot its ID routes to, so no ID is counted twice.
func (s *Store) NodeHistoryCount() (int, error) {
	return s.sumHistoryCounts(func(shard *badgerShard) (int, error) { return shard.NodeHistoryCount() })
}

// RelHistoryCount is NodeHistoryCount for relationships.
func (s *Store) RelHistoryCount() (int, error) {
	return s.sumHistoryCounts(func(shard *badgerShard) (int, error) { return shard.RelHistoryCount() })
}

func (s *Store) sumHistoryCounts(count func(*badgerShard) (int, error)) (int, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	per := make([]int, len(s.shards))
	if err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		n, err := count(shard)
		per[idx] = n
		return err
	}); err != nil {
		return 0, err
	}
	total := 0
	for _, n := range per {
		total += n
	}
	return total, nil
}
