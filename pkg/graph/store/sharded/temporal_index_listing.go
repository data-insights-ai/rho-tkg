package sharded

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.TemporalIndexListingCapability = (*Store)(nil)

// TemporalIndexLabels lists the label tokens carrying a temporal interval
// index, from the anchor shard (every shard holds the same definitions).
func (s *Store) TemporalIndexLabels() ([]uint16, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return s.anchor().TemporalIndexLabels()
}

// RelTemporalIndexTypes lists the relationship-type tokens carrying a
// temporal interval index, from the anchor shard.
func (s *Store) RelTemporalIndexTypes() ([]uint16, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	return s.anchor().RelTemporalIndexTypes()
}
