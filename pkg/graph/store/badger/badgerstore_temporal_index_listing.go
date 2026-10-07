package badger

import (
	"maps"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.TemporalIndexListingCapability = (*Store)(nil)

// TemporalIndexLabels lists the label tokens carrying a temporal interval
// index, ascending.
func (bs *Store) TemporalIndexLabels() ([]uint16, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return slices.Sorted(maps.Keys(bs.temporalIndexes)), nil
}

// RelTemporalIndexTypes lists the relationship-type tokens carrying a
// temporal interval index, ascending.
func (bs *Store) RelTemporalIndexTypes() ([]uint16, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	return slices.Sorted(maps.Keys(bs.relTypeTemporalIndexes)), nil
}
