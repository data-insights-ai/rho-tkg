package memory

import (
	"maps"
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.TemporalIndexListingCapability = (*Store)(nil)

// TemporalIndexLabels lists the label tokens carrying a temporal interval
// index, ascending.
func (ms *Store) TemporalIndexLabels() ([]uint16, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(ms.temporalIndexes)), nil
}

// RelTemporalIndexTypes lists the relationship-type tokens carrying a
// temporal interval index, ascending.
func (ms *Store) RelTemporalIndexTypes() ([]uint16, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(ms.relTypeTemporalIndexes)), nil
}
