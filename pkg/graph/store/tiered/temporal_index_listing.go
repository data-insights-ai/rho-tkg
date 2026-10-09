package tiered

import (
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.TemporalIndexListingCapability = (*Store)(nil)

// TemporalIndexLabels lists the label tokens carrying a temporal interval
// index, ascending, from the store-wide list CreateTemporalIndex maintains.
func (ts *Store) TemporalIndexLabels() ([]uint16, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	ts.tempIdxMu.Lock()
	out := slices.Clone(ts.tempIdxLabels)
	ts.tempIdxMu.Unlock()
	slices.Sort(out)
	return out, nil
}
