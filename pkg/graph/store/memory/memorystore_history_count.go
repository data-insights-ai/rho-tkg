package memory

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.HistoryCountCapability = (*Store)(nil)

// NodeHistoryCount is the number of node IDs with history entries — the IDs
// ForEachNodeHistoryID visits (store.HistoryCountCapability). O(1).
func (ms *Store) NodeHistoryCount() (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return len(ms.nodeHistory), nil
}

// RelHistoryCount is NodeHistoryCount for relationships.
func (ms *Store) RelHistoryCount() (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return len(ms.relHistory), nil
}
