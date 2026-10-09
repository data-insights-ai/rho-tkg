package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// HasNodeHistory reports whether the node has at least one history row
// (store.HistoryPresenceCapability): len(GetNodeHistory(id)) > 0 without
// copying the rows.
func (ms *Store) HasNodeHistory(id types.NodeID) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return false, err
	}
	return len(ms.nodeHistory[id]) > 0, nil
}

// HasRelHistory is HasNodeHistory for relationships.
func (ms *Store) HasRelHistory(id types.RelID) (bool, error) {
	if ms == nil {
		return false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return false, err
	}
	return len(ms.relHistory[id]) > 0, nil
}
