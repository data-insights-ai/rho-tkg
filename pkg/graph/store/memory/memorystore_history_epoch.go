package memory

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

// History epochs (store.HistoryEpochCapability, backlog 68): every history row
// write passes putNodeHistoryRowLocked / putRelHistoryRowLocked, which note
// it; every other history change only removes rows, which the row count
// checked by storeutil.HistoryEpochs catches (the stamps sidecar's argument,
// memorystore_history_stamps.go).

// NodeHistoryEpoch returns the node's history token (O(1)).
func (ms *Store) NodeHistoryEpoch(id types.NodeID) (uint64, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return 0, err
	}
	return ms.nodeHistEpochs.Epoch(id, len(ms.nodeHistory[id])), nil
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (ms *Store) RelHistoryEpoch(id types.RelID) (uint64, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return 0, err
	}
	return ms.relHistEpochs.Epoch(id, len(ms.relHistory[id])), nil
}

// HistoryGeneration moves at every node or relationship history row write
// and removal and at Clear (round 8 request 3): a stamp under which a fold
// over history rows stays valid. O(1).
func (ms *Store) HistoryGeneration() uint64 {
	if ms == nil {
		return 0
	}
	return ms.histGen.Load()
}
