package badger

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

// History epochs (store.HistoryEpochCapability, backlog 68): every history
// key op enters the write buffer through noteHistoryKey under wbMu, which
// notes the ID's watch entry (a key it cannot attribute drops the table);
// Clear drops the tables. No row is read.

// NodeHistoryEpoch returns the node's history token (O(1)).
func (bs *Store) NodeHistoryEpoch(id types.NodeID) (uint64, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return 0, err
	}
	return bs.histNodeEpochs.Epoch(id.SnowflakeID(), -1), nil
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (bs *Store) RelHistoryEpoch(id types.RelID) (uint64, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return 0, err
	}
	return bs.histRelEpochs.Epoch(id.SnowflakeID(), -1), nil
}

// HistoryGeneration moves whenever a node or relationship history key enters
// the write buffer (written or deleted) and at Clear, as the HistoryCounts
// cache's epochs do (round 8 request 3): a stamp under which a fold over
// history rows stays valid. O(1).
func (bs *Store) HistoryGeneration() uint64 {
	return bs.histNodeEpoch.Load() + bs.histRelEpoch.Load()
}
