package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// NodeHistoryStamps asks the slot the node routes to
// (store.HistoryStampsCapability): an entity's rows, history included, live on
// that one slot, as GetNodeHistory reads them.
func (s *Store) NodeHistoryStamps(id types.NodeID) (types.Instant, types.Instant, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return 0, 0, false, err
	}
	return shard.NodeHistoryStamps(id)
}

// RelHistoryStamps is NodeHistoryStamps for relationships.
func (s *Store) RelHistoryStamps(id types.RelID) (types.Instant, types.Instant, bool, error) {
	if err := s.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return 0, 0, false, err
	}
	return shard.RelHistoryStamps(id)
}
