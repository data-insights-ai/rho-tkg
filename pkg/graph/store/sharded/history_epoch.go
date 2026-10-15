package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

// NodeHistoryEpoch asks the slot the node routes to
// (store.HistoryEpochCapability): an entity's history lives on that one slot.
func (s *Store) NodeHistoryEpoch(id types.NodeID) (uint64, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return 0, err
	}
	return shard.NodeHistoryEpoch(id)
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (s *Store) RelHistoryEpoch(id types.RelID) (uint64, error) {
	if err := s.checkOpen(); err != nil {
		return 0, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return 0, err
	}
	return shard.RelHistoryEpoch(id)
}
