package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// HasNodeHistory asks the slot the node routes to (store.HistoryPresenceCapability):
// an entity's rows, history included, live on that one slot, as GetNodeHistory reads them.
func (s *Store) HasNodeHistory(id types.NodeID) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return false, err
	}
	return shard.HasNodeHistory(id)
}

// HasRelHistory is HasNodeHistory for relationships.
func (s *Store) HasRelHistory(id types.RelID) (bool, error) {
	if err := s.checkOpen(); err != nil {
		return false, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return false, err
	}
	return shard.HasRelHistory(id)
}
