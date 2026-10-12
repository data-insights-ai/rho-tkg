package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CurrentStampsCapability = (*Store)(nil)

// NodeCurrentStamps asks the slot the node routes to
// (store.CurrentStampsCapability): an entity's current row lives on that one
// slot, as GetNode reads it. No row copy (GetNode's defensive copy and the
// graph core's validated copy of an untrusted store's row are both skipped).
func (s *Store) NodeCurrentStamps(id types.NodeID) (storecontract.RowStamps, error) {
	if err := s.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateNodeID(id); err != nil {
		return storecontract.RowStamps{}, err
	}
	shard, err := s.shardForNodeID(id)
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	return shard.NodeCurrentStamps(id)
}

// RelCurrentStamps is NodeCurrentStamps for relationships.
func (s *Store) RelCurrentStamps(id types.RelID) (storecontract.RowStamps, error) {
	if err := s.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateRelID(id); err != nil {
		return storecontract.RowStamps{}, err
	}
	shard, err := s.shardForRelID(id)
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	return shard.RelCurrentStamps(id)
}
