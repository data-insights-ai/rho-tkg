package sharded

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.MemberIDsCapability = (*Store)(nil)

// NodeIDsByLabel merges every slot's label members (store.MemberIDsCapability),
// ascending, without reading a row.
func (s *Store) NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	per := make([][]types.NodeID, len(s.shards))
	if err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, err := shard.NodeIDsByLabel(labelToken)
		per[idx] = ids
		return err
	}); err != nil {
		return nil, err
	}
	var out []types.NodeID
	for _, ids := range per {
		out = append(out, ids...)
	}
	storepkg.SortNodeIDs(out)
	return out, nil
}

// RelIDsByType merges every slot's type members (store.MemberIDsCapability),
// ascending, without reading a row.
func (s *Store) RelIDsByType(typeToken uint16) ([]types.RelID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(typeToken); err != nil {
		return nil, err
	}
	per := make([][]types.RelID, len(s.shards))
	if err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, err := shard.RelIDsByType(typeToken)
		per[idx] = ids
		return err
	}); err != nil {
		return nil, err
	}
	var out []types.RelID
	for _, ids := range per {
		out = append(out, ids...)
	}
	storepkg.SortRelIDs(out)
	return out, nil
}
