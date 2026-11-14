package sharded

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.MemberIDsCapability = (*Store)(nil)
var _ storecontract.CurrentMemberIDsMeteredCapability = (*Store)(nil)

// NodeIDsByLabel merges every slot's label members (store.MemberIDsCapability),
// ascending, without reading a row.
func (s *Store) NodeIDsByLabel(labelToken uint16) ([]types.NodeID, error) {
	return s.NodeIDsByLabelMetered(labelToken, nil)
}

// NodeIDsByLabelMetered shares admission across every physical shard.
func (s *Store) NodeIDsByLabelMetered(labelToken uint16, t *storecontract.ReadTally) ([]types.NodeID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	per := make([][]types.NodeID, len(s.shards))
	if err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, err := shard.NodeIDsByLabelMetered(labelToken, t)
		per[idx] = ids
		return err
	}); err != nil {
		return nil, err
	}
	n := 0
	for _, ids := range per {
		n += len(ids)
	}
	if err := t.Buffer(8 * n); err != nil {
		return nil, err
	}
	out := make([]types.NodeID, 0, n)
	for _, ids := range per {
		out = append(out, ids...)
	}
	storepkg.SortNodeIDs(out)
	return out, nil
}

// RelIDsByType merges every slot's type members (store.MemberIDsCapability),
// ascending, without reading a row.
func (s *Store) RelIDsByType(typeToken uint16) ([]types.RelID, error) {
	return s.RelIDsByTypeMetered(typeToken, nil)
}

// RelIDsByTypeMetered shares admission across every physical shard.
func (s *Store) RelIDsByTypeMetered(typeToken uint16, t *storecontract.ReadTally) ([]types.RelID, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(typeToken); err != nil {
		return nil, err
	}
	per := make([][]types.RelID, len(s.shards))
	if err := s.forEachShardErr(func(idx int, shard *badgerShard) error {
		ids, err := shard.RelIDsByTypeMetered(typeToken, t)
		per[idx] = ids
		return err
	}); err != nil {
		return nil, err
	}
	n := 0
	for _, ids := range per {
		n += len(ids)
	}
	if err := t.Buffer(8 * n); err != nil {
		return nil, err
	}
	out := make([]types.RelID, 0, n)
	for _, ids := range per {
		out = append(out, ids...)
	}
	storepkg.SortRelIDs(out)
	return out, nil
}
