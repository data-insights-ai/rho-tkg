package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.LabelTxMembershipCapability   = (*Store)(nil)
	_ storecontract.RelTypeTxMembershipCapability = (*Store)(nil)
	_ storecontract.MemberGatherMeteredCapability = (*Store)(nil)
)

// ForEachLabelTxMember enumerates former label members from every owning slot.
func (s *Store) ForEachLabelTxMember(token uint16, fn func(types.NodeID, types.Instant) bool) error {
	return s.ForEachLabelTxMemberMetered(token, nil, fn)
}

// ForEachRelTypeTxMember is ForEachLabelTxMember for relationship types.
func (s *Store) ForEachRelTypeTxMember(token uint16, fn func(types.RelID, types.Instant) bool) error {
	return s.ForEachRelTypeTxMemberMetered(token, nil, fn)
}

// ForEachLabelTxMemberMetered shares one admission tally across native builds
// and copies. The callback runs serially outside each shard's locks.
func (s *Store) ForEachLabelTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(types.NodeID, types.Instant) bool) error {
	return walkShardTxMembers(s, fn, func(shard *badgerShard, visit func(types.NodeID, types.Instant) bool) error {
		return shard.ForEachLabelTxMemberMetered(token, t, visit)
	})
}

// ForEachRelTypeTxMemberMetered is the relationship-type twin.
func (s *Store) ForEachRelTypeTxMemberMetered(token uint16, t *storecontract.ReadTally, fn func(types.RelID, types.Instant) bool) error {
	return walkShardTxMembers(s, fn, func(shard *badgerShard, visit func(types.RelID, types.Instant) bool) error {
		return shard.ForEachRelTypeTxMemberMetered(token, t, visit)
	})
}

// Every entity and its history belong to the immutable slot in its ID, so
// fanout needs no second union or copy. Native Badger owns all admission;
// stopping the callback must also prevent work on later shards.
func walkShardTxMembers[ID any](s *Store, fn func(ID, types.Instant) bool, one func(*badgerShard, func(ID, types.Instant) bool) error) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	stopped := false
	visit := func(id ID, tx types.Instant) bool {
		stopped = !fn(id, tx)
		return !stopped
	}
	for _, shard := range s.shards {
		if err := one(shard, visit); err != nil {
			return err
		}
		if stopped {
			break
		}
	}
	return nil
}
