package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Property membership sidecars (backlog 8): every shard is a badger store with
// its own sidecar over its own rows, and the property index DDL fans out to
// every shard (rel_property_index.go), so the store-wide ever-members of a
// value are the union of the shards' members. A re-sharding move leaves a
// member on its source shard too (append-only), so the union keeps the lowest
// first transaction time per entity and emits each entity once.

var (
	_ storecontract.RelPropertyTxMembershipCapability   = (*Store)(nil)
	_ storecontract.NodePropertyTxMembershipCapability  = (*Store)(nil)
	_ storecontract.PropertyTxMembershipStatsCapability = (*Store)(nil)
)

// ForEachRelPropertyTxMember implements store.RelPropertyTxMembershipCapability.
// A shard without the index answers ErrIndexNotFound, and so does the store.
func (s *Store) ForEachRelPropertyTxMember(relTypeToken uint16, propertyKey, valueKey string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return s.ForEachRelPropertyTxMemberKeys(relTypeToken, propertyKey, []string{valueKey}, fn)
}

// ForEachNodePropertyTxMember implements store.NodePropertyTxMembershipCapability.
func (s *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return s.ForEachNodePropertyTxMemberKeys(labelToken, propertyKey, []string{valueKey}, fn)
}

var (
	_ storecontract.NodePropertyTxMemberKeysCapability = (*Store)(nil) // round 11
	_ storecontract.RelPropertyTxMemberKeysCapability  = (*Store)(nil)
)

// ForEachRelPropertyTxMemberKeys implements
// store.RelPropertyTxMemberKeysCapability: one pass over the shards, each
// enumerating every key, united (unionShardMembers).
func (s *Store) ForEachRelPropertyTxMemberKeys(relTypeToken uint16, propertyKey string, valueKeys []string, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionShardMembers(s, valueKeys, func(shard *badgerShard, vk string, f func(types.RelID, types.Instant) bool) error {
		return shard.ForEachRelPropertyTxMember(relTypeToken, propertyKey, vk, f)
	}, fn)
}

// ForEachNodePropertyTxMemberKeys implements
// store.NodePropertyTxMemberKeysCapability.
func (s *Store) ForEachNodePropertyTxMemberKeys(labelToken uint16, propertyKey string, valueKeys []string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionShardMembers(s, valueKeys, func(shard *badgerShard, vk string, f func(types.NodeID, types.Instant) bool) error {
		return shard.ForEachNodePropertyTxMember(labelToken, propertyKey, vk, f)
	}, fn)
}

// unionShardMembers runs one on every shard for every key and hands fn the
// union: each entity once, in first-seen order, with the lowest first
// transaction time (unionFirstTx). A shard's error (ErrIndexNotFound for a
// shard without the index) is the store's.
func unionShardMembers[ID comparable](s *Store, valueKeys []string, one func(*badgerShard, string, func(ID, types.Instant) bool) error, fn func(ID, types.Instant) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[ID]types.Instant)
	var order []ID
	add := func(id ID, tx types.Instant) bool {
		if _, seen := union[id]; !seen {
			order = append(order, id)
		}
		unionFirstTx(union, id, tx)
		return true
	}
	for _, shard := range s.shards {
		for _, vk := range valueKeys {
			if err := one(shard, vk, add); err != nil {
				return err
			}
		}
	}
	for _, id := range order {
		if !fn(id, union[id]) {
			return nil
		}
	}
	return nil
}

// PropertyTxMembershipStats sums the shards' sidecar stats: Rel/NodeSidecars
// count per shard (one declared index built on N shards counts N).
func (s *Store) PropertyTxMembershipStats() (storecontract.PropertyTxMembershipStats, error) {
	var out storecontract.PropertyTxMembershipStats
	if err := s.checkOpen(); err != nil {
		return out, err
	}
	for _, shard := range s.shards {
		st, err := shard.PropertyTxMembershipStats()
		if err != nil {
			return storecontract.PropertyTxMembershipStats{}, err
		}
		out.RelSidecars += st.RelSidecars
		out.NodeSidecars += st.NodeSidecars
		out.RelPostings += st.RelPostings
		out.NodePostings += st.NodePostings
		out.Builds += st.Builds
		out.BuildDuration += st.BuildDuration
	}
	return out, nil
}
