package sharded

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberWindowCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberWindowCapability = (*Store)(nil)
)

// Windowed property membership (ledger row 62): every shard answers from its
// own sidecar over its own rows; a member with a row carrying the value in the
// window has that row on some shard, so the union of the shards' answers (an
// entity on several shards once, with the lowest bound) is the store's.

// ForEachRelPropertyTxMemberDuring implements
// store.RelPropertyTxMemberWindowCapability.
func (s *Store) ForEachRelPropertyTxMemberDuring(relTypeToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return s.ForEachRelPropertyTxMemberKeysDuring(relTypeToken, propertyKey, []string{valueKey}, validStart, validEnd, txAt, fn)
}

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability.
func (s *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return s.ForEachNodePropertyTxMemberKeysDuring(labelToken, propertyKey, []string{valueKey}, validStart, validEnd, txAt, fn)
}

// ForEachRelPropertyTxMemberKeysDuring implements the windowed form of
// store.RelPropertyTxMemberKeysCapability (round 11).
func (s *Store) ForEachRelPropertyTxMemberKeysDuring(relTypeToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	return unionShardMembers(s, valueKeys, func(shard *badgerShard, vk string, f func(types.RelID, types.Instant) bool) error {
		return shard.ForEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, vk, validStart, validEnd, txAt, f)
	}, fn)
}

// ForEachNodePropertyTxMemberKeysDuring implements the windowed form of
// store.NodePropertyTxMemberKeysCapability (round 11).
func (s *Store) ForEachNodePropertyTxMemberKeysDuring(labelToken uint16, propertyKey string, valueKeys []string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	return unionShardMembers(s, valueKeys, func(shard *badgerShard, vk string, f func(types.NodeID, types.Instant) bool) error {
		return shard.ForEachNodePropertyTxMemberDuring(labelToken, propertyKey, vk, validStart, validEnd, txAt, f)
	}, fn)
}
