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
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.RelID]types.Instant)
	var order []types.RelID
	for _, shard := range s.shards {
		if err := shard.ForEachRelPropertyTxMemberDuring(relTypeToken, propertyKey, valueKey, validStart, validEnd, txAt, func(id types.RelID, tx types.Instant) bool {
			if _, seen := union[id]; !seen {
				order = append(order, id)
			}
			unionFirstTx(union, id, tx)
			return true
		}); err != nil {
			return err
		}
	}
	for _, id := range order {
		if !fn(id, union[id]) {
			return nil
		}
	}
	return nil
}

// ForEachNodePropertyTxMemberDuring implements
// store.NodePropertyTxMemberWindowCapability.
func (s *Store) ForEachNodePropertyTxMemberDuring(labelToken uint16, propertyKey, valueKey string, validStart, validEnd, txAt types.Instant, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.NodeID]types.Instant)
	var order []types.NodeID
	for _, shard := range s.shards {
		if err := shard.ForEachNodePropertyTxMemberDuring(labelToken, propertyKey, valueKey, validStart, validEnd, txAt, func(id types.NodeID, tx types.Instant) bool {
			if _, seen := union[id]; !seen {
				order = append(order, id)
			}
			unionFirstTx(union, id, tx)
			return true
		}); err != nil {
			return err
		}
	}
	for _, id := range order {
		if !fn(id, union[id]) {
			return nil
		}
	}
	return nil
}
