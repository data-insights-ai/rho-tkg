package sharded

import (
	"slices"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelPropertyTxMemberPageCapability  = (*Store)(nil)
	_ storecontract.NodePropertyTxMemberPageCapability = (*Store)(nil)
)

// Paged property membership (backlog 69): every shard pages its own sidecar
// above the cursor; the store's first limit members are among the union of
// the shards' first limit, merged in ID order (an entity on several shards
// once, with the lowest bound).

// ForEachRelPropertyTxMemberAfter implements
// store.RelPropertyTxMemberPageCapability.
func (s *Store) ForEachRelPropertyTxMemberAfter(relTypeToken uint16, propertyKey, valueKey string, after types.RelID, limit int, fn func(id types.RelID, firstTxFrom types.Instant) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.RelID]types.Instant)
	for _, shard := range s.shards {
		if err := shard.ForEachRelPropertyTxMemberAfter(relTypeToken, propertyKey, valueKey, after, limit, func(id types.RelID, tx types.Instant) bool {
			unionFirstTx(union, id, tx)
			return true
		}); err != nil {
			return err
		}
	}
	emitFirst(union, limit, fn)
	return nil
}

// ForEachNodePropertyTxMemberAfter implements
// store.NodePropertyTxMemberPageCapability.
func (s *Store) ForEachNodePropertyTxMemberAfter(labelToken uint16, propertyKey, valueKey string, after types.NodeID, limit int, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.NodeID]types.Instant)
	for _, shard := range s.shards {
		if err := shard.ForEachNodePropertyTxMemberAfter(labelToken, propertyKey, valueKey, after, limit, func(id types.NodeID, tx types.Instant) bool {
			unionFirstTx(union, id, tx)
			return true
		}); err != nil {
			return err
		}
	}
	emitFirst(union, limit, fn)
	return nil
}

func unionFirstTx[ID comparable](union map[ID]types.Instant, id ID, tx types.Instant) {
	if prev, ok := union[id]; ok {
		union[id] = indexpkg.MergeFirstTx(prev, tx)
		return
	}
	union[id] = tx
}

// emitFirst hands fn the limit smallest IDs of union, ascending.
func emitFirst[ID ~int64](union map[ID]types.Instant, limit int, fn func(ID, types.Instant) bool) {
	ids := make([]ID, 0, len(union))
	for id := range union {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	if len(ids) > limit {
		ids = ids[:max(limit, 0)]
	}
	for _, id := range ids {
		if !fn(id, union[id]) {
			return
		}
	}
}
