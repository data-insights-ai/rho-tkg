package sharded

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
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
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.RelID]types.Instant)
	var order []types.RelID
	for _, shard := range s.shards {
		if err := shard.ForEachRelPropertyTxMember(relTypeToken, propertyKey, valueKey, func(id types.RelID, tx types.Instant) bool {
			if prev, ok := union[id]; ok {
				union[id] = indexpkg.MergeFirstTx(prev, tx)
				return true
			}
			union[id] = tx
			order = append(order, id)
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

// ForEachNodePropertyTxMember implements store.NodePropertyTxMembershipCapability.
func (s *Store) ForEachNodePropertyTxMember(labelToken uint16, propertyKey, valueKey string, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	union := make(map[types.NodeID]types.Instant)
	var order []types.NodeID
	for _, shard := range s.shards {
		if err := shard.ForEachNodePropertyTxMember(labelToken, propertyKey, valueKey, func(id types.NodeID, tx types.Instant) bool {
			if prev, ok := union[id]; ok {
				union[id] = indexpkg.MergeFirstTx(prev, tx)
				return true
			}
			union[id] = tx
			order = append(order, id)
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
