package sharded

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// ValidAxis implements store.ValidAxisCapability.
func (s *Store) ValidAxis() types.ValidAxis { return s.validAxis.Get() }

// SetValidAxis implements store.ValidAxisCapability: the graph sets the axis
// of a store it opens empty, before the first row is written; every shard
// takes it.
func (s *Store) SetValidAxis(a types.ValidAxis) {
	s.validAxis.Set(a)
	for _, shard := range s.shards {
		if shard != nil {
			shard.SetValidAxis(a)
		}
	}
}
