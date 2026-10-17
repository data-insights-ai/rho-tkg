package tiered

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// ValidAxis implements store.ValidAxisCapability.
func (ts *Store) ValidAxis() types.ValidAxis { return ts.validAxis.Get() }

// SetValidAxis implements store.ValidAxisCapability: the graph sets the axis
// of a store it opens empty, before the first row is written; every open
// shard takes it, and every shard opened later (badgerCfg).
func (ts *Store) SetValidAxis(a types.ValidAxis) {
	ts.validAxis.Set(a)
	_ = ts.forEachOpenShard(func(s *BadgerStore) error {
		s.SetValidAxis(a)
		return nil
	})
}
