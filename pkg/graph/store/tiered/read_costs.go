package tiered

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.ReadCostCapability = (*Store)(nil)

// ReadCosts states the tiered store's read costs (store.ReadCosts), HeldRows
// per shard.
func (ts *Store) ReadCosts() storecontract.ReadCosts {
	c := storecontract.ReadCosts{
		Backend:  "tiered",
		LendHeld: 180, LendDecoded: 3800,
		ScanRowHeld: 60, ScanRowDecoded: 1300,
		AdjacencyRelHeld: 300, AdjacencyRelDecoded: 4300,
	}
	if ts != nil {
		c.HeldRows = ts.cacheCap // every shard's caches count entries (CacheCapacity, default 10,000)
	}
	return c
}
