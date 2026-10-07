package sharded

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.ReadCostCapability = (*Store)(nil)

// ReadCosts states the sharded store's read costs (store.ReadCosts): its scans
// go through the graph's validating copy, HeldRows is the anchor shard's
// (per shard).
func (s *Store) ReadCosts() storecontract.ReadCosts {
	c := storecontract.ReadCosts{
		Backend:  "sharded",
		LendHeld: 400, LendDecoded: 3400,
		ScanRowHeld: 250, ScanRowDecoded: 1700,
		AdjacencyRelHeld: 1600, AdjacencyRelDecoded: 9300,
	}
	if s != nil && len(s.shards) > 0 {
		c.HeldRows = s.anchor().ReadCosts().HeldRows
	}
	return c
}
