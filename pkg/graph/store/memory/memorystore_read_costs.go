package memory

import storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

var _ storecontract.ReadCostCapability = (*Store)(nil)

// ReadCosts states the memory store's read costs (store.ReadCosts): every row
// is held, so held and decoded are the same figures.
func (ms *Store) ReadCosts() storecontract.ReadCosts {
	return storecontract.ReadCosts{
		Backend: "memory", HeldRows: -1,
		LendHeld: 20, LendDecoded: 20,
		ScanRowHeld: 10, ScanRowDecoded: 10,
		AdjacencyRelHeld: 80, AdjacencyRelDecoded: 80,
	}
}
