package badger

import (
	"math"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

var _ storecontract.ReadCostCapability = (*Store)(nil)

// badgerReadCosts are BenchmarkReadCosts' badger figures (store.ReadCosts).
var badgerReadCosts = storecontract.ReadCosts{
	Backend:  "badger",
	LendHeld: 35, LendDecoded: 3300,
	ScanRowHeld: 17, ScanRowDecoded: 1300,
	AdjacencyRelHeld: 130, AdjacencyRelDecoded: 3200,
}

// ReadCosts states the badger store's read costs (store.ReadCosts), with
// HeldRows from its entity caches.
func (bs *Store) ReadCosts() storecontract.ReadCosts {
	c := badgerReadCosts
	if bs != nil {
		c.HeldRows = bs.heldRows
	}
	return c
}

// heldRowsOf is ReadCosts.HeldRows for an entity cache of capacity entries:
// -1 when resident, 0 when a byte budget governs (New sets the count capacity
// to MaxInt then).
func heldRowsOf(capacity int, resident bool) int {
	switch {
	case resident:
		return -1
	case capacity == math.MaxInt:
		return 0
	default:
		return capacity
	}
}
