package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// ReadCosts returns the store's statement of what its read doors cost through
// the graph (store.ReadCosts: nominal nanoseconds per row for a row held
// decoded in RAM and one read from the backing store, and how many rows stay
// held), so a query planner can price fetches, scans and adjacency per
// backend. ok=false when the store states none (an external store). A wrapper
// that embeds an in-tree store inherits its statement, as it inherits every
// optional capability. Errors: ErrGraphClosed.
func (s *StatOps) ReadCosts() (storepkg.ReadCosts, bool, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return storepkg.ReadCosts{}, false, err
	}
	stater, ok := c.store.(storepkg.ReadCostCapability)
	if !ok {
		return storepkg.ReadCosts{}, false, nil
	}
	return stater.ReadCosts(), true, nil
}
