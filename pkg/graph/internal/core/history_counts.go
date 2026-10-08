package core

import storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

// HistoryCounts returns how many nodes and relationships have history rows
// (store.HistoryCounts): the IDs a temporal read by property value resolves
// beside the current matches, so a query planner can price that read. Exact,
// without a pass per call (store.HistoryCountCapability: memory, badger,
// sharded). ok=false when the store states none (tiered, external stores).
// Errors: ErrGraphClosed, a store error, a negative count from the store.
func (s *StatOps) HistoryCounts() (storepkg.HistoryCounts, bool, error) {
	c := s.c
	if err := c.checkOpen(); err != nil {
		return storepkg.HistoryCounts{}, false, err
	}
	counter, ok := c.store.(storepkg.HistoryCountCapability)
	if !ok {
		return storepkg.HistoryCounts{}, false, nil
	}
	var out storepkg.HistoryCounts
	err := c.readUnderRLock(func() error {
		nodes, err := counter.NodeHistoryCount()
		if err != nil {
			return err
		}
		if err := validateStoreCount("NodeHistoryCount", nodes); err != nil {
			return err
		}
		rels, err := counter.RelHistoryCount()
		if err != nil {
			return err
		}
		if err := validateStoreCount("RelHistoryCount", rels); err != nil {
			return err
		}
		out = storepkg.HistoryCounts{Nodes: nodes, Rels: rels}
		return nil
	})
	if err != nil {
		return storepkg.HistoryCounts{}, false, err
	}
	return out, true, nil
}
