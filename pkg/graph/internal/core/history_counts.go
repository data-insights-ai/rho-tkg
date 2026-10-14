package core

import (
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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

// RelHistoryCountByType returns how many relationships of one type have
// history rows (round 5 request 7): the relationships with history that ever
// carried the type, from the store's history IDs and the point form of its
// rel-type membership sidecar (store.RelTypeTxMemberLookupCapability: memory,
// badger, tiered). A relationship's type is
// immutable, so this is exact unless an ID was reused across types (it then
// counts for each type it carried: an upper bound, never an undercount, so a
// 0 always means no relationship of the type has history). O(relationships
// with history) map lookups per call. An unknown type is 0. ok=false when the
// store states no per-type answer (sharded, external stores): never a
// substitute count. Errors: a malformed type name, ErrGraphClosed, a store
// error.
func (s *StatOps) RelHistoryCountByType(typeName string) (int, bool, error) {
	c := s.c
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, false, err
	}
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if c.relTypeTxLookup == nil {
		return 0, false, nil
	}
	var count int
	err := c.readUnderRLock(func() error {
		tok, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil
		}
		var ids []types.RelID
		if err := c.forEachRelHistoryIDByDepth(storepkg.DepthAll, func(id types.RelID) bool {
			ids = append(ids, id)
			return true
		}); err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		return c.relTypeTxLookup.RelTypeTxMembersOf(tok, ids, func(types.RelID, types.Instant) bool {
			count++
			return true
		})
	})
	if err != nil {
		return 0, false, err
	}
	return count, true, nil
}
