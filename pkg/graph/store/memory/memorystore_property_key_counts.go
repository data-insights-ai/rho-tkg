// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface.
package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeCountByLabelAndPropertyKey returns the number of current nodes carrying
// labelToken with an indexable scalar propertyKey value. O(1).
func (ms *Store) NodeCountByLabelAndPropertyKey(labelToken uint16, propertyKey string) (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return 0, err
	}
	if counts := ms.propertyKeyCounts[labelToken]; counts != nil {
		return counts[propertyKey], nil
	}
	return 0, nil
}

func (ms *Store) addNodePropertyKeyCounts(n *types.Node) {
	ms.adjustNodePropertyKeyCounts(n, 1)
}

func (ms *Store) removeNodePropertyKeyCounts(n *types.Node) {
	ms.adjustNodePropertyKeyCounts(n, -1)
}

func (ms *Store) adjustNodePropertyKeyCounts(n *types.Node, delta int) {
	if n == nil || delta == 0 {
		return
	}
	labelCount := n.LabelTokenCount()
	if labelCount == 0 {
		return
	}
	n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		value, _ := n.GetProperty(propertyKey)
		for i := 0; i < labelCount; i++ {
			tok := n.LabelTokenRawAt(i)
			if tok == 0 {
				continue
			}
			counts := ms.propertyKeyCounts[tok]
			if counts == nil {
				if delta < 0 {
					continue
				}
				counts = make(map[string]int)
				ms.propertyKeyCounts[tok] = counts
			}
			next := counts[propertyKey] + delta
			if next <= 0 {
				delete(counts, propertyKey)
				if len(counts) == 0 {
					delete(ms.propertyKeyCounts, tok)
				}
			} else {
				counts[propertyKey] = next
			}
			ms.adjustNodePropertyStatsOne(tok, propertyKey, valueKey, value, delta)
		}
		return true
	})
}

// adjustNodePropertyStatsOne folds one (label, property key) observation
// into the NDV+min/max accumulator, maintained on the SAME node-mutation
// doors as propertyKeyCounts above (same caller, same loop iteration) so the
// two capabilities' lifecycles never drift apart. delta>0 means the value is
// ENTERING the population (Observe); delta<0 means it is LEAVING (Forget —
// see PropertyStatsAccumulator.Forget for why removal only marks dirty
// rather than exactly recomputing).
func (ms *Store) adjustNodePropertyStatsOne(labelToken uint16, propertyKey, valueKey string, value any, delta int) {
	if delta > 0 {
		entries := ms.propertyStats[labelToken]
		if entries == nil {
			entries = make(map[string]*indexpkg.PropertyStatsAccumulator)
			ms.propertyStats[labelToken] = entries
		}
		acc := entries[propertyKey]
		if acc == nil {
			acc = indexpkg.NewPropertyStatsAccumulator()
			entries[propertyKey] = acc
		}
		acc.Observe(valueKey, value)
		return
	}
	if entries := ms.propertyStats[labelToken]; entries != nil {
		if acc := entries[propertyKey]; acc != nil {
			acc.Forget(value)
		}
	}
}

// NodePropertyStats returns NDV/min/max/count planner statistics for
// (labelToken, propertyKey). Missing/unpopulated pairs return a zero-value
// PropertyStats (Count 0, NDV 0, Min/Max nil) — not an error, mirroring
// NodeCountByLabelAndPropertyKey's "unregistered → 0" convention.
//
// Takes the exclusive write lock (not the RLock the sibling presence counter
// NodeCountByLabelAndPropertyKey takes over its plain ms.propertyKeyCounts map)
// because a dirty accumulator triggers an in-place Rescan over the label's
// current node values — see PropertyStatsAccumulator.Forget/Rescan. Holding one
// lock for the whole call keeps the memory backend fully race-free with no
// generation guard: its node lookups are direct in-process map reads with no
// re-entrant locking risk, unlike badger (whose cache-cold node fetch needs its
// own read lock, forcing the unlocked-collect + write-generation retry in
// badger.Store.NodePropertyStats). This is coarser than the presence counter's
// hot-path read, but NodePropertyStats is a cold-path planner call (invoked per
// query-plan, not per row), so the tradeoff favors simplicity and race-freedom
// over read concurrency.
func (ms *Store) NodePropertyStats(labelToken uint16, propertyKey string) (storecontract.PropertyStats, error) {
	if ms == nil {
		return storecontract.PropertyStats{}, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return storecontract.PropertyStats{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyStats{}, err
	}

	var count int
	if counts := ms.propertyKeyCounts[labelToken]; counts != nil {
		count = counts[propertyKey]
	}
	var acc *indexpkg.PropertyStatsAccumulator
	if entries := ms.propertyStats[labelToken]; entries != nil {
		acc = entries[propertyKey]
	}
	if acc == nil {
		return storecontract.PropertyStats{Count: int64(count)}, nil
	}
	if acc.Dirty() {
		acc.Rescan(ms.collectCurrentPropertyValuesLocked(labelToken, propertyKey))
	}
	ndv, min, max := acc.Snapshot()
	return storecontract.PropertyStats{NDV: ndv, Min: min, Max: max, Count: int64(count)}, nil
}

// NodePropertyStatsSketch is the memory-backend mirror of
// badger.Store.NodePropertyStatsSketch — a store-INTERNAL accessor (NOT part
// of the public store.NodePropertyStatsCapability contract) exposing the RAW
// per-(labelToken, propertyKey) HyperLogLog sketch alongside the exact
// min/max/count, for the tiered backend's cross-shard PropertyStats fold
// (register-max Merge across shards, Estimate() called once on the merged
// result — see docs/adr/0005-tiered-parity.md §3.1). The returned sketch is
// a CLONE, independent of this store's internal accumulator state.
// Missing/unpopulated pairs return (nil, nil, nil, 0, nil) — not an error,
// mirroring NodePropertyStats' own "unregistered → zero value" convention.
func (ms *Store) NodePropertyStatsSketch(labelToken uint16, propertyKey string) (sketch *indexpkg.HyperLogLog, min, max any, count int64, err error) {
	if ms == nil {
		return nil, nil, nil, 0, ErrNilStore
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return nil, nil, nil, 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, nil, nil, 0, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return nil, nil, nil, 0, err
	}

	var c int
	if counts := ms.propertyKeyCounts[labelToken]; counts != nil {
		c = counts[propertyKey]
	}
	var acc *indexpkg.PropertyStatsAccumulator
	if entries := ms.propertyStats[labelToken]; entries != nil {
		acc = entries[propertyKey]
	}
	if acc == nil {
		return nil, nil, nil, int64(c), nil
	}
	if acc.Dirty() {
		acc.Rescan(ms.collectCurrentPropertyValuesLocked(labelToken, propertyKey))
	}
	_, mn, mx := acc.Snapshot()
	return acc.Sketch(), mn, mx, int64(c), nil
}

// collectCurrentPropertyValuesLocked returns propertyKey's value from every
// CURRENT node carrying labelToken. Callers must already hold ms.mu (any
// mode) — it reads ms.labelIdx/ms.nodes directly rather than calling a public
// Store method, so as not to re-enter ms.mu from within NodePropertyStats.
func (ms *Store) collectCurrentPropertyValuesLocked(labelToken uint16, propertyKey string) []any {
	ids := ms.labelIdx[labelToken]
	values := make([]any, 0, len(ids))
	for id := range ids {
		n := ms.nodes[id]
		if n == nil {
			continue
		}
		if v, ok := n.GetProperty(propertyKey); ok {
			values = append(values, v)
		}
	}
	return values
}
