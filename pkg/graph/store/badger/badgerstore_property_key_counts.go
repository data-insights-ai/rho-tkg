package badger

import (
	"sync/atomic"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NodeCountByLabelAndPropertyKey returns the number of current nodes carrying
// labelToken with an indexable scalar propertyKey value. O(1).
func (bs *Store) NodeCountByLabelAndPropertyKey(labelToken uint16, propertyKey string) (int, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return 0, err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if v, ok := bs.propertyKeyCounts.Load(key); ok {
		count := v.(*atomic.Int64).Load()
		if count <= 0 {
			return 0, nil
		}
		return int(count), nil // #nosec G115 — count is non-negative and bounded by node count.
	}
	return 0, nil
}

func (bs *Store) addNodePropertyKeyCounts(n *types.Node) {
	bs.adjustNodePropertyKeyCounts(n, 1)
}

func (bs *Store) removeNodePropertyKeyCounts(n *types.Node) {
	bs.adjustNodePropertyKeyCounts(n, -1)
}

func (bs *Store) adjustNodePropertyKeyCounts(n *types.Node, delta int64) {
	if n == nil || delta == 0 {
		return
	}
	labelCount := n.LabelTokenCount()
	if labelCount == 0 {
		return
	}
	// Type-class counters run their own FULL property sweep on the same call
	// (non-indexable values classify as ClassOther), so the presence, NDV, and
	// type-class capabilities' lifecycles never drift apart.
	bs.adjustNodePropertyTypeClassCounts(n, delta)
	n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
		value, _ := n.GetProperty(propertyKey)
		for i := 0; i < labelCount; i++ {
			tok := n.LabelTokenRawAt(i)
			if tok == 0 {
				continue
			}
			bs.getOrCreatePropertyKeyCounter(tok, propertyKey).Add(delta)
			bs.adjustNodePropertyStatsOne(tok, propertyKey, valueKey, value, delta)
		}
		return true
	})
}

func (bs *Store) getOrCreatePropertyKeyCounter(labelToken uint16, propertyKey string) *atomic.Int64 {
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if v, ok := bs.propertyKeyCounts.Load(key); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.propertyKeyCounts.LoadOrStore(key, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// adjustNodePropertyStatsOne folds one (label, property key) observation
// into the NDV+min/max accumulator (bs.propertyStats), maintained on the
// SAME node-mutation doors as the presence counter above (same caller, same
// loop iteration) — see memory.Store.adjustNodePropertyStatsOne for the
// identical counterpart. Caller MUST already hold bs.idxMu (any mode is
// fine for a fresh map read; every actual mutation call site here holds
// idxMu.Lock() — see the grep audit in badgerstore_property_stats.go).
// delta>0 means the value is ENTERING the population (Observe); delta<0
// means it is LEAVING (Forget).
func (bs *Store) adjustNodePropertyStatsOne(labelToken uint16, propertyKey, valueKey string, value any, delta int64) {
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
	if delta > 0 {
		acc := bs.propertyStats[key]
		if acc == nil {
			acc = indexpkg.NewPropertyStatsAccumulator()
			bs.propertyStats[key] = acc
		}
		acc.Observe(valueKey, value)
		return
	}
	if acc := bs.propertyStats[key]; acc != nil {
		acc.Forget(value)
	}
}
