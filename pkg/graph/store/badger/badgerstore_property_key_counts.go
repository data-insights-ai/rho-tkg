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
	n.ForEachIndexablePropertyValueKey(func(propertyKey, _ string) bool {
		for i := 0; i < labelCount; i++ {
			tok := n.LabelTokenRawAt(i)
			if tok == 0 {
				continue
			}
			bs.getOrCreatePropertyKeyCounter(tok, propertyKey).Add(delta)
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
