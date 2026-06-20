// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface.
package memory

import (
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
	n.ForEachIndexablePropertyValueKey(func(propertyKey, _ string) bool {
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
				continue
			}
			counts[propertyKey] = next
		}
		return true
	})
}
