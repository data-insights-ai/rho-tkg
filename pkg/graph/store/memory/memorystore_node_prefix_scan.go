package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabelPropertyPrefix streams the label's nodes whose STRING propKey
// value begins with prefix, to fn in CONTRACTUAL VALUE ORDER — lexicographic
// ascending, or descending when desc — with ties (equal values) always broken by
// node ID ASCENDING in both directions. It is the string prefix / `STARTS WITH`
// access path, and like the numeric ordered scan it pushes a LIMIT down: fn
// returning false stops the scan at the index level. An empty prefix matches
// every string value.
//
// Unlike the numeric ordered view the string view is EXACT (no float-precision
// over-selection), so candidates handed to fn already satisfy the prefix; fn
// still owns any additional predicate. Current-state only; returns
// ErrIndexNotFound when no property index exists for (token, propKey).
//
// Isolation mirrors ForEachNodeByLabelPropertyRangeOrdered: each page's ID set is
// snapshotted under the store lock, then rows are looked up under brief per-row
// read locks and fn runs with NO lock held.
func (ms *Store) ForEachNodeByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}

	var cur indexpkg.StrOrderedCursor
	for {
		ms.mu.RLock()
		if err := ms.checkOpenLocked(); err != nil {
			ms.mu.RUnlock()
			return err
		}
		idx, ok := ms.propertyIndexes[key]
		if !ok {
			ms.mu.RUnlock()
			return ErrIndexNotFound
		}
		ids, next, done, supported := idx.PrefixOrderedPage(prefix, desc, cur, orderedRangePageSize)
		ms.mu.RUnlock()
		if !supported {
			return ErrIndexNotFound
		}

		for _, rawID := range ids {
			ms.mu.RLock()
			n, exists := ms.nodes[types.NodeID(rawID)]
			ms.mu.RUnlock()
			if !exists || !n.HasLabelTokenRaw(token) {
				continue // deleted since snapshot, or orphaned ordered-view entry
			}
			if !fn(n) {
				return nil
			}
		}
		if done {
			return nil
		}
		cur = next
	}
}
