package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// orderedRangePageSize is the number of candidate node IDs a single
// ForEachNodeByLabelPropertyRangeOrdered page snapshots under the store lock
// before releasing it to materialize + emit rows. Small and constant, so a
// top-k caller (fn stops after k rows) snapshots one page and touches
// O(pageSize + log n) index entries — never the whole range.
const orderedRangePageSize = 256

// ForEachNodeByLabelPropertyRangeOrdered streams the label's nodes whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE
// ORDER — ascending, or descending when desc — with ties (equal values)
// always broken by node ID ASCENDING in both directions. It is the ordered /
// top-k access path serving ORDER BY prop [LIMIT k]: fn returning false stops
// the scan at the index level, so a LIMIT is pushed down (no full-range
// collection).
//
// Candidates come from the property index's ordered numeric view, which
// OVER-SELECTS by design (float64 sort keys, ulp-widened bounds): fn receives
// CANDIDATES and MUST re-check the predicate with exact comparison semantics
// (lesson 23; the inclMin/inclMax flags describe the caller's intended
// bounds — the door never skips a boundary bucket, since int64 magnitudes
// past 2^53 collapse onto neighbouring sort keys, lesson 25 — so the exact
// inclusivity check is fn's responsibility).
//
// Current-state only: there is no temporal QueryOpts parameter — the ordered
// door reflects the live current row set. Returns ErrIndexNotFound when no
// property index exists for (token, propKey).
//
// Isolation mirrors ForEachNodeByLabel: each page's ID set is snapshotted
// under the store lock, then rows are looked up under brief per-row read
// locks and fn runs with NO lock held — fn may freely call back into the
// store. Rows are the store's FROZEN canonical entries — fn must not mutate
// them.
func (ms *Store) ForEachNodeByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Node) bool) error {
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

	var cur indexpkg.RangeOrderedCursor
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
		ids, next, done, supported := idx.RangeOrderedPage(min, max, desc, cur, orderedRangePageSize)
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
