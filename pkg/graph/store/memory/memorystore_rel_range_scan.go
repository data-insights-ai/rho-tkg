package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachRelByTypePropertyRangeOrdered streams the type's relationships whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE
// ORDER — ascending, or descending when desc — with ties (equal values) always
// broken by rel ID ASCENDING in both directions. It is the relationship mirror
// of ForEachNodeByLabelPropertyRangeOrdered (the ORDER BY prop [LIMIT k] / top-k
// access path): fn returning false stops the scan at the index level, so a LIMIT
// is pushed down (no full-range collection).
//
// Candidates come from the rel property index's ordered numeric view, which
// OVER-SELECTS by design (float64 sort keys, ulp-widened bounds): fn receives
// CANDIDATES and MUST re-check the predicate with exact comparison semantics
// (the inclMin/inclMax flags describe the caller's intended bounds — the door
// never skips a boundary bucket, so the exact inclusivity check is fn's
// responsibility). Current-state only; returns ErrIndexNotFound when no rel
// property index exists for (relType, propKey).
//
// Isolation mirrors ForEachRelByTypePropertyPrefix: each page's ID set is
// snapshotted under the store lock, then rows are looked up under brief per-row
// read locks and fn runs with NO lock held — fn may freely call back into the
// store. Rows are the store's FROZEN canonical entries — fn must not mutate them.
func (ms *Store) ForEachRelByTypePropertyRangeOrdered(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Relationship) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	if fn == nil {
		return storecontract.ErrInvalidStoreMutation
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}

	var cur indexpkg.RangeOrderedCursor
	for {
		ms.mu.RLock()
		if err := ms.checkOpenLocked(); err != nil {
			ms.mu.RUnlock()
			return err
		}
		idx, ok := ms.relPropertyIndexes[key]
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
			r, exists := ms.rels[types.RelID(rawID)]
			ms.mu.RUnlock()
			if !exists || !r.HasTypeTokenRaw(relTypeToken) {
				continue // deleted since snapshot, or orphaned ordered-view entry
			}
			if !fn(r) {
				return nil
			}
		}
		if done {
			return nil
		}
		cur = next
	}
}

// RelRangeCardinality is the relationship mirror of NodeRangeCardinality (see the
// badger doc): count of the type's relationships whose numeric propKey value lies in
// [min,max] from the rel property index's per-value bucket sizes, O(distinct values
// in range). exact=false declines when no rel property index exists or the index is
// poisoned. Rel property indexes are RAM-only.
func (ms *Store) RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if ms == nil {
		return 0, false, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false, err
	}
	idx, ok := ms.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}]
	if !ok {
		return 0, false, nil
	}
	count, exact := idx.RangeCardinality(min, max, inclMin, inclMax)
	return count, exact, nil
}
