package badger

import (
	"errors"
	"fmt"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachRelByTypePropertyRangeOrdered streams the type's relationships whose
// NUMERIC propKey value lies within [min, max] to fn in CONTRACTUAL VALUE
// ORDER — ascending, or descending when desc — with ties (equal values) always
// broken by rel ID ASCENDING in both directions. It is the relationship mirror
// of ForEachNodeByLabelPropertyRangeOrdered (the ORDER BY prop [LIMIT k] / top-k
// access path). fn returning false stops the scan (LIMIT pushdown).
//
// Rel property indexes are RAM-only (their definitions persist, the entries do
// not), so there is no on-disk range path — candidates always come from the
// ordered numeric view, which OVER-SELECTS by design (float64 sort keys,
// ulp-widened bounds), so fn receives CANDIDATES and MUST re-check the predicate
// with exact comparison semantics. Returns ErrIndexNotFound when no usable rel
// property index exists for (relType, propKey), or while the index is still
// building (Mutated != nil). Rows are materialized outside idxMu and fn runs with
// no lock held.
func (bs *Store) ForEachRelByTypePropertyRangeOrdered(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Relationship) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
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
		bs.idxMu.RLock()
		idx, ok := bs.relPropertyIndexes[key]
		if !ok || idx.Mutated != nil {
			bs.idxMu.RUnlock()
			return storecontract.ErrIndexNotFound
		}
		ids, next, done, supported := idx.RangeOrderedPage(min, max, desc, cur, orderedRangePageSize)
		bs.idxMu.RUnlock()
		if !supported {
			return storecontract.ErrIndexNotFound
		}
		for _, rawID := range ids {
			r, err := bs.prefetchRelScan(types.RelID(rawID))
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // deleted since snapshot, or orphaned ordered-view entry
				}
				return fmt.Errorf("graph: range-scan relationship %d: %w", rawID, err)
			}
			if !r.HasTypeTokenRaw(relTypeToken) {
				continue
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

// RelRangeCardinality is the relationship mirror of NodeRangeCardinality: the count
// of the type's relationships whose numeric propKey value lies in [min,max]
// (inclusivity per flags), summed directly from the rel property index's sorted
// per-value bucket sizes — O(distinct values in range), NO rel scan. exact=false
// declines (the caller scans-and-counts) when no rel property index exists for
// (relType, propKey) or the index is poisoned (an integer magnitude past 2^53, where
// float64 sort keys can collide). Rel property indexes are RAM-only, so there is no
// on-disk decline arm (unlike the node door's propIdxOnDisk). Fractional values and
// bounds are counted exactly.
func (bs *Store) RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := bs.checkOpen(); err != nil {
		return 0, false, err
	}
	bs.idxMu.RLock()
	idx, ok := bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: relTypeToken, PropertyKey: propKey}]
	var count int64
	exact := false
	if ok {
		count, exact = idx.RangeCardinality(min, max, inclMin, inclMax)
	}
	bs.idxMu.RUnlock()
	return count, exact, nil
}
