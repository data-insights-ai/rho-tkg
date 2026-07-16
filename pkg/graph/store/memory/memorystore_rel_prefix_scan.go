package memory

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachRelByTypePropertyPrefix streams the type's relationships whose STRING
// propKey value begins with prefix to fn in CONTRACTUAL VALUE ORDER —
// lexicographic ascending, or descending when desc — with ties (equal values)
// always broken by rel ID ASCENDING in both directions. It is the relationship
// mirror of ForEachNodeByLabelPropertyPrefix. fn returning false stops the scan
// (LIMIT pushdown); an empty prefix matches every string value. Returns
// ErrIndexNotFound when no rel property index exists for (relType, propKey).
//
// Isolation mirrors the ordered node scan: each page's ID set is snapshotted
// under the store lock, then rows are looked up under brief per-row read locks and
// fn runs with NO lock held.
func (ms *Store) ForEachRelByTypePropertyPrefix(relTypeToken uint16, propKey, prefix string, desc bool, fn func(*types.Relationship) bool) error {
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

	var cur indexpkg.StrOrderedCursor
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
		ids, next, done, supported := idx.PrefixOrderedPage(prefix, desc, cur, orderedRangePageSize)
		ms.mu.RUnlock()
		if !supported {
			return ErrIndexNotFound
		}
		for _, rawID := range ids {
			ms.mu.RLock()
			r, exists := ms.rels[types.RelID(rawID)]
			ms.mu.RUnlock()
			if !exists || !r.HasTypeTokenRaw(relTypeToken) {
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
