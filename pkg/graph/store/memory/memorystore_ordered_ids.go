package memory

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// valuePage reads one page of (ID, value) pairs from an index; ok=false when
// the index cannot answer exactly.
type valuePage func(idx *indexpkg.PropertyIndex) (ids []snowflake.ID, vals []any, done, ok bool)

// walkValuePages streams an index's ordered (ID, value) pages to emit: each
// page is read under the store's read lock (the index found by find), emit
// runs with no lock held. ErrIndexNotFound when find finds no index or the
// index cannot answer exactly.
func (ms *Store) walkValuePages(find func() (*indexpkg.PropertyIndex, bool), page valuePage, emit func(snowflake.ID, any) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	for {
		ms.mu.RLock()
		if err := ms.checkOpenLocked(); err != nil {
			ms.mu.RUnlock()
			return err
		}
		idx, ok := find()
		if !ok {
			ms.mu.RUnlock()
			return ErrIndexNotFound
		}
		ids, vals, done, supported := page(idx)
		ms.mu.RUnlock()
		if !supported {
			return ErrIndexNotFound
		}
		for j, id := range ids {
			if !emit(id, vals[j]) {
				return nil
			}
		}
		if done {
			return nil
		}
	}
}

// rangeValuePage pages RangeOrderedValuePage, advancing its own cursor.
func rangeValuePage(min, max float64, inclMin, inclMax, desc bool) valuePage {
	var cur indexpkg.RangeOrderedCursor
	return func(idx *indexpkg.PropertyIndex) ([]snowflake.ID, []any, bool, bool) {
		ids, vals, next, done, ok := idx.RangeOrderedValuePage(min, max, inclMin, inclMax, desc, cur, orderedRangePageSize)
		cur = next
		return ids, vals, done, ok
	}
}

// prefixValuePage pages PrefixOrderedValuePage, advancing its own cursor.
func prefixValuePage(prefix string, desc bool) valuePage {
	var cur indexpkg.StrOrderedCursor
	return func(idx *indexpkg.PropertyIndex) ([]snowflake.ID, []any, bool, bool) {
		ids, strs, next, done, ok := idx.PrefixOrderedValuePage(prefix, desc, cur, orderedRangePageSize)
		cur = next
		vals := make([]any, len(strs))
		for i, s := range strs {
			vals[i] = s
		}
		return ids, vals, done, ok
	}
}

// ForEachNodeIDByLabelPropertyRangeOrdered streams (ID, value) of the
// label's nodes whose numeric propKey value lies within the EXACT bounds, in
// value order (descending when desc), ties by ID ascending, from the
// property index alone: no row is read (round 7 request 5). The value is the
// stored one in its stored type. Pages of 256 are read under the read lock,
// fn runs with no lock held. ErrIndexNotFound when no property index on
// (label, propKey) exists or it cannot answer exactly (an Elements index, one
// being built, one holding an integer past 2^53).
func (ms *Store) ForEachNodeIDByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.NodeID, any) bool) error {
	if err := validateIndexTarget(storecontract.ValidateLabelToken(token), propKey); err != nil {
		return err
	}
	return ms.walkValuePages(ms.nodeIndex(token, propKey), rangeValuePage(min, max, inclMin, inclMax, desc),
		func(id snowflake.ID, v any) bool { return fn(types.NodeID(id), v) })
}

// ForEachNodeIDByLabelPropertyPrefix is the string twin: (ID, value) of the
// label's nodes whose string value begins with prefix, in byte order
// (descending when desc), ties by ID ascending, from the index alone.
func (ms *Store) ForEachNodeIDByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(types.NodeID, any) bool) error {
	if err := validateIndexTarget(storecontract.ValidateLabelToken(token), propKey); err != nil {
		return err
	}
	return ms.walkValuePages(ms.nodeIndex(token, propKey), prefixValuePage(prefix, desc),
		func(id snowflake.ID, v any) bool { return fn(types.NodeID(id), v) })
}

// ForEachRelIDByTypePropertyRangeOrdered is the relationship mirror of
// ForEachNodeIDByLabelPropertyRangeOrdered over a type's property index.
func (ms *Store) ForEachRelIDByTypePropertyRangeOrdered(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.RelID, any) bool) error {
	if err := validateIndexTarget(storecontract.ValidateRelTypeToken(relTypeToken), propKey); err != nil {
		return err
	}
	return ms.walkValuePages(ms.relIndex(relTypeToken, propKey), rangeValuePage(min, max, inclMin, inclMax, desc),
		func(id snowflake.ID, v any) bool { return fn(types.RelID(id), v) })
}

// ForEachRelIDByTypePropertyPrefix is the relationship mirror of
// ForEachNodeIDByLabelPropertyPrefix.
func (ms *Store) ForEachRelIDByTypePropertyPrefix(relTypeToken uint16, propKey, prefix string, desc bool, fn func(types.RelID, any) bool) error {
	if err := validateIndexTarget(storecontract.ValidateRelTypeToken(relTypeToken), propKey); err != nil {
		return err
	}
	return ms.walkValuePages(ms.relIndex(relTypeToken, propKey), prefixValuePage(prefix, desc),
		func(id snowflake.ID, v any) bool { return fn(types.RelID(id), v) })
}

func validateIndexTarget(tokenErr error, propKey string) error {
	if tokenErr != nil {
		return tokenErr
	}
	return storecontract.ValidateIndexPropertyKey(propKey)
}

// nodeIndex finds the node property index; the caller holds ms.mu.
func (ms *Store) nodeIndex(token uint16, propKey string) func() (*indexpkg.PropertyIndex, bool) {
	key := indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}
	return func() (*indexpkg.PropertyIndex, bool) { idx, ok := ms.propertyIndexes[key]; return idx, ok }
}

// relIndex finds the relationship property index; the caller holds ms.mu.
func (ms *Store) relIndex(token uint16, propKey string) func() (*indexpkg.PropertyIndex, bool) {
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: token, PropertyKey: propKey}
	return func() (*indexpkg.PropertyIndex, bool) { idx, ok := ms.relPropertyIndexes[key]; return idx, ok }
}
