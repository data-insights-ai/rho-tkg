package badger

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
// page is read under idxMu's read lock, emit runs with no lock held.
// ErrIndexNotFound when find finds no index or it cannot answer exactly.
func (bs *Store) walkValuePages(find func() (*indexpkg.PropertyIndex, bool), page valuePage, emit func(snowflake.ID, any) bool) error {
	for {
		if err := bs.checkOpen(); err != nil {
			return err
		}
		bs.idxMu.RLock()
		idx, ok := find()
		if !ok {
			bs.idxMu.RUnlock()
			return storecontract.ErrIndexNotFound
		}
		ids, vals, done, supported := page(idx)
		bs.idxMu.RUnlock()
		if !supported {
			return storecontract.ErrIndexNotFound
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

func rangeValuePage(min, max float64, inclMin, inclMax, desc bool) valuePage {
	var cur indexpkg.RangeOrderedCursor
	return func(idx *indexpkg.PropertyIndex) ([]snowflake.ID, []any, bool, bool) {
		ids, vals, next, done, ok := idx.RangeOrderedValuePage(min, max, inclMin, inclMax, desc, cur, orderedRangePageSize)
		cur = next
		return ids, vals, done, ok
	}
}

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
// value order (descending when desc), ties by ID ascending, from the RAM
// property index alone: no row is decoded (round 7 request 5). Pages of 256
// under idxMu's read lock; fn runs with no lock held. ErrIndexNotFound with
// an index kept on disk (no RAM entries), without an index on (label, propKey),
// or where it cannot answer exactly (Elements, being built, an integer past
// 2^53).
func (bs *Store) ForEachNodeIDByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.NodeID, any) bool) error {
	find, err := bs.nodeValueIndex(token, propKey)
	if err != nil {
		return err
	}
	return bs.walkValuePages(find, rangeValuePage(min, max, inclMin, inclMax, desc),
		func(id snowflake.ID, v any) bool { return fn(types.NodeID(id), v) })
}

// ForEachNodeIDByLabelPropertyPrefix is the string twin (byte order).
func (bs *Store) ForEachNodeIDByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(types.NodeID, any) bool) error {
	find, err := bs.nodeValueIndex(token, propKey)
	if err != nil {
		return err
	}
	return bs.walkValuePages(find, prefixValuePage(prefix, desc),
		func(id snowflake.ID, v any) bool { return fn(types.NodeID(id), v) })
}

// ForEachRelIDByTypePropertyRangeOrdered is the relationship mirror (the rel
// property indexes are in RAM in every mode).
func (bs *Store) ForEachRelIDByTypePropertyRangeOrdered(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(types.RelID, any) bool) error {
	find, err := bs.relValueIndex(relTypeToken, propKey)
	if err != nil {
		return err
	}
	return bs.walkValuePages(find, rangeValuePage(min, max, inclMin, inclMax, desc),
		func(id snowflake.ID, v any) bool { return fn(types.RelID(id), v) })
}

// ForEachRelIDByTypePropertyPrefix is the relationship mirror of
// ForEachNodeIDByLabelPropertyPrefix.
func (bs *Store) ForEachRelIDByTypePropertyPrefix(relTypeToken uint16, propKey, prefix string, desc bool, fn func(types.RelID, any) bool) error {
	find, err := bs.relValueIndex(relTypeToken, propKey)
	if err != nil {
		return err
	}
	return bs.walkValuePages(find, prefixValuePage(prefix, desc),
		func(id snowflake.ID, v any) bool { return fn(types.RelID(id), v) })
}

func (bs *Store) nodeValueIndex(token uint16, propKey string) (func() (*indexpkg.PropertyIndex, bool), error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}
	return func() (*indexpkg.PropertyIndex, bool) { idx, ok := bs.propertyIndexes[key]; return idx, ok }, nil
}

func (bs *Store) relValueIndex(token uint16, propKey string) (func() (*indexpkg.PropertyIndex, bool), error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: token, PropertyKey: propKey}
	return func() (*indexpkg.PropertyIndex, bool) { idx, ok := bs.relPropertyIndexes[key]; return idx, ok }, nil
}
