package badger

import (
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ForEachNodeByLabelPropertyPrefix streams the label's nodes whose STRING propKey
// value begins with prefix to fn in CONTRACTUAL VALUE ORDER — lexicographic
// ascending, or descending when desc — with ties (equal values) always broken by
// node ID ASCENDING in both directions. It is the string prefix / `STARTS WITH`
// access path and pushes a LIMIT down: fn returning false stops the scan at the
// index level. An empty prefix matches every string value.
//
// Candidates come from the property index's ordered STRING view (RAM mode) or the
// persisted 0x0A raw-domain keyspace (PropertyIndexOnDisk). Unlike the numeric
// ordered view the string view is EXACT — no float-precision over-selection — so
// candidates already satisfy the prefix; fn still owns any additional predicate.
//
// Current-state only. Returns ErrIndexNotFound when no property index exists for
// (token, propKey). Same isolation and frozen-row contract as
// ForEachNodeByLabelPropertyRangeOrdered.
func (bs *Store) ForEachNodeByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return err
	}
	key := indexpkg.PropertyIndexKey{LabelToken: token, PropertyKey: propKey}

	if bs.propIdxOnDisk {
		return bs.forEachNodePrefixDisk(token, propKey, key, prefix, desc, fn)
	}
	return bs.forEachNodePrefixRAM(token, key, prefix, desc, fn)
}

// forEachNodePrefixRAM serves the prefix scan from the in-memory ordered string
// view via PrefixOrderedPage, page by page (top-k stays O(k + log n)).
func (bs *Store) forEachNodePrefixRAM(token uint16, key indexpkg.PropertyIndexKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	var cur indexpkg.StrOrderedCursor
	for {
		bs.idxMu.RLock()
		idx, ok := bs.propertyIndexes[key]
		if !ok {
			bs.idxMu.RUnlock()
			return storecontract.ErrIndexNotFound
		}
		ids, next, done, supported := idx.PrefixOrderedPage(prefix, desc, cur, orderedRangePageSize)
		bs.idxMu.RUnlock()
		if !supported {
			return storecontract.ErrIndexNotFound
		}
		stop, err := bs.emitOrderedCandidates(token, ids, fn)
		if err != nil {
			return err
		}
		if stop || done {
			return nil
		}
		cur = next
	}
}

// forEachNodePrefixDisk serves the prefix scan from the persisted 0x0A raw-domain
// keyspace. It snapshots the ordered candidate IDs (overlay-merged) once, then
// streams node materialization with fn-driven early stop.
func (bs *Store) forEachNodePrefixDisk(token uint16, propKey string, key indexpkg.PropertyIndexKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	bs.idxMu.RLock()
	_, ok := bs.propertyIndexes[key]
	if !ok {
		bs.idxMu.RUnlock()
		return storecontract.ErrIndexNotFound
	}
	ids, supported, err := bs.propertyIndexDiskPrefixOrderedLocked(propKey, prefix, desc)
	bs.idxMu.RUnlock()
	if err != nil {
		return err
	}
	if !supported {
		return storecontract.ErrIndexNotFound
	}
	_, err = bs.emitOrderedCandidates(token, ids, fn)
	return err
}
