package tiered

import (
	"errors"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Reads folded over every shard (sigma-tkgd round 10: the stated gaps of
// the tiered store). A node or relationship row lives on exactly one shard
// (the reference shard, the archive or one event shard), so the shards are
// disjoint parts: counts sum, bounds fold (index.FoldPropertyBounds), ordered
// walks merge in the index's order (index.MergeNumericOrdered /
// MergeStringOrdered). Every shard answers under the reference shard's
// (anchor's) index definition or refuses: a shard without a usable copy
// (none, an Elements one, one being built) makes the walk ErrIndexNotFound
// and a count or bound inexact, as one badger store would.

// foldShards reads every shard through read and returns the parts in fold
// order: the reference shard and the archive under the move read lock (an
// entity archived or restored between the two reads would be read twice or
// not at all), then every event shard (DepthAll; cold shards opened as the
// other folds open them).
func foldShards[T any](ts *Store, read func(*BadgerStore) (T, error)) ([]T, error) {
	if ts == nil {
		return nil, ErrNilStore
	}
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	var parts []T
	move := ts.lockMoveRead()
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	p, err := read(ref)
	refCheckin()
	if err != nil {
		return nil, err
	}
	parts = append(parts, p)
	ts.refArchiveGapHook()
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, err
	}
	if archive != nil {
		p, err := read(archive)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		parts = append(parts, p)
	}
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	results := make([]T, len(eventShards))
	errs := make([]error, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			errs[i] = err
			return
		}
		defer release()
		results[i], errs[i] = read(store)
	})
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return append(parts, results...), nil
}

var (
	_ storecontract.NodeOrderedRangeScanCapability       = (*Store)(nil)
	_ storecontract.NodePrefixScanCapability             = (*Store)(nil)
	_ storecontract.RelOrderedRangeScanCapability        = (*Store)(nil)
	_ storecontract.RelPrefixScanCapability              = (*Store)(nil)
	_ storecontract.NodePropertyBoundsCapability         = (*Store)(nil)
	_ storecontract.RelPropertyBoundsCapability          = (*Store)(nil)
	_ storecontract.RelPropertyTypeClassCountsCapability = (*Store)(nil)
)

// rowsOf collects one shard's ordered walk.
func rowsOf[R any](walk func(yield func(R) bool) error) ([]R, error) {
	var rows []R
	err := walk(func(r R) bool {
		rows = append(rows, r)
		return true
	})
	return rows, err
}

// orderedParts reads every shard's ordered walk whole (the shards are read
// under the fold's discipline, not held open while fn runs) and returns them
// as merge parts.
func orderedParts[R any](ts *Store, nilFn bool, walk func(store *BadgerStore, yield func(R) bool) error) ([]indexpkg.OrderedPart[R], error) {
	if nilFn {
		return nil, storecontract.ErrInvalidStoreMutation
	}
	rows, err := foldShards(ts, func(store *BadgerStore) ([]R, error) {
		return rowsOf(func(yield func(R) bool) error { return walk(store, yield) })
	})
	if err != nil {
		return nil, err
	}
	parts := make([]indexpkg.OrderedPart[R], len(rows))
	for i, r := range rows {
		parts[i] = indexpkg.SliceOrderedPart(r)
	}
	return parts, nil
}

// ForEachNodeByLabelPropertyRangeOrdered implements
// store.NodeOrderedRangeScanCapability: every shard's walk read whole (the
// rows in range on each shard, held in memory for the call, so a LIMIT bounds
// what fn gets, not what is read), merged in the index's order; fn runs with
// no shard held (a lazy merge would hold shard checkouts and the move lock
// while fn runs).
func (ts *Store) ForEachNodeByLabelPropertyRangeOrdered(token uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Node) bool) error {
	parts, err := orderedParts(ts, fn == nil, func(store *BadgerStore, yield func(*types.Node) bool) error {
		return store.ForEachNodeByLabelPropertyRangeOrdered(token, propKey, min, max, inclMin, inclMax, desc, yield)
	})
	if err != nil {
		return err
	}
	return indexpkg.MergeNodesNumeric(parts, propKey, desc, fn)
}

// ForEachNodeByLabelPropertyPrefix implements store.NodePrefixScanCapability
// as ForEachNodeByLabelPropertyRangeOrdered.
func (ts *Store) ForEachNodeByLabelPropertyPrefix(token uint16, propKey, prefix string, desc bool, fn func(*types.Node) bool) error {
	parts, err := orderedParts(ts, fn == nil, func(store *BadgerStore, yield func(*types.Node) bool) error {
		return store.ForEachNodeByLabelPropertyPrefix(token, propKey, prefix, desc, yield)
	})
	if err != nil {
		return err
	}
	return indexpkg.MergeNodesString(parts, propKey, desc, fn)
}

// ForEachRelByTypePropertyRangeOrdered implements
// store.RelOrderedRangeScanCapability as the node walk.
func (ts *Store) ForEachRelByTypePropertyRangeOrdered(typeToken uint16, propKey string, min, max float64, inclMin, inclMax, desc bool, fn func(*types.Relationship) bool) error {
	parts, err := orderedParts(ts, fn == nil, func(store *BadgerStore, yield func(*types.Relationship) bool) error {
		return store.ForEachRelByTypePropertyRangeOrdered(typeToken, propKey, min, max, inclMin, inclMax, desc, yield)
	})
	if err != nil {
		return err
	}
	return indexpkg.MergeRelsNumeric(parts, propKey, desc, fn)
}

// ForEachRelByTypePropertyPrefix implements store.RelPrefixScanCapability as
// the node walk.
func (ts *Store) ForEachRelByTypePropertyPrefix(typeToken uint16, propKey, prefix string, desc bool, fn func(*types.Relationship) bool) error {
	parts, err := orderedParts(ts, fn == nil, func(store *BadgerStore, yield func(*types.Relationship) bool) error {
		return store.ForEachRelByTypePropertyPrefix(typeToken, propKey, prefix, desc, yield)
	})
	if err != nil {
		return err
	}
	return indexpkg.MergeRelsString(parts, propKey, desc, fn)
}

// RelPropertyTypeClassCounts implements store.RelPropertyTypeClassCountsCapability:
// the shards' exact counters summed (NodePropertyTypeClassCounts' fold).
func (ts *Store) RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (storecontract.PropertyTypeClassCounts, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return storecontract.PropertyTypeClassCounts{}, err
	}
	return sumClassCounts(foldShards(ts, func(store *BadgerStore) (storecontract.PropertyTypeClassCounts, error) {
		return store.RelPropertyTypeClassCounts(relTypeToken, propertyKey)
	}))
}

func sumClassCounts(parts []storecontract.PropertyTypeClassCounts, err error) (storecontract.PropertyTypeClassCounts, error) {
	var total storecontract.PropertyTypeClassCounts
	if err != nil {
		return total, err
	}
	for _, p := range parts {
		total.Add(p)
	}
	return total, nil
}

// rangeCount is one shard's range count.
type rangeCount struct {
	n     int64
	exact bool
}

// NodeRangeCardinality sums the shards' index range counts; exact only when
// every shard counted exactly (each from its own copy of the anchor's index).
func (ts *Store) NodeRangeCardinality(token uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, false, err
	}
	return sumRangeCounts(foldShards(ts, func(store *BadgerStore) (rangeCount, error) {
		n, exact, err := store.NodeRangeCardinality(token, propKey, min, max, inclMin, inclMax)
		return rangeCount{n, exact}, err
	}))
}

// RelRangeCardinality is NodeRangeCardinality over a relationship type.
func (ts *Store) RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error) {
	if err := storecontract.ValidateRelTypeToken(relTypeToken); err != nil {
		return 0, false, err
	}
	return sumRangeCounts(foldShards(ts, func(store *BadgerStore) (rangeCount, error) {
		n, exact, err := store.RelRangeCardinality(relTypeToken, propKey, min, max, inclMin, inclMax)
		return rangeCount{n, exact}, err
	}))
}

func sumRangeCounts(parts []rangeCount, err error) (int64, bool, error) {
	if err != nil {
		return 0, false, err
	}
	counts, exact := make([]int64, len(parts)), make([]bool, len(parts))
	for i, p := range parts {
		counts[i], exact[i] = p.n, p.exact
	}
	total, ok := indexpkg.SumRangeCounts(counts, exact)
	return total, ok, nil
}

// boundsPart is one shard's bounds.
type boundsPart struct {
	b  storecontract.PropertyBounds
	ok bool
}

// NodePropertyBounds implements store.NodePropertyBoundsCapability: the
// shards' bounds folded; ok=false when any shard cannot state its own.
func (ts *Store) NodePropertyBounds(labelToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	return foldBounds(foldShards(ts, func(store *BadgerStore) (boundsPart, error) {
		b, ok, err := store.NodePropertyBounds(labelToken, propertyKey)
		return boundsPart{b, ok}, err
	}))
}

// RelPropertyBounds implements store.RelPropertyBoundsCapability as
// NodePropertyBounds.
func (ts *Store) RelPropertyBounds(relTypeToken uint16, propertyKey string) (storecontract.PropertyBounds, bool, error) {
	return foldBounds(foldShards(ts, func(store *BadgerStore) (boundsPart, error) {
		b, ok, err := store.RelPropertyBounds(relTypeToken, propertyKey)
		return boundsPart{b, ok}, err
	}))
}

func foldBounds(parts []boundsPart, err error) (storecontract.PropertyBounds, bool, error) {
	if err != nil {
		return storecontract.PropertyBounds{}, false, err
	}
	bounds, oks := make([]storecontract.PropertyBounds, len(parts)), make([]bool, len(parts))
	for i, p := range parts {
		bounds[i], oks[i] = p.b, p.ok
	}
	b, ok := indexpkg.FoldStatedBounds(bounds, oks)
	return b, ok, nil
}

// StatesPropertyBounds implements store.PropertyBoundsStatedCapability: the
// shards are opened with the store's configuration (shardConfig).
func (ts *Store) StatesPropertyBounds() (nodes, rels bool) {
	if ts == nil {
		return false, false
	}
	return !ts.disablePlannerStats && !ts.propertyIndexOnDisk, !ts.disablePlannerStats
}
