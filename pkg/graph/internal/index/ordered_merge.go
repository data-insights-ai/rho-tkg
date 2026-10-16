package index

import (
	"cmp"
	"iter"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered walks of a composite store (sharded slots, tiered shards; sigma-tkgd
// round 10): each part streams its rows in the index's order (the ordered
// numeric view's float64 sort key, or the string value, descending when desc,
// ties by ID ascending), and the composite merges the streams in that same
// order. The merge keys every row with the rule its part ordered it by
// (numericSortKey / stringSortKey of the row's index key), so the composite's
// order is the parts' order by construction. A part streams candidates (a
// row's value may have moved since its index entry was read): a row whose
// value has no sort key now keeps its part's previous key, so it leaves the
// merge where its part handed it out; the consumer re-checks every row, as
// with a single store.

// OrderedPart is one part of a merge: it calls yield for each row in order
// and returns its error once done; yield false stops it.
type OrderedPart[R any] func(yield func(R) bool) error

// mergeOrdered streams the rows of parts in cmp order (each part already in
// it), fn false stops. A part's error stops the merge and is returned; every
// part is stopped before return.
func mergeOrdered[R any](parts []OrderedPart[R], less func(a, b R) bool, fn func(R) bool) error {
	type head struct {
		next func() (R, bool)
		stop func()
		err  *error
		row  R
		live bool
	}
	heads := make([]*head, 0, len(parts))
	defer func() {
		for _, h := range heads {
			h.stop()
		}
	}()
	for _, part := range parts {
		h := &head{err: new(error)}
		seq := func(yield func(R) bool) { *h.err = part(yield) }
		h.next, h.stop = iter.Pull(iter.Seq[R](seq))
		heads = append(heads, h)
		h.row, h.live = h.next()
		if !h.live && *h.err != nil {
			return *h.err
		}
	}
	for {
		var best *head
		for _, h := range heads {
			if h.live && (best == nil || less(h.row, best.row)) {
				best = h
			}
		}
		if best == nil {
			return nil
		}
		if !fn(best.row) {
			return nil
		}
		best.row, best.live = best.next()
		if !best.live && *best.err != nil {
			return *best.err
		}
	}
}

// keyed is a row with the sort key its part ordered it by; lead marks a
// row its part handed out before any row with a key (it leaves first).
type keyed[R any, K cmp.Ordered] struct {
	row  R
	key  K
	lead bool
	id   int64
}

// mergeKeyed merges parts by key (descending when desc), ties by ID
// ascending; keyOf is the index's sort key of a row (ok=false: none now).
func mergeKeyed[R any, K cmp.Ordered](parts []OrderedPart[R], keyOf func(R) (K, bool), id func(R) int64, desc bool, fn func(R) bool) error {
	wrapped := make([]OrderedPart[keyed[R, K]], len(parts))
	for i, part := range parts {
		wrapped[i] = func(yield func(keyed[R, K]) bool) error {
			var last K
			lead := true
			return part(func(r R) bool {
				if k, ok := keyOf(r); ok {
					last, lead = k, false
				}
				return yield(keyed[R, K]{row: r, key: last, lead: lead, id: id(r)})
			})
		}
	}
	less := func(a, b keyed[R, K]) bool {
		if a.lead != b.lead {
			return a.lead
		}
		if c := cmp.Compare(a.key, b.key); c != 0 {
			return (c < 0) != desc
		}
		return a.id < b.id
	}
	return mergeOrdered(wrapped, less, func(k keyed[R, K]) bool { return fn(k.row) })
}

// MergeNumericOrdered merges parts that stream rows in the ordered numeric
// view's order of property key (RangeOrderedPage: the value's float64 sort
// key, descending when desc, ties by ID ascending) into one stream in that
// order. value reads a row's property, id its ID.
func MergeNumericOrdered[R any](parts []OrderedPart[R], key string, value func(R, string) (any, bool), id func(R) int64, desc bool, fn func(R) bool) error {
	return mergeKeyed(parts, func(r R) (float64, bool) {
		v, ok := value(r, key)
		if !ok {
			return 0, false
		}
		return numericSortKey(types.IndexablePropertyValueKey(v))
	}, id, desc, fn)
}

// MergeStringOrdered is MergeNumericOrdered for the string view
// (PrefixOrderedPage: the string value, descending when desc, ties by ID
// ascending).
func MergeStringOrdered[R any](parts []OrderedPart[R], key string, value func(R, string) (any, bool), id func(R) int64, desc bool, fn func(R) bool) error {
	return mergeKeyed(parts, func(r R) (string, bool) {
		v, ok := value(r, key)
		if !ok {
			return "", false
		}
		return stringSortKey(types.IndexablePropertyValueKey(v))
	}, id, desc, fn)
}

// SliceOrderedPart is a part over rows already read in order.
func SliceOrderedPart[R any](rows []R) OrderedPart[R] {
	return func(yield func(R) bool) error {
		for _, r := range rows {
			if !yield(r) {
				return nil
			}
		}
		return nil
	}
}

// The row forms the composite stores use (sharded, tiered): one place that
// names a node's and a relationship's property and ID for the merge.

// MergeNodesNumeric is MergeNumericOrdered over nodes.
func MergeNodesNumeric(parts []OrderedPart[*types.Node], key string, desc bool, fn func(*types.Node) bool) error {
	return MergeNumericOrdered(parts, key, (*types.Node).GetProperty, nodeRawID, desc, fn)
}

// MergeNodesString is MergeStringOrdered over nodes.
func MergeNodesString(parts []OrderedPart[*types.Node], key string, desc bool, fn func(*types.Node) bool) error {
	return MergeStringOrdered(parts, key, (*types.Node).GetProperty, nodeRawID, desc, fn)
}

// MergeRelsNumeric is MergeNumericOrdered over relationships.
func MergeRelsNumeric(parts []OrderedPart[*types.Relationship], key string, desc bool, fn func(*types.Relationship) bool) error {
	return MergeNumericOrdered(parts, key, (*types.Relationship).GetProperty, relRawID, desc, fn)
}

// MergeRelsString is MergeStringOrdered over relationships.
func MergeRelsString(parts []OrderedPart[*types.Relationship], key string, desc bool, fn func(*types.Relationship) bool) error {
	return MergeStringOrdered(parts, key, (*types.Relationship).GetProperty, relRawID, desc, fn)
}

func nodeRawID(n *types.Node) int64        { return int64(n.ID()) }
func relRawID(r *types.Relationship) int64 { return int64(r.ID()) }

// SumRangeCounts is a composite store's range count from its disjoint
// parts' counts: the sum, exact only when every part counted exactly.
func SumRangeCounts(counts []int64, exact []bool) (int64, bool) {
	var total int64
	for i, n := range counts {
		if !exact[i] {
			return 0, false
		}
		total += n
	}
	return total, true
}
