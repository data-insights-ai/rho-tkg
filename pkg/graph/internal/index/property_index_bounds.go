package index

import (
	"math"
	"slices"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// first returns the smallest key, ok=false when the set is empty.
func (o *sortedChunks[T]) first() (T, bool) {
	for _, c := range o.chunks {
		if len(c) > 0 {
			return c[0], true
		}
	}
	var zero T
	return zero, false
}

// last returns the largest key, ok=false when the set is empty.
func (o *sortedChunks[T]) last() (T, bool) {
	for i := len(o.chunks) - 1; i >= 0; i-- {
		if c := o.chunks[i]; len(c) > 0 {
			return c[len(c)-1], true
		}
	}
	var zero T
	return zero, false
}

// NumericBounds returns the smallest and largest numeric sort key of the
// ordered view (NaN is never in it) with a stored value at each, in O(1)
// apart from the per-type value lookups at the two keys. has=false when no
// numeric value is indexed. supported=false where the view cannot state them
// exactly, as for RangeOrderedValuePage: a nil or Elements index, an index
// being built, or one holding an integer of magnitude past 2^53. The stored
// value is the first non-empty set of typedSets' order (float64, float32,
// then the integer types widest first).
func (pi *PropertyIndex) NumericBounds() (min, max float64, minValue, maxValue any, has, supported bool) {
	if pi == nil || pi.elements || pi.Mutated != nil || pi.numImpreciseCount > 0 {
		return 0, 0, nil, nil, false, false
	}
	lo, okLo := pi.numKeys.first()
	hi, okHi := pi.numKeys.last()
	if !okLo || !okHi {
		return 0, 0, nil, nil, false, true
	}
	loSets, hiSets := pi.typedSets(lo), pi.typedSets(hi)
	if len(loSets) == 0 || len(hiSets) == 0 {
		return 0, 0, nil, nil, false, false // a key whose value the view cannot name
	}
	return lo, hi, loSets[0].value, hiSets[0].value, true, true
}

// FoldPropertyBounds is the PropertyBounds of a composite store from its
// disjoint parts' (sharded slots, tiered shards, each an entity's only home):
// the classes summed (Missing too), the least Min and the greatest Max with a
// stored value at each; where parts hold equal keys in different widths the
// value is the one a single index would name (typedSets' order:
// types.EqualIndexValues of the key, then an inexact float32).
func FoldPropertyBounds(parts []storepkg.PropertyBounds) storepkg.PropertyBounds {
	var out storepkg.PropertyBounds
	bounded := false
	for _, p := range parts {
		out.Classes.Add(p.Classes)
		if !p.HasNumeric() {
			continue
		}
		if !bounded || p.Min < out.Min || (p.Min == out.Min && boundValueRank(p.Min, p.MinValue) < boundValueRank(out.Min, out.MinValue)) {
			out.Min, out.MinValue = p.Min, p.MinValue
		}
		if !bounded || p.Max > out.Max || (p.Max == out.Max && boundValueRank(p.Max, p.MaxValue) < boundValueRank(out.Max, out.MaxValue)) {
			out.Max, out.MaxValue = p.Max, p.MaxValue
		}
		bounded = true
	}
	return out
}

// boundValueRank is v's position in typedSets' order at key k (lower is
// named first).
func boundValueRank(k float64, v any) int {
	if v == nil {
		return math.MaxInt
	}
	vk := types.IndexablePropertyValueKey(v)
	keys := types.EqualIndexKeys(k)
	if i := slices.Index(keys, vk); i >= 0 {
		return i
	}
	return len(keys) // the inexact float32 typedSets adds last
}

// PropertyBoundsOf assembles store.PropertyBounds from an index, the exact
// type-class counts of the same entities (Missing ignored) and the label's
// (type's) entity count, all read under the lock that guards their writes.
// ok=false for a nil index, one NumericBounds cannot answer for, and where
// the index and the counts disagree on whether a numeric value exists (an
// index that does not cover the counted entities states nothing).
func PropertyBoundsOf(pi *PropertyIndex, classes storepkg.PropertyTypeClassCounts, total int64) (storepkg.PropertyBounds, bool) {
	lo, hi, loV, hiV, has, supported := pi.NumericBounds()
	if !supported || has != (classes.Numeric > 0) {
		return storepkg.PropertyBounds{}, false
	}
	classes.Missing = 0
	if missing := total - classes.Present(); missing > 0 {
		classes.Missing = missing
	}
	out := storepkg.PropertyBounds{Classes: classes}
	if has {
		out.Min, out.Max, out.MinValue, out.MaxValue = lo, hi, loV, hiV
	}
	return out, true
}
