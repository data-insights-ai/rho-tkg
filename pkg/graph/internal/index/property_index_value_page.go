package index

import (
	"math"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RangeOrderedValuePage is RangeOrderedPage with each ID's stored value,
// aligned with ids, and EXACT bounds (round 7 request 5: an ordered walk that
// reads no row): the page holds the IDs whose numeric value v satisfies
// min <(=) v <(=) max per the inclusivity flags, in value order (descending
// when desc), ties by ID ascending, each with its value in its stored type
// (int64 3 and float64 3.0 are one sort key, told apart by their value keys).
// A float32 value sorts by its shortest decimal rendering, as in every
// ordered numeric read. The page may hold fewer than limit IDs before the
// range ends (candidates outside the exact bounds are dropped); done reports
// the end. supported=false where the view cannot answer exactly: a nil or
// Elements index, an index being built, or one holding an integer of
// magnitude past 2^53 (its sort key is not its value).
func (pi *PropertyIndex) RangeOrderedValuePage(min, max float64, inclMin, inclMax, desc bool, after RangeOrderedCursor, limit int) (ids []snowflake.ID, vals []any, next RangeOrderedCursor, done bool, supported bool) {
	if pi == nil || pi.elements || pi.Mutated != nil || pi.numImpreciseCount > 0 {
		return nil, nil, RangeOrderedCursor{}, true, false
	}
	var keys []float64
	raw, next, done, _ := pi.rangeOrderedPage(min, max, desc, after, limit, &keys)
	ids = raw[:0]
	var bucketKey float64
	var typed valueSets
	for j, id := range raw {
		k := keys[j]
		if (k < min || (k == min && !inclMin)) || (k > max || (k == max && !inclMax)) {
			continue
		}
		if typed == nil || k != bucketKey {
			bucketKey, typed = k, pi.typedSets(k)
		}
		v, ok := typed.find(id)
		if !ok {
			return nil, nil, RangeOrderedCursor{}, true, false // an entry whose key the view cannot name
		}
		ids = append(ids, id)
		vals = append(vals, v)
	}
	return ids, vals, next, done, true
}

// valueSet is one stored value and the IDs holding it.
type valueSet struct {
	value any
	ids   map[snowflake.ID]struct{}
}

type valueSets []valueSet

// find returns the stored value of id among the sets.
func (s valueSets) find(id snowflake.ID) (any, bool) {
	for _, set := range s {
		if _, ok := set.ids[id]; ok {
			return set.value, true
		}
	}
	return nil, false
}

// typedSets returns the value sets of every stored numeric type whose sort
// key is k (non-empty sets only).
func (pi *PropertyIndex) typedSets(k float64) valueSets {
	var out valueSets
	add := func(v any) {
		if set := pi.Entries[types.IndexablePropertyValueKey(v)]; len(set) > 0 {
			out = append(out, valueSet{value: v, ids: set})
		}
	}
	add(k)
	if math.Abs(k) <= math.MaxFloat32 || math.IsInf(k, 0) {
		add(float32(k))
	}
	if k != math.Trunc(k) || math.Abs(k) > maxExactInt {
		return out
	}
	i := int64(k)
	add(i)
	add(int(i))
	if i >= math.MinInt32 && i <= math.MaxInt32 {
		add(int32(i))
	}
	if i >= math.MinInt16 && i <= math.MaxInt16 {
		add(int16(i))
	}
	if i >= math.MinInt8 && i <= math.MaxInt8 {
		add(int8(i))
	}
	if i >= 0 {
		u := uint64(i)
		add(u)
		add(uint(u))
		if u <= math.MaxUint32 {
			add(uint32(u))
		}
		if u <= math.MaxUint16 {
			add(uint16(u))
		}
		if u <= math.MaxUint8 {
			add(uint8(u))
		}
	}
	return out
}
