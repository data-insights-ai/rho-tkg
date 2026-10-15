package index

import (
	"math"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// maxExactInt is 2^53: from it on an integer and a float64 no longer compare one
// to one, so EqualCount declines.
const maxExactInt = 1 << 53

// EqualCount counts the entities whose value equals value as Cypher equality
// keeps it, from the index's per-value sets (their sizes; no ID is read):
// numbers by numeric value across every stored width (3 matches 3, int8(3),
// uint(3), 3.0 and float32(3)), a float32 stored value compared as its
// float64 widening, no string/number/boolean crossing; nil and NaN count 0.
// exact=false where the index cannot say: a nil index, an index still being
// built (Mutated is set: the backfill has not landed), an Elements index (a
// list's elements share the keys of scalars), a number of magnitude 2^53 or more
// (integer/float comparison there is not one to one), a temporal value (equal
// instants may be stored in different renderings), a list, a map or another
// unindexed type.
func (pi *PropertyIndex) EqualCount(value any) (count int64, exact bool) {
	if pi == nil || pi.elements || pi.Mutated != nil {
		return 0, false
	}
	size := func(v any) int64 { return int64(len(pi.Entries[types.IndexablePropertyValueKey(v)])) }
	switch v := value.(type) {
	case nil:
		return 0, true
	case string, bool:
		return size(v), true
	}
	f, isNumber := equalNumeric(value)
	if !isNumber {
		return 0, false
	}
	switch {
	case math.IsNaN(f):
		return 0, true
	case math.IsInf(f, 0):
		return size(f) + size(float32(f)), true
	case math.Abs(f) >= maxExactInt:
		return 0, false
	}
	count = size(f)
	if float64(float32(f)) == f {
		count += size(float32(f))
	}
	if f != math.Trunc(f) {
		return count, true
	}
	i := int64(f)
	count += size(int(i)) + size(i)
	if i >= math.MinInt32 && i <= math.MaxInt32 {
		count += size(int32(i))
	}
	if i >= math.MinInt16 && i <= math.MaxInt16 {
		count += size(int16(i))
	}
	if i >= math.MinInt8 && i <= math.MaxInt8 {
		count += size(int8(i))
	}
	if i >= 0 {
		u := uint64(i)
		count += size(uint(u)) + size(u)
		if u <= math.MaxUint32 {
			count += size(uint32(u))
		}
		if u <= math.MaxUint16 {
			count += size(uint16(u))
		}
		if u <= math.MaxUint8 {
			count += size(uint8(u))
		}
	}
	return count, true
}

// equalNumeric widens a stored numeric value to float64; past 2^53 the
// widening may round, which EqualCount's magnitude check declines.
func equalNumeric(v any) (float64, bool) {
	switch x := v.(type) {
	case int:
		return float64(x), true
	case int8:
		return float64(x), true
	case int16:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint:
		return float64(x), true
	case uint8:
		return float64(x), true
	case uint16:
		return float64(x), true
	case uint32:
		return float64(x), true
	case uint64:
		return float64(x), true
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
