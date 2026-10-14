package index

import (
	"math"
	"strconv"
	"strings"
)

// Range-CARDINALITY (R1): a range count is the sum of bucket sizes over the
// sorted keys in [min,max] (numBuckets), with no node fetches or candidate
// materialisation.

// exactInt64FromVK decodes a canonical value key to its exact int64, or
// ok=false for non-integers (non-numeric, NaN, fractional float, or a
// "u"/"u64" value beyond MaxInt64). Used to detect integers past 2^53 whose
// float64 sort key may collide (numImpreciseCount).
func exactInt64FromVK(vk string) (int64, bool) {
	colon := strings.IndexByte(vk, ':')
	if colon < 0 {
		return 0, false
	}
	prefix, payload := vk[:colon], vk[colon+1:]
	switch prefix {
	case "i", "i8", "i16", "i32", "i64":
		n, err := strconv.ParseInt(payload, 10, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	case "u8", "u16", "u32":
		// u8/u16/u32 always fit in int64.
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil {
			return 0, false
		}
		return int64(n), true // #nosec G115 -- u32 max is below MaxInt64
	case "u", "u64":
		// Bare "u" is a 64-bit uint: same overflow gate as u64.
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil || n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

// isNumericImprecise reports whether vk is an integer beyond 2^53, whose sort
// key may collide with a neighbour.
func isNumericImprecise(vk string) bool {
	v, ok := exactInt64FromVK(vk)
	if !ok {
		return false
	}
	const f64IntCeil = int64(1) << 53
	return v > f64IntCeil || v < -f64IntCeil
}

// noteNumericPrecision increments numImpreciseCount for a poisoning value
// (BACKLOG 16j). Caller holds the write lock.
func (pi *PropertyIndex) noteNumericPrecision(vk string) {
	if isNumericImprecise(vk) {
		pi.numImpreciseCount++
	}
}

// noteNumericPrecisionRemoved is the symmetric decrement for the same (id, vk)
// pair (BACKLOG 16j). Caller holds the write lock.
func (pi *PropertyIndex) noteNumericPrecisionRemoved(vk string) {
	if isNumericImprecise(vk) && pi.numImpreciseCount > 0 {
		pi.numImpreciseCount--
	}
}

// RangeCardinality returns the count of indexed nodes whose numeric value lies
// in [min,max], with no node fetches: two prefix sums on a range-counts index,
// else the ordered bucket sizes. ok=false declines (caller scans) when the
// index holds an integer past 2^53. A NaN bound counts 0. The caller must
// ensure the bounds capture the whole predicate and the query is non-temporal.
func (pi *PropertyIndex) RangeCardinality(min, max float64, inclMin, inclMax bool) (int64, bool) {
	if pi == nil || pi.elements || pi.numImpreciseCount > 0 {
		return 0, false
	}
	if pi.rangeCounts {
		return pi.numKeys.countRange(min, max, inclMin, inclMax), true
	}
	if pi.numBuckets == nil || math.IsNaN(min) || math.IsNaN(max) {
		// No numeric values, or a NaN bound: exactly zero.
		return 0, true
	}
	var count int64
	pi.numKeys.forEachFrom(min, func(k float64) bool {
		if !inclMin && k == min {
			return true // exclusive lower bound: skip the boundary key
		}
		if inclMax {
			if k > max {
				return false
			}
		} else if k >= max {
			return false
		}
		count += int64(len(pi.numBuckets[k]))
		return true
	})
	return count, true
}
