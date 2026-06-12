package index

import (
	"math"
	"strconv"
	"strings"
)

// Range-CARDINALITY (R1) — the read-side accelerator for `count(p) WHERE p.k > x`.
//
// The existing chunked ordered view (property_index_range.go) answers range
// SCANS by materialising candidate IDs; COUNTING them then re-fetches every
// candidate node to re-check the predicate (the over-select contract), which on
// a broad predicate is O(matches) node decodes — measured at 132ms / 1.6M allocs
// for `count WHERE age>37` over 100k nodes. But the ordered view ALREADY groups
// nodes by value (numBuckets) with O(1) per-bucket cardinality, so a range count
// is just the sum of bucket sizes over the sorted keys in [min,max] — O(distinct
// values in range), no node fetches, no candidate materialisation.
//
// Note (R1 finding): a roaring64 bit-sliced index (BSI) was prototyped and
// MEASURED for this. Its CompareValue is big.Int-based and cost ~30ms over 100k
// regardless of bit width — only ~4x over the scan and far slower than the
// sorted-bucket sum below. The BSI wins only when the property is BOTH
// high-cardinality (so the bucket sum degrades to O(matches)) AND the count is
// hot; no current benchmark shape has that profile, so the simpler exact sum is
// the right tool here.

// exactInt64FromVK decodes a canonical value key to its EXACT int64, or
// ok=false when the value is not an integer (non-numeric, NaN, fractional float,
// or u64 beyond MaxInt64). Used to detect integers past 2^53 whose float64 sort
// key may collide — those poison the sorted-bucket count (numImprecise).
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
	case "u", "u8", "u16", "u32":
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil {
			return 0, false
		}
		return int64(n), true // u32 max < MaxInt64
	case "u64":
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil || n > math.MaxInt64 {
			return 0, false
		}
		return int64(n), true
	}
	return 0, false
}

// noteNumericPrecision flags the index as imprecise if vk decodes to an integer
// whose magnitude exceeds 2^53 (float64's exact-integer ceiling), since its sort
// key may then collide with a neighbouring integer. Caller holds the write lock.
func (pi *PropertyIndex) noteNumericPrecision(vk string) {
	if pi.numImprecise {
		return
	}
	if v, ok := exactInt64FromVK(vk); ok {
		const f64IntCeil = int64(1) << 53
		if v > f64IntCeil || v < -f64IntCeil {
			pi.numImprecise = true
		}
	}
}

// RangeCardinality returns the count of indexed nodes whose numeric value lies
// in [min,max] (inclusivity per flags), summed directly from the ordered
// bucket sizes — O(distinct values in range), NO node fetches. ok=false declines
// (the caller scans) when the index holds an integer past 2^53 whose float64
// sort key may collide. The bounds must already capture the WHOLE predicate and
// the query must be non-temporal — the caller enforces that.
func (pi *PropertyIndex) RangeCardinality(min, max float64, inclMin, inclMax bool) (int64, bool) {
	if pi == nil || pi.numImprecise {
		return 0, false
	}
	if pi.numBuckets == nil {
		return 0, true // index exists but holds no numeric values — exactly zero
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
