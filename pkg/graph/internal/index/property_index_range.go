package index

import (
	"math"
	"strconv"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered numeric view of a PropertyIndex (range queries): a chunked sorted
// set of distinct float64 sort keys plus per-key ID buckets. Sort keys are
// float64, so int64 values beyond 2^53 collapse onto neighbours: the view is an
// over-selecting candidate filter. Callers widen bounds by one ulp
// (RangeNodeIDs does) and post-filter with exact comparison.

// orderedKeyChunk is the split threshold half-width: chunks hold at most
// 2*orderedKeyChunk keys and split into two of orderedKeyChunk each.
const orderedKeyChunk = 512

// RangeOrderedCursor is the opaque resumption point for a paged ordered range
// scan (RangeOrderedPage). The zero value starts at the beginning; callers
// only thread the returned `next` into the next call.
type RangeOrderedCursor struct {
	has bool
	val float64
	id  snowflake.ID
}

// RangeOrderedPage collects up to limit candidate node IDs in value order
// (descending when desc; ties within one value always by node ID ascending),
// strictly after `after`. It returns the page, the cursor for the next page and
// done=true once the range is exhausted. An over-selecting filter like
// RangeNodeIDs: callers MUST post-filter exactly (lesson 23; int64 past 2^53,
// lesson 25). supported=false only when the index is nil or an Elements index.
func (pi *PropertyIndex) RangeOrderedPage(min, max float64, desc bool, after RangeOrderedCursor, limit int) (ids []snowflake.ID, next RangeOrderedCursor, done bool, supported bool) {
	return pi.rangeOrderedPage(min, max, desc, after, limit, nil)
}

// serves reports whether the index answers the doors that read it as a
// whole (ordered walks, range and equality counts, bounds, value
// statistics): a plain index, built. A nil index, an Elements index (a
// list's elements are no row value) and one being built (Mutated set: its
// entries are partial) decline, so no door answers from a partial index.
// The one usability rule of those doors.
func (pi *PropertyIndex) serves() bool {
	return pi != nil && !pi.elements && pi.Mutated == nil
}

// rangeOrderedPage is RangeOrderedPage; a non-nil keys receives each ID's
// sort key, aligned with ids. supported=false where the index does not
// serve (serves).
func (pi *PropertyIndex) rangeOrderedPage(min, max float64, desc bool, after RangeOrderedCursor, limit int, keys *[]float64) (ids []snowflake.ID, next RangeOrderedCursor, done bool, supported bool) {
	if !pi.serves() {
		return nil, RangeOrderedCursor{}, true, false
	}
	if limit <= 0 || pi.numKeys.n == 0 {
		return nil, RangeOrderedCursor{}, true, true
	}
	lo := math.Nextafter(min, math.Inf(-1))
	hi := math.Nextafter(max, math.Inf(1))

	next = after
	emitKey := func(k float64) bool {
		// Ties by node ID ascending in asc AND desc value order: the bucket is
		// sorted, so resuming inside a value is a seek past after.id.
		var from snowflake.ID
		if after.has && k == after.val {
			from = after.id
		}
		for id := range pi.numBuckets[k].After(from) {
			ids = append(ids, id)
			if keys != nil {
				*keys = append(*keys, k)
			}
			next = RangeOrderedCursor{has: true, val: k, id: id}
			if len(ids) >= limit {
				return false
			}
		}
		return true
	}

	if !desc {
		start := lo
		if after.has && after.val > lo {
			start = after.val
		}
		pi.numKeys.forEachFrom(start, func(k float64) bool {
			if k > hi {
				return false
			}
			return emitKey(k)
		})
	} else {
		start := hi
		if after.has && after.val < hi {
			start = after.val
		}
		pi.numKeys.forEachDownFrom(start, func(k float64) bool {
			if k < lo {
				return false
			}
			return emitKey(k)
		})
	}
	done = len(ids) < limit
	return ids, next, done, true
}

// numericSortKey decodes the canonical value key's numeric value. ok=false
// for non-numeric keys and NaN (NaN is unordered — equality lookups still
// work through the hash entries).
func numericSortKey(vk string) (float64, bool) {
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
		return float64(n), true
	case "u", "u8", "u16", "u32", "u64":
		n, err := strconv.ParseUint(payload, 10, 64)
		if err != nil {
			return 0, false
		}
		return float64(n), true
	case "f32", "f64":
		if payload == "nan" {
			return 0, false
		}
		f, err := strconv.ParseFloat(payload, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	}
	return 0, false
}

// addOrdered inserts id under vk's numeric sort key, if any.
// Caller holds the store's write lock (same discipline as AddKey).
func (pi *PropertyIndex) addOrdered(id snowflake.ID, vk string) {
	k, ok := numericSortKey(vk)
	if !ok {
		return
	}
	if pi.numBuckets == nil {
		pi.numBuckets = make(map[float64]*IDPostings)
	}
	if pi.rangeCounts {
		pi.addOrderedCounted(id, k)
		pi.noteNumericPrecision(vk) // R1: flag >2^53 integers (float64 key collision)
		return
	}
	bucket, exists := pi.numBuckets[k]
	if !exists {
		bucket = &IDPostings{}
		pi.numBuckets[k] = bucket
		pi.numKeys.insert(k)
	}
	bucket.add(id)
	pi.noteNumericPrecision(vk) // R1: flag >2^53 integers (float64 key collision)
}

// addOrderedCounted is addOrdered's bucket step with range counts: the count
// moves only when id was not already in the bucket.
func (pi *PropertyIndex) addOrderedCounted(id snowflake.ID, k float64) {
	bucket, exists := pi.numBuckets[k]
	if !exists {
		bucket = &IDPostings{}
		pi.numBuckets[k] = bucket
	}
	if bucket.add(id) {
		pi.numKeys.addCount(k, 1)
	}
}

// removeOrdered removes id from vk's numeric bucket, if any.
func (pi *PropertyIndex) removeOrdered(id snowflake.ID, vk string) {
	if pi.numBuckets == nil {
		return
	}
	k, ok := numericSortKey(vk)
	if !ok {
		return
	}
	bucket, exists := pi.numBuckets[k]
	if !exists {
		return
	}
	if pi.rangeCounts {
		if bucket.remove(id) {
			pi.numKeys.addCount(k, -1) // drops the key with its last member
		}
		if bucket.Len() == 0 {
			delete(pi.numBuckets, k)
		}
	} else {
		bucket.remove(id)
		if bucket.Len() == 0 {
			delete(pi.numBuckets, k)
			pi.numKeys.remove(k)
		}
	}
	pi.noteNumericPrecisionRemoved(vk) // BACKLOG 16j: symmetric with addOrdered's noteNumericPrecision
}

// purgeOrdered removes id from every numeric bucket (corruption-path
// mirror of PurgeNodeFromAllPropertyIndexes' brute-force sweep).
func (pi *PropertyIndex) purgeOrdered(id snowflake.ID) {
	if pi.numBuckets == nil {
		return
	}
	for k, bucket := range pi.numBuckets {
		if !bucket.remove(id) {
			continue
		}
		if pi.rangeCounts {
			pi.numKeys.addCount(k, -1)
		}
		if bucket.Len() == 0 {
			delete(pi.numBuckets, k)
			if !pi.rangeCounts {
				pi.numKeys.remove(k)
			}
		}
	}
}

// RangeNodeIDs returns the candidate node IDs whose numeric sort key lies
// within [min, max] as a caller-owned slice. Bounds are widened by one ulp, so
// callers must post-filter exactly (inclusivity is theirs too).
// supported=false only when the index is nil or an Elements index.
func (pi *PropertyIndex) RangeNodeIDs(min, max float64, inclMin, inclMax bool) (ids []types.NodeID, supported bool) {
	if pi == nil || pi.elements {
		return nil, false
	}
	if pi.numKeys.n == 0 {
		return nil, true
	}
	lo := math.Nextafter(min, math.Inf(-1))
	hi := math.Nextafter(max, math.Inf(1))
	pi.numKeys.forEachFrom(lo, func(k float64) bool {
		if k > hi {
			return false
		}
		// Boundary buckets are never skipped, even on exclusive bounds: keys
		// past 2^53 collide, so the bound bucket may hold in-range values.
		for id := range pi.numBuckets[k].All() {
			ids = append(ids, types.NodeID(id))
		}
		return true
	})
	return ids, true
}
