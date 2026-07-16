package index

import (
	"math"
	"slices"
	"strconv"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered numeric view of a PropertyIndex (range queries).
//
// Every property index transparently maintains, alongside its hash-shaped
// Entries, an ORDERED view of the numeric values it has seen: a chunked
// sorted set of distinct float64 sort keys plus per-key ID buckets. Range
// scans (`WHERE n.age > $x`) search the key set instead of scanning the
// label.
//
// Two deliberate properties:
//
//   - Sort keys are float64. int64 values beyond 2^53 collapse onto
//     neighboring keys, so the view is an OVER-SELECTING candidate filter:
//     callers must widen query bounds by one ulp (RangeNodeIDs does this)
//     and post-filter candidates with exact comparison semantics. The view
//     never under-selects within widened bounds.
//   - Key storage is a two-level chunked sorted set (orderedKeys below) —
//     inserting a NEW distinct key costs O(log chunks + chunkSize) instead
//     of the flat sorted slice's O(D) memmove. This lifted the previous
//     100k distinct-key cap (rangeDisabled): high-cardinality numeric
//     indexes now keep their ordered view instead of degrading to the
//     label-scan path.

// orderedKeyChunk is the split threshold half-width: chunks hold at most
// 2*orderedKeyChunk keys and split into two of orderedKeyChunk each.
// 512 float64s = one 4KB page per half-chunk — large enough that the
// directory stays small (10M keys ≈ 10-20k chunks), small enough that the
// in-chunk insert memmove is cheap.
const orderedKeyChunk = 512

// RangeOrderedCursor is the opaque resumption point for a paged ordered range
// scan (RangeOrderedPage). The zero value is "start from the beginning".
// Callers thread the `next` cursor returned by one page into the next call
// and never construct or interpret its fields themselves.
type RangeOrderedCursor struct {
	has bool
	val float64
	id  snowflake.ID
}

// RangeOrderedPage collects up to `limit` candidate node IDs from the ordered
// numeric view in the CONTRACTUAL order — value ascending (or descending when
// desc), TIES within one value ALWAYS by node ID ascending regardless of desc
// — starting strictly AFTER `after`. It returns the page (already in order),
// the cursor to resume the NEXT page from, and done=true once the range is
// exhausted.
//
// Like RangeNodeIDs this is an OVER-SELECTING candidate filter: bounds are
// widened by one ulp and boundary buckets are never skipped, so callers MUST
// post-filter each candidate with exact comparison semantics (lesson 23) and
// remember that int64 magnitudes past 2^53 collapse onto neighbouring float64
// sort keys (lesson 25). Paging (rather than one big collection) is what keeps
// a top-k caller's work O(k + pageSize + log n): the caller stops threading
// pages the moment it has enough rows, so the scan never materializes the
// whole range. supported=false only when the index is nil; an enabled view
// with no numeric keys returns an authoritative empty page (done=true).
func (pi *PropertyIndex) RangeOrderedPage(min, max float64, desc bool, after RangeOrderedCursor, limit int) (ids []snowflake.ID, next RangeOrderedCursor, done bool, supported bool) {
	if pi == nil {
		return nil, RangeOrderedCursor{}, true, false
	}
	if limit <= 0 || pi.numKeys.n == 0 {
		return nil, RangeOrderedCursor{}, true, true
	}
	lo := math.Nextafter(min, math.Inf(-1))
	hi := math.Nextafter(max, math.Inf(1))

	next = after
	emitKey := func(k float64) bool {
		bucket := pi.numBuckets[k]
		if len(bucket) == 0 {
			return true
		}
		kids := make([]snowflake.ID, 0, len(bucket))
		for id := range bucket {
			kids = append(kids, id)
		}
		slices.Sort(kids) // ties by node ID ascending, in asc AND desc value order
		for _, id := range kids {
			// Skip everything at or before the resume cursor's position. The
			// per-value tie order is ascending in BOTH scan directions, so the
			// within-group skip test is `id <= after.id` regardless of desc.
			if after.has && k == after.val && id <= after.id {
				continue
			}
			ids = append(ids, id)
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
		pi.numBuckets = make(map[float64]map[snowflake.ID]struct{})
	}
	bucket, exists := pi.numBuckets[k]
	if !exists {
		bucket = make(map[snowflake.ID]struct{})
		pi.numBuckets[k] = bucket
		pi.numKeys.insert(k)
	}
	bucket[id] = struct{}{}
	pi.noteNumericPrecision(vk) // R1: flag >2^53 integers (float64 key collision)
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
	delete(bucket, id)
	if len(bucket) == 0 {
		delete(pi.numBuckets, k)
		pi.numKeys.remove(k)
	}
}

// purgeOrdered removes id from every numeric bucket (corruption-path
// mirror of PurgeNodeFromAllPropertyIndexes' brute-force sweep).
func (pi *PropertyIndex) purgeOrdered(id snowflake.ID) {
	if pi.numBuckets == nil {
		return
	}
	for k, bucket := range pi.numBuckets {
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(pi.numBuckets, k)
			pi.numKeys.remove(k)
		}
	}
}

// RangeNodeIDs returns the candidate node IDs whose numeric property sort
// key lies within [min, max] (bound inclusivity per flags), as a
// caller-owned slice. Bounds are WIDENED by one ulp on each side before the
// search — the float64 sort keys make this view an over-selecting filter,
// and callers must post-filter with exact comparison semantics.
// supported=false only when the index is nil; an enabled view with no
// numeric keys returns an authoritative empty result (non-numeric values
// compare null against numeric bounds and can never match).
func (pi *PropertyIndex) RangeNodeIDs(min, max float64, inclMin, inclMax bool) (ids []types.NodeID, supported bool) {
	if pi == nil {
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
		// Boundary buckets are NEVER skipped, even on exclusive bounds:
		// int64 values past 2^53 collide onto neighboring float64 sort
		// keys, so the exact-bound bucket can contain values genuinely
		// inside the range (review finding — skipping under-selected).
		// The caller's exact post-filter applies the bound inclusivity.
		for id := range pi.numBuckets[k] {
			ids = append(ids, types.NodeID(id))
		}
		return true
	})
	return ids, true
}
