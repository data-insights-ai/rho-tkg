package index

import (
	"math"
	"sort"
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

// orderedKeys is a chunked sorted set of distinct float64 keys — a B+ tree
// of order 2*orderedKeyChunk with a flat root directory. The directory
// (chunk slice) is ordered: every key in chunks[i] is less than every key
// in chunks[i+1]. NaN is never inserted (numericSortKey rejects it).
type orderedKeys struct {
	chunks [][]float64
	n      int
}

// chunkIdx returns the index of the chunk that does or would contain k:
// the first chunk whose last element is >= k. Returns len(chunks) when k is
// greater than every stored key.
func (o *orderedKeys) chunkIdx(k float64) int {
	return sort.Search(len(o.chunks), func(i int) bool {
		c := o.chunks[i]
		return c[len(c)-1] >= k
	})
}

// insert adds a key known to be absent (callers guard via the bucket map).
func (o *orderedKeys) insert(k float64) {
	o.n++
	if len(o.chunks) == 0 {
		o.chunks = append(o.chunks, append(make([]float64, 0, orderedKeyChunk), k))
		return
	}
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		ci-- // greater than every key: extend the last chunk
	}
	c := o.chunks[ci]
	pos := sort.SearchFloat64s(c, k)
	c = append(c, 0)
	copy(c[pos+1:], c[pos:])
	c[pos] = k
	o.chunks[ci] = c

	if len(c) > 2*orderedKeyChunk {
		// Split: left half stays, right half becomes a new chunk. Copy the
		// right half so the two chunks stop sharing a backing array —
		// appends to the left would otherwise clobber the right.
		mid := len(c) / 2
		right := append(make([]float64, 0, orderedKeyChunk+len(c)-mid), c[mid:]...)
		o.chunks[ci] = c[:mid:mid]
		o.chunks = append(o.chunks, nil)
		copy(o.chunks[ci+2:], o.chunks[ci+1:])
		o.chunks[ci+1] = right
	}
}

// remove deletes a key if present.
func (o *orderedKeys) remove(k float64) {
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		return
	}
	c := o.chunks[ci]
	pos := sort.SearchFloat64s(c, k)
	if pos >= len(c) || c[pos] != k {
		return
	}
	o.chunks[ci] = append(c[:pos], c[pos+1:]...)
	o.n--
	if len(o.chunks[ci]) == 0 {
		o.chunks = append(o.chunks[:ci], o.chunks[ci+1:]...)
	}
}

// forEachFrom calls fn for every key >= lo in ascending order until fn
// returns false.
func (o *orderedKeys) forEachFrom(lo float64, fn func(k float64) bool) {
	start := o.chunkIdx(lo)
	for ci := start; ci < len(o.chunks); ci++ {
		c := o.chunks[ci]
		pos := 0
		if ci == start {
			pos = sort.SearchFloat64s(c, lo)
		}
		for ; pos < len(c); pos++ {
			if !fn(c[pos]) {
				return
			}
		}
	}
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
