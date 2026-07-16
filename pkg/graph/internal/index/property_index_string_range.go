package index

import (
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered STRING view of a PropertyIndex (prefix / string-range scans).
//
// Alongside the hash-shaped Entries and the numeric ordered view, every property
// index maintains an ORDERED view of the STRING values it has seen: a chunked
// sorted set (sortedChunks[string]) of distinct string values in lexicographic
// (canonical byte) order, plus per-value ID buckets. A prefix scan
// (`WHERE n.name STARTS WITH $p`) is a contiguous range [p, p⁺) over that set,
// where p⁺ is the prefix successor — so the scan searches the key set instead of
// scanning the label. Unlike the numeric view there is NO precision caveat:
// string sort keys are exact, so the view neither over- nor under-selects.
//
// The value key for a string property is "s:"+value (IndexablePropertyValueKey),
// so a value key sorts identically to its underlying string and stringSortKey
// simply strips the "s:" tag.

// StrOrderedCursor is the opaque resumption point for a paged ordered prefix /
// string-range scan (PrefixOrderedPage). The zero value is "start from the
// beginning". Callers thread the returned `next` cursor into the next call and
// never construct or interpret its fields.
type StrOrderedCursor struct {
	has bool
	val string
	id  snowflake.ID
}

// stringSortKey returns the underlying string of a canonical value key, ok=false
// for non-string keys. String keys are exactly "s:"+value.
func stringSortKey(vk string) (string, bool) {
	if len(vk) >= 2 && vk[0] == 's' && vk[1] == ':' {
		return vk[2:], true
	}
	return "", false
}

// prefixSuccessor returns the smallest string strictly greater than every string
// beginning with p — the EXCLUSIVE upper bound of the prefix range. ok=false when
// no finite successor exists: an empty prefix (every string matches) or a prefix
// that is all 0xFF bytes (no larger same-length prefix). Callers treat ok=false
// as "range open to the top".
func prefixSuccessor(p string) (string, bool) {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return string(b[:i+1]), true
		}
	}
	return "", false
}

// addOrderedStr inserts id under vk's string sort key, if any.
// Caller holds the store's write lock (same discipline as AddKey).
func (pi *PropertyIndex) addOrderedStr(id snowflake.ID, vk string) {
	s, ok := stringSortKey(vk)
	if !ok {
		return
	}
	if pi.strBuckets == nil {
		pi.strBuckets = make(map[string]map[snowflake.ID]struct{})
	}
	bucket, exists := pi.strBuckets[s]
	if !exists {
		bucket = make(map[snowflake.ID]struct{})
		pi.strBuckets[s] = bucket
		pi.strKeys.insert(s)
	}
	bucket[id] = struct{}{}
}

// removeOrderedStr removes id from vk's string bucket, if any.
func (pi *PropertyIndex) removeOrderedStr(id snowflake.ID, vk string) {
	if pi.strBuckets == nil {
		return
	}
	s, ok := stringSortKey(vk)
	if !ok {
		return
	}
	bucket, exists := pi.strBuckets[s]
	if !exists {
		return
	}
	delete(bucket, id)
	if len(bucket) == 0 {
		delete(pi.strBuckets, s)
		pi.strKeys.remove(s)
	}
}

// purgeOrderedStr removes id from every string bucket (corruption-path mirror of
// PurgeNodeFromAllPropertyIndexes' brute-force sweep).
func (pi *PropertyIndex) purgeOrderedStr(id snowflake.ID) {
	if pi.strBuckets == nil {
		return
	}
	for s, bucket := range pi.strBuckets {
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(pi.strBuckets, s)
			pi.strKeys.remove(s)
		}
	}
}

// PrefixNodeIDs returns the candidate node IDs whose STRING property value begins
// with prefix, as a caller-owned slice (unordered). An empty prefix matches every
// string value. supported=false only when the index is nil; an enabled view with
// no string keys returns an authoritative empty result.
func (pi *PropertyIndex) PrefixNodeIDs(prefix string) (ids []types.NodeID, supported bool) {
	if pi == nil {
		return nil, false
	}
	if pi.strKeys.n == 0 {
		return nil, true
	}
	upper, hasUpper := prefixSuccessor(prefix)
	pi.strKeys.forEachFrom(prefix, func(k string) bool {
		if hasUpper && k >= upper {
			return false // past the prefix range
		}
		for id := range pi.strBuckets[k] {
			ids = append(ids, types.NodeID(id))
		}
		return true
	})
	return ids, true
}

// PrefixOrderedPage collects up to `limit` candidate node IDs whose STRING value
// begins with prefix, in CONTRACTUAL order — value lexicographically ascending
// (or descending when desc), TIES within one value ALWAYS by node ID ascending
// regardless of desc — starting strictly AFTER `after`. It returns the page (in
// order), the cursor to resume the NEXT page from, and done=true once the range
// is exhausted. Paging keeps a top-k caller's work O(k + pageSize + log n): the
// caller stops threading pages once it has enough rows. Unlike the numeric view
// this NEVER over-selects (string keys are exact). supported=false only when the
// index is nil; an enabled view with no string keys returns an authoritative
// empty page (done=true).
func (pi *PropertyIndex) PrefixOrderedPage(prefix string, desc bool, after StrOrderedCursor, limit int) (ids []snowflake.ID, next StrOrderedCursor, done bool, supported bool) {
	if pi == nil {
		return nil, StrOrderedCursor{}, true, false
	}
	if limit <= 0 || pi.strKeys.n == 0 {
		return nil, StrOrderedCursor{}, true, true
	}
	upper, hasUpper := prefixSuccessor(prefix)

	next = after
	emitKey := func(k string) bool {
		bucket := pi.strBuckets[k]
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
			next = StrOrderedCursor{has: true, val: k, id: id}
			if len(ids) >= limit {
				return false
			}
		}
		return true
	}

	if !desc {
		start := prefix
		if after.has && after.val > prefix {
			start = after.val
		}
		pi.strKeys.forEachFrom(start, func(k string) bool {
			if hasUpper && k >= upper {
				return false // past the prefix range
			}
			return emitKey(k)
		})
	} else {
		descEmit := func(k string) bool {
			if hasUpper && k >= upper {
				return true // overshoot above the range: keep scanning down
			}
			if k < prefix {
				return false // below the range: stop
			}
			if after.has && k > after.val {
				return true // not yet at the resume point
			}
			return emitKey(k)
		}
		if hasUpper {
			pi.strKeys.forEachDownFrom(upper, descEmit)
		} else {
			pi.strKeys.forEachDownAll(descEmit)
		}
	}
	done = len(ids) < limit
	return ids, next, done, true
}
