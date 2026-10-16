package index

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Ordered STRING view of a PropertyIndex (prefix / string-range scans): a
// sortedChunks[string] of distinct values in byte order plus per-value ID
// buckets. A prefix scan is the range [p, p+) over it. String sort keys are
// exact, so the view never over-selects. The value key is "s:"+value;
// stringSortKey strips the tag.

// StrOrderedCursor is the opaque resumption point for PrefixOrderedPage. The
// zero value starts at the beginning; callers only thread the returned `next`.
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

// prefixSuccessor returns the smallest string greater than every string with
// prefix p (the exclusive upper bound). ok=false when none exists (empty or
// all-0xFF prefix): the range is open to the top.
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

// addOrderedStr adds a new value key's string to the ordered string view, if
// vk is a string key. Called when vk's posting list is created.
func (pi *PropertyIndex) addOrderedStr(vk string) {
	if s, ok := stringSortKey(vk); ok {
		pi.strKeys.insert(s)
	}
}

// removeOrderedStr drops an emptied string value key's string from the view.
func (pi *PropertyIndex) removeOrderedStr(vk string) {
	if s, ok := stringSortKey(vk); ok {
		pi.strKeys.remove(s)
	}
}

// strPostings returns the posting list of the string value s; buf is scratch
// for the value key (the map lookup does not allocate).
func (pi *PropertyIndex) strPostings(buf *[]byte, s string) *IDPostings {
	*buf = append(append((*buf)[:0], "s:"...), s...)
	return pi.Entries[string(*buf)]
}

// PrefixNodeIDs returns the node IDs whose string value begins with prefix, as
// a caller-owned unordered slice (empty prefix matches all strings).
// supported=false only when the index is nil or an Elements index.
func (pi *PropertyIndex) PrefixNodeIDs(prefix string) (ids []types.NodeID, supported bool) {
	if pi == nil || pi.elements {
		return nil, false
	}
	if pi.strKeys.n == 0 {
		return nil, true
	}
	upper, hasUpper := prefixSuccessor(prefix)
	var buf []byte
	pi.strKeys.forEachFrom(prefix, func(k string) bool {
		if hasUpper && k >= upper {
			return false // past the prefix range
		}
		for id := range pi.strPostings(&buf, k).All() {
			ids = append(ids, types.NodeID(id))
		}
		return true
	})
	return ids, true
}

// PrefixOrderedPage collects up to limit node IDs whose string value begins
// with prefix, in value order (descending when desc; ties by node ID ascending
// in both directions), strictly after `after`. It returns the page, the cursor
// for the next page and done=true once exhausted. Never over-selects.
// supported=false only when the index is nil or an Elements index.
func (pi *PropertyIndex) PrefixOrderedPage(prefix string, desc bool, after StrOrderedCursor, limit int) (ids []snowflake.ID, next StrOrderedCursor, done bool, supported bool) {
	return pi.prefixOrderedPage(prefix, desc, after, limit, nil)
}

// PrefixOrderedValuePage is PrefixOrderedPage with each ID's string value,
// aligned with ids (round 7 request 5: an ordered walk that reads no row).
// supported=false also while the index is being built (Mutated is set).
func (pi *PropertyIndex) PrefixOrderedValuePage(prefix string, desc bool, after StrOrderedCursor, limit int) (ids []snowflake.ID, vals []string, next StrOrderedCursor, done bool, supported bool) {
	ids, next, done, supported = pi.prefixOrderedPage(prefix, desc, after, limit, &vals)
	return ids, vals, next, done, supported
}

// prefixOrderedPage is PrefixOrderedPage; a non-nil keys receives each ID's
// string, aligned with ids.
func (pi *PropertyIndex) prefixOrderedPage(prefix string, desc bool, after StrOrderedCursor, limit int, keys *[]string) (ids []snowflake.ID, next StrOrderedCursor, done bool, supported bool) {
	if !pi.serves() {
		return nil, StrOrderedCursor{}, true, false
	}
	if limit <= 0 || pi.strKeys.n == 0 {
		return nil, StrOrderedCursor{}, true, true
	}
	upper, hasUpper := prefixSuccessor(prefix)

	next = after
	var buf []byte
	emitKey := func(k string) bool {
		// Ties by node ID ascending in asc AND desc value order: the posting
		// list is sorted, so resuming inside a value is a seek past after.id.
		var from snowflake.ID
		if after.has && k == after.val {
			from = after.id
		}
		for id := range pi.strPostings(&buf, k).After(from) {
			ids = append(ids, id)
			if keys != nil {
				*keys = append(*keys, k)
			}
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
