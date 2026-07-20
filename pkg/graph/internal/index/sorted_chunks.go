package index

import (
	"cmp"
	"slices"
)

// sortedChunks is a two-level chunked sorted set of DISTINCT ordered keys — a
// B+ tree of order 2*orderedKeyChunk with a flat root directory. The directory
// (chunk slice) is ordered: every key in chunks[i] is less than every key in
// chunks[i+1]. Inserting a NEW distinct key costs O(log chunks + chunkSize)
// instead of a flat sorted slice's O(D) memmove — this is what lets a
// high-cardinality property index keep its ordered view instead of degrading to
// the label-scan path.
//
// It is generic over any cmp.Ordered key type so BOTH the numeric ordered view
// (sortedChunks[float64]) and the string ordered view (sortedChunks[string], for
// prefix scans) share one tested implementation. Callers exclude sentinel keys
// (NaN for float64) BEFORE inserting — the set stores whatever it is given.
type sortedChunks[T cmp.Ordered] struct {
	chunks [][]T
	n      int
}

// chunkIdx returns the index of the chunk that does or would contain k: the
// first chunk whose last element is >= k. Returns len(chunks) when k is greater
// than every stored key.
func (o *sortedChunks[T]) chunkIdx(k T) int {
	lo, hi := 0, len(o.chunks)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		c := o.chunks[mid]
		if c[len(c)-1] >= k {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// insert adds a key known to be absent (callers guard via the bucket map).
func (o *sortedChunks[T]) insert(k T) {
	o.n++
	if len(o.chunks) == 0 {
		o.chunks = append(o.chunks, append(make([]T, 0, orderedKeyChunk), k))
		return
	}
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		ci-- // greater than every key: extend the last chunk
	}
	c := o.chunks[ci]
	pos, _ := slices.BinarySearch(c, k)
	c = append(c, k) // grow by one; overwritten below
	copy(c[pos+1:], c[pos:])
	c[pos] = k
	o.chunks[ci] = c

	if len(c) > 2*orderedKeyChunk {
		// Split: left half stays, right half becomes a new chunk. Copy the right
		// half so the two chunks stop sharing a backing array — appends to the
		// left would otherwise clobber the right.
		mid := len(c) / 2
		right := append(make([]T, 0, orderedKeyChunk+len(c)-mid), c[mid:]...)
		o.chunks[ci] = c[:mid:mid]
		o.chunks = append(o.chunks, nil)
		copy(o.chunks[ci+2:], o.chunks[ci+1:])
		o.chunks[ci+1] = right
	}
}

// remove deletes a key if present.
func (o *sortedChunks[T]) remove(k T) {
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		return
	}
	c := o.chunks[ci]
	pos, found := slices.BinarySearch(c, k)
	if !found {
		return
	}
	c = append(c[:pos], c[pos+1:]...)
	o.chunks[ci] = c
	o.n--
	if len(c) == 0 {
		o.chunks = append(o.chunks[:ci], o.chunks[ci+1:]...)
		return
	}
	o.mergeIfUndersized(ci)
}

// mergeIfUndersized merges chunk ci into an adjacent chunk when ci has
// shrunk below half occupancy AND the combined size still fits under the
// insert-side split cap (2*orderedKeyChunk). Without this, long-lived
// high-churn indexes that repeatedly insert and remove near a chunk
// boundary — without ever fully draining any single chunk to empty — leave
// the chunk directory growing toward O(distinct keys ever inserted) instead
// of O(live keys / orderedKeyChunk), degrading chunkIdx's binary search and
// full-scan iteration. Tries the right neighbor first (no index shift needed
// beyond removing the now-empty slot), falling back to the left neighbor.
// A single merge attempt per remove is sufficient: the split cap never gets
// exceeded post-merge, so cascading further wouldn't shrink the directory
// any faster than letting the next remove re-check.
func (o *sortedChunks[T]) mergeIfUndersized(ci int) {
	c := o.chunks[ci]
	if len(c) >= orderedKeyChunk/2 {
		return
	}
	if ci+1 < len(o.chunks) {
		right := o.chunks[ci+1]
		if len(c)+len(right) <= 2*orderedKeyChunk {
			o.chunks[ci] = append(c, right...)
			o.chunks = append(o.chunks[:ci+1], o.chunks[ci+2:]...)
			return
		}
	}
	if ci > 0 {
		left := o.chunks[ci-1]
		if len(left)+len(c) <= 2*orderedKeyChunk {
			o.chunks[ci-1] = append(left, c...)
			o.chunks = append(o.chunks[:ci], o.chunks[ci+1:]...)
			return
		}
	}
}

// forEachFrom calls fn for every key >= lo in ascending order until fn returns
// false.
func (o *sortedChunks[T]) forEachFrom(lo T, fn func(k T) bool) {
	start := o.chunkIdx(lo)
	for ci := start; ci < len(o.chunks); ci++ {
		c := o.chunks[ci]
		pos := 0
		if ci == start {
			pos, _ = slices.BinarySearch(c, lo)
		}
		for ; pos < len(c); pos++ {
			if !fn(c[pos]) {
				return
			}
		}
	}
}

// forEachDownFrom calls fn for every key <= hi in DESCENDING order until fn
// returns false — the reverse-iteration mirror of forEachFrom, used by the
// descending ordered-scan (ORDER BY prop DESC) top-k path.
func (o *sortedChunks[T]) forEachDownFrom(hi T, fn func(k T) bool) {
	start := o.chunkIdx(hi)
	if start == len(o.chunks) {
		start = len(o.chunks) - 1 // hi is greater than every key: start at the last chunk
	}
	for ci := start; ci >= 0; ci-- {
		c := o.chunks[ci]
		pos := len(c) - 1
		if ci == start {
			// First chunk: begin at the greatest key <= hi.
			p, found := slices.BinarySearch(c, hi)
			if found {
				pos = p // exact hit
			} else {
				pos = p - 1 // p is the first key > hi, so p-1 is the last <= hi
			}
		}
		for ; pos >= 0; pos-- {
			if !fn(c[pos]) {
				return
			}
		}
	}
}

// forEachDownAll calls fn for every key in DESCENDING order from the largest,
// until fn returns false. Used by the descending prefix scan when the prefix has
// no finite upper bound (empty prefix, or a prefix that is all 0xFF bytes), where
// there is no `hi` to seed forEachDownFrom.
func (o *sortedChunks[T]) forEachDownAll(fn func(k T) bool) {
	for ci := len(o.chunks) - 1; ci >= 0; ci-- {
		c := o.chunks[ci]
		for pos := len(c) - 1; pos >= 0; pos-- {
			if !fn(c[pos]) {
				return
			}
		}
	}
}
