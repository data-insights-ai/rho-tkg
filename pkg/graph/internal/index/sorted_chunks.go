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
//
// Counted mode (an index created with range counts, round 4 R2): addCount
// keeps a multiplicity per key — cnt[i][j] for chunks[i][j] — the total of
// each chunk in tot, and a Fenwick tree over tot in fen, so countRange sums
// the multiplicities of a key range from two prefix sums. A structure is in
// counted mode from its first addCount on and is then changed through
// addCount only; set mode (insert/remove, every other index) never touches
// cnt, tot or fen and costs what it cost before counted mode existed.
type sortedChunks[T cmp.Ordered] struct {
	chunks [][]T
	n      int

	counted bool
	cnt     [][]int64
	tot     []int64
	fen     []int64 // 1-based Fenwick tree over tot; len(tot)+1 entries
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

// --- counted mode (round 4 R2) ---

// addCount adds d to k's multiplicity: an absent key with d > 0 is inserted,
// a key whose multiplicity reaches 0 or below is removed. O(log chunks +
// chunk size); a split, a merge or a drained chunk also rebuilds the prefix
// sums, O(chunks).
func (o *sortedChunks[T]) addCount(k T, d int64) {
	if d == 0 {
		return
	}
	o.counted = true
	if ci := o.chunkIdx(k); ci < len(o.chunks) {
		if pos, found := slices.BinarySearch(o.chunks[ci], k); found {
			if o.cnt[ci][pos]+d <= 0 {
				o.removeCountedAt(ci, pos)
				return
			}
			o.cnt[ci][pos] += d
			o.tot[ci] += d
			o.fenAdd(ci, d)
			return
		}
	}
	if d > 0 {
		o.insertCounted(k, d)
	}
}

// insertCounted adds an absent key with multiplicity c.
func (o *sortedChunks[T]) insertCounted(k T, c int64) {
	o.n++
	if len(o.chunks) == 0 {
		o.chunks = append(o.chunks, append(make([]T, 0, orderedKeyChunk), k))
		o.cnt = append(o.cnt, append(make([]int64, 0, orderedKeyChunk), c))
		o.tot = append(o.tot, c)
		o.rebuildFenwick()
		return
	}
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		ci-- // greater than every key: extend the last chunk
	}
	pos, _ := slices.BinarySearch(o.chunks[ci], k)
	o.chunks[ci] = slices.Insert(o.chunks[ci], pos, k)
	o.cnt[ci] = slices.Insert(o.cnt[ci], pos, c)
	o.tot[ci] += c
	if len(o.chunks[ci]) <= 2*orderedKeyChunk {
		o.fenAdd(ci, c)
		return
	}
	// Split as insert does: the right half is copied so the two chunks stop
	// sharing a backing array; counts and totals split with the keys.
	full, fc := o.chunks[ci], o.cnt[ci]
	mid := len(full) / 2
	right := append(make([]T, 0, orderedKeyChunk+len(full)-mid), full[mid:]...)
	rc := append(make([]int64, 0, orderedKeyChunk+len(fc)-mid), fc[mid:]...)
	o.chunks[ci], o.cnt[ci] = full[:mid:mid], fc[:mid:mid]
	o.chunks = slices.Insert(o.chunks, ci+1, right)
	o.cnt = slices.Insert(o.cnt, ci+1, rc)
	var rt int64
	for _, x := range rc {
		rt += x
	}
	o.tot[ci] -= rt
	o.tot = slices.Insert(o.tot, ci+1, rt)
	o.rebuildFenwick()
}

// removeCountedAt deletes the key at chunks[ci][pos] with its multiplicity,
// merging an undersized chunk as remove does.
func (o *sortedChunks[T]) removeCountedAt(ci, pos int) {
	c := o.cnt[ci][pos]
	o.chunks[ci] = slices.Delete(o.chunks[ci], pos, pos+1)
	o.cnt[ci] = slices.Delete(o.cnt[ci], pos, pos+1)
	o.tot[ci] -= c
	o.n--
	if len(o.chunks[ci]) == 0 {
		o.chunks = slices.Delete(o.chunks, ci, ci+1)
		o.cnt = slices.Delete(o.cnt, ci, ci+1)
		o.tot = slices.Delete(o.tot, ci, ci+1)
		o.rebuildFenwick()
		return
	}
	if o.mergeCountedIfUndersized(ci) {
		return // the merge rebuilt the prefix sums
	}
	o.fenAdd(ci, -c)
}

// mergeCountedIfUndersized is mergeIfUndersized for counted mode: the same
// rule, with counts and totals merged and the prefix sums rebuilt.
func (o *sortedChunks[T]) mergeCountedIfUndersized(ci int) bool {
	c := o.chunks[ci]
	if len(c) >= orderedKeyChunk/2 {
		return false
	}
	at := -1
	if ci+1 < len(o.chunks) && len(c)+len(o.chunks[ci+1]) <= 2*orderedKeyChunk {
		at = ci
	} else if ci > 0 && len(o.chunks[ci-1])+len(c) <= 2*orderedKeyChunk {
		at = ci - 1
	}
	if at < 0 {
		return false
	}
	o.chunks[at] = append(o.chunks[at], o.chunks[at+1]...)
	o.chunks = slices.Delete(o.chunks, at+1, at+2)
	o.cnt[at] = append(o.cnt[at], o.cnt[at+1]...)
	o.cnt = slices.Delete(o.cnt, at+1, at+2)
	o.tot[at] += o.tot[at+1]
	o.tot = slices.Delete(o.tot, at+1, at+2)
	o.rebuildFenwick()
	return true
}

// countRange returns the summed multiplicities of the keys in [lo, hi]
// (bound inclusivity per flags) of a counted structure. Both ends inside one
// chunk: the counts between them. Otherwise two prefix sums, each a Fenwick
// prefix over the chunk totals plus at most half a chunk of per-key counts:
// O(log chunks + chunk size), independent of the range's width. A NaN bound
// compares false with every key: 0.
func (o *sortedChunks[T]) countRange(lo, hi T, inclLo, inclHi bool) int64 {
	if unordered(lo) || unordered(hi) || lo > hi || (lo == hi && (!inclLo || !inclHi)) {
		return 0
	}
	// The range is [start, end): start is the first key >= lo (> lo when
	// exclusive), end the first key > hi (>= hi when exclusive).
	ciS, posS := o.locate(lo, !inclLo)
	ciE, posE := o.locate(hi, inclHi)
	if ciS == ciE {
		var n int64
		if ciS < len(o.cnt) && posS < posE {
			for _, x := range o.cnt[ciS][posS:posE] {
				n += x
			}
		}
		return n
	}
	return max(o.countBefore(ciE, posE)-o.countBefore(ciS, posS), 0)
}

// locate returns the position of the first key >= k (> k when past), as a
// chunk index and an offset in it; (len(chunks), 0) past every key.
func (o *sortedChunks[T]) locate(k T, past bool) (int, int) {
	ci := o.chunkIdx(k)
	if ci == len(o.chunks) {
		return ci, 0
	}
	pos, found := slices.BinarySearch(o.chunks[ci], k)
	if found && past {
		pos++
	}
	return ci, pos
}

// countBefore sums the multiplicities of the keys before position (ci, pos).
func (o *sortedChunks[T]) countBefore(ci, pos int) int64 {
	if ci == len(o.chunks) {
		return o.prefixTotal(ci)
	}
	cs := o.cnt[ci]
	var part int64
	if pos <= len(cs)/2 {
		for _, x := range cs[:pos] {
			part += x
		}
	} else {
		part = o.tot[ci]
		for _, x := range cs[pos:] {
			part -= x
		}
	}
	return o.prefixTotal(ci) + part
}

// prefixTotal is the sum of the totals of chunks [0, i).
func (o *sortedChunks[T]) prefixTotal(i int) int64 {
	var s int64
	for ; i > 0; i -= i & -i {
		s += o.fen[i]
	}
	return s
}

// fenAdd adds d to chunk i's total in the Fenwick tree.
func (o *sortedChunks[T]) fenAdd(i int, d int64) {
	for i++; i < len(o.fen); i += i & -i {
		o.fen[i] += d
	}
}

// rebuildFenwick rebuilds the Fenwick tree from tot in O(chunks).
func (o *sortedChunks[T]) rebuildFenwick() {
	n := len(o.tot) + 1
	if cap(o.fen) >= n {
		o.fen = o.fen[:n]
		clear(o.fen)
	} else {
		o.fen = make([]int64, n)
	}
	for i, t := range o.tot {
		j := i + 1
		o.fen[j] += t
		if p := j + (j & -j); p < n {
			o.fen[p] += o.fen[j]
		}
	}
}

// unordered reports whether x compares false with every value (a NaN).
func unordered[T cmp.Ordered](x T) bool {
	return x != x //nolint:staticcheck // SA4000: true only for NaN
}
