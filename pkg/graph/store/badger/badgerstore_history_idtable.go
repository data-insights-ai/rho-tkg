package badger

import (
	"math"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// idTable is the RAM representation of the history-presence set and the
// history-stamps sidecar (backlog 41): per ID a top version and a payload V,
// kept sorted by ID in packed column blocks of at most idTableBlock entries
// (struct-of-arrays: ids, tops, vals). A Go map pays a padded slot plus its
// load-factor slack per ID (presence 24-38 B, stamps 52-84 B at 10 K-1 M IDs);
// a block pays the columns (presence 12 B: V is struct{} and takes no room;
// stamps 28 B) plus about 1 B of routing, full after a build, about three
// quarters full after inserts split it.
//
// Lookup: a radix directory over the ID range (idDirBucketsPerBlock buckets
// per block) names the block, or the few blocks a dense run of IDs packs into
// one bucket; inside the block a summary of every idSegment-th ID names the
// segment to search. An insert or remove moves at most one block's tail; a
// full block splits in two (or, when the new ID falls past its end, the ID
// starts or joins the next block), and the directory is rebuilt only when a
// block is added or dropped or a block's first ID changes, so no write copies
// the whole table. Blocks grow by doubling up to idTableBlock and shrink when
// a quarter full. The zero value is an empty table. Not safe for concurrent
// use: the owner's mutex guards it.
type idTable[V any] struct {
	firsts []snowflake.ID // firsts[i] == blocks[i].ids[0]
	blocks []*idBlock[V]
	n      int

	// dir[k] is the last block whose first ID is <= dirLo + k<<dirShift (0
	// when none); the block of an ID in bucket k lies in [dir[k], dir[k+1]].
	// Empty while the table has fewer than idDirMinBlocks blocks.
	dir      []int32
	dirLo    snowflake.ID
	dirShift uint
}

// idTableBlock is the capacity of one block: a lookup searches at most this
// many IDs after the routing, an insert moves at most this many entries.
const idTableBlock = 256

// idBlockMinCap is the capacity a new block starts with outside a split.
const idBlockMinCap = 8

// idDirMinBlocks is the block count from which the table routes by the
// directory instead of a search over the first IDs; idDirBucketsPerBlock is
// how many directory buckets it keeps per block (most buckets then fall
// inside one block even where snowflake IDs cluster; 0.25 B per ID).
const (
	idDirMinBlocks       = 8
	idDirBucketsPerBlock = 16
)

type idBlock[V any] struct {
	// keys[j] is ids[j*idSegment] (math.MaxInt64 past the end): a lookup
	// searches this summary, then one segment, a few cache lines whatever the
	// IDs' spacing.
	keys [idSegments]snowflake.ID
	ids  []snowflake.ID
	tops []uint32
	vals []V
}

// idSegment is the span of one block summary entry.
const (
	idSegment  = 16
	idSegments = idTableBlock / idSegment
)

func newIDBlock[V any](capacity int) *idBlock[V] {
	return &idBlock[V]{
		ids:  make([]snowflake.ID, 0, capacity),
		tops: make([]uint32, 0, capacity),
		vals: make([]V, 0, capacity),
	}
}

// resize moves b's entries into columns of the given capacity (>= len).
func (b *idBlock[V]) resize(capacity int) {
	ids := make([]snowflake.ID, len(b.ids), capacity)
	tops := make([]uint32, len(b.ids), capacity)
	vals := make([]V, len(b.ids), capacity)
	copy(ids, b.ids)
	copy(tops, b.tops)
	copy(vals, b.vals)
	b.ids, b.tops, b.vals = ids, tops, vals
}

// insertAt inserts at position i; the block has room or grows (by doubling,
// at most to idTableBlock; the caller splits a full block first).
func (b *idBlock[V]) insertAt(i int, id snowflake.ID, top uint32, v V) {
	if len(b.ids) == cap(b.ids) {
		b.resize(min(max(2*cap(b.ids), idBlockMinCap), idTableBlock))
	}
	b.ids = slices.Insert(b.ids, i, id)
	b.tops = slices.Insert(b.tops, i, top)
	b.vals = slices.Insert(b.vals, i, v)
	b.summarize(i)
}

// search returns the first position whose ID is >= id: the last segment
// whose first ID is <= id from the block's summary, then that segment.
func (b *idBlock[V]) search(id snowflake.ID) int {
	j := 0
	for step := idSegments / 2; step > 0; step /= 2 {
		if b.keys[j+step] <= id {
			j += step
		}
	}
	// The sentinel equals an ID of math.MaxInt64: stay in the last used segment.
	lo := min(j, (len(b.ids)-1)/idSegment) * idSegment
	return lo + searchIDs(b.ids[lo:min(lo+idSegment, len(b.ids))], id)
}

// summarize refreshes the summary from position i on (after a change there).
func (b *idBlock[V]) summarize(i int) {
	for j := i / idSegment; j < idSegments; j++ {
		if k := j * idSegment; k < len(b.ids) {
			b.keys[j] = b.ids[k]
		} else {
			b.keys[j] = math.MaxInt64 // past the end: never <= an ID below it
		}
	}
}

// searchIDs returns the first index whose ID is >= id (len(ids) when none).
func searchIDs(ids []snowflake.ID, id snowflake.ID) int {
	lo, n := 0, len(ids)
	for n > 0 {
		half := n / 2
		if ids[lo+half] < id {
			lo += half + 1
			n -= half + 1
		} else {
			n = half
		}
	}
	return lo
}

// searchIDsAbove returns the first index whose ID is > id (len(ids) when none).
func searchIDsAbove(ids []snowflake.ID, id snowflake.ID) int {
	lo, n := 0, len(ids)
	for n > 0 {
		half := n / 2
		if ids[lo+half] <= id {
			lo += half + 1
			n -= half + 1
		} else {
			n = half
		}
	}
	return lo
}

// Len is the number of IDs in the table.
func (t *idTable[V]) Len() int { return t.n }

// blockFor returns the last block whose first ID is <= id, or block 0 when id
// is below every first ID. The table is not empty.
func (t *idTable[V]) blockFor(id snowflake.ID) int {
	lo, hi := 0, len(t.firsts)-1 // the answer is in [lo, hi]
	if len(t.dir) > 0 && id >= t.dirLo {
		k := min(uint64(id-t.dirLo)>>t.dirShift, uint64(len(t.dir)-2)) // #nosec G115 -- id >= dirLo
		lo, hi = int(t.dir[k]), int(t.dir[k+1])
	}
	if lo == hi {
		return lo
	}
	// The first block in (lo, hi] whose first ID is above id, minus one.
	return lo + searchIDsAbove(t.firsts[lo+1:hi+1], id)
}

// locate returns the block that holds id or would take it (-1 on an empty
// table) and id's position in that block.
func (t *idTable[V]) locate(id snowflake.ID) (bi, i int, found bool) {
	if len(t.blocks) == 0 {
		return -1, 0, false
	}
	bi = t.blockFor(id)
	b := t.blocks[bi]
	i = b.search(id)
	return bi, i, i < len(b.ids) && b.ids[i] == id
}

// get returns id's top and payload.
func (t *idTable[V]) get(id snowflake.ID) (top uint32, v V, ok bool) {
	bi, i, found := t.locate(id)
	if !found {
		return 0, v, false
	}
	b := t.blocks[bi]
	return b.tops[i], b.vals[i], true
}

// contains reports whether id is in the table.
func (t *idTable[V]) contains(id snowflake.ID) bool {
	_, _, found := t.locate(id)
	return found
}

// put sets id's top and payload, inserting id when absent.
func (t *idTable[V]) put(id snowflake.ID, top uint32, v V) {
	bi, i, found := t.locate(id)
	if found {
		b := t.blocks[bi]
		b.tops[i], b.vals[i] = top, v
		return
	}
	t.n++
	if bi < 0 {
		b := newIDBlock[V](idBlockMinCap)
		b.insertAt(0, id, top, v)
		t.insertBlock(0, b)
		return
	}
	b := t.blocks[bi]
	if len(b.ids) == idTableBlock {
		switch {
		case i == idTableBlock && bi+1 < len(t.blocks) && len(t.blocks[bi+1].ids) < idTableBlock:
			// Past b's end, below the next block's first: the next block's
			// front.
			bi, b, i = bi+1, t.blocks[bi+1], 0
		case i == idTableBlock:
			// Past b's end with no room after it: a new block (ascending
			// inserts keep the blocks behind them full).
			nb := newIDBlock[V](idBlockMinCap)
			nb.insertAt(0, id, top, v)
			t.insertBlock(bi+1, nb)
			return
		default:
			const half = idTableBlock / 2
			nb := newIDBlock[V](half)
			nb.ids = append(nb.ids, b.ids[half:]...)
			nb.tops = append(nb.tops, b.tops[half:]...)
			nb.vals = append(nb.vals, b.vals[half:]...)
			clear(b.vals[half:])
			b.ids, b.tops, b.vals = b.ids[:half], b.tops[:half], b.vals[:half]
			b.summarize(half)
			nb.summarize(0)
			t.insertBlock(bi+1, nb)
			if i > half {
				bi, b, i = bi+1, nb, i-half
			}
		}
	}
	b.insertAt(i, id, top, v)
	if i == 0 {
		t.firsts[bi] = id
		t.reindex()
	}
}

// remove deletes id; false when it is absent.
func (t *idTable[V]) remove(id snowflake.ID) bool {
	bi, i, found := t.locate(id)
	if !found {
		return false
	}
	t.n--
	b := t.blocks[bi]
	if len(b.ids) == 1 {
		t.firsts = slices.Delete(t.firsts, bi, bi+1)
		t.blocks = slices.Delete(t.blocks, bi, bi+1)
		t.reindex()
		return true
	}
	b.ids = slices.Delete(b.ids, i, i+1)
	b.tops = slices.Delete(b.tops, i, i+1)
	b.vals = slices.Delete(b.vals, i, i+1)
	b.summarize(i)
	if c := cap(b.ids); c > idBlockMinCap && len(b.ids) <= c/4 {
		b.resize(max(2*len(b.ids), idBlockMinCap))
	}
	if i == 0 {
		t.firsts[bi] = b.ids[0]
		t.reindex()
	}
	return true
}

func (t *idTable[V]) insertBlock(bi int, b *idBlock[V]) {
	t.firsts = slices.Insert(t.firsts, bi, b.ids[0])
	t.blocks = slices.Insert(t.blocks, bi, b)
	t.reindex()
}

// reindex rebuilds the directory over the first IDs: about
// idDirBucketsPerBlock buckets per block over [firsts[0], firsts[last]], the
// bucket width a power of two.
func (t *idTable[V]) reindex() {
	nb := len(t.firsts)
	if nb < idDirMinBlocks {
		t.dir = t.dir[:0]
		return
	}
	lo := t.firsts[0]
	span := uint64(t.firsts[nb-1] - lo) // #nosec G115 -- firsts ascend
	shift := uint(0)
	for span>>shift >= uint64(idDirBucketsPerBlock*nb) {
		shift++
	}
	buckets := int(span>>shift) + 1 // #nosec G115 -- below idDirBucketsPerBlock*nb
	if cap(t.dir) < buckets+1 || cap(t.dir) > 4*(buckets+1) {
		t.dir = make([]int32, buckets+1, buckets+1+buckets/4)
	}
	t.dir = t.dir[:buckets+1]
	b := 0
	for k := 0; k < buckets; k++ {
		start := lo + snowflake.ID(uint64(k)<<shift) // #nosec G115 -- within span
		for b+1 < nb && t.firsts[b+1] <= start {
			b++
		}
		t.dir[k] = int32(b) // #nosec G115 -- block counts fit int32
	}
	t.dir[buckets] = int32(nb - 1) // #nosec G115 -- block counts fit int32
	t.dirLo, t.dirShift = lo, shift
}

// appendAscending adds id above every ID in the table (a build streams its
// sorted scan through it); blocks fill to idTableBlock.
func (t *idTable[V]) appendAscending(id snowflake.ID, top uint32, v V) {
	if len(t.blocks) == 0 {
		t.put(id, top, v)
		return
	}
	last := t.blocks[len(t.blocks)-1]
	switch {
	case last.ids[len(last.ids)-1] >= id:
		t.put(id, top, v) // not above the last ID: an ordinary insert
	case len(last.ids) == idTableBlock:
		t.n++
		nb := newIDBlock[V](idBlockMinCap)
		nb.insertAt(0, id, top, v)
		t.insertBlock(len(t.blocks), nb)
	default:
		t.n++
		last.insertAt(len(last.ids), id, top, v)
	}
}

// appendIDs appends every ID in ascending order.
func (t *idTable[V]) appendIDs(dst []snowflake.ID) []snowflake.ID {
	for _, b := range t.blocks {
		dst = append(dst, b.ids...)
	}
	return dst
}
