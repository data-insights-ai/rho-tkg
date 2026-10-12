package index

import (
	"cmp"
	"math"
	"slices"
)

// veKey is one entry of a keyOrderedIDs: an instant and the entity it keys.
type veKey struct{ k, id int64 }

func cmpVEKey(a, b veKey) int {
	if c := cmp.Compare(a.k, b.k); c != 0 {
		return c
	}
	return cmp.Compare(a.id, b.id)
}

// veChunkMax bounds one chunk of a keyOrderedIDs: an insert or remove moves
// at most this many 16-byte entries.
const veChunkMax = 256

// keyOrderedIDs is a set of (instant, entity) pairs ordered by instant, then
// entity: sorted chunks of at most veChunkMax entries, the chunks in order.
// Insert and remove cost O(log n + veChunkMax); iterating the entries at or
// above an instant costs O(log n) plus the entries visited. 16 bytes per
// entry plus the chunks' spare capacity. Not safe for concurrent use.
type keyOrderedIDs struct {
	chunks [][]veKey
}

// chunkFor returns the index of the first chunk whose last entry is at or
// above x (len(chunks) when none is).
func (s *keyOrderedIDs) chunkFor(x veKey) int {
	i, _ := slices.BinarySearchFunc(s.chunks, x, func(c []veKey, x veKey) int { return cmpVEKey(c[len(c)-1], x) })
	return i
}

func (s *keyOrderedIDs) insert(x veKey) {
	if len(s.chunks) == 0 {
		s.chunks = append(s.chunks, []veKey{x})
		return
	}
	i := s.chunkFor(x)
	if i == len(s.chunks) {
		i-- // above every entry: the last chunk
	}
	c := s.chunks[i]
	j, found := slices.BinarySearchFunc(c, x, cmpVEKey)
	if found {
		return
	}
	c = slices.Insert(c, j, x)
	if len(c) <= veChunkMax {
		s.chunks[i] = c
		return
	}
	half := len(c) / 2
	lo := slices.Clone(c[:half])
	hi := slices.Clone(c[half:])
	s.chunks[i] = lo
	s.chunks = slices.Insert(s.chunks, i+1, hi)
}

func (s *keyOrderedIDs) remove(x veKey) {
	i := s.chunkFor(x)
	if i == len(s.chunks) {
		return
	}
	c := s.chunks[i]
	j, found := slices.BinarySearchFunc(c, x, cmpVEKey)
	if !found {
		return
	}
	c = slices.Delete(c, j, j+1)
	if len(c) == 0 {
		s.chunks = slices.Delete(s.chunks, i, i+1)
		return
	}
	s.chunks[i] = c
}

// ascendFrom calls fn for every entry with instant at or above k, ascending;
// fn returning false stops.
func (s *keyOrderedIDs) ascendFrom(k int64, fn func(veKey) bool) {
	start := veKey{k: k, id: math.MinInt64}
	i := s.chunkFor(start)
	if i == len(s.chunks) {
		return
	}
	j, _ := slices.BinarySearchFunc(s.chunks[i], start, cmpVEKey)
	for ; i < len(s.chunks); i, j = i+1, 0 {
		for _, e := range s.chunks[i][j:] {
			if !fn(e) {
				return
			}
		}
	}
}

// len returns the number of entries.
func (s *keyOrderedIDs) len() int {
	n := 0
	for _, c := range s.chunks {
		n += len(c)
	}
	return n
}

// capBytes returns the bytes the chunks hold (capacity, 16 per entry slot).
func (s *keyOrderedIDs) capBytes() int {
	n := 0
	for _, c := range s.chunks {
		n += 16*cap(c) + 24
	}
	return n
}
