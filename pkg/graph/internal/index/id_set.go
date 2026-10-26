package index

import (
	"iter"
	"math"
)

// IDSet is a set of entity IDs kept as sorted chunks of int64 (EXISTMAP): about
// 8 B per ID where a Go map of IDs costs 30 B or more, and an ascending stream
// of snowflake IDs fills exact chunks. Membership is O(log chunks + log chunk
// size). The zero value is empty and ready. Not safe for concurrent use: reads
// may run concurrently with each other, a write needs the caller's exclusive
// lock, and fn passed to Ascend must not change the set.
type IDSet struct {
	s sortedChunks[int64]
}

// NewIDSetFrom builds a set from DISTINCT ascending IDs in exact chunks.
func NewIDSetFrom(sorted []int64) IDSet {
	return IDSet{s: *newSortedChunksFrom(sorted)}
}

// Has reports whether id is in the set.
func (s *IDSet) Has(id int64) bool { return s.s.has(id) }

// Add adds id and reports whether it was absent.
func (s *IDSet) Add(id int64) bool { return s.s.put(id) }

// Remove removes id and reports whether it was present.
func (s *IDSet) Remove(id int64) bool { return s.s.remove(id) }

// Len returns the number of IDs.
func (s *IDSet) Len() int { return s.s.n }

// Clone returns a copy in exact chunks.
func (s *IDSet) Clone() IDSet {
	ids := make([]int64, 0, s.Len())
	s.Ascend(func(id int64) bool { ids = append(ids, id); return true })
	return NewIDSetFrom(ids)
}

// All yields every ID in ascending order; the loop body must not change the set.
func (s *IDSet) All() iter.Seq[int64] {
	return func(yield func(int64) bool) { s.Ascend(yield) }
}

// Ascend calls fn for every ID in ascending order until fn returns false.
func (s *IDSet) Ascend(fn func(id int64) bool) { s.s.forEachFrom(math.MinInt64, fn) }
