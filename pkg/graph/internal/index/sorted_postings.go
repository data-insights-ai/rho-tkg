package index

import (
	"iter"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// IDPostings is the posting list of one equality index key: the IDs of the
// entities holding it, kept sorted ascending at write time in a sortedChunks
// set (the structure of the ordered views and of the backlog-69 member pages).
// It serves every ID set of the property, rel property and composite indexes
// (Entries) and the numeric view's buckets (backlog 72), so a keyset page
// (After, Limit) seeks its cursor in O(log n) and reads only its page: no copy
// and no sort of the key per call.
//
// Cost: 8 B per member in exact 512-ID chunks when IDs arrive ascending (the
// snowflake write path and a rebuild from sorted IDs); chunks split and merge
// under out-of-order writes as the ordered views' do. A write is O(log n) plus
// a move within one chunk of at most 1,024 IDs.
//
// Not safe for concurrent use: the owning store guards it with its index lock.
// A nil *IDPostings reads as empty.
type IDPostings struct {
	s sortedChunks[snowflake.ID]
}

// Len returns the number of IDs held.
func (p *IDPostings) Len() int {
	if p == nil {
		return 0
	}
	return p.s.n
}

// Has reports whether id is held.
func (p *IDPostings) Has(id snowflake.ID) bool {
	return p != nil && p.s.has(id)
}

// add inserts id and reports whether it was absent.
func (p *IDPostings) add(id snowflake.ID) bool {
	return p.s.put(id)
}

// remove deletes id and reports whether it was held.
func (p *IDPostings) remove(id snowflake.ID) bool {
	return p.s.remove(id)
}

// All yields every ID ascending.
func (p *IDPostings) All() iter.Seq[snowflake.ID] {
	return p.After(0)
}

// After yields the IDs above after ascending (every ID when after is 0, IDs
// being positive): one seek, then the IDs in order until the caller stops.
func (p *IDPostings) After(after snowflake.ID) iter.Seq[snowflake.ID] {
	return func(yield func(snowflake.ID) bool) {
		if p == nil {
			return
		}
		p.s.forEachFrom(after, func(id snowflake.ID) bool {
			return id == after || yield(id)
		})
	}
}

// AppendAfter appends to dst at most limit IDs above after, ascending (every
// one when limit <= 0), and returns dst: the copy a caller takes under the
// index lock before reading rows without it.
func (p *IDPostings) AppendAfter(dst []snowflake.ID, after snowflake.ID, limit int) []snowflake.ID {
	n := 0
	for id := range p.After(after) {
		dst = append(dst, id)
		if n++; limit > 0 && n >= limit {
			break
		}
	}
	return dst
}
