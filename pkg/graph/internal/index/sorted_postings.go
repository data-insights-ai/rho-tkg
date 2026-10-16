package index

import (
	"iter"
	"slices"

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

// PostingsBuild collects an index build's (value key, ID) pairs in any order
// — an index backfill reads rows in label-snapshot (map) order — and hands each
// key's IDs back sorted and distinct, so the live index is filled in ascending
// ID order: every posting list then grows by appends into exact 512-ID chunks
// (sortedChunks.add) instead of out-of-order inserts. Not safe for concurrent
// use.
type PostingsBuild struct {
	keys     map[string][]snowflake.ID
	elements bool
}

// NewPostingsBuild returns an empty build; elements selects per-element keys
// for list values, as the index being built (PropertyIndex.Elements).
func NewPostingsBuild(elements bool) *PostingsBuild {
	return &PostingsBuild{keys: make(map[string][]snowflake.ID), elements: elements}
}

// AddKey records id under the value key vk (ignored when vk is empty).
func (b *PostingsBuild) AddKey(id snowflake.ID, vk string) {
	if vk != "" {
		b.keys[vk] = append(b.keys[vk], id)
	}
}

// AddEntityKeys records one entity's keys for propertyKey as
// PropertyIndex.AddEntityKeys indexes them.
func (b *PostingsBuild) AddEntityKeys(id snowflake.ID, propertyKey string, keys func(propertyKey string, elements bool, fn func(valueKey string) bool)) {
	keys(propertyKey, b.elements, func(vk string) bool {
		b.AddKey(id, vk)
		return true
	})
}

// Range calls fn with each value key and its IDs, ascending and distinct
// (sorted in place; fn must not keep ids), until fn returns false.
func (b *PostingsBuild) Range(fn func(vk string, ids []snowflake.ID) bool) {
	for vk, ids := range b.keys {
		slices.Sort(ids)
		if !fn(vk, slices.Compact(ids)) {
			return
		}
	}
}

// FinishBuild merges the backfill b into the live index pi and ends its build
// (Mutated = nil). An ID a concurrent write handled during the build (in
// Mutated when the merge starts) or that alive rejects is skipped. Tracking
// ends BEFORE the merge because AddKey marks every ID it adds while tracking:
// it would skip an entity's second key, so an Elements index built over
// existing lists kept one element per row (WP-31). alive's error stops the
// merge and is returned with tracking restored (the build stays unfinished).
// Caller holds the index's write lock.
func (pi *PropertyIndex) FinishBuild(b *PostingsBuild, alive func(snowflake.ID) (bool, error)) error {
	handled := pi.Mutated
	pi.Mutated = nil
	var err error
	b.Range(func(vk string, ids []snowflake.ID) bool {
		for _, id := range ids {
			if _, mutated := handled[id]; mutated {
				continue // concurrent write handled this ID during the build
			}
			ok, aerr := alive(id)
			if aerr != nil {
				err = aerr
				return false
			}
			if ok {
				// AddKey, not a direct Entries write: it keeps the ordered views.
				pi.AddKey(id, vk)
			}
		}
		return true
	})
	if err != nil {
		pi.Mutated = handled
		return err
	}
	return nil
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
