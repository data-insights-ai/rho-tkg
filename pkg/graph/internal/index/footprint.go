package index

import (
	"cmp"
	"reflect"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Footprint is what a property index or membership sidecar holds in RAM
// (IDXSTATS): its distinct values, its (entity, value) postings and an
// estimate of its bytes. It walks every value: the caller holds the owner's
// lock, at least shared.
type Footprint struct {
	Values   int64
	Postings int64
	Bytes    int64
}

// mapSlotBytes estimates one entry of a Go map whose keys take k and values v
// bytes: the slot and its control byte at the table's average load. A table
// grows at 7/8 full and doubles, so its load lies between 7/16 and 7/8: two
// thirds on average, half again the slot.
func mapSlotBytes(k, v uintptr) int64 {
	return int64(k+v+1) * 3 / 2 // #nosec G115 -- a type's size, far below 2^63
}

// allocBytes is the heap a small allocation of n bytes takes: its Go size
// class (runtime/sizeclasses.go) up to 1 KB, beyond it n (a posting chunk's
// power-of-two capacity is a class of its own).
func allocBytes(n int64) int64 {
	for _, c := range sizeClasses {
		if n <= c {
			return c
		}
	}
	return n
}

var sizeClasses = []int64{0, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320,
	352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024}

// mapBytes estimates a whole map of n entries: a small one (up to a group of
// eight slots) takes its header and that group whatever n, a larger one its
// header, a table and its directory pointer per table, and its slots
// (mapSlotBytes).
func mapBytes(n int, k, v uintptr) int64 {
	if n == 0 {
		return 0
	}
	if n <= mapGroupSlots {
		return allocBytes(mapHeaderBytes) + allocBytes(int64(mapGroupSlots*(k+v)+mapGroupSlots)) // #nosec G115 -- a type's size
	}
	tables := int64(1 + n/mapTableEntries)
	return allocBytes(mapHeaderBytes) + tables*mapTableBytes + int64(n)*mapSlotBytes(k, v)
}

// A map's header (internal/runtime/maps.Map); a group's slots, the least a
// map with entries allocates; a table's struct and its directory pointer (two
// size classes of their own), one per up to 1024 slots, which hold about 680
// entries at the average load.
const (
	mapHeaderBytes  = 48
	mapGroupSlots   = 8
	mapTableBytes   = 32 + 8
	mapTableEntries = 1024 * 2 / 3
)

// sizeOf is T's size in bytes.
func sizeOf[T any]() uintptr { return reflect.TypeFor[T]().Size() }

// sliceHeaderBytes is a slice header's size.
var sliceHeaderBytes = sizeOf[[]byte]()

// chunksBytes estimates s's chunks at capacity, their directory and, in
// counted mode, the counts (not s's own header: its owner's; nor the counted
// mode's per-chunk sums, 16 B for 256 keys or more).
func chunksBytes[T cmp.Ordered](s *sortedChunks[T]) int64 {
	elem := int64(sizeOf[T]())                          // #nosec G115 -- a type's size
	b := int64(cap(s.chunks)) * int64(sliceHeaderBytes) // #nosec G115 -- a type's size
	for _, c := range s.chunks {
		b += allocBytes(int64(cap(c)) * elem)
	}
	if cc := s.chunkCounts; cc != nil {
		b += int64(cap(cc.cnt)) * int64(sliceHeaderBytes) // #nosec G115 -- a type's size
		for _, c := range cc.cnt {
			b += int64(cap(c)) * 8
		}
	}
	return b
}

// postingsBytes estimates a posting list: its header and its chunks.
func postingsBytes(p *IDPostings) int64 {
	if p == nil {
		return 0
	}
	return allocBytes(int64(sizeOf[IDPostings]())) + chunksBytes(&p.s) // #nosec G115 -- a type's size
}

// Footprint reports the index's values, postings and bytes: the value map,
// each value's posting list, and the ordered views (the numeric buckets hold
// posting lists of their own). An index kept on disk holds none of them.
func (pi *PropertyIndex) Footprint() Footprint {
	var f Footprint
	if pi == nil {
		return f
	}
	slot := mapSlotBytes(sizeOf[string](), sizeOf[*IDPostings]())
	for vk, p := range pi.Entries {
		f.Values++
		f.Postings += int64(p.Len())
		f.Bytes += slot + allocBytes(int64(len(vk))) + postingsBytes(p)
	}
	bucket := mapSlotBytes(sizeOf[float64](), sizeOf[*IDPostings]())
	for _, p := range pi.numBuckets {
		f.Bytes += bucket + postingsBytes(p)
	}
	f.Bytes += chunksBytes(&pi.numKeys) + chunksBytes(&pi.strKeys)
	return f
}

// Footprint reports the composite's key tuples, postings and bytes: a tuple
// held by one entity is a slot of its own map, a tuple held by more a posting
// list.
func (ci *CompositePropertyIndex) Footprint() Footprint {
	var f Footprint
	if ci == nil {
		return f
	}
	one := mapSlotBytes(sizeOf[string](), sizeOf[snowflake.ID]())
	for k := range ci.one {
		f.Values++
		f.Postings++
		f.Bytes += one + allocBytes(int64(len(k)))
	}
	many := mapSlotBytes(sizeOf[string](), sizeOf[*IDPostings]())
	for k, p := range ci.many {
		f.Values++
		f.Postings += int64(p.Len())
		f.Bytes += many + allocBytes(int64(len(k))) + postingsBytes(p)
	}
	return f
}

// Footprint reports the sidecar's values, postings and bytes: each value's
// first posting inline, the further ones in a map of their own, and the
// ordered member lists a paged lookup opened. The windowed and numeric views
// (opened by those lookups) are not counted.
func (m *PropertyTxMembers[ID]) Footprint() Footprint {
	var f Footprint
	if m == nil {
		return f
	}
	slot := mapSlotBytes(sizeOf[string](), sizeOf[txPostings[ID]]())
	for vk, p := range m.values {
		f.Values++
		f.Bytes += slot + allocBytes(int64(len(vk)))
		f.Bytes += mapBytes(len(p.more), sizeOf[ID](), sizeOf[txPosting]())
	}
	f.Postings = m.postings
	ordered := mapSlotBytes(sizeOf[string](), sizeOf[*sortedChunks[ID]]())
	for _, s := range m.ordered {
		f.Bytes += ordered + int64(sizeOf[sortedChunks[ID]]()) + chunksBytes(s) // #nosec G115 -- a type's size
	}
	return f
}
