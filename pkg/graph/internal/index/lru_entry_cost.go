package index

import (
	"container/list"
	"math/bits"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// entryOverhead is the cache's own heap cost of one entry beyond its value:
// the list.Element and the Entry, each at its allocator size class (RHO-1).
// The maps' slots are charged apart, by cacheMapBytes, at the maps' growth.
func entryOverhead[V any]() int64 {
	return allocBytes(int64(sizeOf[list.Element]())) + allocBytes(int64(sizeOf[Entry[V]]())) // #nosec G115 -- a type's size
}

// Go's swiss map layout for the cache's map[snowflake.ID]*list.Element: a
// group holds 8 slots and 8 control bytes; a table holds at most 1,024 slots,
// doubles (or, at that cap, splits) once 7/8 of them are full and keeps them.
var (
	cacheGroupBytes = int64(mapGroupSlots) * (1 + int64(sizeOf[snowflake.ID]()+sizeOf[*list.Element]())) // #nosec G115 -- a type's size
	cacheTableBytes = largeAllocBytes(1024 / mapGroupSlots * cacheGroupBytes)                            // 17,408 B from the 18,432 B class
)

// cacheMapBytes is the heap of a cache map that has grown to peak entries
// (or was made with that hint): the slots of its largest size, since a Go map
// never gives them back. Tables split one by one, so near a power-of-two
// boundary the real count lies up to twice this; elsewhere it matches.
func cacheMapBytes(peak int) int64 {
	if peak == 0 {
		return allocBytes(mapHeaderBytes) // the group comes with the first entry
	}
	if peak <= mapGroupSlots {
		return allocBytes(mapHeaderBytes) + allocBytes(cacheGroupBytes)
	}
	slots := nextPow2((peak*8 + 6) / 7)
	if slots <= 1024 {
		return allocBytes(mapHeaderBytes) + largeAllocBytes(int64(slots/mapGroupSlots)*cacheGroupBytes)
	}
	tables := int64(slots / 1024)
	return allocBytes(mapHeaderBytes) + tables*(cacheTableBytes+mapTableBytes)
}

// largeAllocBytes is the heap an allocation of n bytes takes: its small size
// class up to 1 KiB, then rounded to 2 KiB up to 32 KiB (the classes there
// are at most that far apart), then to whole pages.
func largeAllocBytes(n int64) int64 {
	switch {
	case n <= 1024:
		return allocBytes(n)
	case n <= 32<<10:
		return (n + 2047) &^ 2047
	default:
		return (n + 8191) &^ 8191
	}
}

func nextPow2(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1)) // #nosec G115 -- n > 1
}
