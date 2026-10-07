package index

import (
	"cmp"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rowIndex finds an entity's row in a snapshot's sorted ID vector by hashing
// its ID instead of binary-searching the vector: one probe sequence over a
// table of row positions (open addressing, linear probing, load ≤ 0.5), so a
// point lookup touches one or two cache lines instead of log2(n). It is built
// lazily on the snapshot's first point lookup — a snapshot read only by scans
// never pays for it — and, like the snapshot, is immutable once built.
// Memory: 4 bytes per slot, 8 to 16 bytes per row.
type rowIndex struct {
	once  sync.Once
	slots []int32 // row+1; 0 = empty
	mask  uint64
}

// rowIndexMinRows is the size below which a lookup binary-searches: a short
// vector fits in a cache line or two and needs no table.
const rowIndexMinRows = 64

// hashRowID mixes a snowflake ID's bits (the splitmix64 finalizer): IDs share
// their high timestamp bits and step by small amounts, so the low bits alone
// would cluster.
func hashRowID(v uint64) uint64 {
	v ^= v >> 30
	v *= 0xbf58476d1ce4e5b9
	v ^= v >> 27
	v *= 0x94d049bb133111eb
	v ^= v >> 31
	return v
}

func (x *rowIndex) build(ids func(int) uint64, n int) {
	size := uint64(1)
	for size < uint64(n)*2 { // #nosec G115 -- n is a row count, ≥ 0
		size <<= 1
	}
	x.slots = make([]int32, size)
	x.mask = size - 1
	for row := range n {
		h := hashRowID(ids(row)) & x.mask
		for x.slots[h] != 0 {
			h = (h + 1) & x.mask
		}
		x.slots[h] = int32(row + 1) // #nosec G115 -- n ≤ MaxDocValuesNodes (10M) fits int32
	}
}

// lookup returns the ordinal of id, building the table on first use, or
// (-1, false) if id is not a member. Equivalent to a binary search with the
// comparator BuildDocValues sorted by (TestRowIndexAgreesWithBinarySearch).
func (l *DocValues[T]) lookup(id T) (int, bool) {
	if len(l.ids) < rowIndexMinRows {
		return slices.BinarySearchFunc(l.ids, id, func(a, target T) int {
			return cmp.Compare(a.SnowflakeID(), target.SnowflakeID())
		})
	}
	l.rows.once.Do(func() {
		l.rows.build(func(i int) uint64 { return uint64(l.ids[i].SnowflakeID()) }, len(l.ids)) // #nosec G115 -- bit pattern of a positive ID
	})
	want := id.SnowflakeID()
	h := hashRowID(uint64(want)) & l.rows.mask // #nosec G115 -- bit pattern
	for {
		slot := l.rows.slots[h]
		if slot == 0 {
			return -1, false
		}
		if l.ids[slot-1].SnowflakeID() == want {
			return int(slot - 1), true
		}
		h = (h + 1) & l.rows.mask
	}
}

var _ types.NodeColumnRowReader = (*PointSnapshot[types.NodeID])(nil)
