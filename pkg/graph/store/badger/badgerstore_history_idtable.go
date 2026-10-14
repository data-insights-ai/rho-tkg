package badger

import (
	"math/bits"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// idTable is the RAM representation of the history-presence set and the
// history-stamps sidecar (backlog 41): per ID a top version and a payload V,
// in an open-addressing hash table with linear probing and columnar slots
// (keys, tops, vals; key 0 marks a free slot, IDs are positive). A Go map
// pays a padded slot plus a control byte per slot (presence 16+1 B, stamps
// 32+1 B); the columns pay 12 B (presence: V is struct{} and takes no room)
// and 28 B (stamps) per slot. The table doubles before it is more than
// idTableMaxLoad full; a delete shifts the following run back (no
// tombstones). The zero value is an empty table. Not safe for concurrent
// use: the owner's mutex guards it.
type idTable[V any] struct {
	keys  []snowflake.ID
	tops  []uint32
	vals  []V
	n     int
	shift uint // 64 - log2(len(keys))
}

// idTableMaxLoad bounds n/len(keys) (numerator / denominator).
const (
	idTableMaxLoadNum = 4
	idTableMaxLoadDen = 5
	idTableMinSlots   = 16
)

// newIDTable returns a table sized for n IDs without growing.
func newIDTable[V any](n int) idTable[V] {
	var t idTable[V]
	if n > 0 {
		t.resize(slotsFor(n))
	}
	return t
}

// slotsFor is the power-of-two slot count that holds n IDs within the load
// bound.
func slotsFor(n int) int {
	slots := idTableMinSlots
	for n*idTableMaxLoadDen > slots*idTableMaxLoadNum {
		slots *= 2
	}
	return slots
}

// home is id's first slot: Fibonacci hashing, the product's top bits (the
// low bits repeat for IDs a power of two apart).
func (t *idTable[V]) home(id snowflake.ID) int {
	return int((uint64(id) * 0x9E3779B97F4A7C15) >> t.shift) // #nosec G115 -- below len(keys)
}

// slot returns id's slot, or the free slot where it would go (-1: no slots).
func (t *idTable[V]) slot(id snowflake.ID) (int, bool) {
	if len(t.keys) == 0 {
		return -1, false
	}
	mask := len(t.keys) - 1
	for i := t.home(id); ; i = (i + 1) & mask {
		switch t.keys[i] {
		case id:
			return i, true
		case 0:
			return i, false
		}
	}
}

// Len is the number of IDs in the table.
func (t *idTable[V]) Len() int { return t.n }

// get returns id's top and payload.
func (t *idTable[V]) get(id snowflake.ID) (top uint32, v V, ok bool) {
	i, found := t.slot(id)
	if !found {
		return 0, v, false
	}
	return t.tops[i], t.vals[i], true
}

// contains reports whether id is in the table.
func (t *idTable[V]) contains(id snowflake.ID) bool {
	_, found := t.slot(id)
	return found
}

// put sets id's top and payload, inserting id when absent.
func (t *idTable[V]) put(id snowflake.ID, top uint32, v V) {
	i, found := t.slot(id)
	if !found {
		if (t.n+1)*idTableMaxLoadDen > len(t.keys)*idTableMaxLoadNum {
			t.resize(max(2*len(t.keys), idTableMinSlots))
			i, _ = t.slot(id)
		}
		t.keys[i] = id
		t.n++
	}
	t.tops[i], t.vals[i] = top, v
}

// remove deletes id (false when absent) and shifts back the entries of the
// probe run behind it that its slot now lets nearer their home.
func (t *idTable[V]) remove(id snowflake.ID) bool {
	i, found := t.slot(id)
	if !found {
		return false
	}
	mask := len(t.keys) - 1
	for j := (i + 1) & mask; t.keys[j] != 0; j = (j + 1) & mask {
		// The entry at j may fill the hole at i unless its home lies
		// cyclically in (i, j].
		if h := t.home(t.keys[j]); (j-h)&mask >= (j-i)&mask {
			t.keys[i], t.tops[i], t.vals[i] = t.keys[j], t.tops[j], t.vals[j]
			i = j
		}
	}
	var zero V
	t.keys[i], t.tops[i], t.vals[i] = 0, 0, zero
	t.n--
	return true
}

// resize rehashes every entry into slots slots.
func (t *idTable[V]) resize(slots int) {
	old := *t
	t.keys = make([]snowflake.ID, slots)
	t.tops = make([]uint32, slots)
	t.vals = make([]V, slots)
	t.shift = uint(64 - bits.TrailingZeros(uint(slots)))
	for i, id := range old.keys {
		if id != 0 {
			j, _ := t.slot(id)
			t.keys[j], t.tops[j], t.vals[j] = id, old.tops[i], old.vals[i]
		}
	}
}

// forEach calls fn for every entry, in no particular order.
func (t *idTable[V]) forEach(fn func(id snowflake.ID, top uint32, v V)) {
	for i, id := range t.keys {
		if id != 0 {
			fn(id, t.tops[i], t.vals[i])
		}
	}
}

// appendIDs appends every ID, in no particular order.
func (t *idTable[V]) appendIDs(dst []snowflake.ID) []snowflake.ID {
	for _, id := range t.keys {
		if id != 0 {
			dst = append(dst, id)
		}
	}
	return dst
}
