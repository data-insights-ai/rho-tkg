package badger

// revTable holds the write revisions of a store's entities in a fixed number
// of stripes (EXISTMAP: a map of one revision per entity cost ~39 B per node).
// A reader captures an entity's revision, reads its row without the lock, and
// compares the revision under the lock before it trusts the row (prefetch
// handoff, fillNodeHash). Every write and delete of an entity bumps its stripe
// to a value never seen before (next only grows), so a changed entity never
// compares equal; an entity sharing the stripe of a written one compares
// unequal too, which only forces the locked re-read. 0 means "unknown" and
// never compares current; the stripes start at 1, so an entity loaded at open
// has a usable revision without a per-entity write. Callers hold idxMu: read
// for get, write for bump and bumpAll. The zero value answers 0 until the first
// bump.
type revTable struct {
	stripes []uint64
	next    uint64
}

const revStripeBits = 14 // 16 K stripes, 128 KB per table

func newRevTable() revTable {
	t := revTable{stripes: make([]uint64, 1<<revStripeBits), next: 1}
	for i := range t.stripes {
		t.stripes[i] = 1
	}
	return t
}

func revStripe(id int64) uint64 {
	return (uint64(id) * 0x9E3779B97F4A7C15) >> (64 - revStripeBits) // #nosec G115 — a hash, not a conversion of meaning
}

// get returns id's revision; 0 before the table is made.
func (t *revTable) get(id int64) uint64 {
	if t.stripes == nil {
		return 0
	}
	return t.stripes[revStripe(id)]
}

// bump gives id's stripe a new revision.
func (t *revTable) bump(id int64) {
	if t.stripes == nil {
		*t = newRevTable()
	}
	t.next++
	t.stripes[revStripe(id)] = t.next
}

// bumpAll gives every stripe a new revision (Clear): no revision captured
// before matches afterwards.
func (t *revTable) bumpAll() {
	if t.stripes == nil {
		*t = newRevTable()
	}
	t.next++
	for i := range t.stripes {
		t.stripes[i] = t.next
	}
}
