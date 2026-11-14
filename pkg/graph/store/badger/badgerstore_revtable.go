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
	flat    bool // no stripes made, every one still 1 (newFlatRevTable)
}

const revStripeBits = 14 // 16 K stripes, 128 KB per table

func newRevTable() revTable {
	t := revTable{stripes: make([]uint64, 1<<revStripeBits), next: 1}
	for i := range t.stripes {
		t.stripes[i] = 1
	}
	return t
}

// newFlatRevTable is newRevTable without the 128 KB: every stripe is still 1, so
// get answers 1 and the first bump or bumpAll makes the table from the state
// newRevTable starts in. A store that never writes (a transient read-only open,
// Config.TransientRead) keeps it for good.
func newFlatRevTable() revTable { return revTable{next: 1, flat: true} }

// newRevTableFor is the table a store opens with: the flat one for a transient
// read-only open, which never writes, the made one otherwise.
func newRevTableFor(cfg Config) revTable {
	if cfg.TransientRead && cfg.ReadOnly {
		return newFlatRevTable()
	}
	return newRevTable()
}

func revStripe(id int64) uint64 {
	return (uint64(id) * 0x9E3779B97F4A7C15) >> (64 - revStripeBits) // #nosec G115 — a hash, not a conversion of meaning
}

// get returns id's revision; 0 before the table is made, 1 while a flat table
// is (an untouched stripe).
func (t *revTable) get(id int64) uint64 {
	if t.stripes == nil {
		if t.flat {
			return 1
		}
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
