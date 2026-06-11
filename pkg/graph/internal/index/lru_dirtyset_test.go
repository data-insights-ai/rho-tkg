package index

import (
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestCache_DirtySetMatchesFullScan pins the dirty-index invariant behind
// the O(dirty) CollectDirty (pre-fix it walked the whole LRU under c.mu per
// 100ms flush cycle — ingestion stalled at multi-million capacities):
// after ANY operation sequence, CollectDirty must return exactly the
// entries a full-list scan finds dirty. 2,000 random ops across Put /
// MarkDeleted / LoadClean / Get / MarkFlushed / EvictForTest with a small
// capacity so eviction churns continuously.
func TestCache_DirtySetMatchesFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(99)) //nolint:gosec // deterministic test
	c := NewCache[int](8)

	fullScan := func() map[snowflake.ID]uint64 {
		out := map[snowflake.ID]uint64{}
		for el := c.order.Front(); el != nil; el = el.Next() {
			e := el.Value.(*Entry[int])
			if e.DirtyVer > 0 {
				out[e.Key] = e.DirtyVer
			}
		}
		return out
	}

	for op := 0; op < 2000; op++ {
		key := snowflake.ID(1 + rng.Intn(40))
		switch rng.Intn(6) {
		case 0, 1: // writes dominate — the regime the dirty set exists for
			c.Put(key, int(key))
		case 2:
			c.MarkDeleted(key)
		case 3:
			c.LoadClean(key, int(key))
		case 4:
			c.Get(key)
		case 5:
			// Flush a random subset of currently-dirty entries.
			dirty := c.CollectDirty()
			flushed := map[snowflake.ID]uint64{}
			for _, e := range dirty {
				if rng.Intn(2) == 0 {
					flushed[e.Key] = e.DirtyVer
				}
			}
			// Occasionally re-dirty one mid-"flush" to exercise the
			// version-mismatch retention path.
			if len(dirty) > 0 && rng.Intn(3) == 0 {
				c.Put(dirty[rng.Intn(len(dirty))].Key, -1)
			}
			c.MarkFlushed(flushed)
		}

		c.mu.Lock()
		want := fullScan()
		c.mu.Unlock()
		got := c.CollectDirty()
		if len(got) != len(want) {
			t.Fatalf("op %d: CollectDirty %d entries, full scan %d", op, len(got), len(want))
		}
		for i, e := range got {
			ver, ok := want[e.Key]
			if !ok || ver != e.DirtyVer {
				t.Fatalf("op %d: entry %d key %d ver %d not in full scan (%v %d)", op, i, e.Key, e.DirtyVer, ok, ver)
			}
			if i > 0 && got[i-1].Key >= e.Key {
				t.Fatalf("op %d: CollectDirty not sorted by key at %d", op, i)
			}
		}
	}
}
