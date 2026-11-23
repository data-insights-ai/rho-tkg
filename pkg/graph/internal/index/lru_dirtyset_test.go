package index

import (
	"math/rand"
	"reflect"
	"runtime"
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
	t.Parallel()
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

// TestDirtySetMadeAnewAfterABurst (MEMHEAP): a Go map keeps the buckets of its
// largest size, so a cache whose 200,000 dirty entries were flushed held their
// dirty-set buckets (about 2 MB at the 512 MiB memtable arm of
// TestMemoryAccountFollowsTheHeap) under no entry. A flush that empties a set
// that held more than dirtySetShrinkAt entries makes it anew, so the live heap
// after it drops by at least the old buckets; a small set is kept.
func TestDirtySetMadeAnewAfterABurst(t *testing.T) {
	const n = 200_000
	c := NewCache[int](n)
	for i := range n {
		c.Put(snowflake.ID(i+1), i)
	}
	flush := func() {
		done := map[snowflake.ID]uint64{}
		for _, e := range c.CollectDirty() {
			done[e.Key] = e.DirtyVer
		}
		c.MarkFlushed(done)
	}
	heap := func() uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := heap()
	flush()
	after := heap()
	if len(c.dirtySet) != 0 || c.dirtyPeak != 0 {
		t.Fatalf("after the flush: %d dirty, peak %d; want an empty set made anew", len(c.dirtySet), c.dirtyPeak)
	}
	if freed := int64(before) - int64(after); freed < 1<<20 {
		t.Errorf("the flush of %d dirty entries freed %d B of live heap; want the dirty set's buckets (at least 1 MiB)", n, freed)
	}
	for i := range 10 {
		c.Put(snowflake.ID(i+1), i)
	}
	small := reflect.ValueOf(c.dirtySet).Pointer()
	flush()
	if reflect.ValueOf(c.dirtySet).Pointer() != small {
		t.Error("a flush of 10 dirty entries made the set anew; want it kept")
	}
	runtime.KeepAlive(c)
}
