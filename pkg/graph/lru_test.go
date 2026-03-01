package graph

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ─── Basic operations ────────────────────────────────────────────────────────

func TestLRUGetMiss(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[*int](10)
	_, status := c.Get(snowflake.ID(1))
	if status != cacheMiss {
		t.Fatalf("expected cacheMiss, got %d", status)
	}
}

func TestLRUPutAndGet(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "hello")

	v, status := c.Get(snowflake.ID(1))
	if status != cacheHit {
		t.Fatalf("expected cacheHit, got %d", status)
	}
	if v != "hello" {
		t.Fatalf("expected hello, got %s", v)
	}
}

func TestLRUPutOverwrite(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "first")
	c.Put(snowflake.ID(1), "second")

	v, status := c.Get(snowflake.ID(1))
	if status != cacheHit || v != "second" {
		t.Fatalf("expected second, got %s (status %d)", v, status)
	}
	if c.Len() != 1 {
		t.Fatalf("expected 1 entry, got %d", c.Len())
	}
}

// ─── Tombstone semantics ─────────────────────────────────────────────────────

func TestLRUMarkDeleted(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "hello")
	c.MarkDeleted(snowflake.ID(1))

	_, status := c.Get(snowflake.ID(1))
	if status != cacheDeleted {
		t.Fatalf("expected cacheDeleted, got %d", status)
	}
}

func TestLRUMarkDeletedNonExistent(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.MarkDeleted(snowflake.ID(42))

	_, status := c.Get(snowflake.ID(42))
	if status != cacheDeleted {
		t.Fatalf("expected cacheDeleted for tombstone, got %d", status)
	}
}

func TestLRUPutAfterMarkDeleted(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "original")
	c.MarkDeleted(snowflake.ID(1))

	// Re-insert clears the tombstone.
	c.Put(snowflake.ID(1), "revived")
	v, status := c.Get(snowflake.ID(1))
	if status != cacheHit || v != "revived" {
		t.Fatalf("expected revived, got %s (status %d)", v, status)
	}
}

// ─── Capacity and eviction ───────────────────────────────────────────────────

func TestLRUEvictsCleanEntries(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](3)

	// Load 3 clean entries.
	c.LoadClean(snowflake.ID(1), "a")
	c.LoadClean(snowflake.ID(2), "b")
	c.LoadClean(snowflake.ID(3), "c")
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}

	// 4th clean entry should evict LRU (ID=1).
	c.LoadClean(snowflake.ID(4), "d")
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries after eviction, got %d", c.Len())
	}
	_, status := c.Get(snowflake.ID(1))
	if status != cacheMiss {
		t.Fatal("expected ID 1 to be evicted")
	}
}

func TestLRUDirtyEntriesNotEvicted(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](2)

	// Two dirty entries (via Put).
	c.Put(snowflake.ID(1), "a")
	c.Put(snowflake.ID(2), "b")

	// Adding a 3rd should not evict dirty entries — cache grows beyond capacity.
	c.Put(snowflake.ID(3), "c")
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries (dirty overflow), got %d", c.Len())
	}

	// All entries still accessible.
	for _, id := range []snowflake.ID{1, 2, 3} {
		_, status := c.Get(id)
		if status != cacheHit {
			t.Fatalf("expected cacheHit for ID %d", id)
		}
	}
}

func TestLRUEvictsCleanKeepsDirty(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](3)

	c.LoadClean(snowflake.ID(1), "clean1")
	c.Put(snowflake.ID(2), "dirty2") // dirty
	c.LoadClean(snowflake.ID(3), "clean3")

	// Adding 4th entry: clean1 (LRU clean) should be evicted, not dirty2.
	c.LoadClean(snowflake.ID(4), "clean4")

	_, status := c.Get(snowflake.ID(1))
	if status != cacheMiss {
		t.Fatal("expected clean1 to be evicted")
	}
	_, status = c.Get(snowflake.ID(2))
	if status != cacheHit {
		t.Fatal("dirty2 should not be evicted")
	}
}

func TestLRUTombstoneNotEvicted(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](2)

	c.MarkDeleted(snowflake.ID(1)) // dirty tombstone
	c.Put(snowflake.ID(2), "b")    // dirty

	// Adding 3rd entry — neither tombstone nor dirty entry should be evicted.
	c.Put(snowflake.ID(3), "c")
	_, status := c.Get(snowflake.ID(1))
	if status != cacheDeleted {
		t.Fatal("tombstone should not be evicted")
	}
}

// ─── LoadClean ──────────────────────────────────────────────────────────────

func TestLRULoadCleanDoesNotOverwriteExisting(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "dirty")

	// LoadClean should be a no-op since key exists.
	c.LoadClean(snowflake.ID(1), "stale")
	v, status := c.Get(snowflake.ID(1))
	if status != cacheHit || v != "dirty" {
		t.Fatalf("expected dirty, got %s (status %d)", v, status)
	}
}

func TestLRULoadCleanDoesNotOverwriteTombstone(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.MarkDeleted(snowflake.ID(1))

	// LoadClean should not overwrite tombstone.
	c.LoadClean(snowflake.ID(1), "stale")
	_, status := c.Get(snowflake.ID(1))
	if status != cacheDeleted {
		t.Fatal("LoadClean should not overwrite tombstone")
	}
}

// ─── CollectDirty (read-only snapshot) ──────────────────────────────────────

func TestLRUCollectDirtyReturnsAll(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "a")
	c.Put(snowflake.ID(2), "b")
	c.LoadClean(snowflake.ID(3), "c")

	dirty := c.CollectDirty()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty entries, got %d", len(dirty))
	}

	// CollectDirty is read-only — entries are still dirty.
	dirty2 := c.CollectDirty()
	if len(dirty2) != 2 {
		t.Fatalf("expected 2 dirty entries on second collect, got %d", len(dirty2))
	}

	// MarkFlushed clears dirty for matching versions.
	flushed := make(map[snowflake.ID]uint64, len(dirty))
	for _, e := range dirty {
		flushed[e.key] = e.dirtyVer
	}
	c.MarkFlushed(flushed)

	// After MarkFlushed, no more dirty entries.
	dirty3 := c.CollectDirty()
	if len(dirty3) != 0 {
		t.Fatalf("expected 0 dirty after MarkFlushed, got %d", len(dirty3))
	}
}

func TestLRUCollectDirtyIsReadOnly(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "a")

	// Multiple CollectDirty calls should not change state.
	c.CollectDirty()
	c.CollectDirty()
	c.CollectDirty()

	dirty := c.CollectDirty()
	if len(dirty) != 1 {
		t.Fatalf("expected 1 dirty after multiple CollectDirty, got %d", len(dirty))
	}
}

func TestLRUCollectDirtyTombstoneIncluded(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.MarkDeleted(snowflake.ID(1))

	dirty := c.CollectDirty()
	if len(dirty) != 1 {
		t.Fatalf("expected 1 dirty entry, got %d", len(dirty))
	}
	if !dirty[0].deleted {
		t.Fatal("expected tombstone in dirty output")
	}
}

// ─── MarkFlushed ────────────────────────────────────────────────────────────

func TestLRUMarkFlushedRemovesCleanTombstones(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "a")
	c.MarkDeleted(snowflake.ID(2))

	dirty := c.CollectDirty()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty entries, got %d", len(dirty))
	}

	// MarkFlushed with matching versions.
	flushed := make(map[snowflake.ID]uint64, len(dirty))
	for _, e := range dirty {
		flushed[e.key] = e.dirtyVer
	}
	c.MarkFlushed(flushed)

	// Tombstone for ID 2 should be removed after flush (now clean tombstone).
	if c.Len() != 1 {
		t.Fatalf("expected 1 entry (tombstone removed), got %d", c.Len())
	}

	// ID 1 is still cached (dirty→clean), ID 2 is gone.
	_, status := c.Get(snowflake.ID(1))
	if status != cacheHit {
		t.Fatal("ID 1 should still be cached as clean")
	}
	_, status = c.Get(snowflake.ID(2))
	if status != cacheMiss {
		t.Fatal("ID 2 tombstone should be removed after MarkFlushed")
	}
}

func TestLRUMarkFlushedVersionMismatch(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "v1")

	// Collect dirty with v1's version.
	dirty := c.CollectDirty()
	if len(dirty) != 1 {
		t.Fatalf("expected 1, got %d", len(dirty))
	}
	oldVer := dirty[0].dirtyVer

	// Re-dirty the entry (simulates concurrent write during flush).
	c.Put(snowflake.ID(1), "v2")

	// MarkFlushed with old version — should NOT clear dirty.
	c.MarkFlushed(map[snowflake.ID]uint64{snowflake.ID(1): oldVer})

	// Entry should still be dirty (version mismatch).
	dirty2 := c.CollectDirty()
	if len(dirty2) != 1 {
		t.Fatalf("expected 1 dirty after version-mismatch MarkFlushed, got %d", len(dirty2))
	}
	if dirty2[0].value != "v2" {
		t.Fatalf("expected v2, got %s", dirty2[0].value)
	}
}

func TestLRUMarkFlushedOnlyTargetedIDs(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "a")
	c.Put(snowflake.ID(2), "b")
	c.Put(snowflake.ID(3), "c")

	dirty := c.CollectDirty()
	if len(dirty) != 3 {
		t.Fatalf("expected 3 dirty, got %d", len(dirty))
	}

	// Find the entry for ID 1 and only mark it as flushed.
	var id1Ver uint64
	for _, e := range dirty {
		if e.key == snowflake.ID(1) {
			id1Ver = e.dirtyVer
			break
		}
	}
	c.MarkFlushed(map[snowflake.ID]uint64{snowflake.ID(1): id1Ver})

	// IDs 2 and 3 should still be dirty.
	remaining := c.CollectDirty()
	if len(remaining) != 2 {
		t.Fatalf("expected 2 dirty after partial MarkFlushed, got %d", len(remaining))
	}
}

func TestLRUDirtyEntryNotEvictedDuringFlush(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](2)

	c.Put(snowflake.ID(1), "a") // dirty

	// Collect dirty (snapshot).
	dirty := c.CollectDirty()

	// Simulate concurrent write: re-dirty the same entry.
	c.Put(snowflake.ID(1), "a_updated")

	// MarkFlushed with old version — entry stays dirty.
	c.MarkFlushed(map[snowflake.ID]uint64{snowflake.ID(1): dirty[0].dirtyVer})

	// Add clean entries to trigger eviction pressure.
	c.LoadClean(snowflake.ID(2), "b")
	c.LoadClean(snowflake.ID(3), "c")

	// ID 1 should NOT be evicted (still dirty due to version mismatch).
	v, status := c.Get(snowflake.ID(1))
	if status != cacheHit {
		t.Fatal("re-dirtied entry should not be evicted")
	}
	if v != "a_updated" {
		t.Fatalf("expected a_updated, got %s", v)
	}
}

func TestLRUMarkFlushedTombstoneOnly(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.MarkDeleted(snowflake.ID(1))

	dirty := c.CollectDirty()
	if len(dirty) != 1 || !dirty[0].deleted {
		t.Fatal("expected 1 dirty tombstone")
	}

	c.MarkFlushed(map[snowflake.ID]uint64{snowflake.ID(1): dirty[0].dirtyVer})

	if c.Len() != 0 {
		t.Fatalf("expected 0 entries after flushing tombstone, got %d", c.Len())
	}
	_, status := c.Get(snowflake.ID(1))
	if status != cacheMiss {
		t.Fatal("expected cacheMiss after tombstone flushed and removed")
	}
}

func TestLRUEvictCleanSinglePass(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[int](5)

	// Fill to 2x capacity with dirty entries.
	for i := 1; i <= 10; i++ {
		c.Put(snowflake.ID(i), i)
	}
	if c.Len() != 10 {
		t.Fatalf("expected 10 (dirty overflow), got %d", c.Len())
	}

	// Flush all dirty → clean.
	dirty := c.CollectDirty()
	flushed := make(map[snowflake.ID]uint64, len(dirty))
	for _, e := range dirty {
		flushed[e.key] = e.dirtyVer
	}
	c.MarkFlushed(flushed)

	// Now 10 clean entries, capacity 5. Adding 1 dirty should evict 6 clean
	// (LRU entries 1-6) in a single pass — O(N), not O(N²).
	c.Put(snowflake.ID(100), 100)
	if c.Len() != 5 {
		t.Fatalf("expected 5 after bulk eviction, got %d", c.Len())
	}

	// Newest entry (dirty) must survive.
	v, status := c.Get(snowflake.ID(100))
	if status != cacheHit || v != 100 {
		t.Fatal("ID 100 (dirty, newest) should survive eviction")
	}

	// LRU clean entries (1-6) should be evicted.
	_, status = c.Get(snowflake.ID(1))
	if status != cacheMiss {
		t.Fatal("ID 1 (LRU clean) should be evicted")
	}

	// More-recently-used clean entries (7-10) should survive.
	_, status = c.Get(snowflake.ID(10))
	if status != cacheHit {
		t.Fatal("ID 10 (MRU clean) should survive eviction")
	}
}

func TestLRUEvictCleanSkipsDirtyInSinglePass(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[int](3)

	// Create a mix: 3 dirty + 3 clean, with dirty at the LRU end.
	c.LoadClean(snowflake.ID(1), 1) // clean, LRU
	c.LoadClean(snowflake.ID(2), 2) // clean
	c.LoadClean(snowflake.ID(3), 3) // clean
	c.Put(snowflake.ID(4), 4)       // dirty — triggers eviction of clean(1)
	c.Put(snowflake.ID(5), 5)       // dirty — triggers eviction of clean(2)
	c.LoadClean(snowflake.ID(6), 6) // clean — triggers eviction of clean(3)

	// Should have: [6, 5, 4] (capacity 3). All clean evicted, 2 dirty + 1 clean.
	if c.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", c.Len())
	}

	// Dirty entries 4 and 5 must survive.
	for _, id := range []snowflake.ID{4, 5} {
		_, status := c.Get(id)
		if status != cacheHit {
			t.Fatalf("dirty entry %d should survive", id)
		}
	}
}

// ─── LRU order ──────────────────────────────────────────────────────────────

func TestLRUAccessUpdatesOrder(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](3)
	c.LoadClean(snowflake.ID(1), "a")
	c.LoadClean(snowflake.ID(2), "b")
	c.LoadClean(snowflake.ID(3), "c")

	// Access ID 1, making it most recent.
	c.Get(snowflake.ID(1))

	// Add 4th — should evict ID 2 (now LRU), not ID 1.
	c.LoadClean(snowflake.ID(4), "d")

	_, status := c.Get(snowflake.ID(1))
	if status != cacheHit {
		t.Fatal("ID 1 should be kept (recently accessed)")
	}
	_, status = c.Get(snowflake.ID(2))
	if status != cacheMiss {
		t.Fatal("ID 2 should be evicted (LRU)")
	}
}

// ─── Edge cases ─────────────────────────────────────────────────────────────

func TestLRUMinCapacity(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](0) // should be clamped to 1
	c.LoadClean(snowflake.ID(1), "a")
	c.LoadClean(snowflake.ID(2), "b")

	if c.Len() != 1 {
		t.Fatalf("expected 1 entry at min capacity, got %d", c.Len())
	}
}

func TestLRUEmptyCollectDirty(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	dirty := c.CollectDirty()
	if len(dirty) != 0 {
		t.Fatalf("expected 0 dirty on empty cache, got %d", len(dirty))
	}
}

func TestLRUMarkFlushedNonExistentID(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	// MarkFlushed with an ID not in the cache — should be a no-op.
	c.MarkFlushed(map[snowflake.ID]uint64{snowflake.ID(999): 42})
	if c.Len() != 0 {
		t.Fatalf("expected 0 entries, got %d", c.Len())
	}
}

// ─── CleanCount tracking ────────────────────────────────────────────────────

func TestLRUEvictCleanSkipsWhenAllDirty(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[int](5)

	// Insert 20 dirty entries — all exceed capacity, but none are evictable.
	for i := 1; i <= 20; i++ {
		c.Put(snowflake.ID(i), i)
	}

	// All 20 must be present — dirty entries are never evicted.
	if c.Len() != 20 {
		t.Fatalf("Len() = %d, want 20 (dirty entries never evicted)", c.Len())
	}
	if c.CleanCount() != 0 {
		t.Fatalf("CleanCount() = %d, want 0 (all entries are dirty)", c.CleanCount())
	}
}

func TestLRUCleanCountAccuracy(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](100) // large capacity so eviction doesn't interfere

	// Step 1: Put 10 entries (all dirty) — cleanCount = 0.
	for i := 1; i <= 10; i++ {
		c.Put(snowflake.ID(i), "dirty")
	}
	if cc := c.CleanCount(); cc != 0 {
		t.Fatalf("after 10 puts: CleanCount() = %d, want 0", cc)
	}

	// Step 2: Flush all 10 (dirty → clean) — cleanCount = 10.
	dirty := c.CollectDirty()
	flushed := make(map[snowflake.ID]uint64, len(dirty))
	for _, e := range dirty {
		flushed[e.key] = e.dirtyVer
	}
	c.MarkFlushed(flushed)
	if cc := c.CleanCount(); cc != 10 {
		t.Fatalf("after flush: CleanCount() = %d, want 10", cc)
	}

	// Step 3: Overwrite 3 clean entries (clean → dirty) — cleanCount = 7.
	c.Put(snowflake.ID(1), "redirtied")
	c.Put(snowflake.ID(2), "redirtied")
	c.Put(snowflake.ID(3), "redirtied")
	if cc := c.CleanCount(); cc != 7 {
		t.Fatalf("after 3 overwrites: CleanCount() = %d, want 7", cc)
	}

	// Step 4: MarkDeleted on 2 clean entries — cleanCount = 5.
	c.MarkDeleted(snowflake.ID(4))
	c.MarkDeleted(snowflake.ID(5))
	if cc := c.CleanCount(); cc != 5 {
		t.Fatalf("after 2 deletes: CleanCount() = %d, want 5", cc)
	}

	// Step 5: LoadClean 2 new entries — cleanCount = 7.
	c.LoadClean(snowflake.ID(100), "clean100")
	c.LoadClean(snowflake.ID(101), "clean101")
	if cc := c.CleanCount(); cc != 7 {
		t.Fatalf("after 2 LoadClean: CleanCount() = %d, want 7", cc)
	}

	// Step 6: LoadClean on existing key (no-op) — cleanCount unchanged.
	c.LoadClean(snowflake.ID(1), "stale")
	if cc := c.CleanCount(); cc != 7 {
		t.Fatalf("after LoadClean no-op: CleanCount() = %d, want 7", cc)
	}
}

// ─── Concurrent access ─────────────────────────────────────────────────────

func TestLRUConcurrentAccess(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[int](100)
	const goroutines = 50
	const opsPerGoroutine = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(offset int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				id := snowflake.ID(offset*opsPerGoroutine + i)
				c.Put(id, i)
				c.Get(id)
				if i%3 == 0 {
					c.MarkDeleted(id)
				}
				if i%5 == 0 {
					c.LoadClean(snowflake.ID(offset*opsPerGoroutine+i+10000), i)
				}
				if i%7 == 0 {
					dirty := c.CollectDirty()
					flushed := make(map[snowflake.ID]uint64, len(dirty))
					for _, e := range dirty {
						flushed[e.key] = e.dirtyVer
					}
					c.MarkFlushed(flushed)
				}
			}
		}(g)
	}
	wg.Wait()
}
