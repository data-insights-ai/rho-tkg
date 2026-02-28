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

// ─── CollectDirty ───────────────────────────────────────────────────────────

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

	// After collection, no more dirty entries.
	dirty2 := c.CollectDirty()
	if len(dirty2) != 0 {
		t.Fatalf("expected 0 dirty after second collect, got %d", len(dirty2))
	}
}

func TestLRUCollectDirtyRemovesCleanTombstones(t *testing.T) {
	t.Parallel()
	c := newEntityLRU[string](10)
	c.Put(snowflake.ID(1), "a")
	c.MarkDeleted(snowflake.ID(2))

	dirty := c.CollectDirty()
	if len(dirty) != 2 {
		t.Fatalf("expected 2 dirty entries, got %d", len(dirty))
	}

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
		t.Fatal("ID 2 tombstone should be removed after collect")
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
					c.CollectDirty()
				}
			}
		}(g)
	}
	wg.Wait()
}
