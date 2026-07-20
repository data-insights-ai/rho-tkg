package index

import (
	"math/rand"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Byte-budget probes (enterprise-scale ceiling 4 — caches budgeted by
// count, not bytes).

// sizerLen sizes string values by length — entries differ in size so the
// budget probes exercise mixed payloads.
func sizerLen(v string) int64 { return int64(len(v)) }

// TestCacheBudget_AccountingMatchesFullScan pins the totalBytes invariant
// the same way the dirty-set test pins its index: after ANY operation
// sequence, totalBytes must equal the sum of Entry.Size over a full list
// walk. Random Put / MarkDeleted / LoadClean / Get / flush / EvictForTest
// churn with values of randomized size against a small count capacity AND
// a small byte budget so both eviction triggers fire continuously.
func TestCacheBudget_AccountingMatchesFullScan(t *testing.T) {
	rng := rand.New(rand.NewSource(7)) //nolint:gosec // deterministic test
	c := NewCacheWithBudget(8, 2_000, sizerLen)

	sumScan := func() int64 {
		var sum int64
		for el := c.order.Front(); el != nil; el = el.Next() {
			sum += el.Value.(*Entry[string]).Size
		}
		return sum
	}

	randVal := func() string {
		return string(make([]byte, 1+rng.Intn(600))) // 1-600B + overhead
	}

	for op := 0; op < 2000; op++ {
		key := snowflake.ID(1 + rng.Intn(40))
		switch rng.Intn(6) {
		case 0, 1:
			c.Put(key, randVal())
		case 2:
			c.MarkDeleted(key)
		case 3:
			c.LoadClean(key, randVal())
		case 4:
			c.Get(key)
		case 5:
			dirty := c.CollectDirty()
			flushed := map[snowflake.ID]uint64{}
			for _, e := range dirty {
				if rng.Intn(2) == 0 {
					flushed[e.Key] = e.DirtyVer
				}
			}
			c.MarkFlushed(flushed)
		}

		c.mu.Lock()
		want := sumScan()
		got := c.totalBytes
		over := c.budget > 0 && got > c.budget && c.cleanCount > 0
		c.mu.Unlock()
		if got != want {
			t.Fatalf("op %d: totalBytes %d, full-scan sum %d", op, got, want)
		}
		if got < 0 {
			t.Fatalf("op %d: totalBytes negative: %d", op, got)
		}
		// Eviction invariant: over budget AND clean entries available must
		// never coexist after an operation completes.
		if over {
			t.Fatalf("op %d: over budget (%d > %d) with %d clean evictables", op, got, c.budget, c.CleanCount())
		}
	}
}

// TestCacheBudget_EvictsByBytesNotCount pins the core behavior: entries
// fit the COUNT capacity comfortably but blow the BYTE budget — the cache
// must shed clean LRU entries by bytes. The count-only cache (NewCache)
// must NOT evict for the same load.
func TestCacheBudget_EvictsByBytesNotCount(t *testing.T) {
	t.Parallel()
	big := string(make([]byte, 1_000))

	budgeted := NewCacheWithBudget(1_000_000, 3_000, sizerLen)
	for i := 1; i <= 10; i++ {
		budgeted.LoadClean(snowflake.ID(i), big)
	}
	if got := budgeted.Bytes(); got > 3_000 {
		t.Fatalf("budgeted cache holds %dB, budget 3000B", got)
	}
	if n := budgeted.Len(); n >= 10 {
		t.Fatalf("budgeted cache kept all %d entries", n)
	}
	// Newest entries survive (LRU eviction from the back).
	if _, status := budgeted.Get(snowflake.ID(10)); status != CacheHit {
		t.Fatalf("most recent entry evicted (status %v)", status)
	}

	unbudgeted := NewCache[string](1_000_000)
	for i := 1; i <= 10; i++ {
		unbudgeted.LoadClean(snowflake.ID(i), big)
	}
	if n := unbudgeted.Len(); n != 10 {
		t.Fatalf("count-only cache evicted: %d entries", n)
	}
	if got := unbudgeted.Bytes(); got != 0 {
		t.Fatalf("count-only cache accounted %dB, want 0 (accounting off)", got)
	}
}

// TestCacheBudget_DirtyEntriesExceedBudgetThenFlushSheds pins the soft-limit
// contract: dirty entries are NEVER evicted even over budget; once flushed
// clean, the cache sheds down to the budget.
func TestCacheBudget_DirtyEntriesExceedBudgetThenFlushSheds(t *testing.T) {
	t.Parallel()
	big := string(make([]byte, 1_000))
	c := NewCacheWithBudget(1_000_000, 2_500, sizerLen)

	for i := 1; i <= 8; i++ {
		c.Put(snowflake.ID(i), big) // dirty — unevictable
	}
	if n := c.Len(); n != 8 {
		t.Fatalf("dirty entries evicted: %d remain, want 8", n)
	}
	if got := c.Bytes(); got <= 2_500 {
		t.Fatalf("expected over-budget dirty cache, got %dB", got)
	}

	dirty := c.CollectDirty()
	flushed := map[snowflake.ID]uint64{}
	for _, e := range dirty {
		flushed[e.Key] = e.DirtyVer
	}
	c.MarkFlushed(flushed)

	if got := c.Bytes(); got > 2_500 {
		t.Fatalf("post-flush cache still over budget: %dB", got)
	}
	if n := c.Len(); n >= 8 {
		t.Fatalf("post-flush cache kept all %d entries", n)
	}
}

// TestCacheBudget_UpdateResizesEntry pins the delta accounting on updates:
// shrinking a value must release its bytes, growing must charge them —
// stale Size on update was the likeliest implementation bug.
func TestCacheBudget_UpdateResizesEntry(t *testing.T) {
	t.Parallel()
	c := NewCacheWithBudget(100, 1_000_000, sizerLen)

	key := snowflake.ID(1)
	c.Put(key, string(make([]byte, 500)))
	grew := c.Bytes()
	c.Put(key, string(make([]byte, 2_000)))
	if got := c.Bytes(); got != grew+1_500 {
		t.Fatalf("grow: bytes %d, want %d", got, grew+1_500)
	}
	c.Put(key, string(make([]byte, 10)))
	if got := c.Bytes(); got != grew-490 {
		t.Fatalf("shrink: bytes %d, want %d", got, grew-490)
	}
	if n := c.Len(); n != 1 {
		t.Fatalf("update created entries: %d", n)
	}
}

// TestCacheBudget_TombstoneLifecycleReleasesBytes pins that flushing a
// tombstone removes its accounted bytes — a leak here grows totalBytes
// monotonically and eventually evicts everything.
func TestCacheBudget_TombstoneLifecycleReleasesBytes(t *testing.T) {
	t.Parallel()
	c := NewCacheWithBudget(100, 1_000_000, sizerLen)

	key := snowflake.ID(1)
	c.Put(key, string(make([]byte, 100)))
	c.MarkDeleted(key)

	dirty := c.CollectDirty()
	if len(dirty) != 1 || !dirty[0].Deleted {
		t.Fatalf("expected one tombstone, got %+v", dirty)
	}
	c.MarkFlushed(map[snowflake.ID]uint64{key: dirty[0].DirtyVer})

	if got := c.Bytes(); got != 0 {
		t.Fatalf("flushed tombstone left %dB accounted", got)
	}
	if n := c.Len(); n != 0 {
		t.Fatalf("flushed tombstone left %d entries", n)
	}
}

// TestCacheBudget_MarkDeletedShrinksAlreadyCachedEntry (BACKLOG 16m) pins
// that MarkDeleted on a key ALREADY in the cache releases the stale
// payload's accounted bytes immediately, not just after flush. Get/
// GetNoPromote/Peek never return entry.Value once Deleted is set, so
// retaining the full pre-delete payload's Size is pure waste — and because
// dirty entries are never evicted, an unshrunk tombstone would hold its
// full byte weight against the budget for the entire flush interval, not
// just until the next MarkFlushed (see
// TestCacheBudget_TombstoneLifecycleReleasesBytes, which only checks the
// POST-flush state and would not catch a leak in the intermediate window).
func TestCacheBudget_MarkDeletedShrinksAlreadyCachedEntry(t *testing.T) {
	t.Parallel()
	c := NewCacheWithBudget(100, 1_000_000, sizerLen)

	key := snowflake.ID(1)
	big := string(make([]byte, 100_000))
	c.Put(key, big)

	wantBefore := int64(len(big)) + perEntryOverhead
	if got := c.Bytes(); got != wantBefore {
		t.Fatalf("Bytes() after Put = %d, want %d", got, wantBefore)
	}

	c.MarkDeleted(key)

	wantAfter := int64(perEntryOverhead)
	if got := c.Bytes(); got != wantAfter {
		t.Fatalf("Bytes() after MarkDeleted on an already-cached key = %d, want %d "+
			"(tombstone-only footprint) — stale payload still accounted before flush", got, wantAfter)
	}

	// Flushing must be idempotent with the pre-shrunk size (no double-release).
	dirty := c.CollectDirty()
	if len(dirty) != 1 || !dirty[0].Deleted {
		t.Fatalf("expected one tombstone, got %+v", dirty)
	}
	c.MarkFlushed(map[snowflake.ID]uint64{key: dirty[0].DirtyVer})
	if got := c.Bytes(); got != 0 {
		t.Fatalf("Bytes() after flushing the shrunk tombstone = %d, want 0", got)
	}
}

// TestCacheBudget_DisabledByZeroBudgetOrNilSizer pins the off switches:
// NewCacheWithBudget(_, 0, sizer) and (_, budget, nil) must behave exactly
// like NewCache — no byte accounting, no byte eviction.
func TestCacheBudget_DisabledByZeroBudgetOrNilSizer(t *testing.T) {
	t.Parallel()
	for name, c := range map[string]*Cache[string]{
		"zero budget": NewCacheWithBudget(5, 0, sizerLen),
		"nil sizer":   NewCacheWithBudget[string](5, 1, nil),
	} {
		c.Put(snowflake.ID(1), string(make([]byte, 10_000)))
		if got := c.Bytes(); got != 0 {
			t.Fatalf("%s: accounted %dB, want 0", name, got)
		}
		if got := c.Budget(); got != 0 {
			t.Fatalf("%s: budget %d, want 0 (disabled)", name, got)
		}
	}
}
