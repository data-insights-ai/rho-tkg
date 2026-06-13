package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestSetNoEvict_NoEviction pins resident mode: with eviction disabled, clean
// entries loaded past the capacity are KEPT (so a backing store never re-decodes
// the same entity twice), whereas the default cache evicts them. This is the
// big-O fix for graph-larger-than-cache traversal (re-decode -> super-linear).
func TestSetNoEvict_NoEviction(t *testing.T) {
	key := func(i int) snowflake.ID { return snowflake.ID(int64(i + 1)) }

	// Control: default cache (capacity 2) evicts clean entries past capacity.
	def := NewCache[int](2)
	for i := 0; i < 50; i++ {
		def.LoadClean(key(i), i)
	}
	if def.Len() > 4 { // soft limit; allow a little slack, but nowhere near 50
		t.Fatalf("default cache should have evicted: Len=%d, want <=4", def.Len())
	}

	// Resident: same capacity, but SetNoEvict keeps every clean entry.
	res := NewCache[int](2)
	res.SetNoEvict()
	for i := 0; i < 50; i++ {
		res.LoadClean(key(i), i)
	}
	if res.Len() != 50 {
		t.Fatalf("resident cache must keep all entries: Len=%d, want 50", res.Len())
	}
	// All entries still resolvable (no re-fetch needed).
	for i := 0; i < 50; i++ {
		if v, st := res.GetNoPromote(key(i)); st != CacheHit || v != i {
			t.Fatalf("resident miss for %d: status=%v v=%d", i, st, v)
		}
	}
}
