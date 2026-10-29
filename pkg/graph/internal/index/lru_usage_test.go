package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestCacheUsage pins CACHESTATS' walk: dirty entries (written, a tombstone
// included) and clean ones apart, each entry's bytes its value's size plus
// perEntryOverhead (a tombstone holds no value), for the single-mutex and the
// sharded cache alike; a flush turns the written entry clean and drops the
// tombstone.
func TestCacheUsage(t *testing.T) {
	t.Parallel()
	size := func(s string) int64 { return int64(len(s)) }
	for name, c := range map[string]EntityCache[string]{"single": NewCache[string](10), "sharded": NewShardedCache[string](16, 4)} {
		c.Put(1, "aaaa")
		c.Put(2, "bb")
		c.MarkDeleted(2)
		c.LoadCleanAt(3, "cccccc", c.FlushEpoch())
		if got, want := c.Usage(size), (CacheUsage{CleanEntries: 1, CleanBytes: 6 + perEntryOverhead,
			DirtyEntries: 2, DirtyBytes: 4 + 2*perEntryOverhead}); got != want {
			t.Fatalf("%s before the flush: %+v, want %+v", name, got, want)
		}
		flushed := make(map[snowflake.ID]uint64)
		for _, e := range c.CollectDirty() {
			flushed[e.Key] = e.DirtyVer
		}
		c.MarkFlushed(flushed)
		if got, want := c.Usage(size), (CacheUsage{CleanEntries: 2, CleanBytes: 10 + 2*perEntryOverhead}); got != want {
			t.Fatalf("%s after the flush: %+v, want %+v", name, got, want)
		}
	}
}
