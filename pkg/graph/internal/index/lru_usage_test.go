package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestCacheUsage pins Usage's walk: dirty entries (written, a tombstone
// included) and clean ones apart, each entry's bytes its value's size plus
// entryOverhead (a tombstone holds no value), plus the maps' slots
// (cacheMapBytes: the dirty set's to the dirty bytes, the items map's shared by
// entry count), for the single-mutex and the sharded cache alike; a flush
// turns the written entry clean and drops the tombstone.
func TestCacheUsage(t *testing.T) {
	t.Parallel()
	size := func(s string) int64 { return int64(len(s)) }
	oh := entryOverhead[string]()
	for name, c := range map[string]EntityCache[string]{"single": NewCache[string](10), "sharded": NewShardedCache[string](16, 4)} {
		c.Put(1, "aaaa")
		c.Put(2, "bb")
		c.MarkDeleted(2)
		c.LoadCleanAt(3, "cccccc", c.FlushEpoch())
		want := addMaps(c, CacheUsage{CleanEntries: 1, CleanBytes: 6 + oh, DirtyEntries: 2, DirtyBytes: 4 + 2*oh})
		if got := c.Usage(size); got != want {
			t.Fatalf("%s before the flush: %+v, want %+v", name, got, want)
		}
		flushed := make(map[snowflake.ID]uint64)
		for _, e := range c.CollectDirty() {
			flushed[e.Key] = e.DirtyVer
		}
		c.MarkFlushed(flushed)
		want = addMaps(c, CacheUsage{CleanEntries: 2, CleanBytes: 10 + 2*oh})
		if got := c.Usage(size); got != want {
			t.Fatalf("%s after the flush: %+v, want %+v", name, got, want)
		}
	}
}

// addMaps adds each cache shard's map slots to u's bytes, as Usage does.
func addMaps(c EntityCache[string], u CacheUsage) CacheUsage {
	var shards []*Cache[string]
	switch c := c.(type) {
	case *Cache[string]:
		shards = []*Cache[string]{c}
	case *ShardedCache[string]:
		shards = c.shards
	}
	for _, sh := range shards {
		dirty := 0
		for _, el := range sh.items {
			if el.Value.(*Entry[string]).DirtyVer != 0 {
				dirty++
			}
		}
		u.DirtyBytes += cacheMapBytes(sh.dirtyPeak)
		items := cacheMapBytes(sh.itemsPeak)
		if n := len(sh.items); n > 0 {
			u.DirtyBytes += items * int64(dirty) / int64(n)
			items -= items * int64(dirty) / int64(n)
		}
		u.CleanBytes += items
	}
	return u
}
