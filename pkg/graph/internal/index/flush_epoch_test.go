package index

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// The flush epoch lets a reader that missed the cache tell whether a flush
// marked entries clean (and so made them evictable) since it read the epoch.
// A badger read taken after that epoch read may predate the flush's commit;
// LoadCleanAt refuses to cache such a read.

type epochCache interface {
	Put(key snowflake.ID, value int)
	CollectDirty() []Entry[int]
	MarkFlushed(flushed map[snowflake.ID]uint64)
	FlushEpoch() uint64
	LoadCleanAt(key snowflake.ID, value int, epoch uint64) bool
	Peek(key snowflake.ID) (int, CacheStatus)
}

func markAllFlushed(c epochCache) {
	f := make(map[snowflake.ID]uint64)
	for _, e := range c.CollectDirty() {
		f[e.Key] = e.DirtyVer
	}
	c.MarkFlushed(f)
}

func TestFlushEpoch(t *testing.T) {
	impls := map[string]func() epochCache{
		"Cache":        func() epochCache { return NewCache[int](8) },
		"ZeroCache":    func() epochCache { return &Cache[int]{} },
		"ShardedCache": func() epochCache { return NewShardedCache[int](64, 16) },
	}
	for name, mk := range impls {
		t.Run(name+"/AdvancesOnMarkFlushed", func(t *testing.T) {
			c := mk()
			e0 := c.FlushEpoch()
			c.MarkFlushed(nil)
			if c.FlushEpoch() != e0 {
				t.Fatal("an empty MarkFlushed advanced the epoch")
			}
			c.Put(1, 1)
			markAllFlushed(c)
			if c.FlushEpoch() == e0 {
				t.Fatal("MarkFlushed did not advance the epoch")
			}
		})
		t.Run(name+"/LoadCleanAtCurrentEpochFills", func(t *testing.T) {
			c := mk()
			if !c.LoadCleanAt(1, 7, c.FlushEpoch()) {
				t.Fatal("LoadCleanAt at the current epoch refused")
			}
			if v, st := c.Peek(1); st != CacheHit || v != 7 {
				t.Fatalf("Peek = %d/%v, want 7/hit", v, st)
			}
		})
		t.Run(name+"/LoadCleanAtStaleEpochRefuses", func(t *testing.T) {
			c := mk()
			e0 := c.FlushEpoch()
			c.Put(2, 2)
			markAllFlushed(c)
			if c.LoadCleanAt(1, 7, e0) {
				t.Fatal("LoadCleanAt accepted a read older than the last flush")
			}
			if _, st := c.Peek(1); st != CacheMiss {
				t.Fatalf("stale fill left key cached (status %v)", st)
			}
		})
		t.Run(name+"/LoadCleanAtNeverOverwrites", func(t *testing.T) {
			c := mk()
			c.Put(1, 2)
			if c.LoadCleanAt(1, 1, c.FlushEpoch()) {
				t.Fatal("LoadCleanAt reported a fill over an existing entry")
			}
			if v, _ := c.Peek(1); v != 2 {
				t.Fatalf("Peek = %d, want 2 (in-memory state takes precedence)", v)
			}
		})
	}
}

// A sharded cache must advance ONE epoch for every shard: a flush that only
// touches shard A must still invalidate a fill for a key in shard B, because a
// scan's badger snapshot is shared across all keys.
func TestShardedCacheFlushEpochIsShared(t *testing.T) {
	c := NewShardedCache[int](64, 16)
	var a, b snowflake.ID = 1, 0
	for k := snowflake.ID(2); k < 1000; k++ {
		if c.indexFor(k) != c.indexFor(a) {
			b = k
			break
		}
	}
	if b == 0 {
		t.Fatal("fixture: no key in a second shard")
	}
	e0 := c.FlushEpoch()
	c.Put(a, 1)
	markAllFlushed(c)
	if c.FlushEpoch() == e0 {
		t.Fatal("flush of shard A did not advance the shared epoch")
	}
	if c.LoadCleanAt(b, 1, e0) {
		t.Fatal("shard B accepted a fill older than a flush of shard A")
	}
}
