package index

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// GetNoPromote is the non-promoting read used by the scan path: it returns the
// same status as Get but takes only a read lock and never touches the recency
// list. These tests pin (1) status parity, (2) the one intended behavioral
// difference — it does NOT save a key from eviction — and (3) race-freedom
// under concurrent writers.

func TestLRUGetNoPromoteStatus(t *testing.T) {
	t.Parallel()
	c := NewCache[string](10)
	if _, st := c.GetNoPromote(snowflake.ID(1)); st != CacheMiss {
		t.Fatalf("miss: got %d", st)
	}
	c.Put(snowflake.ID(1), "x")
	if v, st := c.GetNoPromote(snowflake.ID(1)); st != CacheHit || v != "x" {
		t.Fatalf("hit: got %q/%d", v, st)
	}
	c.MarkDeleted(snowflake.ID(1))
	if _, st := c.GetNoPromote(snowflake.ID(1)); st != CacheDeleted {
		t.Fatalf("deleted: got %d", st)
	}
}

// TestLRUGetNoPromoteDoesNotPromote is the load-bearing behavioral pin: a Get
// promotes the key off the LRU tail (saving it from the next eviction), while
// GetNoPromote leaves it as the eviction victim. Both branches start from the
// identical cache state.
func TestLRUGetNoPromoteDoesNotPromote(t *testing.T) {
	t.Parallel()
	mk := func() *Cache[string] {
		c := NewCache[string](3)
		c.LoadClean(snowflake.ID(1), "a") // LRU (oldest)
		c.LoadClean(snowflake.ID(2), "b")
		c.LoadClean(snowflake.ID(3), "c") // MRU (newest)
		return c
	}

	// GetNoPromote(1) leaves 1 as the LRU victim → LoadClean(4) evicts 1.
	c1 := mk()
	if _, st := c1.GetNoPromote(snowflake.ID(1)); st != CacheHit {
		t.Fatalf("GetNoPromote(1) = %d, want CacheHit", st)
	}
	c1.LoadClean(snowflake.ID(4), "d")
	if _, st := c1.GetNoPromote(snowflake.ID(1)); st != CacheMiss {
		t.Fatal("GetNoPromote must NOT save 1 from eviction — 1 should be evicted")
	}

	// Contrast: Get(1) promotes 1 → LoadClean(4) evicts 2 instead, 1 survives.
	c2 := mk()
	if _, st := c2.Get(snowflake.ID(1)); st != CacheHit {
		t.Fatalf("Get(1) = %d, want CacheHit", st)
	}
	c2.LoadClean(snowflake.ID(4), "d")
	if _, st := c2.GetNoPromote(snowflake.ID(1)); st != CacheHit {
		t.Fatal("Get must save 1 from eviction — 1 should still be present")
	}
	if _, st := c2.GetNoPromote(snowflake.ID(2)); st != CacheMiss {
		t.Fatal("Get(1) should have left 2 as the eviction victim")
	}
}

// TestLRUGetNoPromoteConcurrent runs under -race: 8 GetNoPromote readers + 1
// writer mutating the same keys (Put / MarkDeleted). The RLock readers must
// never observe a torn items map or entry while the writer holds the exclusive
// Lock.
func TestLRUGetNoPromoteConcurrent(t *testing.T) {
	c := NewCache[string](1000)
	const keys = 500
	for i := 0; i < keys; i++ {
		c.LoadClean(snowflake.ID(i), "v")
	}

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for j := 0; ; j++ {
			select {
			case <-stop:
				return
			default:
			}
			id := snowflake.ID(j % keys)
			if j%2 == 0 {
				c.Put(id, "w")
			} else {
				c.MarkDeleted(id)
			}
		}
	}()

	var readers sync.WaitGroup
	for r := 0; r < 8; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for k := 0; k < 50000; k++ {
				c.GetNoPromote(snowflake.ID(k % keys))
			}
		}()
	}
	readers.Wait()
	close(stop)
	writer.Wait()
}

func TestShardedGetNoPromote(t *testing.T) {
	t.Parallel()
	c := NewShardedCache[string](256, 16)
	for i := 0; i < 100; i++ {
		c.Put(snowflake.ID(i), "v")
	}
	for i := 0; i < 100; i++ {
		if v, st := c.GetNoPromote(snowflake.ID(i)); st != CacheHit || v != "v" {
			t.Fatalf("GetNoPromote(%d) = %q/%d, want v/CacheHit", i, v, st)
		}
	}
	if _, st := c.GetNoPromote(snowflake.ID(99999)); st != CacheMiss {
		t.Fatalf("GetNoPromote(absent) = %d, want CacheMiss", st)
	}
	// Routes to the same shard as Get: a key Put then GetNoPromote'd must hit.
	c.Put(snowflake.ID(42), "y")
	if v, st := c.GetNoPromote(snowflake.ID(42)); st != CacheHit || v != "y" {
		t.Fatalf("GetNoPromote routing: got %q/%d", v, st)
	}
}
