package index

import (
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Test 1 — concurrency / race. The reason ShardedCache exists.
//
// G goroutines hammer a shared key space with the full routed method set while a
// flusher goroutine runs CollectDirty→MarkFlushed and an observer reads the
// aggregate scalars. Pass condition is purely "did not race / panic / deadlock":
// this probe asserts nothing about values. Run under -race.
func TestShardedCache_ConcurrentHammer_RaceFree(t *testing.T) {
	const (
		workers  = 24
		keySpace = 512
		duration = 200 * time.Millisecond
	)
	c := NewShardedCache[int](4096, 16)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				key := snowflake.ID(rng.Intn(keySpace) + 1)
				switch rng.Intn(6) {
				case 0:
					c.Get(key)
				case 1:
					c.Put(key, int(key))
				case 2:
					c.MarkDeleted(key)
				case 3:
					c.LoadClean(key, int(key))
				case 4:
					c.Peek(key)
				case 5:
					c.EvictForTest(key)
				}
			}
		}(int64(w) + 1)
	}

	// Flusher: exercises C3 (re-dirty protection) under the race detector.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			dirty := c.CollectDirty()
			flushed := make(map[snowflake.ID]uint64, len(dirty))
			for _, e := range dirty {
				flushed[e.Key] = e.DirtyVer
			}
			c.MarkFlushed(flushed)
		}
	}()

	// Observer: aggregate scalars concurrent with mutation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = c.Len()
			_ = c.Bytes()
			_ = c.Hits()
			_ = c.Misses()
			_ = c.CleanCount()
		}
	}()

	time.Sleep(duration)
	close(stop)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("goroutines did not finish — possible deadlock")
	}
}

// Test 2a — collect completeness (invariant C1). Put K distinct keys spanning
// shards; CollectDirty must return exactly K entries (no key lost to a wrong
// shard or a skipped shard in the merge walk).
func TestShardedCache_CollectDirty_AllKeysAcrossShards(t *testing.T) {
	c := NewShardedCache[int](1<<16, 16) // capacity >> K so nothing evicts
	const k = 5000
	want := make(map[snowflake.ID]struct{}, k)
	for i := 1; i <= k; i++ {
		id := snowflake.ID(i)
		c.Put(id, i)
		want[id] = struct{}{}
	}

	dirty := c.CollectDirty()
	if len(dirty) != k {
		t.Fatalf("CollectDirty returned %d entries, want %d (a key was lost to a wrong/skipped shard)", len(dirty), k)
	}
	got := make(map[snowflake.ID]struct{}, len(dirty))
	for _, e := range dirty {
		got[e.Key] = struct{}{}
	}
	if len(got) != k {
		t.Fatalf("CollectDirty returned %d distinct keys, want %d (duplicate or missing key)", len(got), k)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Fatalf("key %d dirtied but missing from CollectDirty", id)
		}
	}
}

// Test 2b — flush clears dirty (invariants C2/C5). After MarkFlushed of the
// collected versions, CollectDirty is empty, live keys are clean, tombstones are
// removed.
func TestShardedCache_MarkFlushed_ClearsDirtyAcrossShards(t *testing.T) {
	c := NewShardedCache[int](1<<16, 16)
	const live = 4000
	const tombstones = 500
	for i := 1; i <= live; i++ {
		c.Put(snowflake.ID(i), i)
	}
	for i := live + 1; i <= live+tombstones; i++ {
		c.MarkDeleted(snowflake.ID(i))
	}

	dirty := c.CollectDirty()
	flushed := make(map[snowflake.ID]uint64, len(dirty))
	for _, e := range dirty {
		flushed[e.Key] = e.DirtyVer
	}
	c.MarkFlushed(flushed)

	if remaining := c.CollectDirty(); len(remaining) != 0 {
		t.Fatalf("after MarkFlushed CollectDirty returned %d entries, want 0", len(remaining))
	}
	if cc := c.CleanCount(); cc != live {
		t.Fatalf("CleanCount = %d, want %d (live non-tombstone keys)", cc, live)
	}
	// Tombstones removed; live keys retained.
	if l := c.Len(); l != live {
		t.Fatalf("Len = %d, want %d (tombstones should be removed after flush)", l, live)
	}
	// A flushed tombstone now reads as a Miss, not Deleted.
	if _, st := c.Get(snowflake.ID(live + 1)); st != CacheMiss {
		t.Fatalf("flushed tombstone status = %v, want CacheMiss", st)
	}
}

// Test 2c — global sort (behavioral pin). With keys inserted shuffled across
// shards, CollectDirty must return them sorted ascending by Key. Pins the
// merged-batch ordering the flusher depends on: fails the instant the post-concat
// slices.SortFunc is dropped.
func TestShardedCache_CollectDirty_GloballySorted(t *testing.T) {
	c := NewShardedCache[int](1<<16, 16)
	ids := make([]snowflake.ID, 0, 3000)
	for i := 1; i <= 3000; i++ {
		ids = append(ids, snowflake.ID(i))
	}
	rng := rand.New(rand.NewSource(7))
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	for _, id := range ids {
		c.Put(id, int(id))
	}

	dirty := c.CollectDirty()
	if !slices.IsSortedFunc(dirty, func(a, b Entry[int]) int { return int(a.Key - b.Key) }) {
		t.Fatal("CollectDirty result is not globally sorted by Key — post-concat sort was dropped")
	}
}

// Test 2d — re-dirty protection (invariant C3, the subtle one). Put X (v1),
// collect v1, Put X again (v2 > v1), MarkFlushed{X:v1}: X must STAY dirty (next
// collect includes X with v2) and not be counted clean. Mirrors the un-sharded
// Cache behavior; catches per-shard nextVer / routing mismatch.
func TestShardedCache_MarkFlushed_RedirtyProtection(t *testing.T) {
	c := NewShardedCache[int](1<<16, 16)
	x := snowflake.ID(123456789) // a real-ish snowflake-shaped id

	c.Put(x, 1)
	dirty := c.CollectDirty()
	var v1 uint64
	for _, e := range dirty {
		if e.Key == x {
			v1 = e.DirtyVer
		}
	}
	if v1 == 0 {
		t.Fatal("X not collected as dirty after first Put")
	}

	c.Put(x, 2) // re-dirty: stamps v2 > v1 on X's shard
	c.MarkFlushed(map[snowflake.ID]uint64{x: v1})

	again := c.CollectDirty()
	var v2 uint64
	for _, e := range again {
		if e.Key == x {
			v2 = e.DirtyVer
		}
	}
	if v2 == 0 {
		t.Fatal("X was cleared by a stale-version MarkFlushed — re-dirty protection failed (write would be lost)")
	}
	if v2 <= v1 {
		t.Fatalf("re-dirty version v2=%d not greater than v1=%d", v2, v1)
	}
	if cc := c.CleanCount(); cc != 0 {
		t.Fatalf("CleanCount = %d, want 0 — re-dirtied X must not count clean", cc)
	}
}

// Test 2e — route stability. Put then MarkFlushed the same key; it must actually
// clear, proving indexFor is deterministic and MarkFlushed's bucketing routes to
// the same shard as Put. Sweeps many keys so a routing bug surfaces somewhere.
func TestShardedCache_RouteStability(t *testing.T) {
	c := NewShardedCache[int](1<<16, 16)
	for i := 1; i <= 2000; i++ {
		id := snowflake.ID(i * 0x100000001) // large, spread across the 64-bit space
		c.Put(id, i)
		dirty := c.CollectDirty()
		flushed := make(map[snowflake.ID]uint64, len(dirty))
		for _, e := range dirty {
			flushed[e.Key] = e.DirtyVer
		}
		c.MarkFlushed(flushed)
		if rem := c.CollectDirty(); len(rem) != 0 {
			t.Fatalf("key %d not cleared by MarkFlushed — Put/MarkFlushed routing diverged", id)
		}
	}
}

// Test 3 — equivalence-to-Cache oracle (differential). Run an identical scripted
// op sequence against a plain Cache and a ShardedCache with capacity >> key count
// (so nothing evicts and the documented shard-local LRU divergence cannot
// surface) and assert observable aggregates agree. Eviction-order-dependent
// quantities are intentionally NOT compared.
func TestShardedCache_EquivalentToCache_NoEviction(t *testing.T) {
	const cap = 1 << 16
	const keySpace = 1000
	const ops = 20000

	plain := NewCache[int](cap)
	sharded := NewShardedCache[int](cap, 16)
	rng := rand.New(rand.NewSource(99))

	checkpoint := func(step int) {
		// Observable per-key status must match for every key.
		for k := 1; k <= keySpace; k++ {
			id := snowflake.ID(k)
			_, ps := plain.Peek(id)
			_, ss := sharded.Peek(id)
			if ps != ss {
				t.Fatalf("step %d key %d: plain status %v != sharded status %v", step, k, ps, ss)
			}
		}
		if pl, sl := plain.Len(), sharded.Len(); pl != sl {
			t.Fatalf("step %d: plain Len %d != sharded Len %d", step, pl, sl)
		}
		if pc, sc := plain.CleanCount(), sharded.CleanCount(); pc != sc {
			t.Fatalf("step %d: plain CleanCount %d != sharded CleanCount %d", step, pc, sc)
		}
	}

	for i := 0; i < ops; i++ {
		id := snowflake.ID(rng.Intn(keySpace) + 1)
		switch rng.Intn(5) {
		case 0:
			plain.Put(id, i)
			sharded.Put(id, i)
		case 1:
			plain.MarkDeleted(id)
			sharded.MarkDeleted(id)
		case 2:
			plain.LoadClean(id, i)
			sharded.LoadClean(id, i)
		case 3:
			plain.Get(id)
			sharded.Get(id)
		case 4:
			// Flush both deterministically.
			flushBoth(plain, sharded)
		}
		if i%2000 == 0 {
			checkpoint(i)
		}
	}
	flushBoth(plain, sharded)
	checkpoint(ops)

	// Post-identical-flush, both must be fully clean.
	if d := sharded.CollectDirty(); len(d) != 0 {
		t.Fatalf("sharded has %d dirty after final flush, want 0", len(d))
	}
	if d := plain.CollectDirty(); len(d) != 0 {
		t.Fatalf("plain has %d dirty after final flush, want 0", len(d))
	}
}

func flushBoth(plain *Cache[int], sharded *ShardedCache[int]) {
	pd := plain.CollectDirty()
	pf := make(map[snowflake.ID]uint64, len(pd))
	for _, e := range pd {
		pf[e.Key] = e.DirtyVer
	}
	plain.MarkFlushed(pf)

	sd := sharded.CollectDirty()
	sf := make(map[snowflake.ID]uint64, len(sd))
	for _, e := range sd {
		sf[e.Key] = e.DirtyVer
	}
	sharded.MarkFlushed(sf)
}

// Test 4 — shard balance (hash quality). The adversarial input for a
// time-ordered key space is a MONOTONICALLY INCREASING id sequence, exactly what
// snowflakes produce. key%N or raw low-bit routing would cluster time-adjacent
// ids into few shards. Assert per-shard Len spread is within ±20% of total/N.
func TestShardedCache_ShardBalance_MonotonicIDs(t *testing.T) {
	const n = 16
	const total = 100_000
	c := NewShardedCache[int](1<<20, n) // capacity >> total: nothing evicts
	if len(c.shards) != n {
		t.Fatalf("expected %d shards, got %d", n, len(c.shards))
	}

	// Monotonically increasing, snowflake-shaped (timestamp in high bits, so the
	// sequence increments the high bits). Use a base offset + stride so the high
	// bits move, mimicking real time-ordered allocation.
	base := snowflake.ID(1 << 40)
	for i := 0; i < total; i++ {
		c.Put(base+snowflake.ID(i), i)
	}

	ideal := total / n
	low := ideal - ideal/5  // -20%
	high := ideal + ideal/5 // +20%
	for i, sh := range c.shards {
		l := sh.Len()
		if l < low || l > high {
			t.Fatalf("shard %d holds %d entries, outside ±20%% of ideal %d (avalanche hash failed to spread monotonic ids)", i, l, ideal)
		}
	}
}

// Test 5 — tiny-capacity clamp. capacity=4 with a 16-shard hint must NOT create
// 16 shards each floored to cap 1 (total 16, 4x the request). The clamp caps
// shards at capacity. Break test for the per-shard floor-to-1 explosion.
func TestShardedCache_TinyCapacityClamp(t *testing.T) {
	c := NewShardedCache[int](4, 16)
	if len(c.shards) > 4 {
		t.Fatalf("capacity=4 produced %d shards, want <= 4 (per-shard floor would explode total capacity)", len(c.shards))
	}
	if total := c.Cap(); total > 8 {
		t.Fatalf("aggregate Cap = %d, want close to 4 (clamp failed)", total)
	}
}

// Budget round-trip: aggregate Budget/Cap must equal the configured totals so the
// store's Clear() round-trip (newCache(old.Cap(), old.Budget())) is stable.
func TestShardedCache_BudgetCapRoundTrip(t *testing.T) {
	const cap = 80_000
	const budget = int64(64 << 20) // 64 MiB
	sizer := func(v int) int64 { return 8 }
	c := NewShardedCacheWithBudget(cap, budget, sizer, 16)

	// Aggregate Cap == configured (evenly divisible: 80000/16 = 5000 per shard).
	if got := c.Cap(); got != cap {
		t.Fatalf("aggregate Cap = %d, want %d", got, cap)
	}
	// Aggregate Budget == configured (64<<20 / 16 is even).
	if got := c.Budget(); got != budget {
		t.Fatalf("aggregate Budget = %d, want %d", got, budget)
	}
}
