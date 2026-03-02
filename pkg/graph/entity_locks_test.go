package graph

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// ─── Basic lock/unlock ──────────────────────────────────────────────────────

func TestEntityLockManagerSingleLock(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	lm.LockEntity(snowflake.ID(42))
	lm.UnlockEntity(snowflake.ID(42))
	// No deadlock — test passes if we reach here.
}

func TestEntityLockManagerLockTwoDifferentShards(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Choose IDs that map to different shards. shardIndex uses bits 22-29
	// (low 8 bits of the timestamp field), so shift values above bit 22.
	a := snowflake.ID(0 << 22) // shard 0
	b := snowflake.ID(1 << 22) // shard 1

	lm.LockTwo(a, b)
	lm.UnlockTwo(a, b)
}

func TestEntityLockManagerLockTwoSameShard(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Same ID → same shard → single lock.
	id := snowflake.ID(99)
	lm.LockTwo(id, id)
	lm.UnlockTwo(id, id)
}

func TestEntityLockManagerLockTwoReverseOrder(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// LockTwo should normalize order, so (b, a) == (a, b).
	a := snowflake.ID(0 << 22)
	b := snowflake.ID(1 << 22)

	lm.LockTwo(b, a)
	lm.UnlockTwo(b, a)
}

// ─── Shard index ────────────────────────────────────────────────────────────

func TestShardIndexRange(t *testing.T) {
	t.Parallel()

	// Verify shard indices are always 0-255.
	// Values must place bits in the timestamp region (bits 22+) for meaningful shard diversity.
	for _, id := range []snowflake.ID{0, 1 << 22, 255 << 22, 256 << 22, 1000 << 22, ^snowflake.ID(0)} {
		si := shardIndex(id)
		if int(si) >= entityLockShards {
			t.Fatalf("shardIndex(%d) = %d, out of range", id, si)
		}
	}
}

func TestShardIndexDistribution(t *testing.T) {
	t.Parallel()

	// Entities created at different millisecond timestamps should distribute
	// across shards. With the old step-based sharding, all of these would
	// map to shard 0 (step=0 for each).
	seen := make(map[uint8]struct{})
	for ms := int64(0); ms < 256; ms++ {
		id := snowflake.ID(ms << 22) // ms in timestamp field, step=0, node=0
		seen[shardIndex(id)] = struct{}{}
	}
	if len(seen) != 256 {
		t.Errorf("256 consecutive ms timestamps used %d/256 shards, want 256", len(seen))
	}
}

// ─── Deadlock prevention ────────────────────────────────────────────────────

func TestEntityLockManagerNoDeadlock(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Two IDs in different shards (timestamp bits determine shard).
	a := snowflake.ID(0 << 22) // shard 0
	b := snowflake.ID(1 << 22) // shard 1

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: locks (a, b)
	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockTwo(a, b)
			lm.UnlockTwo(a, b)
		}
	}()

	// Goroutine 2: locks (b, a) — same shards, reversed.
	// If LockTwo doesn't normalize order, this deadlocks.
	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockTwo(b, a)
			lm.UnlockTwo(b, a)
		}
	}()

	wg.Wait()
}

// ─── Concurrent stress ─────────────────────────────────────────────────────

func TestEntityLockManagerConcurrentStress(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	const goroutines = 50
	const opsPerGoroutine = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(gid int) {
			defer wg.Done()
			for i := range opsPerGoroutine {
				a := snowflake.ID(int64(gid*100+i) << 22)
				b := snowflake.ID(int64(gid*100+i+1) << 22)

				if i%2 == 0 {
					lm.LockEntity(a)
					lm.UnlockEntity(a)
				} else {
					lm.LockTwo(a, b)
					lm.UnlockTwo(a, b)
				}
			}
		}(g)
	}
	wg.Wait()
}

// ─── LockMany / UnlockMany ──────────────────────────────────────────────────

func TestLockMany_Basic(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// 3 IDs across different shards — no deadlock.
	ids := []snowflake.ID{
		snowflake.ID(0 << 22),
		snowflake.ID(1 << 22),
		snowflake.ID(2 << 22),
	}
	lm.LockMany(ids)
	lm.UnlockMany(ids)
}

func TestLockMany_SameShard(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Multiple IDs mapping to same shard — dedup means single lock.
	ids := []snowflake.ID{
		snowflake.ID(0 << 22),
		snowflake.ID(256 << 22), // wraps back to shard 0
	}
	lm.LockMany(ids)
	lm.UnlockMany(ids)
}

func TestLockMany_DeadlockFree(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Two goroutines with overlapping ID sets — ascending shard order prevents deadlock.
	setA := []snowflake.ID{snowflake.ID(0 << 22), snowflake.ID(1 << 22), snowflake.ID(2 << 22)}
	setB := []snowflake.ID{snowflake.ID(2 << 22), snowflake.ID(1 << 22), snowflake.ID(3 << 22)}

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockMany(setA)
			lm.UnlockMany(setA)
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockMany(setB)
			lm.UnlockMany(setB)
		}
	}()

	wg.Wait()
}

func TestLockMany_MutualExclusion(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Shared counter protected by LockMany — must be correct.
	ids := []snowflake.ID{snowflake.ID(0 << 22), snowflake.ID(1 << 22)}
	counter := 0
	const total = 10000

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			for range total / 2 {
				lm.LockMany(ids)
				counter++
				lm.UnlockMany(ids)
			}
		}()
	}
	wg.Wait()

	if counter != total {
		t.Fatalf("expected %d, got %d — mutual exclusion violated", total, counter)
	}
}

// ─── Mutual exclusion correctness ───────────────────────────────────────────

func TestEntityLockManagerMutualExclusion(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	id := snowflake.ID(42)
	counter := 0
	const total = 10000

	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			for range total / 2 {
				lm.LockEntity(id)
				counter++
				lm.UnlockEntity(id)
			}
		}()
	}
	wg.Wait()

	if counter != total {
		t.Fatalf("expected %d, got %d — mutual exclusion violated", total, counter)
	}
}
