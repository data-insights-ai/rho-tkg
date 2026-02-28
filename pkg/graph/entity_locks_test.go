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

	// Choose IDs that definitely map to different shards.
	a := snowflake.ID(0) // shard 0
	b := snowflake.ID(1) // shard 1

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
	a := snowflake.ID(0)
	b := snowflake.ID(1)

	lm.LockTwo(b, a)
	lm.UnlockTwo(b, a)
}

// ─── Shard index ────────────────────────────────────────────────────────────

func TestShardIndexRange(t *testing.T) {
	t.Parallel()

	// Verify shard indices are always 0-255.
	for _, id := range []snowflake.ID{0, 1, 255, 256, 1000, ^snowflake.ID(0)} {
		si := shardIndex(id)
		if int(si) >= entityLockShards {
			t.Fatalf("shardIndex(%d) = %d, out of range", id, si)
		}
	}
}

// ─── Deadlock prevention ────────────────────────────────────────────────────

func TestEntityLockManagerNoDeadlock(t *testing.T) {
	t.Parallel()
	lm := newEntityLockManager()

	// Find two IDs in different shards.
	a := snowflake.ID(0) // shard 0
	b := snowflake.ID(1) // shard 1

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
				a := snowflake.ID(gid*100 + i)
				b := snowflake.ID(gid*100 + i + 1)

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
