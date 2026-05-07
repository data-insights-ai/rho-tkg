package locks

import (
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
)

// newTestGen creates a snowflake generator for testing.
// Mirrors the production layout (5 node bits, 10 step bits, microsecond precision).
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(snowflakepkg.Epoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}

// differentShardIDs returns two IDs guaranteed to map to different shards.
func differentShardIDs(t *testing.T) (snowflake.ID, snowflake.ID) {
	t.Helper()
	gen := newTestGen(t, 0)
	a := gen.Generate()
	sa := ShardIndex(a)
	for {
		b := gen.Generate()
		if ShardIndex(b) != sa {
			return a, b
		}
	}
}

// ─── Basic lock/unlock ──────────────────────────────────────────────────────

func TestEntityLockManagerSingleLock(t *testing.T) {
	t.Parallel()
	lm := NewManager()

	lm.LockEntity(snowflake.ID(42))
	lm.UnlockEntity(snowflake.ID(42))
	// No deadlock — test passes if we reach here.
}

func TestEntityLockManagerLockTwoDifferentShards(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	a, b := differentShardIDs(t)
	lm.LockTwo(a, b)
	lm.UnlockTwo(a, b)
}

func TestEntityLockManagerLockTwoSameShard(t *testing.T) {
	t.Parallel()
	lm := NewManager()

	// Same ID → same shard → single lock.
	id := snowflake.ID(99)
	lm.LockTwo(id, id)
	lm.UnlockTwo(id, id)
}

func TestEntityLockManagerLockTwoReverseOrder(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	a, b := differentShardIDs(t)
	lm.LockTwo(b, a)
	lm.UnlockTwo(b, a)
}

// ─── Shard index ────────────────────────────────────────────────────────────

func TestShardIndexRange(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	// Verify shard indices are always 0-255.
	for range 100 {
		si := ShardIndex(gen.Generate())
		if int(si) >= ShardCount {
			t.Fatalf("ShardIndex out of range: %d", si)
		}
	}
	// Edge case: zero and max IDs.
	for _, id := range []snowflake.ID{0, ^snowflake.ID(0)} {
		si := ShardIndex(id)
		if int(si) >= ShardCount {
			t.Fatalf("ShardIndex(%d) = %d, out of range", id, si)
		}
	}
}

func TestShardIndexDistribution(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	// Generate enough IDs to span 256+ microsecond ticks (1024 IDs per tick).
	// 256 consecutive time values cover all 256 shard indices.
	seen := make(map[uint8]struct{})
	for range 262_144 {
		seen[ShardIndex(gen.Generate())] = struct{}{}
	}
	if len(seen) != 256 {
		t.Errorf("%d/256 shards used, want 256", len(seen))
	}
}

// ─── Deadlock prevention ────────────────────────────────────────────────────

func TestEntityLockManagerNoDeadlock(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	a, b := differentShardIDs(t)

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
	lm := NewManager()
	gen := newTestGen(t, 0)

	// Pre-generate IDs for all goroutines.
	const goroutines = 50
	const opsPerGoroutine = 500
	allIDs := make([]snowflake.ID, goroutines*opsPerGoroutine*2)
	for i := range allIDs {
		allIDs[i] = gen.Generate()
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := range goroutines {
		go func(gid int) {
			defer wg.Done()
			base := gid * opsPerGoroutine * 2
			for i := range opsPerGoroutine {
				a := allIDs[base+i*2]
				b := allIDs[base+i*2+1]

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
	lm := NewManager()
	gen := newTestGen(t, 0)

	ids := []snowflake.ID{gen.Generate(), gen.Generate(), gen.Generate()}
	lm.LockMany(ids)
	lm.UnlockMany(ids)
}

func TestLockMany_SameShard(t *testing.T) {
	t.Parallel()
	lm := NewManager()

	// Two IDs in the same shard — dedup means single lock.
	id := snowflake.ID(99)
	ids := []snowflake.ID{id, id}
	lm.LockMany(ids)
	lm.UnlockMany(ids)
}

func TestLockMany_DeadlockFree(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	gen := newTestGen(t, 0)

	// Generate distinct IDs for two overlapping sets.
	ids := make([]snowflake.ID, 4)
	for i := range ids {
		ids[i] = gen.Generate()
	}
	setA := []snowflake.ID{ids[0], ids[1], ids[2]}
	setB := []snowflake.ID{ids[2], ids[1], ids[3]}

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
	lm := NewManager()
	a, b := differentShardIDs(t)
	ids := []snowflake.ID{a, b}

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
	lm := NewManager()

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
