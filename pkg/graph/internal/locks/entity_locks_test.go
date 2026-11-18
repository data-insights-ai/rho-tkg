package locks

import (
	"slices"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
)

// The exact-shard tests below prove which mutexes each Lock* call holds and that
// the matching Unlock* releases them. The NoDeadlock tests fail only by hanging
// (go test -timeout), and the MutualExclusion counters reliably catch a broken
// lock only under -race.

// idAtTick returns an ID whose timestamp is tick microseconds after the epoch
// (node and step bits zero), so its shard is tick mod 256 by construction,
// independent of the wall clock.
func idAtTick(tick int64) snowflake.ID {
	return snowflakepkg.Layout.MinIDForTime(snowflakepkg.Layout.Epoch().Add(time.Duration(tick) * time.Microsecond))
}

// differentShardIDs returns two IDs on shards 3 and 200.
func differentShardIDs() (snowflake.ID, snowflake.ID) {
	return idAtTick(3), idAtTick(200)
}

// threeDifferentShardIDs returns three IDs on shards 3, 77 and 200.
func threeDifferentShardIDs() (snowflake.ID, snowflake.ID, snowflake.ID) {
	return idAtTick(3), idAtTick(77), idAtTick(200)
}

// heldShards returns the indices of the mutexes in mus that are locked, probing
// each with TryLock (and releasing the probe at once).
func heldShards(mus []sync.Mutex) []int {
	var held []int
	for i := range mus {
		if mus[i].TryLock() {
			mus[i].Unlock()
			continue
		}
		held = append(held, i)
	}
	return held
}

// ─── Exact shards held ──────────────────────────────────────────────────────

// TestManagerLocksExactShards proves each Lock* door holds exactly the
// deduplicated shards of its IDs (a duplicate shard locked twice would hang
// on the non-reentrant mutex) and its Unlock* twin releases all of them.
func TestManagerLocksExactShards(t *testing.T) {
	t.Parallel()
	a, b, c := idAtTick(3), idAtTick(77), idAtTick(200)
	aSameShard := idAtTick(3 + ShardCount) // a different ID on a's shard

	cases := []struct {
		name   string
		lock   func(*Manager)
		unlock func(*Manager)
		want   []int
	}{
		{"LockEntity", func(m *Manager) { m.LockEntity(b) }, func(m *Manager) { m.UnlockEntity(b) }, []int{77}},
		{"LockTwo same id", func(m *Manager) { m.LockTwo(a, a) }, func(m *Manager) { m.UnlockTwo(a, a) }, []int{3}},
		{"LockTwo same shard", func(m *Manager) { m.LockTwo(a, aSameShard) }, func(m *Manager) { m.UnlockTwo(a, aSameShard) }, []int{3}},
		{"LockTwo descending", func(m *Manager) { m.LockTwo(c, a) }, func(m *Manager) { m.UnlockTwo(c, a) }, []int{3, 200}},
		{"LockThree same id", func(m *Manager) { m.LockThree(a, a, a) }, func(m *Manager) { m.UnlockThree(a, a, a) }, []int{3}},
		{"LockThree two shards", func(m *Manager) { m.LockThree(c, a, aSameShard) }, func(m *Manager) { m.UnlockThree(c, a, aSameShard) }, []int{3, 200}},
		{"LockThree three shards", func(m *Manager) { m.LockThree(c, a, b) }, func(m *Manager) { m.UnlockThree(c, a, b) }, []int{3, 77, 200}},
		{"LockMany empty", func(m *Manager) { m.LockMany(nil) }, func(m *Manager) { m.UnlockMany(nil) }, nil},
		{"LockMany same shard", func(m *Manager) { m.LockMany([]snowflake.ID{a, aSameShard, a}) }, func(m *Manager) { m.UnlockMany([]snowflake.ID{a, aSameShard, a}) }, []int{3}},
		{"LockMany three shards", func(m *Manager) { m.LockMany([]snowflake.ID{c, a, b, a}) }, func(m *Manager) { m.UnlockMany([]snowflake.ID{c, a, b, a}) }, []int{3, 77, 200}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lm := NewManager()
			tc.lock(lm)
			if got := heldShards(lm.shards[:]); !slices.Equal(got, tc.want) {
				t.Fatalf("held after lock = %v, want %v", got, tc.want)
			}
			tc.unlock(lm)
			if got := heldShards(lm.shards[:]); len(got) != 0 {
				t.Fatalf("held after unlock = %v, want none", got)
			}
		})
	}
}

// ─── Shard index ────────────────────────────────────────────────────────────

// TestShardIndexIsLowTimeByte proves ShardIndex is the low 8 bits of the
// timestamp: every tick of two full rounds maps to tick mod 256, whatever the
// node and step bits hold, so all 256 shards are reached and tick 256 wraps.
func TestShardIndexIsLowTimeByte(t *testing.T) {
	t.Parallel()
	for tick := int64(0); tick < 2*ShardCount; tick++ {
		at := snowflakepkg.Layout.Epoch().Add(time.Duration(tick) * time.Microsecond)
		want := uint8(tick % ShardCount)
		if got := ShardIndex(snowflakepkg.Layout.MinIDForTime(at)); got != want {
			t.Fatalf("ShardIndex(min id at tick %d) = %d, want %d", tick, got, want)
		}
		if got := ShardIndex(snowflakepkg.Layout.MaxIDForTime(at)); got != want {
			t.Fatalf("ShardIndex(max id at tick %d) = %d, want %d", tick, got, want)
		}
	}
}

// ─── Deadlock prevention ────────────────────────────────────────────────────

func TestEntityLockManagerNoDeadlock(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	a, b := differentShardIDs()

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

func TestEntityLockManagerLockThreeNoDeadlock(t *testing.T) {
	t.Parallel()
	lm := NewManager()
	a, b, c := threeDifferentShardIDs()

	const iterations = 1000
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockThree(a, b, c)
			lm.UnlockThree(a, b, c)
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			lm.LockThree(c, b, a)
			lm.UnlockThree(c, b, a)
		}
	}()

	wg.Wait()
}

// ─── Concurrent stress ─────────────────────────────────────────────────────

func TestEntityLockManagerConcurrentStress(t *testing.T) {
	t.Parallel()
	lm := NewManager()

	// Pre-build IDs for all goroutines, one tick apart so they cycle all shards.
	const goroutines = 50
	const opsPerGoroutine = 500
	allIDs := make([]snowflake.ID, goroutines*opsPerGoroutine*2)
	for i := range allIDs {
		allIDs[i] = idAtTick(int64(i))
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

func TestLockMany_DeadlockFree(t *testing.T) {
	t.Parallel()
	lm := NewManager()

	// Four IDs on four shards for two overlapping sets.
	ids := []snowflake.ID{idAtTick(10), idAtTick(20), idAtTick(30), idAtTick(40)}
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
	a, b := differentShardIDs()
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
