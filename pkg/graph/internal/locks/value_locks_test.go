package locks

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

func TestValueStripeInRange(t *testing.T) {
	t.Parallel()
	for i := 0; i < 1000; i++ {
		s := ValueStripe(uint16(i), uint16(i*7), []byte{byte(i), byte(i >> 8)})
		if int(s) >= ValueShardCount {
			t.Fatalf("stripe %d out of range [0,%d)", s, ValueShardCount)
		}
	}
}

func TestValueStripeDeterministic(t *testing.T) {
	t.Parallel()
	a := ValueStripe(3, 9, []byte("s:alice@example.com"))
	b := ValueStripe(3, 9, []byte("s:alice@example.com"))
	if a != b {
		t.Fatalf("stripe not deterministic: %d != %d", a, b)
	}
	// Different value bytes should (very likely) not always collide; assert at
	// least that the identity above holds and a distinct triple can differ.
	c := ValueStripe(3, 9, []byte("s:bob@example.com"))
	_ = c // collision is possible but the identity invariant above is the contract
}

// Same-value create storm: N goroutines contend on ONE value stripe. The stripe
// must serialize them so a "check index then write" critical section admits
// exactly one at a time (no concurrent occupancy).
func TestValueStripeSerializesSameValue(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()

	const goroutines = 100
	var inside atomic.Int32
	var maxInside atomic.Int32
	var wg sync.WaitGroup
	wg.Add(goroutines)
	value := []byte("s:same@example.com")
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s := vm.LockValue(7, 11, value)
			defer vm.UnlockStripe(s)
			cur := inside.Add(1)
			for {
				m := maxInside.Load()
				if cur <= m || maxInside.CompareAndSwap(m, cur) {
					break
				}
			}
			inside.Add(-1)
		}()
	}
	wg.Wait()
	if got := maxInside.Load(); got != 1 {
		t.Fatalf("max concurrent occupancy = %d, want 1 (stripe did not serialize)", got)
	}
}

// The classic interleaving from the ADR risk section: a two-phase delete
// (LockMany over entity locks) frees a value while a create waits on that
// value's stripe. This asserts the global entity -> value order is deadlock
// free: the "delete" goroutine takes entity locks FIRST then the value stripe;
// the "create" goroutine takes ONLY the value stripe. Neither can form a cycle.
func TestEntityThenValueOrderNoDeadlock(t *testing.T) {
	t.Parallel()
	em := NewManager()
	vm := NewValueManager()

	ids := []snowflake.ID{100, 200, 300}
	value := []byte("s:freed@example.com")

	const rounds = 500
	var wg sync.WaitGroup
	wg.Add(2)

	// "delete": entity locks (LockMany) then the value stripe, ascending order.
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			em.LockMany(ids)
			s := vm.LockValue(1, 2, value)
			vm.UnlockStripe(s)
			em.UnlockMany(ids)
		}
	}()

	// "create": only the value stripe (no entity lock — fresh generated ID).
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			s := vm.LockValue(1, 2, value)
			vm.UnlockStripe(s)
		}
	}()

	wg.Wait() // hangs (test timeout) on a deadlock; completes otherwise.
}

func TestLockStripesAscendingDedup(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	// Deliberately duplicate + unsorted stripe indices.
	ordered := vm.LockStripes([]uint8{200, 5, 200, 5, 42})
	if len(ordered) != 3 {
		t.Fatalf("LockStripes dedup = %d stripes, want 3", len(ordered))
	}
	for i := 1; i < len(ordered); i++ {
		if ordered[i-1] >= ordered[i] {
			t.Fatalf("LockStripes not ascending: %v", ordered)
		}
	}
	vm.UnlockStripes(ordered)

	// Re-lock proves everything was released (would deadlock/hang otherwise).
	ordered2 := vm.LockStripes([]uint8{5, 42, 200})
	vm.UnlockStripes(ordered2)
}

// BACKLOG 15c: LockStripesExcept had zero direct or indirect test coverage —
// the concurrency-critical skip-already-held-stripe path GetOrCreateByKey
// relies on to create a node while holding the keyed value's stripe without
// self-deadlocking (a sync.Mutex is not reentrant; re-locking a held stripe
// from the same goroutine hangs forever).

// TestLockStripesExceptEmptyHeldDelegatesToLockStripes pins the documented
// "when held is empty this is exactly LockStripes" contract: same
// ascending-order, deduplicated behavior.
func TestLockStripesExceptEmptyHeldDelegatesToLockStripes(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	kept := vm.LockStripesExcept([]uint8{200, 5, 200, 5, 42}, nil)
	want := []uint8{5, 42, 200}
	if len(kept) != len(want) {
		t.Fatalf("kept = %v, want %v", kept, want)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("kept = %v, want %v", kept, want)
		}
	}
	vm.UnlockStripes(kept)
}

// TestLockStripesExceptSkipsHeldStripes proves the skip set is honored: a
// held stripe is excluded from BOTH the returned "kept" list AND from being
// re-locked — verified by actually holding it (not merely simulating) and
// confirming LockStripesExcept still returns promptly with the held stripe
// excluded.
func TestLockStripesExceptSkipsHeldStripes(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	held := []uint8{5, 42}
	for _, s := range held {
		vm.shards[s].Lock()
	}
	defer func() {
		for _, s := range held {
			vm.shards[s].Unlock()
		}
	}()

	kept := vm.LockStripesExcept([]uint8{5, 10, 42, 100}, held)
	want := []uint8{10, 100}
	if len(kept) != len(want) {
		t.Fatalf("kept = %v, want %v", kept, want)
	}
	for i := range want {
		if kept[i] != want[i] {
			t.Fatalf("kept = %v, want %v", kept, want)
		}
	}
	vm.UnlockStripes(kept)
}

// TestLockStripesExceptAllRequestedAreHeld proves the degenerate case: every
// requested stripe is already held, so nothing new is locked and an empty
// (non-nil-vs-nil is not asserted; only emptiness matters) slice is returned.
func TestLockStripesExceptAllRequestedAreHeld(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	held := []uint8{5, 42}
	for _, s := range held {
		vm.shards[s].Lock()
	}
	defer func() {
		for _, s := range held {
			vm.shards[s].Unlock()
		}
	}()

	kept := vm.LockStripesExcept([]uint8{42, 5, 5}, held)
	if len(kept) != 0 {
		t.Fatalf("kept = %v, want empty (every requested stripe already held)", kept)
	}
	vm.UnlockStripes(kept) // no-op over an empty slice — must not panic
}

// TestLockStripesExceptDoesNotDeadlockOnHeldStripe is the direct regression
// guard for the bug this function exists to prevent: if the skip logic were
// ever broken (e.g. an off-by-one, or comparing the wrong stripe set),
// LockStripesExcept would try to re-lock a stripe THIS SAME GOROUTINE already
// holds via a non-reentrant sync.Mutex — an unconditional self-deadlock, not
// a race requiring -race or multiple goroutines to observe. Run on a
// timeout-guarded goroutine so a regression fails the test instead of hanging
// the whole suite.
func TestLockStripesExceptDoesNotDeadlockOnHeldStripe(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	const heldStripe = 77
	vm.shards[heldStripe].Lock()
	defer vm.shards[heldStripe].Unlock()

	done := make(chan []uint8, 1)
	go func() {
		// heldStripe appears in BOTH stripes and held — must be skipped, not
		// re-locked, by the same goroutine that already holds it.
		done <- vm.LockStripesExcept([]uint8{heldStripe, 1, 2}, []uint8{heldStripe})
	}()

	select {
	case kept := <-done:
		want := []uint8{1, 2}
		if len(kept) != len(want) || kept[0] != want[0] || kept[1] != want[1] {
			t.Fatalf("kept = %v, want %v", kept, want)
		}
		vm.UnlockStripes(kept)
	case <-time.After(2 * time.Second):
		t.Fatal("LockStripesExcept deadlocked re-locking an already-held stripe")
	}
}

// TestLockStripesExceptGetOrCreateByKeyShape mirrors the real call shape
// GetOrCreateByKey uses: the caller already holds the KEYED value's stripe
// (taken before this call, exactly like enforceUniqueForNodeHeld's caller),
// and the constraint-check set may or may not include that same stripe again
// (a hash collision, or the same key appearing in multiple constrained
// tuples). Either way, the held stripe must never be double-locked, and every
// OTHER stripe must be genuinely acquired (verified via a concurrent
// contender that only succeeds after UnlockStripes releases them).
func TestLockStripesExceptGetOrCreateByKeyShape(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	const keyedStripe = 11
	vm.shards[keyedStripe].Lock()
	defer vm.shards[keyedStripe].Unlock()

	kept := vm.LockStripesExcept([]uint8{keyedStripe, 22, 33}, []uint8{keyedStripe})
	if len(kept) != 2 || kept[0] != 22 || kept[1] != 33 {
		t.Fatalf("kept = %v, want [22 33]", kept)
	}

	// A concurrent contender for stripe 22 must block until UnlockStripes runs.
	acquired := make(chan struct{})
	go func() {
		vm.shards[22].Lock()
		close(acquired)
		vm.shards[22].Unlock()
	}()
	select {
	case <-acquired:
		t.Fatal("stripe 22 was not actually locked by LockStripesExcept — contender acquired it too early")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	vm.UnlockStripes(kept)
	select {
	case <-acquired:
		// expected: contender now proceeds
	case <-time.After(2 * time.Second):
		t.Fatal("contender never acquired stripe 22 after UnlockStripes — stripe leaked locked")
	}
}

// Two updates crossing the same pair of value stripes in opposite argument
// order must not deadlock, because LockStripes sorts before acquiring.
func TestLockStripesCrossPairNoDeadlock(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	a := ValueStripe(1, 1, []byte("s:aaa"))
	b := ValueStripe(2, 2, []byte("s:bbb"))
	if a == b {
		t.Skip("stripes collided; pick different values")
	}

	const rounds = 1000
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			o := vm.LockStripes([]uint8{a, b})
			vm.UnlockStripes(o)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			o := vm.LockStripes([]uint8{b, a})
			vm.UnlockStripes(o)
		}
	}()
	wg.Wait()
}
