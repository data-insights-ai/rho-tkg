package locks

import (
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestValueStripeGoldenVectors pins the EXACT stripe values ValueStripe
// produces for a fixed set of inputs — captured from the earlier
// fnv.New64a()-based implementation before it was replaced with an inline
// FNV-1a accumulator (avoiding the heap-allocating hash.Hash64 interface on
// every call). "Deterministic across process restarts" is a documented
// contract (see ValueStripe's doc comment); a self-consistency check (call
// twice, compare) cannot catch an algorithm-drift bug in a REWRITE the way
// a golden vector can — this test is what proves the inline rewrite is
// byte-for-byte equivalent to the original, not merely "still
// self-consistent within itself".
func TestValueStripeGoldenVectors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		labelToken, keyToken uint16
		value                []byte
		want                 uint8
	}{
		{3, 9, []byte("s:alice@example.com"), 219},
		{1, 1, []byte("s:aaa"), 117},
		{2, 2, []byte("s:bbb"), 2},
		{0, 0, []byte{}, 245},
		{65535, 65535, []byte("x"), 171},
	}
	for _, c := range cases {
		if got := ValueStripe(c.labelToken, c.keyToken, c.value); got != c.want {
			t.Fatalf("ValueStripe(%d, %d, %q) = %d, want %d (golden vector — algorithm drift?)",
				c.labelToken, c.keyToken, c.value, got, c.want)
		}
	}
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
	if !slices.Equal(ordered, []uint8{5, 42, 200}) {
		t.Fatalf("LockStripes = %v, want [5 42 200]", ordered)
	}
	if got := heldShards(vm.shards[:]); !slices.Equal(got, []int{5, 42, 200}) {
		t.Fatalf("held after LockStripes = %v, want [5 42 200]", got)
	}
	vm.UnlockStripes(ordered)
	if got := heldShards(vm.shards[:]); len(got) != 0 {
		t.Fatalf("held after UnlockStripes = %v, want none", got)
	}
}

// LockStripesExcept is the concurrency-critical skip-already-held-stripe path
// GetOrCreateByKey relies on to create a node while holding the keyed value's
// stripe without self-deadlocking (a sync.Mutex is not reentrant; re-locking a
// held stripe from the same goroutine hangs forever).

// TestLockStripesExceptEmptyHeldDelegatesToLockStripes pins the documented
// "when held is empty this is exactly LockStripes" contract: same
// ascending-order, deduplicated behavior.
func TestLockStripesExceptEmptyHeldDelegatesToLockStripes(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	kept := vm.LockStripesExcept([]uint8{200, 5, 200, 5, 42}, nil)
	want := []uint8{5, 42, 200}
	if !slices.Equal(kept, want) {
		t.Fatalf("kept = %v, want %v", kept, want)
	}
	if got := heldShards(vm.shards[:]); !slices.Equal(got, []int{5, 42, 200}) {
		t.Fatalf("held = %v, want [5 42 200]", got)
	}
	vm.UnlockStripes(kept)
}

// TestLockStripesExceptSkipsHeldStripes mirrors the call shape GetOrCreateByKey
// uses: the caller already holds the keyed value's stripes, and the
// constraint-check set names them again (a hash collision, or the same key in
// several constrained tuples). A held stripe must be left out of the returned
// list and never re-locked (a sync.Mutex is not reentrant, so a broken skip
// hangs; the call runs on a guarded goroutine so that fails the test), and every
// other requested stripe must really be locked until UnlockStripes.
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

	done := make(chan []uint8, 1)
	go func() { done <- vm.LockStripesExcept([]uint8{100, 5, 10, 42, 5}, held) }()
	var kept []uint8
	select {
	case kept = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("LockStripesExcept deadlocked re-locking an already-held stripe")
	}
	if !slices.Equal(kept, []uint8{10, 100}) {
		t.Fatalf("kept = %v, want [10 100]", kept)
	}
	if got := heldShards(vm.shards[:]); !slices.Equal(got, []int{5, 10, 42, 100}) {
		t.Fatalf("held after LockStripesExcept = %v, want [5 10 42 100]", got)
	}
	vm.UnlockStripes(kept)
	if got := heldShards(vm.shards[:]); !slices.Equal(got, []int{5, 42}) {
		t.Fatalf("held after UnlockStripes = %v, want only the caller's [5 42]", got)
	}
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
	if got := heldShards(vm.shards[:]); !slices.Equal(got, []int{5, 42}) {
		t.Fatalf("held = %v, want only the caller's [5 42]", got)
	}
	vm.UnlockStripes(kept) // no-op over an empty slice — must not panic
}

// Two updates crossing the same pair of value stripes in opposite argument
// order must not deadlock, because LockStripes sorts before acquiring.
func TestLockStripesCrossPairNoDeadlock(t *testing.T) {
	t.Parallel()
	vm := NewValueManager()
	a := ValueStripe(1, 1, []byte("s:aaa"))
	b := ValueStripe(2, 2, []byte("s:bbb")) // 117 and 2: TestValueStripeGoldenVectors pins both
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
