package locks

import (
	"sync"
	"sync/atomic"
	"testing"

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
