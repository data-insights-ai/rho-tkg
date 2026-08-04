package core

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestShardedRWMutexExcludesReaders is the gold-standard correctness probe: a
// writer mutates a PLAIN (non-atomic) int under Lock while many readers read it
// under RLockShard on varied stripes. If the striped mutex ever let a reader's
// critical section overlap the writer's, -race fires here. Run with
// `go test -race -run TestShardedRWMutexExcludesReaders`.
func TestShardedRWMutexExcludesReaders(t *testing.T) {
	var m shardedRWMutex
	var shared int // deliberately non-atomic — protected only by m
	var wg sync.WaitGroup

	const writers = 4
	const readers = 16
	const iters = 2000

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				m.Lock()
				shared++ // sole mutation, must be fully excluded
				m.Unlock()
			}
		}()
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Vary the stripe across the whole range, including via the
				// drop-in stripe-0 RLock and via RLockShard.
				if i%2 == 0 {
					tok := m.RLockShard(uint(id + i))
					_ = shared // read under the lock
					m.RUnlockShard(tok)
				} else {
					m.RLock()
					_ = shared
					m.RUnlock()
				}
			}
		}(r)
	}
	wg.Wait()

	if shared != writers*iters {
		t.Fatalf("shared = %d, want %d (lost writer updates => broken exclusion)", shared, writers*iters)
	}
}

// TestShardedRWMutexWriterSeesNoReaders asserts the exclusion invariant
// directly with an atomic reader counter: while a writer holds Lock, the number
// of readers in their critical section must be exactly zero.
func TestShardedRWMutexWriterSeesNoReaders(t *testing.T) {
	var m shardedRWMutex
	var activeReaders atomic.Int64
	var violations atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: assert no readers are active inside the exclusive section.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				m.Lock()
				if n := activeReaders.Load(); n != 0 {
					violations.Add(1)
				}
				m.Unlock()
			}
		}()
	}
	// Readers on many stripes: bump the active count inside the read section.
	for r := 0; r < 12; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				tok := m.RLockShard(uint(id*7 + n))
				activeReaders.Add(1)
				activeReaders.Add(-1)
				m.RUnlockShard(tok)
				n++
			}
		}(r)
	}

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	if v := violations.Load(); v != 0 {
		t.Fatalf("writer observed active readers %d times (exclusion broken)", v)
	}
}

// TestShardedRWMutexReadersDoNotExcludeAcrossStripes verifies the whole point of
// striping: two readers on DIFFERENT stripes can be inside their critical
// sections simultaneously. A single sync.RWMutex would also allow this, but this
// guards against a regression to an accidental single-stripe implementation
// where reader B waits for reader A.
func TestShardedRWMutexReadersDoNotExcludeAcrossStripes(t *testing.T) {
	var m shardedRWMutex
	bothIn := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup

	enter := func(hint uint) {
		defer wg.Done()
		tok := m.RLockShard(hint)
		defer m.RUnlockShard(tok)
		// Signal arrival, then hold until both readers have arrived.
		bothIn <- struct{}{}
		<-release
	}
	wg.Add(2)
	go enter(1)
	go enter(2)

	// Both readers must be able to report arrival while both still hold their
	// stripes. If the second blocked behind the first, this times out.
	timeout := time.After(2 * time.Second)
	for got := 0; got < 2; got++ {
		select {
		case <-bothIn:
		case <-timeout:
			t.Fatal("second reader on a different stripe blocked behind the first")
		}
	}
	close(release)
	wg.Wait()
}

// TestShardedRWMutexRLockShardPairing checks the stripe-index contract:
// RLockShard returns the resolved stripe (hint masked into range) and distinct
// hints landing on the same stripe still pair correctly.
func TestShardedRWMutexRLockShardPairing(t *testing.T) {
	var m shardedRWMutex
	// hint and hint+stripes map to the same stripe; both must lock/unlock cleanly.
	tok := m.RLockShard(5)
	if tok != 5 {
		t.Fatalf("RLockShard(5) = %d, want 5", tok)
	}
	m.RUnlockShard(tok)

	tok2 := m.RLockShard(5 + shardedRWMutexStripes)
	if tok2 != 5 {
		t.Fatalf("RLockShard(5+stripes) = %d, want 5 (wrap)", tok2)
	}
	m.RUnlockShard(tok2)

	// A writer after all readers released must acquire immediately.
	done := make(chan struct{})
	go func() {
		// The empty critical section IS the assertion: a writer must ACQUIRE
		// immediately once all readers released (SA2001).
		m.Lock()
		m.Unlock() //nolint:staticcheck // see above
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writer blocked after all readers released")
	}
}
