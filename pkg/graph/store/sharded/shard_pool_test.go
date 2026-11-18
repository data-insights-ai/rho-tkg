package sharded

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRunShardPoolBoundsConcurrency covers the rule that fan-out
// helpers (forEachShardErr, fanOutUniform, fanOutUniformCreate, parallelShards)
// must use a bounded worker pool, not one goroutine per shard, so a large shard
// count cannot translate into unbounded scheduler/memory pressure. Drives
// runShardPool with more tasks (n=24) than maxShardWorkers: exactly
// maxShardWorkers bodies run at once (the first ones park until released),
// no more ever overlap (an in-flight high-water mark), the bodies run on at
// most maxShardWorkers goroutines, and every task completes. The bound is
// decided by counts, not a quiet period; the timeouts only turn a hang into a
// failure.
func TestRunShardPoolBoundsConcurrency(t *testing.T) {
	t.Parallel()
	const n = 24 // > maxShardWorkers, exercises the pool boundary
	if n <= maxShardWorkers {
		t.Fatalf("test setup invalid: n (%d) must exceed maxShardWorkers (%d)", n, maxShardWorkers)
	}

	var inFlight, peak atomic.Int32
	var mu sync.Mutex
	goroutines := make(map[string]bool)
	ran := make([]int, n)
	entered := make(chan int, n)
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		runShardPool(n, func(idx int) {
			cur := inFlight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			mu.Lock()
			goroutines[goid()] = true
			ran[idx]++
			mu.Unlock()
			entered <- idx
			<-release
			inFlight.Add(-1)
		})
		close(done)
	}()

	// maxShardWorkers bodies enter and park: that many run concurrently.
	timeout := time.After(10 * time.Second)
	for seen := 0; seen < maxShardWorkers; seen++ {
		select {
		case <-entered:
		case <-timeout:
			t.Fatalf("only %d/%d workers entered concurrently within timeout", seen, maxShardWorkers)
		}
	}
	close(release)
	select {
	case <-done:
	case <-timeout:
		t.Fatal("runShardPool did not return after release")
	}

	if got := peak.Load(); got != maxShardWorkers {
		t.Fatalf("peak in-flight bodies = %d, want maxShardWorkers (%d)", got, maxShardWorkers)
	}
	if len(goroutines) > maxShardWorkers {
		t.Fatalf("bodies ran on %d goroutines, want at most maxShardWorkers (%d)", len(goroutines), maxShardWorkers)
	}
	for idx, c := range ran {
		if c != 1 {
			t.Fatalf("index %d ran %d times, want 1", idx, c)
		}
	}
}
