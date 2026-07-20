package sharded

import (
	"testing"
	"time"
)

// TestRunShardPoolBoundsConcurrency covers BACKLOG 20k / lesson 8: fan-out
// helpers (forEachShardErr, fanOutUniform, fanOutUniformCreate, parallelShards)
// must use a bounded worker pool, not one goroutine per shard, so a large shard
// count cannot translate into unbounded scheduler/memory pressure. Drives
// runShardPool with more tasks (n=24) than maxShardWorkers and proves directly
// that AT MOST maxShardWorkers invocations of fn are ever in flight at once,
// while every task still eventually completes.
func TestRunShardPoolBoundsConcurrency(t *testing.T) {
	const n = 24 // > maxShardWorkers, exercises the pool boundary
	if n <= maxShardWorkers {
		t.Fatalf("test setup invalid: n (%d) must exceed maxShardWorkers (%d)", n, maxShardWorkers)
	}

	entered := make(chan int, n)
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		runShardPool(n, func(idx int) {
			entered <- idx
			<-release
		})
		close(done)
	}()

	// Drain exactly maxShardWorkers entries — proves that many run concurrently.
	seen := 0
	timeout := time.After(5 * time.Second)
	for seen < maxShardWorkers {
		select {
		case <-entered:
			seen++
		case <-timeout:
			t.Fatalf("only %d/%d workers entered concurrently within timeout", seen, maxShardWorkers)
		}
	}

	// Every pool worker is now blocked inside fn awaiting release — no further
	// task can start until one finishes. A send on entered here would mean more
	// than maxShardWorkers goroutines ran fn concurrently.
	select {
	case <-entered:
		t.Fatalf("BACKLOG 20k regression: more than maxShardWorkers (%d) goroutines ran fn concurrently", maxShardWorkers)
	case <-time.After(150 * time.Millisecond):
	}

	close(release)

	// The remaining n-maxShardWorkers tasks must still all run to completion as
	// the pool works through the rest of the queue.
	for seen < n {
		select {
		case <-entered:
			seen++
		case <-timeout:
			t.Fatalf("only %d/%d tasks completed within timeout after release", seen, n)
		}
	}

	select {
	case <-done:
	case <-timeout:
		t.Fatal("runShardPool did not return after every task completed")
	}
}

// TestRunShardPoolZeroAndSmallN covers the boundary cases runShardPool's
// workers := min(maxShardWorkers, n) clamp must handle: n=0 (no tasks, no
// goroutines spawned) and n < maxShardWorkers (fewer workers than the cap).
func TestRunShardPoolZeroAndSmallN(t *testing.T) {
	// n=0 must return immediately without spawning or blocking.
	done := make(chan struct{})
	go func() {
		runShardPool(0, func(idx int) { t.Errorf("fn called with n=0, idx=%d", idx) })
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runShardPool(0, ...) did not return")
	}

	// n < maxShardWorkers: every index must run exactly once.
	const n = maxShardWorkers - 1
	seenCh := make(chan int, n)
	runShardPool(n, func(idx int) { seenCh <- idx })
	close(seenCh)
	seen := make(map[int]bool, n)
	for idx := range seenCh {
		if seen[idx] {
			t.Errorf("idx %d ran more than once", idx)
		}
		seen[idx] = true
	}
	if len(seen) != n {
		t.Errorf("ran %d/%d indices", len(seen), n)
	}
}
