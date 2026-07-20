package index

import (
	"fmt"
	"sync"
	"testing"
)

// BACKLOG 16g: hnswVisitedBuf replaces a fresh []bool allocated on every
// searchLayer call with a pooled, generation-tagged buffer. These tests
// pin the buffer's own correctness in isolation, independent of the full
// HNSW search machinery (which the package's existing recall/determinism
// tests already re-verify end to end after this change).

// TestHNSWVisitedBuf_FreshBorrowStartsAllUnvisited proves a freshly-grown
// buffer reports every index unvisited before anything is marked.
func TestHNSWVisitedBuf_FreshBorrowStartsAllUnvisited(t *testing.T) {
	vb := getHNSWVisitedBuf(10)
	defer putHNSWVisitedBuf(vb)
	for i := int32(0); i < 10; i++ {
		if vb.visited(i) {
			t.Fatalf("index %d reported visited on a fresh buffer", i)
		}
	}
}

// TestHNSWVisitedBuf_MarkVisited proves marking one index does not affect
// its neighbors.
func TestHNSWVisitedBuf_MarkVisited(t *testing.T) {
	vb := getHNSWVisitedBuf(5)
	defer putHNSWVisitedBuf(vb)
	vb.markVisited(2)
	for i := int32(0); i < 5; i++ {
		want := i == 2
		if got := vb.visited(i); got != want {
			t.Fatalf("index %d visited = %v, want %v", i, got, want)
		}
	}
}

// TestHNSWVisitedBuf_ReuseDoesNotLeakStaleMarks is the core correctness
// property of the generation scheme: a buffer returned to the pool and
// re-borrowed (simulating the next searchLayer call reusing the same
// underlying allocation) must NOT report indices visited during a PRIOR
// borrow as still visited — the whole point of bumping the generation
// instead of physically clearing the slice.
func TestHNSWVisitedBuf_ReuseDoesNotLeakStaleMarks(t *testing.T) {
	vb1 := getHNSWVisitedBuf(8)
	vb1.markVisited(0)
	vb1.markVisited(3)
	vb1.markVisited(7)
	putHNSWVisitedBuf(vb1)

	// Borrow again — sync.Pool has at most one buffer in flight in this
	// single-goroutine test, so this MUST be the same underlying buffer,
	// exercising the generation-bump reuse path (not a fresh allocation).
	vb2 := getHNSWVisitedBuf(8)
	defer putHNSWVisitedBuf(vb2)
	for i := int32(0); i < 8; i++ {
		if vb2.visited(i) {
			t.Fatalf("index %d visited after reborrow — stale mark leaked across generations", i)
		}
	}
	// The reborrowed buffer must still work correctly for new marks.
	vb2.markVisited(3)
	if !vb2.visited(3) {
		t.Fatal("markVisited on a reborrowed buffer did not take effect")
	}
	if vb2.visited(0) || vb2.visited(7) {
		t.Fatal("marking one index after reborrow incorrectly marked others")
	}
}

// TestHNSWVisitedBuf_GrowsForLargerGraph proves a buffer borrowed for a
// smaller graph is safely resized (and its stale generation state reset)
// when a later call needs a larger one — the case where g.nodes has grown
// since the pooled buffer was last sized.
func TestHNSWVisitedBuf_GrowsForLargerGraph(t *testing.T) {
	vb1 := getHNSWVisitedBuf(4)
	vb1.markVisited(1)
	putHNSWVisitedBuf(vb1)

	vb2 := getHNSWVisitedBuf(20)
	defer putHNSWVisitedBuf(vb2)
	if len(vb2.gen) != 20 {
		t.Fatalf("grown buffer len = %d, want 20", len(vb2.gen))
	}
	for i := int32(0); i < 20; i++ {
		if vb2.visited(i) {
			t.Fatalf("index %d visited on a grown buffer — stale state from the smaller prior use", i)
		}
	}
}

// TestHNSWVisitedBuf_ConcurrentBorrowersDoNotShareState is the concurrency
// safety proof: search runs under vi.mu.RLock, which allows MULTIPLE
// goroutines to call searchLayer on the SAME graph concurrently. Each
// concurrent borrower must get an independent buffer (or an independently
// generation-tagged view) — one goroutine's marks must never be visible to
// another's concurrently-running search.
func TestHNSWVisitedBuf_ConcurrentBorrowersDoNotShareState(t *testing.T) {
	const goroutines = 16
	const n = 500
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for iter := 0; iter < 50; iter++ {
				vb := getHNSWVisitedBuf(n)
				// Mark only this goroutine's own private set of indices.
				markIdx := int32((id*37 + iter) % n)
				if vb.visited(markIdx) {
					errCh <- fmt.Errorf("goroutine %d iter %d idx %d: already visited before marking — cross-borrower state leak", id, iter, markIdx)
					putHNSWVisitedBuf(vb)
					return
				}
				vb.markVisited(markIdx)
				if !vb.visited(markIdx) {
					errCh <- fmt.Errorf("goroutine %d iter %d idx %d: mark did not take effect", id, iter, markIdx)
					putHNSWVisitedBuf(vb)
					return
				}
				putHNSWVisitedBuf(vb)
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}
