//go:build measure

// Measurement level (René 4 Nov, `make measure`): the bytes a page allocates
// are read from the process-wide allocation counter, which other tests and
// host load move, so this bound runs by hand, not in a gate (MEMGATE).

package memory

import (
	"runtime"
	"testing"
)

// bytesPerCall returns the heap bytes one call of fn allocates, averaged.
func bytesPerCall(fn func()) uint64 {
	const calls = 20
	fn() // warm
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < calls; i++ {
		fn()
	}
	runtime.ReadMemStats(&after)
	return (after.TotalAlloc - before.TotalAlloc) / calls
}

// TestPostingsPageCostIndependentOfKeySize is backlog 72's cost (see
// sorted_postings_cost_test.go): one page of 16 allocates far below one 8-byte
// copy of its key.
func TestPostingsPageCostIndependentOfKeySize(t *testing.T) {
	n := 200_000
	if testing.Short() {
		n = 50_000
	}
	ms, nids, rids := fillOneKey(t, n)
	const limit = 16
	// The bound is what a page of 16 rows may allocate: the result slice and
	// iteration state, far below one 8-byte copy of the key (n*8 bytes).
	const bound = 4 << 10
	for _, d := range postingsPageDoors(t, ms, nids, rids, limit) {
		if got := d.call(); got != limit {
			t.Fatalf("%s: page holds %d rows, want %d", d.name, got, limit)
		}
		b := bytesPerCall(func() { d.call() })
		t.Logf("%s: %d B per page of %d over a key of %d", d.name, b, limit, n)
		if b > bound {
			t.Errorf("%s: one page of %d allocates %d B over a key of %d members (bound %d B): the page copies or sorts the key", d.name, limit, b, n, bound)
		}
	}
}
