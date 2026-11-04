//go:build measure

// Measurement level (René 4 Nov, `make measure`): the bytes a page allocates
// are read from the process-wide allocation counter, which other tests,
// badger's own goroutines and host load move, so this bound runs by hand,
// not in a gate (MEMGATE).

package badger

import (
	"runtime"
	"testing"
)

// TestPostingsPageCostIndependentOfKeySizeBadger is backlog 72's cost on
// badger (see sorted_postings_cost_test.go): a page of 16 allocates a page's
// rows and batch, a quarter of one 8-byte copy of the key.
func TestPostingsPageCostIndependentOfKeySizeBadger(t *testing.T) {
	n := 60_000
	if testing.Short() {
		n = 20_000
	}
	const limit = 16
	// Rows come from the cache or are decoded per page; the bound is a page's
	// rows and batch, a quarter of one 8-byte copy of the key.
	bound := uint64(n * 2)
	for _, d := range postingsPageDoorsBadger(t, n, limit) {
		if got := d.call(); got != limit {
			t.Fatalf("%s: page holds %d rows, want %d", d.name, got, limit)
		}
		const calls = 20
		runtime.GC()
		var before, afterStats runtime.MemStats
		runtime.ReadMemStats(&before)
		for i := 0; i < calls; i++ {
			d.call()
		}
		runtime.ReadMemStats(&afterStats)
		b := (afterStats.TotalAlloc - before.TotalAlloc) / calls
		t.Logf("%s: %d B per page of %d over a key of %d", d.name, b, limit, n)
		if b > bound {
			t.Errorf("%s: one page of %d allocates %d B over a key of %d members (bound %d B): the page copies or sorts the key", d.name, limit, b, n, bound)
		}
	}
}
