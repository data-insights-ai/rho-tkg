//go:build measure

// Measurement level (René 4 Nov, `make measure`): live heap after a GC moves
// with other tests, badger's background work and host load, so this bound
// runs by hand, not in a gate (MEMGATE).

package badger

import (
	"math"
	"runtime"
	"testing"
)

// TestRelPlannerStatsMemoBoundedHeap is backlog 53's RAM bound (the gated
// count is TestRelPlannerStatsMemoBounded/entries): the heap the relationship
// planner statistics retain — stats on minus stats off, same writes, flushed
// before measuring — at 10 K and 100 K relationships grows by a constant, not
// an amount per relationship.
func TestRelPlannerStatsMemoBoundedHeap(t *testing.T) {
	// The least of three: the measure is the process heap, and leftover
	// background work of earlier tests (badger compaction) only ever adds to
	// one run (130 MB once in the long tier, 25 Oct; LONGTIER). -short
	// measures at 1K and 10K: the retained difference is a constant of a few
	// KB at every size (4 Nov: 0.1-18 KB from 1K to 20K rels), so 9K rels
	// against the 8 B/rel bar leave a 72 KB margin.
	nSmall, nLarge := 10_000, 100_000
	if testing.Short() {
		nSmall, nLarge = 1_000, 10_000
	}
	var small, large int64
	perRel := math.Inf(1)
	for range 3 {
		s, l := storeRelStatsRetainedHeap(t, nSmall), storeRelStatsRetainedHeap(t, nLarge)
		if p := float64(l-s) / float64(nLarge-nSmall); p < perRel {
			small, large, perRel = s, l, p
		}
	}
	t.Logf("planner-stat heap: %d B at %d rels, %d B at %d rels, growth %.1f B per rel", small, nSmall, large, nLarge, perRel)
	if perRel > 8 {
		t.Fatalf("planner-stat heap grows %.1f B per relationship written (%d: %d B, %d: %d B); want a constant (≤ 8 B/rel)", perRel, nSmall, small, nLarge, large)
	}
}

func storeRelStatsRetainedHeap(t *testing.T, n int) int64 {
	t.Helper()
	return storeRelRetainedHeap(t, n, false) - storeRelRetainedHeap(t, n, true)
}

// storeRelRetainedHeap writes n relationships into a store that never flushes
// on its own (openStoreRelStats), so the badger engine allocates the same in
// both modes, and returns the live heap growth.
func storeRelRetainedHeap(t *testing.T, n int, disableStats bool) int64 {
	t.Helper()
	before := storeLiveHeap()
	bs := openStoreRelStats(t, n, disableStats)
	after := storeLiveHeap()
	runtime.KeepAlive(bs)
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return after - before
}

func storeLiveHeap() int64 {
	var ms runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc) // #nosec G115 -- heap size fits int64
}
