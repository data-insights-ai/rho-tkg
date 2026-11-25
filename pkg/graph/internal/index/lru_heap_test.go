package index

import (
	"runtime"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// TestCacheUsageFollowsTheHeap (RHO-1): a cache filled with 100,000 entries
// of a 48-byte value reports within 5 % of the live heap the fill grew,
// written (dirty, in both maps) and loaded (clean, in the items map, made
// with that capacity) alike. The heap is read after one GC on each side; the
// median of three fills is taken.
func TestCacheUsageFollowsTheHeap(t *testing.T) {
	const n = 100_000
	size := func(*[48]byte) int64 { return 48 }
	for name, fill := range map[string]func() *Cache[*[48]byte]{
		"written": func() *Cache[*[48]byte] {
			c := NewCache[*[48]byte](1)
			for i := range n {
				c.Put(snowflake.ID(i+1), new([48]byte))
			}
			return c
		},
		"loaded": func() *Cache[*[48]byte] {
			c := NewCache[*[48]byte](n)
			for i := range n {
				c.LoadClean(snowflake.ID(i+1), new([48]byte))
			}
			return c
		},
	} {
		var ratios []float64
		for range 3 {
			before := heapAfterGC()
			c := fill()
			grown := heapAfterGC() - before
			u := c.Usage(size)
			runtime.KeepAlive(c)
			ratios = append(ratios, float64(grown)/float64(u.CleanBytes+u.DirtyBytes))
		}
		slices.Sort(ratios)
		t.Logf("%s: heap grown / account %.3f (runs %.3f)", name, ratios[1], ratios)
		if r := ratios[1]; r < 0.95 || r > 1.05 {
			t.Fatalf("%s: heap grown / account = %.3f, want within 5 %%", name, r)
		}
	}
}

func heapAfterGC() int64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapAlloc) // #nosec G115 -- a heap size, far below 2^63
}
