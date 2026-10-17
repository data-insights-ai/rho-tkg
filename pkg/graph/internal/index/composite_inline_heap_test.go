//go:build !race

package index

import (
	"fmt"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// heapPerKey returns the live heap growth per key of build over n keys.
func heapPerKey(n int, build func(keys []string) any) float64 {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("composite-value-key-%08d", i)
	}
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	before := m.HeapAlloc
	v := build(keys)
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	runtime.KeepAlive(v)
	return float64(int64(m.HeapAlloc)-int64(before)) / float64(n)
}

// Row 66 (sigma: overview keys are almost all single-ID): a single-ID key
// costs its map slot, not a posting list (v4.60.0: 99 B per key, now 19), also
// after a second ID came and went (demotion).
func TestCompositeSingleIDKeyHeap(t *testing.T) {
	const n = 50_000
	const bound = 50.0 // map slot (key header + ID) with growth slack: ~19 B; v4.60.0 ~99
	single := heapPerKey(n, func(keys []string) any {
		ci := NewCompositePropertyIndex([]string{"a", "b"})
		for i, k := range keys {
			ci.AddKey(snowflake.ID(i+1), k)
		}
		return ci
	})
	demoted := heapPerKey(n, func(keys []string) any {
		ci := NewCompositePropertyIndex([]string{"a", "b"})
		for i, k := range keys {
			ci.AddKey(snowflake.ID(2*i+1), k)
			ci.AddKey(snowflake.ID(2*i+2), k)
			ci.removeKey(snowflake.ID(2*i+2), k)
		}
		return ci
	})
	t.Logf("B per single-ID key: %.1f inserted, %.1f demoted", single, demoted)
	if single > bound || demoted > bound {
		t.Fatalf("B per single-ID key: %.1f inserted, %.1f demoted; want <= %.0f", single, demoted, bound)
	}
}
