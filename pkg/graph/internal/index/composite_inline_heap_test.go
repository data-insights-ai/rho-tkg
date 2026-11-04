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
// costs its map slot, not a posting list (v4.60.0: 99 B per key, now ~19),
// also after a second ID came and went (demotion). The gate is the structure:
// every key inline, no posting list; the live heap per key is logged.
func TestCompositeSingleIDKeyHeap(t *testing.T) {
	const n = 50_000
	var built []*CompositePropertyIndex
	single := heapPerKey(n, func(keys []string) any {
		ci := NewCompositePropertyIndex([]string{"a", "b"})
		for i, k := range keys {
			ci.AddKey(snowflake.ID(i+1), k)
		}
		built = append(built, ci)
		return ci
	})
	demoted := heapPerKey(n, func(keys []string) any {
		ci := NewCompositePropertyIndex([]string{"a", "b"})
		for i, k := range keys {
			ci.AddKey(snowflake.ID(2*i+1), k)
			ci.AddKey(snowflake.ID(2*i+2), k)
			ci.removeKey(snowflake.ID(2*i+2), k)
		}
		built = append(built, ci)
		return ci
	})
	t.Logf("B per single-ID key: %.1f inserted, %.1f demoted (v4.60.0: ~99)", single, demoted)
	for i, ci := range built {
		if len(ci.one) != n || len(ci.many) != 0 {
			t.Fatalf("%s: %d keys inline, %d posting lists; want %d and 0", []string{"inserted", "demoted"}[i], len(ci.one), len(ci.many), n)
		}
	}
}
