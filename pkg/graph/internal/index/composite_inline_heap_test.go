package index

import (
	"fmt"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
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

// Sigma's overview keys are almost all single-ID: a single-ID key
// costs its map slot, not a posting list (v4.60.0: 99 B per key, now ~19),
// also after a second ID came and went (demotion). Two gates: the structure
// (every key inline, no posting list) and the live heap per key below
// maxBytesPerKey, twice the measured ~19 B and well under the 99 B a posting
// list per key costs.
func TestCompositeSingleIDKeyHeap(t *testing.T) {
	testtier.Slow(t)
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
	const maxBytesPerKey = 40
	if single > maxBytesPerKey || demoted > maxBytesPerKey {
		t.Errorf("live heap per single-ID key: %.1f B inserted, %.1f B demoted; want <= %d B each", single, demoted, maxBytesPerKey)
	}
	for i, ci := range built {
		if len(ci.one) != n || len(ci.many) != 0 {
			t.Fatalf("%s: %d keys inline, %d posting lists; want %d and 0", []string{"inserted", "demoted"}[i], len(ci.one), len(ci.many), n)
		}
	}
}
