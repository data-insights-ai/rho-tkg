package index

import (
	"fmt"
	"runtime"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFootprintMatchesHeap (IDXSTATS): each structure's estimated bytes lie
// within a fifth of the heap it takes, measured after a GC, at the shapes
// ai-soc's stores have: every value distinct, 20 rows a value, numeric values
// (a second posting list per value), range counts, composites and sidecars.
// Not parallel: it reads the process heap.
func TestFootprintMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~100 MB to measure")
	}
	const n = 200_000
	str := func(i int) string { return types.IndexablePropertyValueKey(fmt.Sprintf("value-%07d", i)) }
	num := func(i int) string { return types.IndexablePropertyValueKey(float64(i) + 0.5) }
	cases := []struct {
		name   string
		values int64
		build  func() (any, func() Footprint)
	}{
		{"strings, every value distinct", n, func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), str(i))
			}
			return pi, pi.Footprint
		}},
		{"strings, 20 rows a value", n / 20, func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), str(i/20))
			}
			return pi, pi.Footprint
		}},
		{"numbers, every value distinct", n, func() (any, func() Footprint) {
			pi := NewPropertyIndex()
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), num(i))
			}
			return pi, pi.Footprint
		}},
		{"numbers with range counts, 20 rows a value", n / 20, func() (any, func() Footprint) {
			pi := NewPropertyIndexWith(true, false)
			for i := range n {
				pi.AddKey(snowflake.ID(i+1), num(i/20))
			}
			return pi, pi.Footprint
		}},
		{"composite, half the tuples held by two", n * 3 / 4, func() (any, func() Footprint) {
			ci := NewCompositePropertyIndex([]string{"a", "b"})
			for i := range n {
				k := i
				if i < n/2 {
					k = n + i/2 // pairs
				}
				ci.AddKey(snowflake.ID(i+1), str(k))
			}
			return ci, ci.Footprint
		}},
		{"sidecar, every value distinct", n, func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 20 rows a value", n / 20, func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/20), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 2 rows a value", n / 2, func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/2), types.NodeID(i+1), types.Instant(i+1))
			}
			return m, m.Footprint
		}},
		{"sidecar, 20 rows a value, each value's list ordered", n / 20, func() (any, func() Footprint) {
			m := NewPropertyTxMembers[types.NodeID]()
			for i := range n {
				m.Record(str(i/20), types.NodeID(i+1), types.Instant(i+1))
			}
			for v := range n / 20 {
				m.OrderValue(str(v))
			}
			return m, m.Footprint
		}},
	}
	for _, c := range cases {
		before := liveHeap()
		held, footprint := c.build()
		measured := liveHeap() - before
		f := footprint()
		runtime.KeepAlive(held)
		ratio := float64(f.Bytes) / float64(measured)
		t.Logf("%-45s values %7d postings %7d estimate %6.1f MB measured %6.1f MB (%.2f)", c.name, f.Values, f.Postings,
			float64(f.Bytes)/1e6, float64(measured)/1e6, ratio)
		if ratio < 0.8 || ratio > 1.2 {
			t.Errorf("%s: estimate %d B, measured %d B: off by more than a fifth", c.name, f.Bytes, measured)
		}
		if f.Values != c.values || f.Postings != n {
			t.Errorf("%s: %d values, %d postings, want %d and %d", c.name, f.Values, f.Postings, c.values, n)
		}
	}
}

// TestMapBytesMatchesHeap (IDXSTATS): mapBytes lies within a fifth of the heap
// of maps whose sizes sweep each growth band, small maps and tables of 16 to
// 256 slots; the slots are two bytes, so the control bytes, the header and
// the table each weigh in it. Not parallel: it reads the process heap.
func TestMapBytesMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("reads the process heap")
	}
	for _, band := range [][2]int{{1, 8}, {9, 14}, {15, 28}, {29, 56}, {57, 112}, {113, 224}} {
		var estimate int64
		held := make([]map[uint8]uint8, 0, 200*(band[1]-band[0]+1))
		before := liveHeap()
		for n := band[0]; n <= band[1]; n++ {
			for range 200 {
				m := map[uint8]uint8{}
				for k := range n {
					m[uint8(k)] = 1 // #nosec G115 -- below 256
				}
				held = append(held, m)
				estimate += mapBytes(n, 1, 1)
			}
		}
		measured := liveHeap() - before
		runtime.KeepAlive(held)
		ratio := float64(estimate) / float64(measured)
		t.Logf("maps of %3d to %3d entries: estimate %7d B measured %7d B (%.2f)", band[0], band[1], estimate, measured, ratio)
		if ratio < 0.8 || ratio > 1.2 {
			t.Errorf("maps of %d to %d entries: estimate %d B, measured %d B: off by more than a fifth", band[0], band[1], estimate, measured)
		}
	}
}

// TestChunksBytesMatchesHeap (IDXSTATS): chunksBytes lies within a fifth of
// the heap of an ordered view, as a set and with range counts (a count per
// key, half its bytes). Not parallel: it reads the process heap.
func TestChunksBytesMatchesHeap(t *testing.T) {
	if testing.Short() {
		t.Skip("reads the process heap")
	}
	const n = 200_000
	for _, counted := range []bool{false, true} {
		before := liveHeap()
		s := &sortedChunks[float64]{}
		for i := range n {
			k := float64(i * 7919 % n)
			if counted {
				s.addCount(k, 3)
			} else {
				s.insert(k)
			}
		}
		measured := liveHeap() - before
		estimate := chunksBytes(s)
		runtime.KeepAlive(s)
		ratio := float64(estimate) / float64(measured)
		t.Logf("ordered view of %d keys, counted %v: estimate %d B measured %d B (%.2f)", n, counted, estimate, measured, ratio)
		if ratio < 0.8 || ratio > 1.2 {
			t.Errorf("counted %v: estimate %d B, measured %d B: off by more than a fifth", counted, estimate, measured)
		}
	}
}

// liveHeap is the live heap after a full collection.
func liveHeap() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc) // #nosec G115 -- a heap size
}
