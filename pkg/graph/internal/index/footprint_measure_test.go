//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): the live heap after a GC
// counts every goroutine of the process, so these comparisons of the
// estimates with it move with load and run by hand, not in a gate.
// TestFootprintCounts gates the counts.

package index

import (
	"runtime"
	"testing"
)

// TestFootprintMatchesHeap (IDXSTATS): each structure's estimated bytes lie
// within a fifth of the heap it takes, measured after a GC, at the shapes
// ai-soc's stores have (footprintCases). Not parallel: it reads the process
// heap.
func TestFootprintMatchesHeap(t *testing.T) {
	const n = 200_000
	for _, c := range footprintCases(n) {
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
	}
}

// TestMapBytesMatchesHeap (IDXSTATS): mapBytes lies within a fifth of the heap
// of maps whose sizes sweep each growth band, small maps and tables of 16 to
// 256 slots; the slots are two bytes, so the control bytes, the header and
// the table each weigh in it. Not parallel: it reads the process heap.
func TestMapBytesMatchesHeap(t *testing.T) {
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
