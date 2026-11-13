//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): the live heap counts every
// goroutine of the process (badger's flushes and compactions included), so
// this slope moves with load and runs by hand, not in a gate.

package graph_test

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// settledHeap is the live heap once it held still for 4 s (badger flushes
// its immutable memtables in the background, ~29 MB a step), at most 90 s.
func settledHeap() uint64 {
	var m runtime.MemStats
	var last uint64
	still := 0
	for i := 0; i < 180 && still < 8; i++ {
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&m)
		if last > 0 && max(m.HeapAlloc, last)-min(m.HeapAlloc, last) < last/200 {
			still++
		} else {
			still = 0
		}
		last = m.HeapAlloc
		time.Sleep(500 * time.Millisecond)
	}
	return m.HeapAlloc
}

// The live heap a badger graph holds per node, as the slope between 40,000
// and 200,000 sigma-overview nodes (6 properties, a 3-key composite index of
// single-ID keys when indexed). v4.60.0: 185 B/node plain, 388 indexed;
// v4.60.1 (row 66, no cached hash string, single-ID keys inline): 88 and 200.
func TestBadgerHeapPerNodeSlope(t *testing.T) {
	if testing.Short() || isRaceEnabled() {
		t.Skip("heap measurement: not in -short, no race detector")
	}
	rels := []string{"s4_seen", "seat_seen", "own_seen", "rf_seen", "rf_pair", "rf_port", "rf_aseen", "rf_apair", "rf_aport"}
	props := func(i int) map[string]any {
		key := fmt.Sprintf("[%d,%d,\"%d\"]", 7340000000000000000+i*7919, 7350000000000000000+i*104729, i%65536)
		return map[string]any{"ov_namespace": "ba", "ov_rel": rels[i%9], "ov_key": key,
			"ov_c3": int64(1717372800000 + i), "ov_c4": int64(1717372805000 + i), "ov_c5": int64(i % 100)}
	}
	for _, tc := range []struct {
		indexed bool
		bound   float64
	}{{false, 120}, {true, 260}} {
		t.Run(fmt.Sprintf("indexed=%v", tc.indexed), func(t *testing.T) {
			bs, err := badger.New(badger.Config{Dir: t.TempDir(), CacheCapacity: 1000, BlockCacheSize: 4 << 20, IndexCacheSize: 4 << 20, MemTableSize: 8 << 20})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: bs, AllowTxBackfill: true})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = g.Close() }()
			if tc.indexed {
				if err := g.Index().CreateComposite("Derived", []string{"ov_namespace", "ov_rel", "ov_key"}); err != nil {
					t.Fatal(err)
				}
			}
			written := 0
			writeTo := func(n int) {
				for written < n {
					if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
						for j := written; j < min(n, written+10_000); j++ {
							if _, err := tx.AddNode([]string{"Derived"}, props(j)); err != nil {
								return err
							}
						}
						return nil
					}); err != nil {
						t.Fatal(err)
					}
					written = min(n, written+10_000)
				}
			}
			const n1, n2 = 40_000, 200_000
			settle := func() uint64 {
				if err := bs.Flush(); err != nil { // the store's write buffer
					t.Fatal(err)
				}
				return settledHeap()
			}
			writeTo(n1)
			h1 := settle()
			writeTo(n2)
			h2 := settle()
			slope := (float64(h2) - float64(h1)) / float64(n2-n1)
			t.Logf("indexed=%v: %.0f B/node (heap %d -> %d)", tc.indexed, slope, h1, h2)
			if slope > tc.bound {
				t.Fatalf("indexed=%v: %.0f B/node, want <= %.0f", tc.indexed, slope, tc.bound)
			}
		})
	}
}
