//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests and
// badger goroutines, so this bound runs by hand, not in a gate (MEMGATE). The
// behaviour tests that pin the exact rows are in badgerstore_history_prefix_test.go.

package badger

import (
	"testing"
	"time"
)

// TestHistoryScanAllocGate is the cost bound. The iterator used to prefetch
// the values of the next 100 keys after the Seek whether or not they shared
// the prefix, so a plain entity (no history) in a store with dense
// neighbouring history allocated per neighbour row. With opts.Prefix the Seek
// ends at the prefix boundary and the cost no longer depends on what the
// neighbours hold. The reference is a plain entity ABOVE every history key
// (the Seek lands past the end, so no neighbour can be prefetched); the
// plain entity in the middle of dense history must cost the same allocations.
func TestHistoryScanAllocGate(t *testing.T) {
	const rows = 600 // > the 100-item prefetch window, history on both sides
	for _, k := range histKinds {
		t.Run(k.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			for i := int64(1); i <= rows; i++ {
				id := 1000 + 2*i // even offsets hold history, odd ones are plain
				k.put(t, bs, id, 0)
				k.put(t, bs, id, 1)
			}
			if err := bs.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			bs, err = New(Config{Dir: dir, FlushInterval: time.Hour})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })

			plainMiddle := int64(1000 + 2*rows/2 + 1) // between two history owners
			plainAbove := int64(1 << 40)              // past the last history key
			if got, _ := k.get(bs, plainMiddle); len(got) != 0 {
				t.Fatalf("plainMiddle %d has history %v", plainMiddle, got)
			}

			// AllocsPerRun counts every allocation in the process, so another
			// store's background goroutines add noise to one measurement now
			// and then. Noise only adds; the minimum of three is the door's
			// own cost.
			measure := func(name string, fn func() error) float64 {
				best := -1.0
				for round := 0; round < 3; round++ {
					a := testing.AllocsPerRun(50, func() {
						if err := fn(); err != nil {
							t.Fatalf("%s: %v", name, err)
						}
					})
					if best < 0 || a < best {
						best = a
					}
				}
				t.Logf("%s: %.1f allocs/op (least of 3)", name, best)
				return best
			}
			doors := []struct {
				name string
				fn   func(id int64) func() error
			}{
				{"history", func(id int64) func() error {
					return func() error { _, err := k.get(bs, id); return err }
				}},
				{"paged", func(id int64) func() error {
					return func() error { _, err := k.page(bs, id, 0, 0); return err }
				}},
			}
			for _, d := range doors {
				ref := measure(d.name+"/aboveAll", d.fn(plainAbove))
				mid := measure(d.name+"/amongDense", d.fn(plainMiddle))
				// A missing prefix costs about 600 allocations per call (one
				// per prefetched neighbour row); a steady background rate in a
				// busy test process added +5 to both measurements, sometimes to
				// one only. Twice the reference plus 10 keeps the defect far
				// above the bar and that noise below it.
				if mid > 2*ref+10 {
					t.Errorf("%s: plain entity among dense neighbour history allocates %.1f/op vs %.1f/op past the last key: "+
						"the iterator reads other entities' rows (missing opts.Prefix)", d.name, mid, ref)
				}
			}
		})
	}
}
