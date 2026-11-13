//go:build measure

// Measurement level (René 4 Nov, `scripts/measure.sh`): testing.AllocsPerRun counts
// every allocation in the process, and this package runs parallel tests and
// badger goroutines, so these bounds run by hand, not in a gate (MEMGATE).

package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

const allocScanBound = 64 // batch buffers and the scan's own bookkeeping

func TestScanRelColumns_AllocationsDoNotGrowWithRows(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates on its own")
	}
	bs := newAllocScanStore(t)
	for _, props := range [][]string{nil, {"w"}} {
		for _, o := range allocScanOpts {
			t.Run(o.name, func(t *testing.T) {
				rows := 0
				scan := relColumnScan(t, bs, props, o.opts, &rows)
				scan() // builds the snapshot and its ordinals
				if rows != allocScanRows {
					t.Fatalf("scan read %d rows, want %d", rows, allocScanRows)
				}
				if !bs.scanRelColumnsColumnar(relScanType, props, o.opts, func(*storecontract.RelColumnBatch) bool { return true }) {
					t.Fatal("the columnar path declined; the bound below would measure the row path")
				}
				if a := minAllocsPerRun(scan); a > allocScanBound {
					t.Fatalf("props %v: %.0f allocations for %d rows, want at most %d", props, a, allocScanRows, allocScanBound)
				}
			})
		}
	}
}

func TestScanNodeColumns_AllocationsDoNotGrowWithRows(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates on its own")
	}
	bs := newAllocScanStore(t)
	for _, o := range allocScanOpts {
		t.Run(o.name, func(t *testing.T) {
			rows := 0
			scan := nodeColumnScan(t, bs, o.opts, &rows)
			scan()
			if rows != allocScanRows {
				t.Fatalf("scan read %d rows, want %d", rows, allocScanRows)
			}
			if a := minAllocsPerRun(scan); a > allocScanBound {
				t.Fatalf("%.0f allocations for %d rows, want at most %d", a, allocScanRows, allocScanBound)
			}
		})
	}
}

// minAllocsPerRun is the smallest of several testing.AllocsPerRun samples:
// AllocsPerRun counts every process malloc (leftover badger goroutines add noise
// under load); the minimum filters that without loosening the bound.
func minAllocsPerRun(scan func()) float64 {
	best := testing.AllocsPerRun(5, scan)
	for i := 0; i < 4 && best > allocScanBound; i++ {
		best = min(best, testing.AllocsPerRun(5, scan))
	}
	return best
}
