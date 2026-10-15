package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Columnar scans read the cached snapshot's arrays; allocations must be batch
// buffers, not a value per row (the rel scan once boxed each row's ID for the
// valid-time filter). Filtered and unfiltered scans are held to a bound that does
// not grow with the rows.

const (
	allocScanRows  = 4096
	allocScanBound = 64 // batch buffers and the scan's own bookkeeping
	allocScanBase  = int64(1) << 42
)

func newAllocScanStore(t *testing.T) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	for i := range int64(allocScanRows) {
		n := types.NewNode(types.NodeID(allocScanBase+2*i), relScanType, nil)
		if i%2 == 0 {
			// Half the rows carry no valid-from: the filter derives it from the
			// ID's mint instant, the path that boxed the ID.
			n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1 + i)})
		}
		if err := n.SetProperty("w", i); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := range int64(allocScanRows) {
		start := types.NodeID(allocScanBase + 2*i)
		end := types.NodeID(allocScanBase + 2*((i+1)%allocScanRows))
		r := types.NewRelationship(types.RelID(allocScanBase+2*i+1), relScanType, start, end)
		if i%2 == 0 {
			r.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1 + i)})
		}
		if err := r.SetProperty("w", i); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}
	return bs
}

var allocScanOpts = []struct {
	name string
	opts storecontract.QueryOpts
}{
	{"unfiltered", storecontract.QueryOpts{}},
	{"valid-at", storecontract.QueryOpts{ValidAt: types.Instant(1 << 62)}},
	{"interval", storecontract.QueryOpts{ValidStart: 1, ValidEnd: types.Instant(1 << 62)}},
}

func TestScanRelColumns_AllocationsDoNotGrowWithRows(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector allocates on its own")
	}
	bs := newAllocScanStore(t)
	for _, props := range [][]string{nil, {"w"}} {
		for _, o := range allocScanOpts {
			t.Run(o.name, func(t *testing.T) {
				rows := 0
				scan := func() {
					rows = 0
					if err := bs.ScanRelColumns(relScanType, props, o.opts, func(b *storecontract.RelColumnBatch) bool {
						rows += len(b.IDs)
						return true
					}); err != nil {
						t.Fatal(err)
					}
				}
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
			scan := func() {
				rows = 0
				if err := bs.ScanNodeColumns(relScanType, []string{"w"}, o.opts, func(b *storecontract.ColumnBatch) bool {
					rows += len(b.IDs)
					return true
				}); err != nil {
					t.Fatal(err)
				}
			}
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
