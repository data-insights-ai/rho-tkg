package badger

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Columnar scans read the cached snapshot's arrays; allocations must be batch
// buffers, not a value per row (the rel scan once boxed each row's ID for the
// valid-time filter). Filtered and unfiltered scans are held to a bound that
// does not grow with the rows: TestScanRelColumns_AllocationsDoNotGrowWithRows
// and TestScanNodeColumns_AllocationsDoNotGrowWithRows, at the measurement
// level (badgerstore_column_scan_allocs_measure_test.go, MEMGATE). Here: the
// scans read every row, and the rel scan takes the columnar path.

const (
	allocScanRows = 4096
	allocScanBase = int64(1) << 42
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

// relColumnScan returns a ScanRelColumns call over newAllocScanStore's rels
// that counts the rows it reads into *rows.
func relColumnScan(t *testing.T, bs *Store, props []string, opts storecontract.QueryOpts, rows *int) func() {
	return func() {
		*rows = 0
		if err := bs.ScanRelColumns(relScanType, props, opts, func(b *storecontract.RelColumnBatch) bool {
			*rows += len(b.IDs)
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// nodeColumnScan is relColumnScan for ScanNodeColumns over property w.
func nodeColumnScan(t *testing.T, bs *Store, opts storecontract.QueryOpts, rows *int) func() {
	return func() {
		*rows = 0
		if err := bs.ScanNodeColumns(relScanType, []string{"w"}, opts, func(b *storecontract.ColumnBatch) bool {
			*rows += len(b.IDs)
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestColumnScans_ReadEveryRowColumnar: every filtered and unfiltered scan
// reads all rows (half without a valid-from), and after the first rel scan
// builds the snapshot the columnar path serves it.
func TestColumnScans_ReadEveryRowColumnar(t *testing.T) {
	bs := newAllocScanStore(t)
	for _, props := range [][]string{nil, {"w"}} {
		for _, o := range allocScanOpts {
			rows := 0
			relColumnScan(t, bs, props, o.opts, &rows)() // builds the snapshot and its ordinals
			if rows != allocScanRows {
				t.Fatalf("rel %v %s: scan read %d rows, want %d", props, o.name, rows, allocScanRows)
			}
			if !bs.scanRelColumnsColumnar(relScanType, props, o.opts, func(*storecontract.RelColumnBatch) bool { return true }) {
				t.Fatalf("rel %v %s: the columnar path declined", props, o.name)
			}
		}
	}
	for _, o := range allocScanOpts {
		rows := 0
		nodeColumnScan(t, bs, o.opts, &rows)()
		if rows != allocScanRows {
			t.Fatalf("node %s: scan read %d rows, want %d", o.name, rows, allocScanRows)
		}
	}
}
