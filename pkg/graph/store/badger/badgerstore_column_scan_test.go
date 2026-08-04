package badger

import (
	"fmt"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R2b has TWO paths behind one signature — a columnar path over the DocValues
// snapshot and a row fallback. The hazard is not that either is wrong on its own;
// it is that they DISAGREE, silently, for inputs that route differently. Every probe
// here is an equivalence or a routing assertion.

const colScanLabel uint16 = 7

// colScanRow is one node's worth of test data.
type colScanRow struct {
	id       int64
	qty      any
	city     string
	from, to int64
}

func newColScanStore(t *testing.T, rows []colScanRow, labelOnDisk bool) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, LabelIndexOnDisk: labelOnDisk})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	for _, r := range rows {
		n := types.NewNode(types.NodeID(r.id), colScanLabel, nil)
		if r.qty != nil {
			if err := n.SetProperty("qty", r.qty); err != nil {
				t.Fatalf("SetProperty qty: %v", err)
			}
		}
		if r.city != "" {
			if err := n.SetProperty("city", r.city); err != nil {
				t.Fatalf("SetProperty city: %v", err)
			}
		}
		n.SetTemporal(&types.TemporalMetadata{
			ValidFrom: types.Instant(r.from), ValidTo: types.Instant(r.to),
		})
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", r.id, err)
		}
	}
	return bs
}

// drained is a flattened, comparable view of everything a scan emitted.
type drained struct {
	ids           []int64
	from, to      []int64
	vals          []string // "kind:value" or "null", per row per column
	batches, cols int
}

func drain(t *testing.T, bs *Store, props []string, opts storecontract.QueryOpts) drained {
	t.Helper()
	var d drained
	d.cols = len(props)
	err := bs.ScanNodeColumns(colScanLabel, props, opts, func(b *storecontract.ColumnBatch) bool {
		d.batches++
		ii := make([]int, len(props))
		for r := range b.IDs {
			d.ids = append(d.ids, int64(b.IDs[r]))
			d.from = append(d.from, b.ValidFrom[r])
			d.to = append(d.to, b.ValidTo[r])
			for c := range props {
				if b.Null[c][r] {
					d.vals = append(d.vals, "null")
					// An absent row still advances the typed cursor: every path pads
					// the typed slice so column indices stay aligned with IDs.
					ii[c]++
					continue
				}
				switch b.Kinds[c] {
				case storecontract.ColString:
					d.vals = append(d.vals, "s:"+b.Strs[c][ii[c]])
				case storecontract.ColFloat64:
					d.vals = append(d.vals, fmt.Sprintf("f:%v", b.Flts[c][ii[c]]))
				default:
					d.vals = append(d.vals, fmt.Sprintf("i:%d", b.Ints[c][ii[c]]))
				}
				ii[c]++
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("ScanNodeColumns: %v", err)
	}
	return d
}

func sameDrain(t *testing.T, what string, a, b drained) {
	t.Helper()
	if len(a.ids) != len(b.ids) {
		t.Fatalf("%s: row counts differ: columnar=%d row=%d", what, len(a.ids), len(b.ids))
	}
	for i := range a.ids {
		if a.ids[i] != b.ids[i] || a.from[i] != b.from[i] || a.to[i] != b.to[i] {
			t.Errorf("%s: row %d differs: columnar id=%d [%d,%d] row id=%d [%d,%d]",
				what, i, a.ids[i], a.from[i], a.to[i], b.ids[i], b.from[i], b.to[i])
		}
	}
	if len(a.vals) != len(b.vals) {
		t.Fatalf("%s: value counts differ: %d vs %d", what, len(a.vals), len(b.vals))
	}
	for i := range a.vals {
		if a.vals[i] != b.vals[i] {
			t.Errorf("%s: value %d differs: columnar=%q row=%q", what, i, a.vals[i], b.vals[i])
		}
	}
}

func baseRows() []colScanRow {
	return []colScanRow{
		{id: 1, qty: int64(10), city: "berlin", from: 100, to: 0},
		{id: 2, qty: int64(20), city: "linz", from: 200, to: 300},
		{id: 3, qty: nil, city: "berlin", from: 150, to: 0},   // absent qty
		{id: 4, qty: int64(40), city: "", from: 400, to: 500}, // absent city
		{id: 5, qty: int64(50), city: "linz", from: 50, to: 120},
	}
}

// TestColumnScan_ColumnarMatchesRowPath is the equivalence oracle. The columnar
// path reads typed arrays and the row path reads *types.Node through GetProperty —
// entirely different code — so only a value-level comparison can catch a disagreement.
// LabelIndexOnDisk forces the row path, giving a genuine A/B on identical data.
func TestColumnScan_ColumnarMatchesRowPath(t *testing.T) {
	props := []string{"qty", "city"}
	for _, tc := range []struct {
		name string
		opts storecontract.QueryOpts
	}{
		{"unfiltered", storecontract.QueryOpts{}},
		{"valid_at_mid", storecontract.QueryOpts{ValidAt: 250}},
		{"valid_at_early", storecontract.QueryOpts{ValidAt: 60}},
		{"valid_range", storecontract.QueryOpts{ValidStart: 100, ValidEnd: 400}},
		{"valid_range_future", storecontract.QueryOpts{ValidStart: 9000, ValidEnd: 9999}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			colStore := newColScanStore(t, baseRows(), false) // columnar eligible
			rowStore := newColScanStore(t, baseRows(), true)  // labelOnDisk -> row path
			sameDrain(t, tc.name,
				drain(t, colStore, props, tc.opts),
				drain(t, rowStore, props, tc.opts))
		})
	}
}

// TestColumnScan_ColumnarPathActuallyRuns is the Pattern-37 assertion. Both paths
// return the same values by design, so equivalence alone cannot prove the columnar
// path was ever entered — a fast path that never fires passes every test above.
// LabelIndexOnDisk is the one input that provably routes to the row fallback, so a
// difference in observable routing behaviour between the two stores proves the
// columnar path exists and is reachable.
func TestColumnScan_ColumnarPathActuallyRuns(t *testing.T) {
	colStore := newColScanStore(t, baseRows(), false)
	rowStore := newColScanStore(t, baseRows(), true)

	// The columnar path builds and caches a DocValues snapshot; the row path never
	// does. That cache is the observable side effect distinguishing them.
	_ = drain(t, colStore, []string{"qty"}, storecontract.QueryOpts{})
	_ = drain(t, rowStore, []string{"qty"}, storecontract.QueryOpts{})

	colStore.docMu.Lock()
	cached := colStore.docColumns[colScanLabel]
	colStore.docMu.Unlock()
	if cached == nil {
		t.Fatal("columnar scan left no DocValues snapshot — the fast path never ran, " +
			"so every equivalence probe above was comparing the row path to itself")
	}
	if !cached.HasTemporal() {
		t.Error("cached snapshot has no temporal columns; the columnar scan cannot " +
			"honour a valid-time predicate without them and should have declined")
	}

	rowStore.docMu.Lock()
	rowCached := rowStore.docColumns[colScanLabel]
	rowStore.docMu.Unlock()
	if rowCached != nil {
		t.Error("labelOnDisk store built a DocValues snapshot; it must decline to the " +
			"row path because its membership is not in RAM")
	}
}

// TestColumnScan_MixedNumericColumnDoesNotSilentlyWiden pins the refusal. A column
// holding both 2 and 2.0 has no single kind; widening it changes which values a
// consumer's equality test matches, so it must refuse or report absent — never
// coerce silently.
func TestColumnScan_MixedNumericColumnDoesNotSilentlyWiden(t *testing.T) {
	rows := []colScanRow{
		{id: 1, qty: int64(2), city: "a", from: 10, to: 0},
		{id: 2, qty: 2.5, city: "a", from: 20, to: 0},
	}
	for _, onDisk := range []bool{false, true} {
		t.Run(fmt.Sprintf("labelOnDisk=%v", onDisk), func(t *testing.T) {
			bs := newColScanStore(t, rows, onDisk)
			var seen []string
			err := bs.ScanNodeColumns(colScanLabel, []string{"qty"}, storecontract.QueryOpts{},
				func(b *storecontract.ColumnBatch) bool {
					for r := range b.IDs {
						if b.Null[0][r] {
							seen = append(seen, "null")
						} else if b.Kinds[0] == storecontract.ColFloat64 {
							seen = append(seen, "f")
						} else {
							seen = append(seen, "i")
						}
					}
					return true
				})
			// Either outcome is acceptable — a refusal, or reporting the odd value
			// absent. What is NOT acceptable is two int64s where one was 2.5, or two
			// float64s where one was an exact int64.
			if err != nil {
				return // refused, which is the documented behaviour
			}
			for i, s := range seen {
				if s == "null" {
					continue
				}
				if (i == 0 && s != "i") || (i == 1 && s != "f") {
					t.Errorf("row %d reported as %q — a mixed numeric column was "+
						"coerced to one kind instead of refusing or reporting absent", i, s)
				}
			}
		})
	}
}

// TestColumnScan_EmptyLabelIsNotAnError pins that an empty label yields no batches
// and no error on both paths, rather than one path erroring.
func TestColumnScan_EmptyLabelIsNotAnError(t *testing.T) {
	for _, onDisk := range []bool{false, true} {
		bs := newColScanStore(t, nil, onDisk)
		n := 0
		err := bs.ScanNodeColumns(colScanLabel, []string{"qty"}, storecontract.QueryOpts{},
			func(*storecontract.ColumnBatch) bool { n++; return true })
		if err != nil {
			t.Errorf("labelOnDisk=%v: empty label errored: %v", onDisk, err)
		}
		if n != 0 {
			t.Errorf("labelOnDisk=%v: empty label emitted %d batches", onDisk, n)
		}
	}
}
