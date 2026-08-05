package badger

import (
	"errors"
	"fmt"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RC4 has the same two-paths-behind-one-signature hazard as the node scan: a
// columnar path over the rel DocValues snapshot and a shared row fallback. Every
// probe here is an equivalence or a routing assertion, never a restatement of the
// implementation.

const relScanType uint16 = 9

type relScanRow struct {
	id            int64
	start, end    int64
	weight        any
	tag           string
	from, validTo int64
}

func newRelScanStore(t *testing.T, rows []relScanRow) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	// Endpoints must exist: the store rejects a relationship whose nodes it cannot
	// find, so the oracle needs real nodes rather than bare IDs.
	seen := map[int64]bool{}
	for _, r := range rows {
		for _, nid := range []int64{r.start, r.end} {
			if seen[nid] {
				continue
			}
			seen[nid] = true
			n := types.NewNode(types.NodeID(nid), relScanType, nil)
			n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(1)})
			if err := bs.PutNode(n); err != nil {
				t.Fatalf("PutNode %d: %v", nid, err)
			}
		}
	}
	for _, r := range rows {
		rel := types.NewRelationship(types.RelID(r.id), relScanType,
			types.NodeID(r.start), types.NodeID(r.end))
		if r.weight != nil {
			if err := rel.SetProperty("weight", r.weight); err != nil {
				t.Fatalf("SetProperty weight: %v", err)
			}
		}
		if r.tag != "" {
			if err := rel.SetProperty("tag", r.tag); err != nil {
				t.Fatalf("SetProperty tag: %v", err)
			}
		}
		rel.SetTemporal(&types.TemporalMetadata{
			ValidFrom: types.Instant(r.from), ValidTo: types.Instant(r.validTo),
		})
		if err := bs.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship %d: %v", r.id, err)
		}
	}
	return bs
}

// relDrained is a flattened, comparable view of everything a rel scan emitted —
// including the ENDPOINTS, which are the half the node oracle has no analogue for
// and therefore the half no existing test covers.
type relDrained struct {
	ids, starts, ends []int64
	from, to          []int64
	vals              []string
	batches           int
}

func flattenRelBatch(d *relDrained, b *storecontract.RelColumnBatch, props []string) {
	d.batches++
	ii := make([]int, len(props))
	for r := range b.IDs {
		d.ids = append(d.ids, int64(b.IDs[r]))
		d.starts = append(d.starts, int64(b.StartIDs[r]))
		d.ends = append(d.ends, int64(b.EndIDs[r]))
		d.from = append(d.from, b.ValidFrom[r])
		d.to = append(d.to, b.ValidTo[r])
		for c := range props {
			if b.Null[c][r] {
				d.vals = append(d.vals, "null")
				ii[c]++ // an absent row still advances the typed cursor
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
}

// drainRelScan runs the capability (columnar when eligible).
func drainRelScan(t *testing.T, bs *Store, props []string, opts storecontract.QueryOpts) relDrained {
	t.Helper()
	var d relDrained
	err := bs.ScanRelColumns(relScanType, props, opts, func(b *storecontract.RelColumnBatch) bool {
		flattenRelBatch(&d, b, props)
		return true
	})
	if err != nil {
		t.Fatalf("ScanRelColumns: %v", err)
	}
	return d
}

// drainRelRowPath runs the SHARED row builder over materialised relationships,
// bypassing the columnar path entirely. This is the oracle: it reads
// *types.Relationship through GetProperty, which is different code from the typed
// arrays the fast path reads, so only a value-level comparison can catch a
// disagreement.
func drainRelRowPath(t *testing.T, bs *Store, props []string, opts storecontract.QueryOpts) relDrained {
	t.Helper()
	rels, err := bs.RelationshipsByType(relScanType, opts)
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	var d relDrained
	if err := storecontract.ScanColumnsFromRels(rels, props, func(b *storecontract.RelColumnBatch) bool {
		flattenRelBatch(&d, b, props)
		return true
	}); err != nil {
		t.Fatalf("ScanColumnsFromRels: %v", err)
	}
	return d
}

func sameRelDrain(t *testing.T, what string, a, b relDrained) {
	t.Helper()
	if len(a.ids) != len(b.ids) {
		t.Fatalf("%s: row counts differ: columnar=%d row=%d", what, len(a.ids), len(b.ids))
	}
	for i := range a.ids {
		if a.ids[i] != b.ids[i] || a.starts[i] != b.starts[i] || a.ends[i] != b.ends[i] ||
			a.from[i] != b.from[i] || a.to[i] != b.to[i] {
			t.Errorf("%s: row %d differs: columnar id=%d (%d->%d) [%d,%d] "+
				"row id=%d (%d->%d) [%d,%d]", what, i,
				a.ids[i], a.starts[i], a.ends[i], a.from[i], a.to[i],
				b.ids[i], b.starts[i], b.ends[i], b.from[i], b.to[i])
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

func baseRelRows() []relScanRow {
	return []relScanRow{
		{id: 1, start: 100, end: 200, weight: int64(10), tag: "a", from: 100, validTo: 0},
		{id: 2, start: 200, end: 300, weight: int64(20), tag: "b", from: 200, validTo: 300},
		{id: 3, start: 300, end: 400, weight: nil, tag: "a", from: 150, validTo: 0},        // absent weight
		{id: 4, start: 400, end: 500, weight: int64(40), tag: "", from: 400, validTo: 500}, // absent tag
		{id: 5, start: 500, end: 100, weight: int64(50), tag: "b", from: 50, validTo: 120},
	}
}

// TestRelColumnScan_ColumnarMatchesRowPath is the RC acceptance oracle: columnar vs
// row path, value for value, including endpoints and the temporal window.
func TestRelColumnScan_ColumnarMatchesRowPath(t *testing.T) {
	props := []string{"weight", "tag"}
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
			bs := newRelScanStore(t, baseRelRows())
			sameRelDrain(t, tc.name,
				drainRelScan(t, bs, props, tc.opts),
				drainRelRowPath(t, bs, props, tc.opts))
		})
	}
}

// TestRelColumnScan_EndpointsMatchTheRelationship pins the half the node oracle
// cannot cover. Endpoints arrive from the snapshot's reserved columns, NOT from the
// relationship, so a wrong column choice (start read from the end array) would still
// produce well-formed aligned output and pass every count-based check.
func TestRelColumnScan_EndpointsMatchTheRelationship(t *testing.T) {
	rows := baseRelRows()
	bs := newRelScanStore(t, rows)

	want := make(map[int64][2]int64, len(rows))
	for _, r := range rows {
		want[r.id] = [2]int64{r.start, r.end}
	}

	got := drainRelScan(t, bs, []string{"weight"}, storecontract.QueryOpts{})
	if len(got.ids) != len(rows) {
		t.Fatalf("got %d rows, want %d", len(got.ids), len(rows))
	}
	for i, id := range got.ids {
		w, ok := want[id]
		if !ok {
			t.Fatalf("unknown rel id %d in scan output", id)
		}
		if got.starts[i] != w[0] || got.ends[i] != w[1] {
			t.Errorf("rel %d: got (%d->%d), want (%d->%d)",
				id, got.starts[i], got.ends[i], w[0], w[1])
		}
	}
}

// TestRelColumnScan_ColumnarPathActuallyRuns is the Pattern-37 routing assertion.
// Both paths return the same values BY DESIGN, so the oracle above cannot prove the
// columnar path was ever entered — a fast path that never fires passes it. The
// snapshot cache is the observable side effect that distinguishes them.
func TestRelColumnScan_ColumnarPathActuallyRuns(t *testing.T) {
	bs := newRelScanStore(t, baseRelRows())
	_ = drainRelScan(t, bs, []string{"weight"}, storecontract.QueryOpts{})

	bs.docMu.Lock()
	cached := bs.relColumns[relScanType]
	bs.docMu.Unlock()
	if cached == nil {
		t.Fatal("rel columnar scan left no DocValues snapshot — the fast path never " +
			"ran, so the equivalence oracle was comparing the row path to itself")
	}
	if !cached.HasTemporal() {
		t.Error("cached rel snapshot has no temporal columns; the columnar scan " +
			"cannot honour a valid-time predicate without them and should decline")
	}
	if _, ok := cached.View(RelStartColumn); !ok {
		t.Error("cached rel snapshot has no start-endpoint column; endpoints are " +
			"structure and must always be built")
	}
}

// TestRelColumnScan_InvertedIntervalRefusesLikeTheRowPath pins a decline that must
// NOT become a silent empty answer.
//
// A start >= end interval makes columnarValidTimeWindow report "cannot serve", so
// the scan falls through to RelationshipsByType — which REJECTS it as an invalid
// range. The fast path must therefore surface that error rather than swallowing it
// and reporting zero rows, which is what "matches nothing" would look like to a
// caller who cannot tell an empty result from a refused one.
func TestRelColumnScan_InvertedIntervalRefusesLikeTheRowPath(t *testing.T) {
	bs := newRelScanStore(t, baseRelRows())
	opts := storecontract.QueryOpts{ValidStart: 400, ValidEnd: 100}

	_, rowErr := bs.RelationshipsByType(relScanType, opts)
	if rowErr == nil {
		t.Fatal("row door accepted an inverted interval; this probe assumes it refuses")
	}

	rows := 0
	scanErr := bs.ScanRelColumns(relScanType, []string{"weight"}, opts,
		func(b *storecontract.RelColumnBatch) bool { rows += len(b.IDs); return true })
	if scanErr == nil {
		t.Fatalf("ScanRelColumns accepted an inverted interval and returned %d rows; "+
			"the row door refuses it, and a caller cannot distinguish a silent empty "+
			"answer from a refusal", rows)
	}
}

// TestRelColumnScan_MixedNumericColumnDoesNotSilentlyWiden pins the shared refusal.
// The rules live in ColumnData.appendRow, shared with the node path, so this asserts
// the RELATIONSHIP door reaches them — a rel-specific copy would be free to drift.
func TestRelColumnScan_MixedNumericColumnDoesNotSilentlyWiden(t *testing.T) {
	rows := []relScanRow{
		{id: 1, start: 1, end: 2, weight: int64(2), tag: "a", from: 10, validTo: 0},
		{id: 2, start: 2, end: 3, weight: 2.5, tag: "b", from: 20, validTo: 0},
	}
	bs := newRelScanStore(t, rows)

	err := bs.ScanRelColumns(relScanType, []string{"weight"}, storecontract.QueryOpts{},
		func(b *storecontract.RelColumnBatch) bool { return true })
	if !errors.Is(err, storecontract.ErrMixedNumericColumn) {
		t.Fatalf("mixed int/float weight column: got err=%v, want ErrMixedNumericColumn — "+
			"widening to float64 changes which values a consumer's equality test matches", err)
	}
}

// TestRelColumnScan_EmptyTypeIsNotAnError — no relationships of a type is zero rows,
// not a failure.
func TestRelColumnScan_EmptyTypeIsNotAnError(t *testing.T) {
	bs := newRelScanStore(t, nil)
	got := drainRelScan(t, bs, []string{"weight"}, storecontract.QueryOpts{})
	if len(got.ids) != 0 || got.batches != 0 {
		t.Fatalf("empty rel type emitted %d rows in %d batches, want none",
			len(got.ids), got.batches)
	}
}

// TestRelColumnScan_NilCallbackRejected — the callback is the only way results leave
// this door, so a nil one is a caller bug, not an empty scan.
func TestRelColumnScan_NilCallbackRejected(t *testing.T) {
	bs := newRelScanStore(t, baseRelRows())
	if err := bs.ScanRelColumns(relScanType, []string{"weight"},
		storecontract.QueryOpts{}, nil); err == nil {
		t.Fatal("nil callback accepted; want an error")
	}
}
