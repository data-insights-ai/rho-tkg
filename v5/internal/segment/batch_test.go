package segment

import (
	"errors"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Ported from v4 batch_test.go: the batch (column) path, ID iteration, ranges.

// TestScanBatches_ColumnsEqualRowsAndRowEqualsScan: every batch column holds
// exactly what the rows built from it carry, and Batch.Row builds the same
// row Scan does.
func TestScanBatches_ColumnsEqualRowsAndRowEqualsScan(t *testing.T) {
	rows := corpus(t, 9000, 3)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 1024))
	want := scanAll(t, seg)
	seen := 0
	err := seg.ScanBatches(func(b *Batch) bool {
		if b.Base != seen {
			t.Fatalf("batch base %d, want %d", b.Base, seen)
		}
		for k := 0; k < b.N; k++ {
			w := want[b.Base+k]
			if b.IDs[k] != int64(w.ID) || b.StartIDs[k] != int64(w.Start) || b.EndIDs[k] != int64(w.End) ||
				b.Revisions[k] != int64(w.Revision) {
				t.Fatalf("row %d: batch identity columns differ from the row", b.Base+k)
			}
			for _, c := range []struct {
				col TimeCol
				w   Coord
			}{{TimeValidFrom, w.ValidFrom}, {TimeValidTo, w.ValidTo}, {TimeTxFrom, w.TxFrom}, {TimeTxTo, w.TxTo}} {
				if got := b.Coord(c.col, k); got != c.w {
					t.Fatalf("row %d: batch coord %d is %v, row has %v", b.Base+k, c.col, got, c.w)
				}
			}
			if v, ok := w.ValidFrom.Value(); ok && b.ValidFrom[k] != v {
				t.Fatalf("row %d: batch ValidFrom slot %d, want %d", b.Base+k, b.ValidFrom[k], v)
			}
			got, err := b.Row(k)
			if err != nil {
				t.Fatal(err)
			}
			assertSameRow(t, w, *got)
		}
		seen += b.N
		return true
	})
	if err != nil || seen != len(rows) {
		t.Fatalf("ScanBatches: %v, %d of %d rows", err, seen, len(rows))
	}
	calls := 0
	if err := seg.ScanBatches(func(*Batch) bool { calls++; return false }); err != nil || calls != 1 {
		t.Fatalf("ScanBatches must stop when fn returns false: %v, %d calls", err, calls)
	}
	var b *Batch
	_ = seg.ScanBatches(func(x *Batch) bool { b = x; return false })
	if _, err := b.Row(-1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Batch.Row(-1) = %v, want ErrOutOfRange", err)
	}
	if _, err := b.Row(b.N); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Batch.Row(N) = %v, want ErrOutOfRange", err)
	}
	empty := mustOpen(t, mustEncode(t, nil, Options{}, PageRows))
	if err := empty.ScanBatches(func(*Batch) bool { t.Fatal("batch in an empty segment"); return true }); err != nil {
		t.Fatal(err)
	}
}

func TestForEachID_AscendingAndPointsAtTheRow(t *testing.T) {
	rows := corpus(t, 5000, 4)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 512))
	all := scanAll(t, seg)
	var ids []int64
	err := seg.ForEachID(func(id types.RelID, row int) bool {
		if all[row].ID != id {
			t.Fatalf("ForEachID: row %d holds id %d, entry says %d", row, all[row].ID, id)
		}
		got, err := seg.IDAt(row)
		if err != nil || got != id {
			t.Fatalf("IDAt(%d) = %d, %v; want %d", row, got, err, id)
		}
		ids = append(ids, int64(id))
		return true
	})
	if err != nil || len(ids) != len(rows) {
		t.Fatalf("ForEachID: %v, %d entries", err, len(ids))
	}
	if !sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }) {
		t.Fatal("ForEachID is not in ascending ID order")
	}
	lo, hi := seg.IDRange()
	if int64(lo) != ids[0] || int64(hi) != ids[len(ids)-1] {
		t.Fatalf("IDRange = %d..%d, want %d..%d", lo, hi, ids[0], ids[len(ids)-1])
	}
	n := 0
	if err := seg.ForEachID(func(types.RelID, int) bool { n++; return n < 3 }); err != nil || n != 3 {
		t.Fatalf("ForEachID must stop when fn returns false: %v, %d", err, n)
	}
	if _, err := seg.IDAt(len(rows)); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("IDAt(Len) = %v, want ErrOutOfRange", err)
	}
	if _, err := seg.IDAt(-1); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("IDAt(-1) = %v, want ErrOutOfRange", err)
	}
	empty := mustOpen(t, mustEncode(t, nil, Options{}, PageRows))
	if lo, hi := empty.IDRange(); lo != 0 || hi != 0 {
		t.Fatalf("empty IDRange = %d..%d", lo, hi)
	}
	if err := empty.ForEachID(func(types.RelID, int) bool { t.Fatal("entry in an empty segment"); return true }); err != nil {
		t.Fatal(err)
	}
}

func TestScanRange_BoundsAndEarlyStop(t *testing.T) {
	rows := corpus(t, 3000, 5)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 256))
	all := scanAll(t, seg)
	for _, rg := range [][2]int{{0, 0}, {10, 20}, {100, 900}, {2990, 3000}} {
		next := rg[0]
		err := seg.ScanRange(rg[0], rg[1], func(i int, r *Row) bool {
			if i != next {
				t.Fatalf("ScanRange(%v) index %d, want %d", rg, i, next)
			}
			assertSameRow(t, all[i], *r)
			next++
			return true
		})
		if err != nil || next != rg[1] {
			t.Fatalf("ScanRange(%v): %v, stopped at %d", rg, err, next)
		}
	}
	calls := 0
	if err := seg.ScanRange(0, 500, func(int, *Row) bool { calls++; return false }); err != nil || calls != 1 {
		t.Fatalf("ScanRange must stop when fn returns false: %v, %d", err, calls)
	}
	for _, bad := range [][2]int{{-1, 3}, {5, 3}, {0, 3001}} {
		if err := seg.ScanRange(bad[0], bad[1], func(int, *Row) bool { return true }); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("ScanRange(%v) = %v, want ErrOutOfRange", bad, err)
		}
	}
}

// TestLookup_EveryIDAcrossPageBoundaries checks the page-level binary search
// with tiny pages, so ID runs (several revisions) straddle page boundaries.
func TestLookup_EveryIDAcrossPageBoundaries(t *testing.T) {
	rows := corpus(t, 4000, 6)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 16))
	all := scanAll(t, seg)
	want := map[types.RelID][]int{}
	for i, r := range all {
		want[r.ID] = append(want[r.ID], i)
	}
	for id, rowsOf := range want {
		got, err := seg.Lookup(id)
		if err != nil {
			t.Fatal(err)
		}
		sort.Ints(got)
		if len(got) != len(rowsOf) {
			t.Fatalf("Lookup(%d) = %v, want %v", id, got, rowsOf)
		}
	}
	lo, hi := seg.IDRange()
	for _, id := range []types.RelID{lo - 1, hi + 1, 0} {
		if got, err := seg.Lookup(id); err != nil || len(got) != 0 {
			t.Fatalf("Lookup(%d) outside the ID range = %v, %v", id, got, err)
		}
	}
}

func TestValidateSchema(t *testing.T) {
	if err := ValidateSchema(testSchema()); err != nil {
		t.Fatalf("valid schema: %v", err)
	}
	for _, s := range []Schema{
		{TypeName: ""},
		{TypeName: "X", Columns: []Column{{"a", KindI64}, {"a", KindBool}}},
		{TypeName: "X", Columns: []Column{{"a", 99}}},
	} {
		if err := ValidateSchema(s); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("ValidateSchema(%+v) = %v, want ErrInvalidSchema", s, err)
		}
	}
}

// TestBatch_TypedColumnsMatchRows: the typed per-column accessors hold what
// the rows carry.
func TestBatch_TypedColumnsMatchRows(t *testing.T) {
	rows := corpus(t, 6000, 9)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 1024))
	want := scanAll(t, seg)
	get := func(r Row, key string) (graphstate.Scalar, bool) {
		for _, p := range r.Props {
			if p.Key == key {
				return p.Value, true
			}
		}
		return graphstate.Scalar{}, false
	}
	strSeen, intSeen := false, false
	err := seg.ScanBatches(func(b *Batch) bool {
		for c, col := range b.Columns() {
			if codes, dict, present, ok := b.StringColumn(c); ok {
				strSeen = true
				for k := 0; k < b.N; k++ {
					v, has := get(want[b.Base+k], col.Name)
					s, isStr := v.StringValue()
					if present(k) != (has && isStr) {
						t.Fatalf("row %d col %s: presence %t, row has %v", b.Base+k, col.Name, present(k), v)
					}
					if present(k) && dict[codes[k]] != s {
						t.Fatalf("row %d col %s: code resolves to %q, row holds %q", b.Base+k, col.Name, dict[codes[k]], s)
					}
				}
			} else if col.Kind == KindString {
				if _, _, ok := b.IntColumn(c); ok {
					t.Fatalf("IntColumn(%s) must refuse a string column", col.Name)
				}
			}
			if vals, present, ok := b.IntColumn(c); ok {
				intSeen = true
				if _, _, _, sok := b.StringColumn(c); sok {
					t.Fatalf("StringColumn(%s) must refuse a non-string column", col.Name)
				}
				for k := 0; k < b.N; k++ {
					v, has := get(want[b.Base+k], col.Name)
					declared := has && kindOf(v) == col.Kind
					if present(k) != declared {
						t.Fatalf("row %d col %s: presence %t, row has kind %d", b.Base+k, col.Name, present(k), v.Kind())
					}
					if declared && vals[k] != valueBits(v) {
						t.Fatalf("row %d col %s: stored %d, row value bits %d", b.Base+k, col.Name, vals[k], valueBits(v))
					}
				}
			}
		}
		return true
	})
	if err != nil || !strSeen || !intSeen {
		t.Fatalf("ScanBatches: %v (string column seen %t, int column seen %t)", err, strSeen, intSeen)
	}
}

func TestSectionBytes_MatchesTheDirectoryAndSumsBelowTheSegment(t *testing.T) {
	data := mustEncode(t, corpus(t, 2000, 10), Options{}, PageRows)
	seg := mustOpen(t, data)
	got := seg.SectionBytes()
	total := 0
	for name, n := range got {
		if n != seg.sectionLen(name) || n <= 0 {
			t.Fatalf("SectionBytes[%s] = %d, directory says %d", name, n, seg.sectionLen(name))
		}
		total += n
	}
	if len(got) != len(seg.secs) || total >= len(data) {
		t.Fatalf("SectionBytes: %d sections, %d bytes of %d", len(got), total, len(data))
	}
	got["nodes"] = -1
	if seg.SectionBytes()["nodes"] == -1 {
		t.Fatal("SectionBytes must return a copy")
	}
}
