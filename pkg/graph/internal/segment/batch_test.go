package segment

import (
	"errors"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ADR-0011 S2 reader doors: the batch (column) path, ID iteration, ranges.

// TestScanBatches_ColumnsEqualRowsAndRowEqualsScan: every batch column holds
// exactly what the rows built from it carry, and Batch.Row builds the same
// row Scan does — one decode serves both the row door and a column consumer.
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
			if b.IDs[k] != int64(w.ID()) || b.StartIDs[k] != int64(w.StartNodeID()) || b.EndIDs[k] != int64(w.EndNodeID()) ||
				b.Versions[k] != int64(w.Version()) {
				t.Fatalf("row %d: batch identity columns differ from the row", b.Base+k)
			}
			var vf, vt, tx int64
			if tm := w.Temporal(); tm != nil {
				vf, vt, tx = int64(tm.ValidFrom), int64(tm.ValidTo), int64(tm.TxFrom)
			}
			if b.ValidFrom[k] != vf || b.ValidTo[k] != vt || b.TxFrom[k] != tx {
				t.Fatalf("row %d: batch temporal columns differ from the row", b.Base+k)
			}
			got, err := b.Row(k)
			if err != nil {
				t.Fatal(err)
			}
			assertSameRow(t, w, got)
			if got.Integrity().Hash != w.Integrity().Hash {
				t.Fatalf("row %d: Batch.Row hash differs", b.Base+k)
			}
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
}

func TestForEachID_AscendingAndPointsAtTheRow(t *testing.T) {
	rows := corpus(t, 5000, 4)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 512))
	all := scanAll(t, seg)
	var ids []int64
	err := seg.ForEachID(func(id types.RelID, row int) bool {
		if all[row].ID() != id {
			t.Fatalf("ForEachID: row %d holds id %d, entry says %d", row, all[row].ID(), id)
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
		err := seg.ScanRange(rg[0], rg[1], func(i int, r *types.Relationship) bool {
			if i != next {
				t.Fatalf("ScanRange(%v) index %d, want %d", rg, i, next)
			}
			assertSameRow(t, all[i], r)
			next++
			return true
		})
		if err != nil || next != rg[1] {
			t.Fatalf("ScanRange(%v): %v, stopped at %d", rg, err, next)
		}
	}
	calls := 0
	if err := seg.ScanRange(0, 500, func(int, *types.Relationship) bool { calls++; return false }); err != nil || calls != 1 {
		t.Fatalf("ScanRange must stop when fn returns false: %v, %d", err, calls)
	}
	for _, bad := range [][2]int{{-1, 3}, {5, 3}, {0, 3001}} {
		if err := seg.ScanRange(bad[0], bad[1], func(int, *types.Relationship) bool { return true }); !errors.Is(err, ErrOutOfRange) {
			t.Fatalf("ScanRange(%v) = %v, want ErrOutOfRange", bad, err)
		}
	}
}

// TestLookup_EveryIDAcrossPageBoundaries checks the page-level binary search
// with tiny pages, so ID runs (several versions) straddle page boundaries.
func TestLookup_EveryIDAcrossPageBoundaries(t *testing.T) {
	rows := corpus(t, 4000, 6)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 16))
	all := scanAll(t, seg)
	want := map[types.RelID][]int{}
	for i, r := range all {
		want[r.ID()] = append(want[r.ID()], i)
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

func TestRowError_NamesTheRefusedRow(t *testing.T) {
	good := mkRow(t, 10, 0, 1, 2, map[string]any{"s": "x"}, nil, nil)
	bad := mkRow(t, 11, 3, 1, 2, map[string]any{"s": "y"}, nil, nil)
	bad.Integrity().Hash = good.Integrity().Hash // content does not reproduce it
	_, err := Encode(testSchema(), []*types.Relationship{good, bad}, Options{})
	var re *RowError
	if !errors.As(err, &re) || re.ID != 11 || re.Version != 3 || !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("Encode with a wrong stored hash = %v, want a RowError for id 11 v3 wrapping ErrHashMismatch", err)
	}
	if re.Error() == "" || !errors.Is(re.Unwrap(), ErrHashMismatch) {
		t.Fatalf("RowError text/unwrap: %q %v", re.Error(), re.Unwrap())
	}
	noHash := mkRow(t, 12, 0, 1, 2, nil, nil, nil)
	noHash.Integrity().Hash = ""
	_, err = Encode(testSchema(), []*types.Relationship{noHash}, Options{})
	if !errors.As(err, &re) || re.ID != 12 {
		t.Fatalf("Encode with no stored hash = %v, want a RowError for id 12", err)
	}
}

func TestValidateSchema(t *testing.T) {
	if err := ValidateSchema(testSchema()); err != nil {
		t.Fatalf("valid schema: %v", err)
	}
	for _, s := range []Schema{
		{TypeName: "", TypeToken: 1},
		{TypeName: "X", TypeToken: 0},
		{TypeName: "X", TypeToken: 1, Columns: []Column{{"tkg_x", KindInt}}},
		{TypeName: "X", TypeToken: 1, Columns: []Column{{"a", KindInt}, {"a", KindBool}}},
		{TypeName: "X", TypeToken: 1, Columns: []Column{{"a", 99}}},
	} {
		if err := ValidateSchema(s); !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("ValidateSchema(%+v) = %v, want ErrInvalidSchema", s, err)
		}
	}
}

// TestBatch_TypedColumnsMatchRows: the typed per-column accessors hold what
// the rows carry — dictionary codes resolve to the row's string, int columns
// hold the declared value's bits, presence matches.
func TestBatch_TypedColumnsMatchRows(t *testing.T) {
	rows := corpus(t, 6000, 9)
	seg := mustOpen(t, mustEncode(t, rows, Options{}, 1024))
	want := scanAll(t, seg)
	strSeen, intSeen := false, false
	err := seg.ScanBatches(func(b *Batch) bool {
		cols := b.Columns()
		for c, col := range cols {
			if codes, dict, present, ok := b.StringColumn(c); ok {
				strSeen = true
				for k := 0; k < b.N; k++ {
					v, has := want[b.Base+k].GetProperty(col.Name)
					s, isStr := v.(string)
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
					v, has := want[b.Base+k].GetProperty(col.Name)
					declared := has && kindOf(v) == col.Kind
					if present(k) != declared {
						t.Fatalf("row %d col %s: presence %t, row has %T", b.Base+k, col.Name, present(k), v)
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
