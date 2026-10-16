package segment

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// WP-09: segment columns are typed by the engine's Scalar kinds.

func sI(v int64) graphstate.Scalar  { return graphstate.I64(v) }
func sS(v string) graphstate.Scalar { return graphstate.String(v) }
func sB(v bool) graphstate.Scalar   { return graphstate.Bool(v) }
func sF(tb testing.TB, v float64) graphstate.Scalar {
	tb.Helper()
	s, err := graphstate.F64(v)
	if err != nil {
		tb.Fatal(err)
	}
	return s
}

func kindsSchema() Schema {
	return Schema{TypeName: testType, Columns: []Column{
		{"i", KindI64}, {"f", KindF64}, {"b", KindBool}, {"s", KindString},
	}}
}

// TestSegmentRejectsWidthTags: the 13-width tags (and 0, the null scalar
// tag, and anything unknown) are no column kind; a footer carrying one is
// corrupt, a format-1 file is unsupported.
func TestSegmentRejectsWidthTags(t *testing.T) {
	valid := map[Kind]bool{KindString: true, KindBool: true, KindI64: true, KindF64: true}
	for k := 0; k < 256; k++ {
		_, err := Encode(Schema{TypeName: testType, Columns: []Column{{"a", Kind(k)}}}, nil, Options{})
		if valid[Kind(k)] != (err == nil) {
			t.Fatalf("kind tag %d: err %v, valid %t", k, err, valid[Kind(k)])
		}
		if err != nil && !errors.Is(err, ErrInvalidSchema) {
			t.Fatalf("kind tag %d: %v, want ErrInvalidSchema", k, err)
		}
	}
	// A footer with a width tag, CRC recomputed.
	data := mustEncodeS(t, kindsSchema(), []Row{mkRow(1, 0, 1, 2, Property{"i", sI(1)})})
	for _, bad := range []byte{0, 1, 6, 9, 13, 14, 200} {
		d := append([]byte(nil), data...)
		fstart, _ := footerBounds(t, d)
		off := fstart + 2 + 2 + 2 + len(testType) + 2 + 2 + len("i")
		d[off] = bad
		resealFooter(t, d)
		if _, err := Open(d); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("footer kind tag %d: %v, want ErrCorrupt", bad, err)
		}
	}
	// A format-1 (width model) file is refused by version, not reinterpreted.
	d := append([]byte(nil), data...)
	binary.LittleEndian.PutUint16(d[8:], 1)
	reseal(d)
	if _, err := Open(d); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("format 1: %v, want ErrUnsupportedVersion", err)
	}
}

// TestSegmentScanReturnsScalarKinds: Scan returns the Scalar kind that went
// in, for matching and mismatching columns; I64(3) and F64(3.0) stay apart
// (no Cypher number coercion in storage) and hash differently.
func TestSegmentScanReturnsScalarKinds(t *testing.T) {
	f3 := sF(t, 3)
	rows := []Row{
		mkRow(1, 0, 1, 2, Property{"i", sI(3)}, Property{"f", f3}, Property{"b", sB(true)}, Property{"s", sS("x")}),
		mkRow(2, 0, 1, 3, Property{"i", f3}, Property{"f", sI(3)}, Property{"b", sS("true")}, Property{"s", sB(false)}), // all mismatched: fallback
		mkRow(3, 0, 1, 4, Property{"i", sI(-1 << 63)}, Property{"f", sF(t, -2.5e300)}, Property{"extra", sF(t, 0)}),
		mkRow(4, 0, 1, 5), // all absent
	}
	data := mustEncodeS(t, kindsSchema(), rows)
	seg := mustOpen(t, data)
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, seg)
	if len(got) != len(rows) {
		t.Fatalf("%d rows, want %d", len(got), len(rows))
	}
	for k := range rows {
		want := canon(rows[k])
		if len(got[k].Props) != len(want.Props) {
			t.Fatalf("row %d: %d props, want %d", want.ID, len(got[k].Props), len(want.Props))
		}
		for j, wp := range want.Props {
			gp := got[k].Props[j]
			same, err := gp.Value.Identical(wp.Value, graphstate.Limits{})
			if gp.Key != wp.Key || err != nil || !same || gp.Value.Kind() != wp.Value.Kind() {
				t.Fatalf("row %d prop %q: got kind %d %v, want kind %d %v", want.ID, wp.Key, gp.Value.Kind(), gp.Value.Render(), wp.Value.Kind(), wp.Value.Render())
			}
		}
	}
	h1 := rowHash(testType, &rows[0], []Property{{"a", sI(3)}})
	h2 := rowHash(testType, &rows[0], []Property{{"a", f3}})
	if h1 == h2 {
		t.Fatal("I64(3) and F64(3.0) hash alike")
	}
}

// TestSegmentRejectsNonColumnScalars: only I64/F64/Bool/String are storable;
// null, the zero Scalar and oversize keys are refused with ErrInvalidRow.
func TestSegmentRejectsNonColumnScalars(t *testing.T) {
	for name, v := range map[string]graphstate.Scalar{"null": graphstate.Null(), "zero": {}} {
		_, err := Encode(kindsSchema(), []Row{mkRow(1, 0, 1, 2, Property{"i", v})}, Options{})
		if !errors.Is(err, ErrInvalidRow) {
			t.Fatalf("%s: %v, want ErrInvalidRow", name, err)
		}
	}
}

// TestSegmentMixedI64F64ColumnSeeds: unit form of the FuzzOpen /
// FuzzOpenResealed seeds with a mixed I64/F64 column: the segment opens,
// verifies, and every single-byte mutation with recomputed CRCs ends in a
// typed error or a segment that survives exercise.
func TestSegmentMixedI64F64ColumnSeeds(t *testing.T) {
	var rows []Row
	for i := 0; i < 6; i++ {
		v := sI(int64(i) - 3)
		if i%2 == 1 {
			v = sF(t, float64(i)-3)
		}
		rows = append(rows, mkRow(uint32(i+1), 0, 1, int64(2+i), Property{"i", v}, Property{"f", v}))
	}
	data, err := encode(kindsSchema(), rows, Options{IntegrityBlockRows: 2}, 4)
	if err != nil {
		t.Fatal(err)
	}
	seg := mustOpen(t, data)
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
	}
	for at := range data {
		d := append([]byte(nil), data...)
		d[at] ^= 0xA5
		reseal(d)
		s, err := Open(d)
		if err != nil {
			if !isTyped(err) {
				t.Fatalf("byte %d: untyped %v", at, err)
			}
			continue
		}
		exercise(s)
	}
}

func mustEncodeS(tb testing.TB, s Schema, rows []Row) []byte {
	tb.Helper()
	data, err := encode(s, rows, Options{}, PageRows)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

func mustHash(tb testing.TB, r *Row) [32]byte {
	tb.Helper()
	h, err := RowHash(testType, r)
	if err != nil {
		tb.Fatal(err)
	}
	return h
}

// TestKindValueRefusesNonCanonicalBits: a column cell no valid Scalar can
// hold (NaN, Inf, -0 bits for F64; 2 for Bool; any value for String) is
// refused, never returned.
func TestKindValueRefusesNonCanonicalBits(t *testing.T) {
	for name, tc := range map[string]struct {
		k  Kind
		x  int64
		ok bool
	}{
		"f64 nan":     {KindF64, int64(0x7ff8000000000001), false},
		"f64 +inf":    {KindF64, int64(0x7ff0000000000000), false},
		"f64 -0":      {KindF64, -1 << 63, false},
		"f64 1.5":     {KindF64, int64(0x3ff8000000000000), true},
		"bool 2":      {KindBool, 2, false},
		"bool -1":     {KindBool, -1, false},
		"bool 1":      {KindBool, 1, true},
		"i64 min":     {KindI64, -1 << 63, true},
		"string cell": {KindString, 0, false},
		"tag 0":       {0, 0, false},
	} {
		if _, ok := kindValue(tc.k, tc.x); ok != tc.ok {
			t.Errorf("%s: ok %t, want %t", name, ok, tc.ok)
		}
	}
}
