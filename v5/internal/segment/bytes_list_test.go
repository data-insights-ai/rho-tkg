package segment

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func sBytes(b []byte) graphstate.Scalar { return graphstate.Bytes(b) }
func sList(tb testing.TB, el ...graphstate.Scalar) graphstate.Scalar {
	tb.Helper()
	v, err := graphstate.List(el)
	if err != nil {
		tb.Fatal(err)
	}
	return v
}

// TestSegmentBytesListFallbackRoundTrip: Bytes and List have no column; they ride
// the fallback column (format stays 2), keep their kind and verify, declared
// or not, and a bytes value never hashes like the string with the same bytes.
func TestSegmentBytesListFallbackRoundTrip(t *testing.T) {
	rows := []Row{
		mkRow(1, 0, 1, 2, Property{"fact_wire", sBytes([]byte{0, 0xff, 0})}, Property{"ov_support", sList(t, sI(3), sI(-7))}, Property{"s", sS("x")}),
		mkRow(2, 0, 1, 3, Property{"fact_wire", sBytes(nil)}, Property{"ov_support", sList(t)}),
		mkRow(3, 0, 1, 4, Property{"s", sBytes([]byte("x"))}, Property{"i", sList(t, sF(t, 3), sS("a"), sB(true), sBytes([]byte{1}))}), // declared columns hold other kinds
	}
	data := mustEncodeS(t, kindsSchema(), rows)
	seg := mustOpen(t, data)
	if err := seg.Verify(); err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, seg)
	if len(got) != len(rows) {
		t.Fatalf("%d rows", len(got))
	}
	for k := range rows {
		want := canon(rows[k])
		if len(got[k].Props) != len(want.Props) {
			t.Fatalf("row %d: %d props want %d", want.ID, len(got[k].Props), len(want.Props))
		}
		for j, wp := range want.Props {
			gp := got[k].Props[j]
			same, err := gp.Value.Identical(wp.Value, graphstate.Limits{})
			if gp.Key != wp.Key || err != nil || !same || gp.Value.Kind() != wp.Value.Kind() {
				t.Fatalf("row %d prop %q: kind %d, want %d", want.ID, wp.Key, gp.Value.Kind(), wp.Value.Kind())
			}
		}
	}
	hb := rowHash(testType, &rows[0], []Property{{"a", sBytes([]byte("x"))}})
	hs := rowHash(testType, &rows[0], []Property{{"a", sS("x")}})
	hl := rowHash(testType, &rows[0], []Property{{"a", sList(t, sS("x"))}})
	if hb == hs || hb == hl || hs == hl {
		t.Fatal("bytes, string and list hash alike")
	}
}

// TestSegmentRefusesOversizeBytesAndList: a value past the wire bound is
// ErrInvalidRow at Encode, never a silently dropped property.
func TestSegmentRefusesOversizeBytesAndList(t *testing.T) {
	big := make([]byte, 64<<20)
	for name, v := range map[string]graphstate.Scalar{"bytes": sBytes(big)} {
		_, err := Encode(kindsSchema(), []Row{mkRow(1, 0, 1, 2, Property{"x", v})}, Options{})
		if !errors.Is(err, ErrInvalidRow) {
			t.Fatalf("%s: %v, want ErrInvalidRow", name, err)
		}
	}
}
