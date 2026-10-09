package timeblock

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func axisFor(t testing.TB, profile temporal.Profile) temporal.Axis {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{1}, Profile: profile, Version: 1, Reference: "test-v1", CanonicalUnit: "unit"}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func mustBlock(t testing.TB, a temporal.Axis, rows []Row, l Layout) *Block {
	t.Helper()
	data, err := Encode(a, rows, l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := OpenOwned(data, a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestExactRowsAndMembership(t *testing.T) {
	// Catches sentinel zero, signed-offset overflow, discarded boundaries and
	// caller mutations replacing the sealed version.
	for _, p := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ} {
		a := axisFor(t, p)
		rows := []Row{{Start: math.MinInt64, Kind: Point}, {Start: 0, Kind: Point}, {Start: math.MaxInt64, Kind: Point}, {Start: -3, End: 2, Kind: ClosedOpen}, {Start: -3, End: 2, Kind: OpenClosed}, {Start: -3, End: 2, Kind: OpenOpen}, {Start: math.MinInt64, End: math.MaxInt64, Kind: ClosedClosed}, {Start: 0, End: 0, Kind: ClosedClosed}}
		for _, l := range []Layout{Partitioned, Sparse, Tagged} {
			b := mustBlock(t, a, rows, l)
			if b.Len() != len(rows) || b.Layout() != l || b.AxisHash() != a.DefinitionHash() {
				t.Fatal("metadata")
			}
			ledger := b.Ledger()
			if ledger.Total != ledger.Envelope+ledger.Starts+ledger.Ends+ledger.Kinds+ledger.Join || ledger.Envelope != headerBytes {
				t.Fatal(ledger)
			}
			n := 0
			err := b.Scan(t.Context(), func(i int, r Row) bool {
				if r != rows[i] {
					t.Fatalf("scan %d: %v != %v", i, r, rows[i])
				}
				n++
				return true
			})
			if err != nil || n != len(rows) {
				t.Fatal(n, err)
			}
			for i, want := range rows {
				got, err := b.Row(i)
				if err != nil || got != want {
					t.Fatal(i, got, err)
				}
				scope := referenceScope(t, a, want)
				for _, x := range []int64{math.MinInt64, -4, -3, -2, -1, 0, 1, 2, 3, math.MaxInt64} {
					pos := position(t, a, x)
					expect, err := scope.Contains(pos, temporal.Limits{})
					if err != nil {
						t.Fatal(err)
					}
					actual, err := b.ContainsAt(i, x)
					if err != nil || actual != expect {
						t.Fatalf("profile %v row %v x %d: %v != %v, %v", p, want, x, actual, expect, err)
					}
				}
			}
			old, _ := b.Row(1)
			rows[1].Start = 99
			sealed, _ := b.Row(1)
			if sealed != old {
				t.Fatal("sealed input alias")
			}
			rows[1].Start = 0
		}
	}
}
func position(t testing.TB, a temporal.Axis, x int64) temporal.Position {
	t.Helper()
	var p temporal.Position
	var err error
	if a.Descriptor().Profile == temporal.ProfileIntegerZ {
		p, err = temporal.IntegerPosition(a, temporal.Int64(x))
	} else {
		p, err = temporal.RationalPosition(a, temporal.RationalInt64(x))
	}
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func referenceScope(t testing.TB, a temporal.Axis, r Row) temporal.Scope {
	t.Helper()
	var s temporal.Scope
	var err error
	if r.Kind == Point {
		s, err = temporal.Point(position(t, a, r.Start))
	} else {
		lo, e := temporal.FiniteBound(position(t, a, r.Start), r.Kind == ClosedOpen || r.Kind == ClosedClosed)
		if e != nil {
			t.Fatal(e)
		}
		hi, e := temporal.FiniteBound(position(t, a, r.End), r.Kind == OpenClosed || r.Kind == ClosedClosed)
		if e != nil {
			t.Fatal(e)
		}
		s, err = temporal.Span(a, lo, hi, temporal.Limits{})
	}
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestProfileIsSemanticsNotIntegerCodec(t *testing.T) {
	z, q := axisFor(t, temporal.ProfileIntegerZ), axisFor(t, temporal.ProfileRationalQ)
	r := []Row{{Start: 0, End: 1, Kind: OpenOpen}}
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		if _, err := Encode(z, r, l, Limits{}); !errors.Is(err, ErrInvalidRow) {
			t.Fatal(err)
		}
		b := mustBlock(t, q, r, l)
		for _, x := range []int64{0, 1} {
			v, err := b.ContainsAt(0, x)
			if err != nil || v {
				t.Fatal(v, err)
			}
		}
	}
	s := referenceScope(t, q, r[0])
	got, err := FromScope(q, s)
	if err != nil || got != r[0] {
		t.Fatal(got, err)
	}
	for _, a := range []temporal.Axis{z, q} {
		s := referenceScope(t, a, Row{Start: 0, Kind: Point})
		r, err := FromScope(a, s)
		if err != nil || r != (Row{Start: 0, Kind: Point}) {
			t.Fatal(r, err)
		}
	}
	half, err := temporal.ParseRational("1/2", temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.RationalPosition(q, half)
	if err != nil {
		t.Fatal(err)
	}
	s, err = temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = FromScope(q, s); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	wide, err := temporal.ParseInteger("9223372036854775808", temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err = temporal.IntegerPosition(z, wide)
	if err != nil {
		t.Fatal(err)
	}
	s, err = temporal.Point(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = FromScope(z, s); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	all, err := temporal.All(z)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = FromScope(z, all); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	lex := axisFor(t, temporal.ProfileLexicographicQN)
	if _, err = Encode(lex, nil, Sparse, Limits{}); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	if _, err = FromScope(z, referenceScope(t, q, Row{Start: 0, Kind: Point})); !errors.Is(err, temporal.ErrAxisMismatch) {
		t.Fatal(err)
	}
}
func TestMalformedCannotAmplifyOrHideCorruption(t *testing.T) {
	a := axisFor(t, temporal.ProfileIntegerZ)
	rows := []Row{{Start: -2, Kind: Point}, {Start: 0, End: 8, Kind: ClosedOpen}, {Start: 7, Kind: Point}}
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		data, err := Encode(a, rows, l, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for i := range len(data) {
			bad := bytes.Clone(data)
			bad[i] ^= 1
			if _, err := OpenBorrowed(bad, a, Limits{}); err == nil {
				t.Fatalf("accepted changed byte %d", i)
			}
		}
		for i := range len(data) {
			if _, err := OpenOwned(data[:i], a, Limits{}); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("trunc %d: %v", i, err)
			}
		}
		bad := bytes.Clone(data)
		binary.LittleEndian.PutUint16(bad[8:], 99)
		if _, err := OpenBorrowed(bad, a, Limits{}); !errors.Is(err, ErrVersion) {
			t.Fatal(err)
		}
		bad = bytes.Clone(data)
		binary.LittleEndian.PutUint32(bad[16:], math.MaxUint32)
		sealDigest(bad)
		if _, err := OpenBorrowed(bad, a, Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		bad = bytes.Clone(data)
		bad[headerBytes] = 65
		sealDigest(bad)
		if _, err := OpenBorrowed(bad, a, Limits{}); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		if _, err := OpenOwned(data, a, Limits{MaxRows: 2}); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
		if _, err := OpenOwned(data, a, Limits{MaxBytes: len(data) - 1}); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
}
func TestAliasingLimitsAndErrors(t *testing.T) {
	a := axisFor(t, temporal.ProfileIntegerZ)
	data, err := Encode(a, []Row{{Start: 3, Kind: Point}}, Sparse, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := OpenOwned(data, a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := OpenBorrowed(data, a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if &owned.data[0] == &data[0] || &borrowed.data[0] != &data[0] {
		t.Fatal("ownership")
	}
	data[0] ^= 1
	if r, err := owned.Row(0); err != nil || r.Start != 3 {
		t.Fatal(r, err)
	}
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		b := mustBlock(t, a, nil, l)
		if b.Len() != 0 {
			t.Fatal(b.Len())
		}
		if err := b.Scan(t.Context(), func(int, Row) bool { return true }); err != nil {
			t.Fatal(err)
		}
	}
	b := owned
	for _, i := range []int{-1, 1} {
		if _, err := b.Row(i); !errors.Is(err, ErrRange) {
			t.Fatal(err)
		}
		if _, err := b.ContainsAt(i, 0); !errors.Is(err, ErrRange) {
			t.Fatal(err)
		}
	}
	if err := b.Scan(t.Context(), nil); !errors.Is(err, ErrCallback) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.Scan(ctx, func(int, Row) bool { return true }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	n := 0
	if err := b.Scan(t.Context(), func(int, Row) bool { n++; return false }); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	var nilBlock *Block
	if _, err := nilBlock.Row(0); !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	if err := nilBlock.Scan(t.Context(), func(int, Row) bool { return true }); !errors.Is(err, ErrRange) {
		t.Fatal(err)
	}
	for _, limits := range []Limits{{MaxRows: -1}, {MaxRows: MaxRows + 1}, {MaxBytes: -1}, {MaxBytes: MaxBytes + 1}} {
		if _, err := Encode(a, nil, Sparse, limits); !errors.Is(err, ErrLimits) {
			t.Fatal(err)
		}
	}
	for _, r := range []Row{{Start: 0, End: 1, Kind: Point}, {Start: 3, End: 2, Kind: ClosedClosed}, {Kind: 99}, {Start: 0, End: 0, Kind: OpenOpen}} {
		if _, err := Encode(a, []Row{r}, Sparse, Limits{}); !errors.Is(err, ErrInvalidRow) {
			t.Fatal(r, err)
		}
	}
	if _, err := Encode(a, nil, 99, Limits{}); !errors.Is(err, ErrLayout) {
		t.Fatal(err)
	}
	if _, err := Encode(temporal.Axis{}, nil, Sparse, Limits{}); !errors.Is(err, temporal.ErrInvalidAxis) {
		t.Fatal(err)
	}
}
func FuzzOpen(f *testing.F) {
	a := axisFor(f, temporal.ProfileIntegerZ)
	for _, l := range []Layout{Partitioned, Sparse, Tagged} {
		data, err := Encode(a, []Row{{Start: math.MinInt64, Kind: Point}, {Start: -1, End: math.MaxInt64, Kind: ClosedOpen}}, l, Limits{})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
	}
	check := func(t *testing.T, data []byte) {
		b, err := OpenBorrowed(data, a, Limits{})
		if err != nil {
			return
		}
		if b.Len() > MaxRows {
			t.Fatal("bound")
		}
		if err := b.Scan(t.Context(), func(i int, r Row) bool {
			got, err := b.Row(i)
			if err != nil || got != r {
				t.Fatal(got, r, err)
			}
			return true
		}); err != nil {
			t.Fatal(err)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		check(t, data)
		// Exercise structural trust boundaries after integrity has passed; bound
		// the cloned input before allocating or repairing attacker-controlled data.
		if len(data) >= headerBytes && len(data) <= MaxBytes {
			repaired := bytes.Clone(data)
			sealDigest(repaired)
			check(t, repaired)
		}
	})
}
