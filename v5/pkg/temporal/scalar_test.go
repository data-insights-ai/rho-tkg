package temporal

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
)

func testAxis(t *testing.T, profile Profile) Axis {
	t.Helper()
	a, err := NewAxis(AxisDescriptor{ID: AxisID{1}, Profile: profile, Version: 1, Reference: "test-clock", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustInteger(t *testing.T, s string) Integer {
	t.Helper()
	v, err := ParseInteger(s, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func mustRational(t *testing.T, s string) Rational {
	t.Helper()
	v, err := ParseRational(s, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestIntegerDoesNotEndAtCodecCeiling(t *testing.T) {
	var zero Integer
	if zero.String() != "0" || zero.Sign() != 0 {
		t.Fatal("zero value is not zero")
	}
	if v, ok := zero.Int64(); !ok || v != 0 {
		t.Fatal("zero Int64")
	}
	for _, v := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		x := Int64(v)
		parsed := mustInteger(t, x.String())
		if order, err := x.Compare(parsed, Limits{}); err != nil || order != Equal {
			t.Fatal(order, err)
		}
		if actual, ok := parsed.Int64(); !ok || actual != v {
			t.Fatal(actual, ok)
		}
	}
	max := Int64(math.MaxInt64)
	next, err := max.Successor(Limits{})
	if err != nil || next.String() != "9223372036854775808" {
		t.Fatal(next, err)
	}
	if _, ok := next.Int64(); ok {
		t.Fatal("wider integer narrowed")
	}
	prev, err := Int64(math.MinInt64).Predecessor(Limits{})
	if err != nil || prev.String() != "-9223372036854775809" {
		t.Fatal(prev, err)
	}
	if max.String() != "9223372036854775807" {
		t.Fatal("successor mutated input")
	}
	back, err := next.Predecessor(Limits{})
	if err != nil || back.String() != max.String() {
		t.Fatal(back, err)
	}
	back, err = prev.Successor(Limits{})
	if err != nil || back.String() != "-9223372036854775808" {
		t.Fatal(back, err)
	}
	for _, s := range []string{"", " ", "1.0", "1/2", "1e9", "--1", "１"} {
		if _, err := ParseInteger(s, Limits{}); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("%q: %v", s, err)
		}
	}
	if mustInteger(t, "+0001").String() != "1" || mustInteger(t, "-000").String() != "0" {
		t.Fatal("parse normalization")
	}
}

func TestRationalExactnessAndComparatorLaws(t *testing.T) {
	var zero Rational
	if zero.String() != "0" || zero.Numerator().String() != "0" || zero.Denominator().String() != "1" {
		t.Fatal("invalid zero rational")
	}
	for input, expected := range map[string]string{"1/3": "1/3", "2/6": "1/3", "-1/-3": "1/3", "0/-9": "0", ".125": "1/8", "-1.25": "-5/4", "12.": "12", "+0001.20": "6/5"} {
		if got := mustRational(t, input).String(); got != expected {
			t.Fatalf("%s => %s", input, got)
		}
	}
	for _, input := range []string{".0", ".000", "+.00", "-.00", "0.0", "-0.000"} {
		if got := mustRational(t, input).String(); got != "0" {
			t.Fatalf("%s => %s", input, got)
		}
	}
	fraction, err := Fraction(Int64(math.MinInt64), Int64(-1), Limits{})
	if err != nil || fraction.String() != "9223372036854775808" {
		t.Fatal(fraction, err)
	}
	if RationalInt64(-2).String() != "-2" {
		t.Fatal("integer rational")
	}
	if _, err := Fraction(Int64(1), Int64(0), Limits{}); !errors.Is(err, ErrZeroDenominator) {
		t.Fatal(err)
	}
	if _, err := ParseRational("1/0", Limits{}); !errors.Is(err, ErrZeroDenominator) {
		t.Fatal(err)
	}
	for _, s := range []string{"", ".", "-.", "+.", "1e1000000000", "1/2/3", "1.2/3", "NaN", "Inf", " 1", "1_000"} {
		if _, err := ParseRational(s, Limits{}); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("%q: %v", s, err)
		}
	}
	values := []Rational{mustRational(t, "-9223372036854775809"), RationalInt64(-1), zero, mustRational(t, "1/3"), mustRational(t, "1/2"), RationalInt64(1), mustRational(t, "9223372036854775808")}
	for i, a := range values {
		for j, b := range values {
			want := Equal
			if i < j {
				want = Less
			}
			if i > j {
				want = Greater
			}
			if got, err := a.Compare(b, Limits{}); err != nil || got != want {
				t.Fatalf("%s vs %s: %v %v", a, b, got, err)
			}
		}
	}
	// Both operands fit the input budget, but their cross product needs twice it.
	a := mustRational(t, "255/253")
	b := mustRational(t, "253/251")
	if got, err := a.Compare(b, Limits{MaxMagnitudeBits: 8}); err != nil || got != Less {
		t.Fatal(got, err)
	}
	if a.String() != "255/253" || b.String() != "253/251" {
		t.Fatal("comparison mutated input")
	}
}

func TestLimitsFailBeforeUnboundedWork(t *testing.T) {
	if err := (Limits{}).Validate(); err != nil {
		t.Fatal(err)
	}
	if got := DefaultLimits(); got.MaxInputBytes != 65536 || got.MaxMagnitudeBits != 4096 || got.MaxValueBytes != 65536 || got.MaxRegionPieces != 4096 || got.MaxDescriptorBytes != 65536 {
		t.Fatal(got)
	}
	for _, l := range []Limits{{MaxInputBytes: -1}, {MaxValueBytes: -1}, {MaxMagnitudeBits: -1}, {MaxRegionPieces: -1}, {MaxDescriptorBytes: -1}, {MaxMagnitudeBits: hardMaxMagnitudeBits + 1}, {MaxInputBytes: hardMaxBytes + 1}} {
		if err := l.Validate(); !errors.Is(err, ErrInvalidLimits) {
			t.Fatal(l, err)
		}
	}
	for _, s := range []string{"255", "-255"} {
		if _, err := ParseInteger(s, Limits{MaxMagnitudeBits: 8}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := ParseInteger("256", Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := ParseInteger("0000", Limits{MaxInputBytes: 3}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := ParseRational(strings.Repeat("9", 65537), Limits{}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Int64(255).Successor(Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Int64(-255).Predecessor(Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Int64(256).Compare(Int64(1), Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := Fraction(Int64(256), Int64(256), Limits{MaxMagnitudeBits: 8}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestAxisAndPositionValidateBeforeMeaning(t *testing.T) {
	a := testAxis(t, ProfileIntegerZ)
	if a.Descriptor().Profile != ProfileIntegerZ || a.DefinitionHash() == ([32]byte{}) {
		t.Fatal("descriptor")
	}
	d := a.Descriptor()
	d.Reference = "changed"
	b, err := NewAxis(d, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := IntegerPosition(a, Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	q, err := IntegerPosition(b, Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ComparePositions(p, q, Limits{}); !errors.Is(err, ErrAxisMismatch) {
		t.Fatal(err)
	}
	if p.Axis().DefinitionHash() != a.DefinitionHash() || p.Profile() != ProfileIntegerZ {
		t.Fatal("position axis")
	}
	if n, ok := p.Integer(); !ok || n.String() != "0" {
		t.Fatal(n, ok)
	}
	if _, ok := p.Rational(); ok {
		t.Fatal("integer exposed as rational")
	}
	if _, _, ok := p.Lex(); ok {
		t.Fatal("integer exposed as lex")
	}
	if _, err := RationalPosition(a, RationalInt64(0)); !errors.Is(err, ErrIncompatibleDomain) {
		t.Fatal(err)
	}
	if _, err := IntegerPosition(Axis{}, Int64(0)); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
	if _, err := ComparePositions(Position{}, p, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	for _, change := range []func(*AxisDescriptor){func(d *AxisDescriptor) { d.ID = AxisID{} }, func(d *AxisDescriptor) { d.Reference = " " }, func(d *AxisDescriptor) { d.CanonicalUnit = "" }} {
		d := a.Descriptor()
		change(&d)
		if _, err := NewAxis(d, Limits{}); !errors.Is(err, ErrInvalidAxis) {
			t.Fatal(err)
		}
	}
	d = a.Descriptor()
	d.Profile = Profile(255)
	if _, err := NewAxis(d, Limits{}); !errors.Is(err, ErrUnknownProfile) {
		t.Fatal(err)
	}
	d = a.Descriptor()
	d.Version = 2
	if _, err := NewAxis(d, Limits{}); !errors.Is(err, ErrUnknownVersion) {
		t.Fatal(err)
	}
	if _, err := NewAxis(a.Descriptor(), Limits{MaxDescriptorBytes: 1}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if _, err := NewAxis(a.Descriptor(), Limits{MaxInputBytes: -1}); !errors.Is(err, ErrInvalidLimits) {
		t.Fatal(err)
	}
}

func TestLexicographicSuccessorIsNotDenseOrCapped(t *testing.T) {
	a := testAxis(t, ProfileLexicographicQN)
	p, err := LexPosition(a, RationalInt64(12), Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	q, err := p.Successor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	model, micro, ok := q.Lex()
	if !ok || model.String() != "12" || micro.String() != "1" {
		t.Fatal(model, micro, ok)
	}
	if _, err := p.Predecessor(Limits{}); !errors.Is(err, ErrNoPredecessor) {
		t.Fatal(err)
	}
	back, err := q.Predecessor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ComparePositions(p, back, Limits{}); err != nil || got != Equal {
		t.Fatal(got, err)
	}
	if got, err := ComparePositions(p, q, Limits{}); err != nil || got != Less {
		t.Fatal(got, err)
	}
	wide, err := LexPosition(a, RationalInt64(12), mustInteger(t, "18446744073709551615"))
	if err != nil {
		t.Fatal(err)
	}
	wide, err = wide.Successor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	_, m, _ := wide.Lex()
	if m.String() != "18446744073709551616" {
		t.Fatal(m)
	}
	if _, err := LexPosition(a, RationalInt64(12), Int64(-1)); !errors.Is(err, ErrInvalidValue) {
		t.Fatal(err)
	}
	ra := testAxis(t, ProfileRationalQ)
	rp, err := RationalPosition(ra, mustRational(t, "1/3"))
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := rp.Rational(); !ok || r.String() != "1/3" {
		t.Fatal(r, ok)
	}
	if _, err := rp.Successor(Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
		t.Fatal(err)
	}
	if _, err := rp.Predecessor(Limits{}); !errors.Is(err, ErrUnsupportedPredicate) {
		t.Fatal(err)
	}
	ip, err := IntegerPosition(testAxis(t, ProfileIntegerZ), Int64(math.MaxInt64))
	if err != nil {
		t.Fatal(err)
	}
	ip, err = ip.Successor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	ip, err = ip.Predecessor(Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := ip.Integer(); v.String() != "9223372036854775807" {
		t.Fatal(v)
	}
}

func TestCodecCanonicalRoundTripAndAtomicAppend(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := testAxis(t, profile)
		var p Position
		var err error
		switch profile {
		case ProfileIntegerZ:
			p, err = IntegerPosition(a, mustInteger(t, "-9223372036854775809"))
		case ProfileRationalQ:
			p, err = RationalPosition(a, mustRational(t, "1/3"))
		case ProfileLexicographicQN:
			p, err = LexPosition(a, mustRational(t, "1/3"), mustInteger(t, "18446744073709551616"))
		}
		if err != nil {
			t.Fatal(err)
		}
		wire, err := AppendPosition(nil, p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := DecodePosition(wire, a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := ComparePositions(p, decoded, Limits{}); err != nil || got != Equal {
			t.Fatal(got, err)
		}
		h, err := PositionHash(p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		h2, err := PositionHash(decoded, Limits{})
		if err != nil || h != h2 {
			t.Fatal(err)
		}
		for n := range len(wire) {
			if _, err := DecodePosition(wire[:n], a, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
				t.Fatalf("truncation %d: %v", n, err)
			}
		}
		if _, err := DecodePosition(append(bytes.Clone(wire), 0), a, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
			t.Fatal(err)
		}
		prefix := make([]byte, 3, 1000)
		copy(prefix, "abc")
		before := bytes.Clone(prefix[:cap(prefix)])
		if _, err := AppendPosition(prefix, p, Limits{MaxValueBytes: 1}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		if !bytes.Equal(prefix[:cap(prefix)], before) {
			t.Fatal("failed append changed backing storage")
		}
		out, err := AppendPosition(prefix, p, Limits{MaxValueBytes: len(wire)})
		if err != nil || !bytes.Equal(out[:3], []byte("abc")) {
			t.Fatal(err)
		}
		if _, err := DecodePosition(wire, a, Limits{MaxValueBytes: len(wire) - 1}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		d := a.Descriptor()
		d.Reference = "other"
		other, err := NewAxis(d, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodePosition(wire, other, Limits{}); !errors.Is(err, ErrAxisMismatch) {
			t.Fatal(err)
		}
	}
	if _, err := AppendPosition(nil, Position{}, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
	if _, err := PositionHash(Position{}, Limits{}); !errors.Is(err, ErrInvalidPosition) {
		t.Fatal(err)
	}
}
