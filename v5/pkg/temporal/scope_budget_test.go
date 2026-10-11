package temporal

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func encodingBudgetAxis(t *testing.T, profile Profile) Axis {
	t.Helper()
	a, err := NewAxis(AxisDescriptor{ID: AxisID{byte(profile)}, Profile: profile, Version: 1, Reference: "encoding-budget:v1", CanonicalUnit: "tick"}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func encodingBudgetPosition(t *testing.T, a Axis, n int64) Position {
	t.Helper()
	var p Position
	var err error
	switch a.Descriptor().Profile {
	case ProfileIntegerZ:
		p, err = IntegerPosition(a, Int64(n))
	case ProfileRationalQ:
		p, err = RationalPosition(a, RationalInt64(n))
	case ProfileLexicographicQN:
		p, err = LexPosition(a, RationalInt64(n), Int64(0))
	}
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func encodingBudgetSpan(t *testing.T, a Axis, lo, hi Position) Scope {
	t.Helper()
	left, err := FiniteBound(lo, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := FiniteBound(hi, false)
	if err != nil {
		t.Fatal(err)
	}
	s, err := Span(a, left, right, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestScopeEncodingBoundsExactWireInlineAndAllocationFree(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := encodingBudgetAxis(t, profile)
		point, err := Point(encodingBudgetPosition(t, a, math.MinInt64))
		if err != nil {
			t.Fatal(err)
		}
		other, err := Point(encodingBudgetPosition(t, a, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}
		region, err := Region(a, []Scope{point, other}, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		empty, err := Empty(a)
		if err != nil {
			t.Fatal(err)
		}
		unplaced, err := Unplaced(a)
		if err != nil {
			t.Fatal(err)
		}
		all, err := All(a)
		if err != nil {
			t.Fatal(err)
		}
		span := encodingBudgetSpan(t, a, encodingBudgetPosition(t, a, -100), encodingBudgetPosition(t, a, 100))
		for _, s := range []Scope{point, other, region, empty, unplaced, all, span} {
			wire, err := AppendScope(nil, s, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			var b ScopeEncodingBounds
			allocations := testing.AllocsPerRun(20, func() { b, err = s.EncodingBounds(Limits{}) })
			if err != nil || allocations != 0 || b.WireBytes != len(wire) || b.Parts != len(s.parts) || b.NumericBytes != 0 {
				t.Fatal("inline encoding introspection changed size or allocated", profile, s.Kind(), b, len(wire), allocations, err)
			}
		}
	}
}
func TestScopeEncodingBoundsWideIntegerRationalLexAndMixedEndpoints(t *testing.T) {
	wide, err := ParseInteger(strings.Repeat("9", 1000), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	den, err := ParseInteger(strings.Repeat("1", 999), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	fraction, err := Fraction(wide, den, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := encodingBudgetAxis(t, profile)
		var p Position
		switch profile {
		case ProfileIntegerZ:
			p, err = IntegerPosition(a, wide)
		case ProfileRationalQ:
			p, err = RationalPosition(a, fraction)
		case ProfileLexicographicQN:
			p, err = LexPosition(a, fraction, wide)
		}
		if err != nil {
			t.Fatal(err)
		}
		point, err := Point(p)
		if err != nil {
			t.Fatal(err)
		}
		span := encodingBudgetSpan(t, a, encodingBudgetPosition(t, a, 0), p)
		negative, err := Point(encodingBudgetPosition(t, a, -1))
		if err != nil {
			t.Fatal(err)
		}
		region, err := Region(a, []Scope{negative, point}, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []Scope{point, span, region} {
			var bounds ScopeEncodingBounds
			allocations := testing.AllocsPerRun(20, func() { bounds, err = s.EncodingBounds(Limits{}) })
			if err != nil || allocations != 0 || bounds.NumericBytes <= 0 {
				t.Fatal("wide preflight did numeric work or omitted scratch", profile, s.Kind(), bounds, allocations, err)
			}
			wire, err := AppendScope(nil, s, Limits{})
			if err != nil || bounds.WireBytes != len(wire) || bounds.Parts != len(s.parts) {
				t.Fatal(bounds, len(wire), err)
			}
			var again []byte
			if allocating := testing.AllocsPerRun(20, func() { again, err = AppendScope(wire[:0], s, Limits{}) }); err != nil || allocating < 1 || len(again) != len(wire) {
				t.Fatal("numeric scratch positive control insensitive", profile, s.Kind(), allocating, err)
			}
			if _, err := s.EncodingBounds(Limits{MaxValueBytes: len(wire) - 1}); !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
			if _, err := s.EncodingBounds(Limits{MaxMagnitudeBits: 64}); !errors.Is(err, ErrResourceLimit) {
				t.Fatal(err)
			}
		}
	}
}
func TestScopeEncodingBoundsErrorsAndCheckedReservationTotals(t *testing.T) {
	a := encodingBudgetAxis(t, ProfileIntegerZ)
	p := encodingBudgetPosition(t, a, 0)
	point, err := Point(p)
	if err != nil {
		t.Fatal(err)
	}
	other, err := Point(encodingBudgetPosition(t, a, 10))
	if err != nil {
		t.Fatal(err)
	}
	region, err := Region(a, []Scope{point, other}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		s    Scope
		l    Limits
		want error
	}{
		{Scope{}, Limits{}, ErrInvalidScope},
		{Scope{axis: Axis{}, kind: ScopeEmpty}, Limits{}, ErrInvalidAxis},
		{point, Limits{MaxValueBytes: -1}, ErrInvalidLimits},
		{point, Limits{MaxValueBytes: 1}, ErrResourceLimit},
		{point, Limits{MaxDescriptorBytes: 1}, ErrResourceLimit},
		{region, Limits{MaxRegionPieces: 1}, ErrResourceLimit},
		{Scope{axis: a, kind: ScopeEmpty, parts: point.parts}, Limits{}, ErrInvalidScope},
		{Scope{axis: a, kind: ScopeRegion}, Limits{}, ErrInvalidScope},
		{Scope{axis: a, kind: ScopeSpan, parts: []interval{{Bound{}, PositiveInfinity()}}}, Limits{}, ErrInvalidBound},
	} {
		got, err := tc.s.EncodingBounds(tc.l)
		if !errors.Is(err, tc.want) || got != (ScopeEncodingBounds{}) {
			t.Fatal(tc.want, got, err)
		}
	}
	b := ScopeEncodingBounds{NumericBytes: math.MaxInt}
	if err := b.addNumeric(1); !errors.Is(err, ErrResourceLimit) || b.NumericBytes != math.MaxInt {
		t.Fatal(b, err)
	}
	if err := b.addNumeric(-1); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
	if err := b.addNumeric(0); err != nil || b.NumericBytes != math.MaxInt {
		t.Fatal(err)
	}
	// An invalid discrete flag stays a canonical codec refusal; bounds never
	// claim to perform that final semantic validation.
	bad := point
	bad.parts = []interval{{Bound{kind: BoundFinite, position: p, inclusive: false}, Bound{kind: BoundFinite, position: encodingBudgetPosition(t, a, 10), inclusive: false}}}
	bad.kind = ScopeSpan
	if _, err := bad.EncodingBounds(Limits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := AppendScope(nil, bad, Limits{}); !errors.Is(err, ErrInvalidEncoding) {
		t.Fatal(err)
	}
}

func TestScopeScratchBoundsCombinedOperandsAndSuccessorGrowth(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ, ProfileLexicographicQN} {
		a := encodingBudgetAxis(t, profile)
		small, _ := Point(encodingBudgetPosition(t, a, 0))
		wideInteger, err := ParseInteger(strings.Repeat("9", 1000), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		fraction, err := Fraction(wideInteger, Int64(3), Limits{})
		if err != nil {
			t.Fatal(err)
		}
		var coordinate Position
		switch profile {
		case ProfileIntegerZ:
			coordinate, err = IntegerPosition(a, wideInteger)
		case ProfileRationalQ:
			coordinate, err = RationalPosition(a, fraction)
		case ProfileLexicographicQN:
			coordinate, err = LexPosition(a, fraction, wideInteger)
		}
		if err != nil {
			t.Fatal(err)
		}
		wide, _ := Point(coordinate)
		var bound ScopeScratchBounds
		allocations := testing.AllocsPerRun(3, func() { bound, err = small.ScratchBounds(wide, Limits{}) })
		if err != nil || allocations != 0 || bound.ComparisonBytes <= 0 || bound.AtomicEncodingBytes < bound.ComparisonBytes {
			t.Fatal(profile, bound, allocations, err)
		}
		reverse, err := wide.ScratchBounds(small, Limits{})
		if err != nil || bound != reverse {
			t.Fatal("asymmetric combined reservation", bound, reverse, err)
		}
		actual, err := ComparePositions(encodingBudgetPosition(t, a, 0), coordinate, Limits{})
		if err != nil || actual != Less {
			t.Fatal(actual, err)
		}
		if _, err = small.ScratchBounds(wide, Limits{MaxMagnitudeBits: 64}); !errors.Is(err, ErrResourceLimit) {
			t.Fatal(err)
		}
		foreign := encodingBudgetAxis(t, profile)
		foreign.desc.ID[1] = 7
		foreign, err = NewAxis(foreign.desc, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		other, _ := All(foreign)
		if _, err = small.ScratchBounds(other, Limits{}); !errors.Is(err, ErrAxisMismatch) {
			t.Fatal(err)
		}
	}
	a := encodingBudgetAxis(t, ProfileIntegerZ)
	edge, _ := Point(encodingBudgetPosition(t, a, math.MaxInt64))
	all, _ := All(a)
	bound, err := all.ScratchBounds(edge, Limits{})
	if err != nil || bound.SuccessorBytes == 0 || bound.ComparisonBytes == 0 {
		t.Fatal("inline successor overflow omitted", bound, err)
	}
	q := encodingBudgetAxis(t, ProfileRationalQ)
	small, _ := Point(encodingBudgetPosition(t, q, 1))
	bound, err = small.ScratchBounds(small, Limits{})
	if err != nil || bound.AtomicEncodingBytes != 0 || bound.ComparisonBytes != 0 || bound.SuccessorBytes != 0 {
		t.Fatal("inline Q reservation", bound, err)
	}
	if _, err = (Scope{}).ScratchBounds(small, Limits{}); !errors.Is(err, ErrInvalidScope) {
		t.Fatal(err)
	}
}

func TestScopeScratchBoundsMergeCrossedOldNumeratorAndDenominator(t *testing.T) {
	axis := encodingBudgetAxis(t, ProfileRationalQ)
	wide, err := ParseInteger(strings.Repeat("9", 1000), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	reciprocal, err := Fraction(Int64(1), wide, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	large, err := Fraction(wide, Int64(1), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	leftPosition, _ := RationalPosition(axis, reciprocal)
	rightPosition, _ := RationalPosition(axis, large)
	left, _ := Point(leftPosition)
	right, _ := Point(rightPosition)
	window, _ := All(axis)
	lb, err := left.ScratchBounds(window, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	rb, err := right.ScratchBounds(window, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var combined ScopeScratchBounds
	allocations := testing.AllocsPerRun(3, func() { combined, err = lb.Merge(rb) })
	if err != nil || allocations != 0 || combined.ComparisonBytes <= max(lb.ComparisonBytes, rb.ComparisonBytes) {
		t.Fatal("lost crossed-width product", lb, rb, combined, allocations, err)
	}
	direct, err := left.ScratchBounds(right, Limits{})
	if err != nil || combined != direct {
		t.Fatal("merged operand envelope differs", combined, direct, err)
	}
	if combined.ComparisonBytes < encodingRationalCompareBytes(reciprocal, large) {
		t.Fatal("named comparison scratch underpriced", combined)
	}
	if _, err = (ScopeScratchBounds{}).Merge(lb); !errors.Is(err, ErrInvalidAxis) {
		t.Fatal(err)
	}
}
