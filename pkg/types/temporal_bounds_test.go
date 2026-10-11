package types

import (
	"errors"
	"testing"
)

func TestTemporalBoundsFiniteZeroAndInfinity(t *testing.T) {
	for _, tc := range []struct {
		r    TemporalRange
		at   Instant
		want bool
	}{
		{TemporalRange{From: TemporalEndpoint{-10, true}, To: TemporalEndpoint{0, true}}, -1, true},
		{TemporalRange{From: TemporalEndpoint{-10, true}, To: TemporalEndpoint{0, true}}, 0, false},
		{TemporalRange{From: TemporalEndpoint{0, true}, To: TemporalEndpoint{10, true}}, 0, true},
		{TemporalRange{}, -100, true},
		{TemporalRange{}, 100, true},
	} {
		if err := tc.r.Validate(); err != nil {
			t.Fatal(err)
		}
		if got := tc.r.Contains(tc.at); got != tc.want {
			t.Fatal(tc, got)
		}
		var m TemporalMetadata
		if err := m.SetValidBounds(tc.r); err != nil {
			t.Fatal(err)
		}
		if got, ok := m.ValidBounds(); !ok || got != tc.r || !m.HasValidBounds() || m.ValidateValidBounds() != nil {
			t.Fatal("explicit range lost", m, got, ok)
		}
	}
	left := TemporalRange{From: TemporalEndpoint{-10, true}, To: TemporalEndpoint{0, true}}
	right := TemporalRange{From: TemporalEndpoint{0, true}, To: TemporalEndpoint{10, true}}
	if left.Overlaps(right) || !left.Overlaps(TemporalRange{}) {
		t.Fatal("half-open/infinite overlap")
	}
	var nilMeta *TemporalMetadata
	if nilMeta.HasValidBounds() || nilMeta.ValidateValidBounds() != nil {
		t.Fatal("nil metadata")
	}
	if _, ok := nilMeta.ValidBounds(); ok {
		t.Fatal("nil explicit bounds")
	}
	if err := nilMeta.SetValidBounds(left); !errors.Is(err, ErrNilTemporalMetadata) {
		t.Fatal(err)
	}
	legacy := &TemporalMetadata{ValidFrom: -10}
	if r, ok := legacy.ValidBounds(); ok || !r.From.Finite || r.To.Finite {
		t.Fatal("legacy scalar interpretation changed", r, ok)
	}
}

func TestTemporalBoundsRejectMalformedShapes(t *testing.T) {
	for _, r := range []TemporalRange{
		{From: TemporalEndpoint{1, false}},
		{To: TemporalEndpoint{1, false}},
		{From: TemporalEndpoint{0, true}, To: TemporalEndpoint{0, true}},
		{From: TemporalEndpoint{1, true}, To: TemporalEndpoint{0, true}},
	} {
		var m TemporalMetadata
		if err := r.Validate(); !errors.Is(err, ErrInvalidTemporalRange) {
			t.Fatal(r, err)
		}
		if err := m.SetValidBounds(r); !errors.Is(err, ErrInvalidTemporalRange) || m.HasValidBounds() {
			t.Fatal("invalid mutation", m, err)
		}
		if r.Contains(0) || r.Overlaps(TemporalRange{}) || (TemporalRange{}).Overlaps(r) {
			t.Fatal("invalid range matched")
		}
	}
	for _, p := range []EndpointPresence{EndpointFromFinite, EndpointToFinite, 128 | EndpointsExplicit} {
		if err := (&TemporalMetadata{ValidEndpoints: p}).ValidateValidBounds(); !errors.Is(err, ErrInvalidTemporalRange) {
			t.Fatal(p, err)
		}
	}
}
