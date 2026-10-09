package types_test

import (
	"bytes"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func instantAxis(t testing.TB, profile temporal.Profile, unit string) temporal.Axis {
	t.Helper()
	a, err := temporal.NewAxis(temporal.AxisDescriptor{ID: temporal.AxisID{19}, Profile: profile, Version: 1, Reference: "caller-qualified-reference", CanonicalUnit: unit}, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func scalar(t testing.TB, a temporal.Axis, text string) temporal.Position {
	t.Helper()
	v, err := temporal.ParseRational(text, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	p, err := temporal.ConvertUnits(v, a.Descriptor().CanonicalUnit, a, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestInstantAdaptersDoNotSelectDomainOrZeroSentinels(t *testing.T) {
	if reflect.TypeFor[types.Instant]().Kind() != reflect.Int64 {
		t.Fatal("Instant changed its signed int64 representation")
	}
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ} {
		a := instantAxis(t, profile, "millisecond")
		for _, value := range []types.Instant{math.MinInt64, -1, 0, 1, math.MaxInt64} {
			p, err := types.InstantPosition(a, value, temporal.Limits{})
			if err != nil || p.Axis().Descriptor() != a.Descriptor() || p.Axis().DefinitionHash() != a.DefinitionHash() || p.Profile() != profile {
				t.Fatal("adapter changed axis/domain", value, p, err)
			}
			got, err := types.InstantFromPosition(p, a, temporal.Limits{MaxValueBytes: 8})
			if err != nil || got != value {
				t.Fatal(value, got, err)
			}
			wire, err := temporal.AppendPosition(nil, p, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := temporal.DecodePosition(wire, a, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err = types.InstantFromPosition(decoded, a, temporal.Limits{})
			if err != nil || got != value {
				t.Fatal("generic wire changed Instant", value, got, err)
			}
		}
		p := scalar(t, a, "7")
		if allocs := testing.AllocsPerRun(100, func() {
			q, err := types.InstantPosition(a, 7, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := types.InstantFromPosition(q, a, temporal.Limits{})
			if err != nil || got != 7 {
				t.Fatal(got, err)
			}
		}); allocs != 0 {
			t.Fatalf("inline adapters allocated %v times", allocs)
		}
		if got, err := temporal.InstantMillis(p, temporal.Limits{}); err != nil || got != 7 {
			t.Fatal(got, err)
		}
	}
}

func TestInstantFromPositionRefusesLossAndReferenceErasure(t *testing.T) {
	for _, profile := range []temporal.Profile{temporal.ProfileIntegerZ, temporal.ProfileRationalQ} {
		a := instantAxis(t, profile, "millisecond")
		for _, text := range []string{"-9223372036854775809", "9223372036854775808"} {
			p := scalar(t, a, text)
			before, err := temporal.AppendPosition(nil, p, temporal.Limits{})
			if err != nil {
				t.Fatal(err)
			}
			if got, err := types.InstantFromPosition(p, a, temporal.Limits{}); got != 0 || !errors.Is(err, temporal.ErrInstantCodecRange) {
				t.Fatal(text, got, err)
			}
			after, err := temporal.AppendPosition(nil, p, temporal.Limits{})
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("codec refusal changed generic wide value", err)
			}
		}
	}
	a := instantAxis(t, temporal.ProfileRationalQ, "millisecond")
	for _, text := range []string{"1/1000", "-1/3"} {
		p := scalar(t, a, text)
		if got, err := types.InstantFromPosition(p, a, temporal.Limits{}); got != 0 || !errors.Is(err, temporal.ErrNonintegralInstantCodec) {
			t.Fatal(text, got, err)
		}
		q, ok := p.Rational()
		if !ok || q.String() != text {
			t.Fatal("codec refusal narrowed exact Q")
		}
	}
	p := scalar(t, a, "7")
	for _, change := range []func(*temporal.AxisDescriptor){
		func(d *temporal.AxisDescriptor) { d.ID[0]++ },
		func(d *temporal.AxisDescriptor) { d.Reference = "different-origin" },
		func(d *temporal.AxisDescriptor) { d.Profile = temporal.ProfileIntegerZ },
	} {
		d := a.Descriptor()
		change(&d)
		other, err := temporal.NewAxis(d, temporal.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := types.InstantFromPosition(p, other, temporal.Limits{}); got != 0 || !errors.Is(err, temporal.ErrAxisMismatch) {
			t.Fatal(got, err)
		}
		if got, err := temporal.InstantMillis(p, temporal.Limits{}); got != 7 || err != nil {
			t.Fatal("numeric helper no longer distinguishes axis binding", got, err)
		}
	}
}

func TestInstantAdaptersRefuseInferredUnitsAndIgnoreNoBudgets(t *testing.T) {
	a := instantAxis(t, temporal.ProfileIntegerZ, "millisecond")
	p := scalar(t, a, "2")
	d := a.Descriptor()
	descriptorBytes := 27 + len(d.Reference) + len(d.CanonicalUnit)
	for _, tc := range []struct {
		axis   temporal.Axis
		limits temporal.Limits
		want   error
	}{
		{temporal.Axis{}, temporal.Limits{}, temporal.ErrInvalidAxis},
		{temporal.Axis{}, temporal.Limits{MaxValueBytes: -1}, temporal.ErrInvalidLimits},
		{a, temporal.Limits{MaxValueBytes: -1}, temporal.ErrInvalidLimits},
		{instantAxis(t, temporal.ProfileIntegerZ, "ordinal"), temporal.Limits{}, temporal.ErrExplicitMappingRequired},
		{instantAxis(t, temporal.ProfileIntegerZ, "microsecond"), temporal.Limits{}, temporal.ErrExplicitMappingRequired},
		{instantAxis(t, temporal.ProfileLexicographicQN, "millisecond"), temporal.Limits{}, temporal.ErrIncompatibleDomain},
		{a, temporal.Limits{MaxDescriptorBytes: descriptorBytes - 1}, temporal.ErrResourceLimit},
		{a, temporal.Limits{MaxMagnitudeBits: 1}, temporal.ErrResourceLimit},
		{a, temporal.Limits{MaxValueBytes: 7}, temporal.ErrResourceLimit},
	} {
		if got, err := types.InstantPosition(tc.axis, 2, tc.limits); got != (temporal.Position{}) || !errors.Is(err, tc.want) {
			t.Fatal("forward refusal", got, err, tc.want)
		}
		if got, err := types.InstantFromPosition(p, tc.axis, tc.limits); got != 0 || !errors.Is(err, tc.want) {
			t.Fatal("reverse refusal", got, err, tc.want)
		}
	}
	for _, source := range []temporal.Position{{}, scalar(t, instantAxis(t, temporal.ProfileIntegerZ, "ordinal"), "2"), scalar(t, instantAxis(t, temporal.ProfileIntegerZ, "microsecond"), "2000")} {
		want := temporal.ErrExplicitMappingRequired
		if source == (temporal.Position{}) {
			want = temporal.ErrInvalidPosition
		}
		if got, err := types.InstantFromPosition(source, a, temporal.Limits{}); got != 0 || !errors.Is(err, want) {
			t.Fatal(got, err, want)
		}
	}
	lex, err := temporal.LexPosition(instantAxis(t, temporal.ProfileLexicographicQN, "millisecond"), temporal.RationalInt64(2), temporal.Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := types.InstantFromPosition(lex, a, temporal.Limits{}); got != 0 || !errors.Is(err, temporal.ErrIncompatibleDomain) {
		t.Fatal(got, err)
	}
	wire, err := temporal.AppendPosition(nil, p, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := types.InstantPosition(a, 2, temporal.Limits{MaxValueBytes: len(wire) - 1}); got != (temporal.Position{}) || !errors.Is(err, temporal.ErrResourceLimit) {
		t.Fatal(got, err)
	}
	if _, err := types.InstantPosition(a, 2, temporal.Limits{MaxValueBytes: len(wire)}); err != nil {
		t.Fatal(err)
	}
	if _, err := types.InstantFromPosition(p, a, temporal.Limits{MaxDescriptorBytes: descriptorBytes, MaxValueBytes: 8}); err != nil {
		t.Fatal(err)
	}
}

func TestInstantFromPositionRefusalPrecedenceIsStable(t *testing.T) {
	a := instantAxis(t, temporal.ProfileRationalQ, "millisecond")
	d := a.Descriptor()
	d.Reference = "a-different-and-longer-reference"
	other, err := temporal.NewAxis(d, temporal.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		position temporal.Position
		axis     temporal.Axis
		limits   temporal.Limits
		want     error
	}{
		{temporal.Position{}, temporal.Axis{}, temporal.Limits{MaxValueBytes: -1}, temporal.ErrInvalidLimits},
		{temporal.Position{}, temporal.Axis{}, temporal.Limits{}, temporal.ErrInvalidAxis},
		{temporal.Position{}, instantAxis(t, temporal.ProfileIntegerZ, "ordinal"), temporal.Limits{}, temporal.ErrExplicitMappingRequired},
		{scalar(t, other, "1/1000"), a, temporal.Limits{}, temporal.ErrNonintegralInstantCodec},
		{scalar(t, other, "9223372036854775808"), a, temporal.Limits{}, temporal.ErrInstantCodecRange},
		{scalar(t, other, "2"), a, temporal.Limits{MaxValueBytes: 7}, temporal.ErrResourceLimit},
		{scalar(t, other, "2"), a, temporal.Limits{MaxDescriptorBytes: 27 + len(a.Descriptor().Reference) + len("millisecond")}, temporal.ErrResourceLimit},
		{scalar(t, other, "2"), a, temporal.Limits{}, temporal.ErrAxisMismatch},
	} {
		if got, err := types.InstantFromPosition(tc.position, tc.axis, tc.limits); got != 0 || !errors.Is(err, tc.want) {
			t.Fatal(got, err, tc.want)
		}
	}
}
