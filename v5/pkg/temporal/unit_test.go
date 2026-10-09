package temporal

import (
	"errors"
	"math"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"
)

func unitAxis(t testing.TB, profile Profile, unit string) Axis {
	t.Helper()
	a, err := NewAxis(AxisDescriptor{ID: AxisID{42}, Profile: profile, Version: 1, Reference: "explicit-test-reference", CanonicalUnit: unit}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func unitCoordinate(t testing.TB, p Position) string {
	t.Helper()
	if n, ok := p.Integer(); ok {
		return n.String()
	}
	if q, ok := p.Rational(); ok {
		return q.String()
	}
	t.Fatal("conversion returned no scalar coordinate")
	return ""
}

func TestConvertUnitsDoesNotRoundOrInferPhysicalUnits(t *testing.T) {
	for _, tc := range []struct {
		name, source, sourceUnit, targetUnit, want string
		profile                                    Profile
		wantErr                                    error
	}{
		{"V0-units-us-to-ms-Z-1000", "1000", "microsecond", "millisecond", "1", ProfileIntegerZ, nil},
		{"V0-units-us-to-ms-Q-1", "1", "microsecond", "millisecond", "1/1000", ProfileRationalQ, nil},
		{"V0-units-us-to-ms-Q-1001", "1001", "microsecond", "millisecond", "1001/1000", ProfileRationalQ, nil},
		{"V0-units-us-to-ms-Z-1", "1", "microsecond", "millisecond", "", ProfileIntegerZ, ErrIncompatibleDomain},
		{"V0-default-ms-identity", "-123", "millisecond", "millisecond", "-123", ProfileIntegerZ, nil},
		{"V0-order-only-no-seconds", "7", "ordinal", "millisecond", "", ProfileIntegerZ, ErrExplicitMappingRequired},
		{"negative fractional Z", "-1001", "microsecond", "millisecond", "", ProfileIntegerZ, ErrIncompatibleDomain},
		{"negative fractional Q", "-1001", "microsecond", "millisecond", "-1001/1000", ProfileRationalQ, nil},
		{"zero with tiny coordinate budget", "0", "microsecond", "millisecond", "0", ProfileRationalQ, nil},
		{"inverse exact fraction", "1/1000", "millisecond", "microsecond", "1", ProfileIntegerZ, nil},
		{"custom identity", "7", "ordinal", "ordinal", "7", ProfileIntegerZ, nil},
		{"dense identity", "1/3", "custom", "custom", "1/3", ProfileRationalQ, nil},
		{"do not infer second", "7", "ordinal", "second", "", ProfileRationalQ, ErrExplicitMappingRequired},
		{"no implicit seconds conversion", "1", "second", "millisecond", "", ProfileRationalQ, ErrExplicitMappingRequired},
		{"no invented microstep", "1000", "microsecond", "millisecond", "", ProfileLexicographicQN, ErrIncompatibleDomain},
		{"empty source unit", "1", "", "millisecond", "", ProfileIntegerZ, ErrExplicitMappingRequired},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := mustRational(t, tc.source)
			axis := unitAxis(t, tc.profile, tc.targetUnit)
			l := Limits{}
			if tc.source == "0" {
				l.MaxMagnitudeBits = 1
			}
			got, err := ConvertUnits(input, tc.sourceUnit, axis, l)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error %v, want %v", err, tc.wantErr)
			}
			if err != nil {
				if got != (Position{}) {
					t.Fatal("failed conversion returned partial position")
				}
				return
			}
			if unitCoordinate(t, got) != tc.want || got.Axis() != axis {
				t.Fatalf("got %s on %v", unitCoordinate(t, got), got.Axis())
			}
			if input.String() != tc.source {
				t.Fatal("conversion mutated input")
			}
		})
	}
}

func TestConvertUnitsChecksInputsAndReducedOutputs(t *testing.T) {
	axis := unitAxis(t, ProfileRationalQ, "microsecond")
	for _, tc := range []struct {
		name, input, source string
		axis                Axis
		limits              Limits
		want                error
	}{
		{"invalid limits", "0", "millisecond", Axis{}, Limits{MaxMagnitudeBits: -1}, ErrInvalidLimits},
		{"invalid axis", "0", "millisecond", Axis{}, Limits{}, ErrInvalidAxis},
		{"descriptor budget", "0", "millisecond", axis, Limits{MaxDescriptorBytes: axisDescriptorBytes(axis) - 1}, ErrResourceLimit},
		{"unit input budget", "0", strings.Repeat("x", 20), axis, Limits{MaxInputBytes: 19}, ErrResourceLimit},
		{"input numerator budget", "1024", "millisecond", axis, Limits{MaxMagnitudeBits: 10}, ErrResourceLimit},
		{"input denominator budget", "1/1024", "millisecond", axis, Limits{MaxMagnitudeBits: 10}, ErrResourceLimit},
		{"output numerator budget", "2", "millisecond", axis, Limits{MaxMagnitudeBits: 10}, ErrResourceLimit},
		{"output denominator budget", "1/2", "microsecond", unitAxis(t, ProfileRationalQ, "millisecond"), Limits{MaxMagnitudeBits: 10}, ErrResourceLimit},
		{"identity output bytes", "0", "microsecond", axis, Limits{MaxValueBytes: 58}, ErrResourceLimit},
		{"converted output bytes", "1", "millisecond", axis, Limits{MaxValueBytes: 58}, ErrResourceLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConvertUnits(mustRational(t, tc.input), tc.source, tc.axis, tc.limits)
			if !errors.Is(err, tc.want) || got != (Position{}) {
				t.Fatalf("got %v: %v", got, err)
			}
		})
	}
	for _, tc := range []struct{ input, source, target, want string }{
		{"1/1000", "millisecond", "microsecond", "1"},
		{"1000", "microsecond", "millisecond", "1"},
		{"1/8", "millisecond", "microsecond", "125"},
		{"-1000/3", "microsecond", "millisecond", "-1/3"},
	} {
		p, err := ConvertUnits(mustRational(t, tc.input), tc.source, unitAxis(t, ProfileRationalQ, tc.target), Limits{MaxMagnitudeBits: 10})
		if err != nil || unitCoordinate(t, p) != tc.want {
			t.Fatal(tc, p, err)
		}
	}
	// Cancellation cannot hide an input that exceeds the caller's policy.
	if _, err := ConvertUnits(mustRational(t, "1024/125"), "microsecond", unitAxis(t, ProfileRationalQ, "millisecond"), Limits{MaxMagnitudeBits: 10}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal(err)
	}
}

func TestConvertUnitsPreservesAxisIdentityAndDefinition(t *testing.T) {
	a := unitAxis(t, ProfileIntegerZ, "millisecond")
	p, err := ConvertUnits(RationalInt64(1000), "microsecond", a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*AxisDescriptor){
		func(d *AxisDescriptor) { d.ID[0]++ },
		func(d *AxisDescriptor) { d.Reference = "different-origin" },
		func(d *AxisDescriptor) { d.CanonicalUnit = "ordinal" },
	} {
		d := a.Descriptor()
		change(&d)
		b, err := NewAxis(d, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		q, err := ConvertUnits(RationalInt64(1), d.CanonicalUnit, b, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ComparePositions(p, q, Limits{}); !errors.Is(err, ErrAxisMismatch) {
			t.Fatal(err)
		}
		wire, err := AppendPosition(nil, p, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := DecodePosition(wire, b, Limits{}); !errors.Is(err, ErrAxisMismatch) {
			t.Fatal(err)
		}
	}
	q, err := ConvertUnits(RationalInt64(1), "millisecond", a, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	ph, err := PositionHash(p, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	qh, err := PositionHash(q, Limits{})
	if err != nil || ph != qh {
		t.Fatal("equivalent explicit unit imports changed semantic hash", err)
	}
}

func TestConvertUnitsAgainstIndependentBigRat(t *testing.T) {
	rng := rand.New(rand.NewPCG(311, 719))
	values := []string{"9223372036854775807", "-9223372036854775808", "9223372036854775808", "-9223372036854775809", "18446744073709551616000/3", "1/18446744073709551616000"}
	for range 100 {
		values = append(values, strconv.FormatInt(rng.Int64(), 10)+"/"+strconv.FormatInt(rng.Int64N(math.MaxInt64)+1, 10))
	}
	for _, value := range values {
		for _, source := range []string{"millisecond", "microsecond"} {
			target := "microsecond"
			factor := big.NewRat(1000, 1)
			if source == "microsecond" {
				target = "millisecond"
				factor = big.NewRat(1, 1000)
			}
			want, ok := new(big.Rat).SetString(value)
			if !ok {
				t.Fatal(value)
			}
			want.Mul(want, factor)
			input := mustRational(t, value)
			before := input.String()
			got, err := ConvertUnits(input, source, unitAxis(t, ProfileRationalQ, target), Limits{})
			if err != nil || unitCoordinate(t, got) != want.RatString() {
				t.Fatal(value, source, got, want, err)
			}
			if input.String() != before {
				t.Fatal("wide arithmetic mutated shared input")
			}
		}
	}
}

func BenchmarkConvertUnits(b *testing.B) {
	for _, tc := range []struct{ name, value, source, target string }{
		{"integral-us-to-ms", "1700000000000000", "microsecond", "millisecond"},
		{"fractional-us-to-ms", "1001", "microsecond", "millisecond"},
		{"integral-ms-to-us", "1700000000000", "millisecond", "microsecond"},
		{"wide-ms-to-us", "9223372036854775808", "millisecond", "microsecond"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			axis := unitAxis(b, ProfileRationalQ, tc.target)
			value, err := ParseRational(tc.value, Limits{})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := ConvertUnits(value, tc.source, axis, Limits{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
