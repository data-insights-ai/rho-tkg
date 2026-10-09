package temporal

import (
	"errors"
	"math"
	"testing"
)

func TestInstantMillisDoesNotNarrowTheSemanticDomain(t *testing.T) {
	for _, profile := range []Profile{ProfileIntegerZ, ProfileRationalQ} {
		a := unitAxis(t, profile, "millisecond")
		for _, value := range []int64{math.MinInt64, -123, 0, math.MaxInt64} {
			p, err := ConvertUnits(RationalInt64(value), "millisecond", a, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := InstantMillis(p, Limits{})
			if err != nil || got != value {
				t.Fatal(got, value, err)
			}
		}
		for _, text := range []string{"-9223372036854775809", "9223372036854775808"} {
			p, err := ConvertUnits(mustRational(t, text), "millisecond", a, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			got, err := InstantMillis(p, Limits{})
			if got != 0 || !errors.Is(err, ErrInstantCodecRange) || unitCoordinate(t, p) != text {
				t.Fatal(got, p, err)
			}
		}
	}
	a := unitAxis(t, ProfileRationalQ, "millisecond")
	for _, text := range []string{"1/1000", "-1/1000"} {
		p, err := ConvertUnits(mustRational(t, text), "millisecond", a, Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if got, err := InstantMillis(p, Limits{}); got != 0 || !errors.Is(err, ErrNonintegralInstantCodec) {
			t.Fatal(got, err)
		}
		if unitCoordinate(t, p) != text {
			t.Fatal("codec refusal changed exact Q value")
		}
	}
}

func TestInstantMillisRequiresQualifiedScalarAndBudgets(t *testing.T) {
	ms := unitAxis(t, ProfileIntegerZ, "millisecond")
	p, err := IntegerPosition(ms, Int64(2))
	if err != nil {
		t.Fatal(err)
	}
	qn, err := LexPosition(unitAxis(t, ProfileLexicographicQN, "millisecond"), RationalInt64(2), Int64(0))
	if err != nil {
		t.Fatal(err)
	}
	ordinal, err := IntegerPosition(unitAxis(t, ProfileIntegerZ, "ordinal"), Int64(7))
	if err != nil {
		t.Fatal(err)
	}
	us, err := IntegerPosition(unitAxis(t, ProfileIntegerZ, "microsecond"), Int64(1000))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		p    Position
		l    Limits
		want error
	}{
		{Position{}, Limits{MaxValueBytes: -1}, ErrInvalidLimits},
		{Position{}, Limits{}, ErrInvalidPosition},
		{ordinal, Limits{}, ErrExplicitMappingRequired},
		{us, Limits{}, ErrExplicitMappingRequired},
		{qn, Limits{}, ErrIncompatibleDomain},
		{p, Limits{MaxDescriptorBytes: axisDescriptorBytes(ms) - 1}, ErrResourceLimit},
		{p, Limits{MaxMagnitudeBits: 1}, ErrResourceLimit},
		{p, Limits{MaxValueBytes: 7}, ErrResourceLimit},
	} {
		if got, err := InstantMillis(tc.p, tc.l); got != 0 || !errors.Is(err, tc.want) {
			t.Fatal(got, err, tc.want)
		}
	}
	if got, err := InstantMillis(p, Limits{MaxValueBytes: 8}); err != nil || got != 2 {
		t.Fatal(got, err)
	}
}
