package types

import (
	"errors"
	"testing"
)

func TestRelateRangesExactEpochAndInfinity(t *testing.T) {
	f := func(a, b Instant) TemporalRange {
		return TemporalRange{From: TemporalEndpoint{a, true}, To: TemporalEndpoint{b, true}}
	}
	for _, tc := range []struct {
		a, b TemporalRange
		want AllenRelation
	}{
		{f(-10, 0), f(1, 2), Before}, {f(1, 2), f(-10, 0), After},
		{f(-10, 0), f(0, 10), Meets}, {f(0, 10), f(-10, 0), MetBy},
		{f(-10, 1), f(0, 10), Overlaps}, {f(0, 10), f(-10, 1), OverlappedBy},
		{f(0, 5), f(0, 10), Starts}, {f(0, 10), f(0, 5), StartedBy},
		{f(1, 5), f(0, 10), During}, {f(0, 10), f(1, 5), Contains},
		{f(1, 10), f(0, 10), Finishes}, {f(0, 10), f(1, 10), FinishedBy},
		{f(0, 10), f(0, 10), Equals}, {TemporalRange{}, f(0, 10), Contains},
		{TemporalRange{}, TemporalRange{}, Equals},
	} {
		if got, err := RelateRanges(tc.a, tc.b); err != nil || got != tc.want {
			t.Fatal(tc, got, err)
		}
	}
	if _, err := RelateRanges(f(0, 0), f(0, 10)); !errors.Is(err, ErrInvalidInterval) {
		t.Fatal(err)
	}
}
