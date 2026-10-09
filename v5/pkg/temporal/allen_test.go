package temporal

import (
	"errors"
	"testing"
)

func TestClassifyAllen(t *testing.T) {
	a := scopeAxis(t, ProfileIntegerZ)
	p := func(n int64) Position { return scopePosition(t, a, n, 1, 0) }
	cases := []struct {
		a, b, c, d int64
		want       AllenRelation
	}{
		{0, 1, 2, 3, AllenBefore}, {0, 1, 1, 2, AllenMeets}, {0, 2, 1, 3, AllenOverlaps}, {0, 2, 0, 3, AllenStarts}, {1, 2, 0, 3, AllenDuring}, {1, 3, 0, 3, AllenFinishes}, {0, 3, 0, 3, AllenEquals}, {2, 3, 0, 1, AllenAfter}, {1, 2, 0, 1, AllenMetBy}, {1, 3, 0, 2, AllenOverlappedBy}, {0, 3, 0, 2, AllenStartedBy}, {0, 3, 1, 2, AllenContains}, {0, 3, 1, 3, AllenFinishedBy},
	}
	for _, c := range cases {
		v, e := ClassifyAllen(p(c.a), p(c.b), p(c.c), p(c.d), Limits{})
		if e != nil || v != c.want {
			t.Fatalf("%+v got %v %v", c, v, e)
		}
	}
	// Raw closed endpoint intervals meet; their support overlaps. The classifier
	// must not use successor-widened endpoints from the region normalizer.
	x, y := scopeSpan(t, a, p(0), p(1), true, true), scopeSpan(t, a, p(1), p(2), true, true)
	over, e := x.Overlaps(y, Limits{})
	if e != nil || !over {
		t.Fatal(over, e)
	}
	half1, half2 := scopeSpan(t, a, p(0), p(1), true, false), scopeSpan(t, a, p(1), p(2), true, false)
	over, e = half1.Overlaps(half2, Limits{})
	if e != nil || over {
		t.Fatal(over, e)
	}
	for _, ends := range [][4]Position{{p(0), p(0), p(1), p(2)}, {p(1), p(0), p(1), p(2)}, {p(0), p(1), p(2), p(2)}} {
		if _, e := ClassifyAllen(ends[0], ends[1], ends[2], ends[3], Limits{}); !errors.Is(e, ErrImproperInterval) {
			t.Fatal(e)
		}
	}
	b := scopeAxis(t, ProfileRationalQ)
	if _, e := ClassifyAllen(p(0), p(1), scopePosition(t, b, 1, 1, 0), scopePosition(t, b, 2, 1, 0), Limits{}); !errors.Is(e, ErrAxisMismatch) {
		t.Fatal(e)
	}
	if _, e := ClassifyAllen(Position{}, p(1), p(0), p(1), Limits{}); !errors.Is(e, ErrInvalidPosition) {
		t.Fatal(e)
	}
	if _, e := ClassifyAllen(p(0), p(1), p(0), p(1), Limits{MaxValueBytes: 1}); !errors.Is(e, ErrResourceLimit) {
		t.Fatal(e)
	}
	if _, e := ClassifyAllen(p(0), p(1), p(0), p(1), Limits{MaxRegionPieces: -1}); !errors.Is(e, ErrInvalidLimits) {
		t.Fatal(e)
	}
}
