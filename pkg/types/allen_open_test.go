package types

import (
	"errors"
	"math"
	"testing"
)

// TestRelateOpen_ClosedMatchesRelate proves RelateOpen agrees with Relate on
// every fully-closed interval — the open-end handling is purely additive and
// must not perturb the classification of intervals that carry no open end.
func TestRelateOpen_ClosedMatchesRelate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		aStart, aEnd, bStart, bEnd Instant
		want                       AllenRelation
	}{
		{"Before", 1, 3, 5, 8, Before},
		{"After", 10, 15, 1, 5, After},
		{"Meets", 1, 5, 5, 10, Meets},
		{"MetBy", 5, 10, 1, 5, MetBy},
		{"Overlaps", 1, 6, 4, 10, Overlaps},
		{"OverlappedBy", 4, 10, 1, 6, OverlappedBy},
		{"Starts", 1, 5, 1, 10, Starts},
		{"StartedBy", 1, 10, 1, 5, StartedBy},
		{"During", 3, 7, 1, 10, During},
		{"Contains", 1, 10, 3, 7, Contains},
		{"Finishes", 5, 10, 1, 10, Finishes},
		{"FinishedBy", 1, 10, 5, 10, FinishedBy},
		{"Equals", 1, 10, 1, 10, Equals},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := RelateOpen(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if err != nil {
				t.Fatalf("RelateOpen: unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("RelateOpen(%d,%d,%d,%d) = %v, want %v",
					tc.aStart, tc.aEnd, tc.bStart, tc.bEnd, got, tc.want)
			}
			// Cross-check: closed inputs must classify identically to Relate.
			ref, _ := Relate(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if got != ref {
				t.Errorf("RelateOpen diverges from Relate on closed input: %v vs %v", got, ref)
			}
		})
	}
}

// TestRelateOpen_OpenEnds covers every classification reachable when one or both
// ends are open (== 0, meaning +∞). An open end is math.MaxInt64, so no real
// start can equal it — Meets/MetBy are therefore unreachable across an open end.
func TestRelateOpen_OpenEnds(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                       string
		aStart, aEnd, bStart, bEnd Instant
		want                       AllenRelation
	}{
		// A open, B closed and entirely before A's start → After.
		{"A_open_after_B", 100, 0, 1, 50, After},
		// A open starting inside B (which is closed and ends after A starts):
		// A=[5,∞) B=[1,10) → A starts after B, ends after B → OverlappedBy.
		{"A_open_overlappedby", 5, 0, 1, 10, OverlappedBy},
		// A open, B closed, same start, A extends past B's end → StartedBy.
		{"A_open_startedby", 1, 0, 1, 10, StartedBy},
		// A open, B closed, B strictly inside [aStart,∞) with aStart<bStart → Contains.
		{"A_open_contains", 1, 0, 3, 7, Contains},
		// Symmetric: B open, A closed before it → Before.
		{"B_open_before", 1, 50, 100, 0, Before},
		// B open, A closed, A starts after B, A ends before ∞ → During.
		{"B_open_during", 5, 8, 1, 0, During},
		// B open, same start, A ends before ∞ → Starts.
		{"B_open_starts", 1, 8, 1, 0, Starts},
		// B open starting inside A=[1,10): A=[1,10) B=[5,∞) → Overlaps.
		{"B_open_overlaps", 1, 10, 5, 0, Overlaps},
		// BOTH open, same start → Equals ([s,∞) == [s,∞)).
		{"both_open_equal_start", 7, 0, 7, 0, Equals},
		// BOTH open, A starts later → During (A=[10,∞) inside B=[3,∞)? No —
		// same infinite end means A finishes B). A=[10,∞) B=[3,∞): starts differ,
		// ends equal (∞) with aStart>bStart → Finishes.
		{"both_open_finishes", 10, 0, 3, 0, Finishes},
		// BOTH open, A starts earlier, ends equal (∞) → FinishedBy.
		{"both_open_finishedby", 3, 0, 10, 0, FinishedBy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := RelateOpen(tc.aStart, tc.aEnd, tc.bStart, tc.bEnd)
			if err != nil {
				t.Fatalf("RelateOpen: unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("RelateOpen(%d,%d,%d,%d) = %v, want %v",
					tc.aStart, tc.aEnd, tc.bStart, tc.bEnd, got, tc.want)
			}
		})
	}
}

// TestRelateOpen_MeetsUnreachableAcrossOpenEnd locks the structural fact that an
// open (eternal) interval can never Meet/MetBy another: abutment needs
// aEnd == bStart, but an open end is math.MaxInt64 which no real start reaches.
func TestRelateOpen_MeetsUnreachableAcrossOpenEnd(t *testing.T) {
	t.Parallel()
	// A=[1,∞): its "end" is MaxInt64; even a B starting at MaxInt64 would be
	// invalid (bStart>=bEnd once bEnd substituted), so Meets is impossible.
	got, err := RelateOpen(1, 0, math.MaxInt64, 0)
	if err != nil {
		// bStart == MaxInt64 and bEnd substituted to MaxInt64 → bStart>=bEnd.
		if !errors.Is(err, ErrInvalidInterval) {
			t.Fatalf("want ErrInvalidInterval, got %v", err)
		}
		return
	}
	if got == Meets {
		t.Fatalf("Meets must be unreachable across an open end, got Meets")
	}
}

// TestRelateOpen_OpenStartRejected proves only ENDS may be open — an open START
// is meaningless (an interval must begin somewhere) and is rejected.
func TestRelateOpen_OpenStartRejected(t *testing.T) {
	t.Parallel()
	if _, err := RelateOpen(0, 10, 1, 5); !errors.Is(err, ErrOpenInterval) {
		t.Errorf("open aStart: want ErrOpenInterval, got %v", err)
	}
	if _, err := RelateOpen(1, 5, 0, 10); !errors.Is(err, ErrOpenInterval) {
		t.Errorf("open bStart: want ErrOpenInterval, got %v", err)
	}
}

// TestRelateOpen_EmptyClosedInterval rejects a start >= end once the (closed)
// end is in play — a zero-width or inverted interval is invalid.
func TestRelateOpen_EmptyClosedInterval(t *testing.T) {
	t.Parallel()
	if _, err := RelateOpen(5, 5, 1, 10); !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("empty A: want ErrInvalidInterval, got %v", err)
	}
	if _, err := RelateOpen(1, 10, 8, 3); !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("inverted B: want ErrInvalidInterval, got %v", err)
	}
	// An open end is never empty regardless of start.
	if _, err := RelateOpen(math.MaxInt64-1, 0, 1, 10); err != nil {
		t.Errorf("open end must never be empty: %v", err)
	}
}
