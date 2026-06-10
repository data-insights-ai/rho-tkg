package storeutil

import (
	"testing"

	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Direct tests for the canonical exported predicates (testing rule 1). The
// boundary instants are the adversarial part: every assertion below flips if
// a <= drifts to < or vice versa — exactly the drift class the canonical
// predicates exist to prevent.

func TestMatchesPointInTimeBoundaries(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	closed := &types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000}
	open := &types.TemporalMetadata{ValidFrom: 1000}

	cases := []struct {
		name string
		tm   *types.TemporalMetadata
		t    types.Instant
		want bool
	}{
		{"before-from", closed, 999, false},
		{"at-from-inclusive", closed, 1000, true},
		{"inside", closed, 1500, true},
		{"last-valid-instant", closed, 1999, true},
		{"at-to-exclusive", closed, 2000, false},
		{"after-to", closed, 2001, false},
		{"open-at-from", open, 1000, true},
		{"open-far-future", open, 1 << 50, true},
		{"open-before-from", open, 999, false},
	}
	for _, tc := range cases {
		if got := MatchesPointInTime(id, tc.tm, tc.t); got != tc.want {
			t.Errorf("%s: MatchesPointInTime(t=%d) = %v, want %v", tc.name, tc.t, got, tc.want)
		}
	}

	// Snowflake fallback: with no asserted ValidFrom the derived instant is
	// the boundary — one millisecond earlier must NOT match.
	derived := types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
	if MatchesPointInTime(id, nil, derived-1) {
		t.Errorf("nil tm: instant before snowflake-derived from must not match")
	}
	if !MatchesPointInTime(id, nil, derived) {
		t.Errorf("nil tm: snowflake-derived from itself must match (inclusive)")
	}
}

func TestMatchesIntervalBoundaries(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	closed := &types.TemporalMetadata{ValidFrom: 1000, ValidTo: 2000}
	open := &types.TemporalMetadata{ValidFrom: 1000}

	cases := []struct {
		name       string
		tm         *types.TemporalMetadata
		start, end types.Instant
		want       bool
	}{
		// Allen "meets" cases are the classic off-by-one trap: touching
		// intervals do NOT overlap under half-open semantics.
		{"meets-left", closed, 0, 1000, false},
		{"overlaps-left-by-one", closed, 0, 1001, true},
		{"meets-right", closed, 2000, 3000, false},
		{"overlaps-right-by-one", closed, 1999, 3000, true},
		{"contains", closed, 0, 1 << 50, true},
		{"contained", closed, 1200, 1300, true},
		{"disjoint-before", closed, 0, 999, false},
		{"disjoint-after", closed, 2001, 3000, false},
		{"open-overlap", open, 5000, 6000, true},
		{"open-meets-left", open, 0, 1000, false},
	}
	for _, tc := range cases {
		if got := MatchesInterval(id, tc.tm, tc.start, tc.end); got != tc.want {
			t.Errorf("%s: MatchesInterval([%d,%d)) = %v, want %v", tc.name, tc.start, tc.end, got, tc.want)
		}
	}

	derived := types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
	if MatchesInterval(id, nil, 0, derived) {
		t.Errorf("nil tm: interval ending exactly at snowflake-derived from must not match")
	}
	if !MatchesInterval(id, nil, 0, derived+1) {
		t.Errorf("nil tm: interval ending one past snowflake-derived from must match")
	}
}
