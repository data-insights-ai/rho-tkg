package storeutil

import (
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestEntityValidFrom_ExplicitValidFrom(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	tm := &types.TemporalMetadata{ValidFrom: 1000}
	got := EntityValidFrom(id, tm)
	if got != 1000 {
		t.Errorf("EntityValidFrom with explicit ValidFrom: got %d, want 1000", got)
	}
}

func TestEntityValidFrom_DerivedFromSnowflake(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	got := EntityValidFrom(id, nil)
	want := types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
	if got != want {
		t.Errorf("EntityValidFrom derived: got %d, want %d", got, want)
	}
}

func TestEntityValidFrom_NilTemporal(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	got := EntityValidFrom(id, nil)
	want := types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
	if got != want {
		t.Errorf("EntityValidFrom nil tm: got %d, want %d", got, want)
	}
}

func TestEntityValidFrom_ZeroValidFrom(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	tm := &types.TemporalMetadata{ValidFrom: 0} // zero = derive from ID
	got := EntityValidFrom(id, tm)
	want := types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
	if got != want {
		t.Errorf("EntityValidFrom zero ValidFrom: got %d, want %d", got, want)
	}
}

func TestMatchesTemporalFilter_NoFilter(t *testing.T) {
	t.Parallel()
	gen := newTestGen(t, 0)
	id := gen.Generate()
	if !MatchesTemporalFilter(id, nil, storepkg.QueryOpts{}) {
		t.Error("no filter should always match")
	}
}

func TestMatchesTemporalFilter_ValidAtMatch(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100}
	id := snowflake.ID(0) // derived time irrelevant because ValidFrom is explicit
	opts := storepkg.QueryOpts{ValidAt: 150}
	if !MatchesTemporalFilter(id, tm, opts) {
		t.Error("ValidAt=150 should match entity valid from 100 with no expiry")
	}
}

func TestMatchesTemporalFilter_ValidAtTooEarly(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 200}
	id := snowflake.ID(0)
	opts := storepkg.QueryOpts{ValidAt: 150}
	if MatchesTemporalFilter(id, tm, opts) {
		t.Error("ValidAt=150 should NOT match entity valid from 200")
	}
}

func TestMatchesTemporalFilter_ValidAtExpired(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100, ValidTo: 200}
	id := snowflake.ID(0)
	opts := storepkg.QueryOpts{ValidAt: 250}
	if MatchesTemporalFilter(id, tm, opts) {
		t.Error("ValidAt=250 should NOT match entity expired at 200")
	}
}

func TestMatchesTemporalFilter_ValidAtOpenEnded(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100} // ValidTo == 0 = open-ended
	id := snowflake.ID(0)
	opts := storepkg.QueryOpts{ValidAt: 999999}
	if !MatchesTemporalFilter(id, tm, opts) {
		t.Error("open-ended entity should match any time after ValidFrom")
	}
}

func TestMatchesTemporalFilter_IntervalMatch(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100, ValidTo: 300}
	id := snowflake.ID(0)
	opts := storepkg.QueryOpts{ValidStart: 200, ValidEnd: 400}
	if !MatchesTemporalFilter(id, tm, opts) {
		t.Error("entity [100,300) should overlap with query [200,400)")
	}
}

func TestMatchesTemporalFilter_IntervalNoOverlap(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100, ValidTo: 200}
	id := snowflake.ID(0)
	opts := storepkg.QueryOpts{ValidStart: 300, ValidEnd: 400}
	if MatchesTemporalFilter(id, tm, opts) {
		t.Error("entity [100,200) should NOT overlap with query [300,400)")
	}
}

func TestMatchesTemporalFilter_InvalidIntervalDoesNotMatchOpenEndedEntity(t *testing.T) {
	t.Parallel()
	tm := &types.TemporalMetadata{ValidFrom: 100}
	id := snowflake.ID(0)
	for _, opts := range []storepkg.QueryOpts{
		{ValidStart: 300, ValidEnd: 300},
		{ValidStart: 400, ValidEnd: 300},
	} {
		if MatchesTemporalFilter(id, tm, opts) {
			t.Fatalf("invalid interval %+v matched open-ended entity", opts)
		}
		if !HasTemporalFilter(opts) {
			t.Fatalf("invalid active interval %+v should still be treated as temporal", opts)
		}
	}
}

func TestMatchesTemporalFilter_ValidAtPrecedence(t *testing.T) {
	t.Parallel()
	// When both ValidAt and interval are set, ValidAt takes precedence.
	tm := &types.TemporalMetadata{ValidFrom: 100, ValidTo: 200}
	id := snowflake.ID(0)
	// ValidAt=150 matches, interval [300,400) would not match.
	opts := storepkg.QueryOpts{ValidAt: 150, ValidStart: 300, ValidEnd: 400}
	if !MatchesTemporalFilter(id, tm, opts) {
		t.Error("ValidAt should take precedence over interval — entity should match at t=150")
	}
}

// BACKLOG 15j: EnvelopeOverlaps backs the B4 candidate-prune optimization on
// every history-aware temporal scan and previously had zero direct unit
// tests — only indirect coverage through higher-level callers. Direct
// coverage below exercises every branch: no-filter passthrough, ValidAt
// (both closed and open-ended envelopes, at every boundary), interval
// filter (both closed and open-ended envelopes, at every boundary), the
// "only one of ValidStart/ValidEnd set" no-filter fallback, and ValidAt
// taking precedence over an interval filter when both are set.

func TestEnvelopeOverlaps_NoFilterAlwaysTrue(t *testing.T) {
	t.Parallel()
	cases := []struct {
		from, to types.Instant
	}{
		{100, 200},
		{100, 0}, // open-ended envelope
		{0, 0},
	}
	for _, c := range cases {
		if !EnvelopeOverlaps(c.from, c.to, storepkg.QueryOpts{}) {
			t.Errorf("EnvelopeOverlaps(%d, %d, no filter) = false, want true", c.from, c.to)
		}
	}
}

func TestEnvelopeOverlaps_ValidAtClosedEnvelope(t *testing.T) {
	t.Parallel()
	// Envelope [100, 200).
	cases := []struct {
		name    string
		validAt types.Instant
		want    bool
	}{
		{"before from", 99, false},
		{"at from (inclusive)", 100, true},
		{"inside", 150, true},
		{"at to (exclusive)", 200, false},
		{"after to", 201, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EnvelopeOverlaps(100, 200, storepkg.QueryOpts{ValidAt: c.validAt})
			if got != c.want {
				t.Errorf("EnvelopeOverlaps(100, 200, ValidAt=%d) = %v, want %v", c.validAt, got, c.want)
			}
		})
	}
}

func TestEnvelopeOverlaps_ValidAtOpenEndedEnvelope(t *testing.T) {
	t.Parallel()
	// Envelope [100, +inf) — to=0 means still valid, no upper bound.
	cases := []struct {
		name    string
		validAt types.Instant
		want    bool
	}{
		{"before from", 99, false},
		{"at from", 100, true},
		{"far future", 1_000_000, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EnvelopeOverlaps(100, 0, storepkg.QueryOpts{ValidAt: c.validAt})
			if got != c.want {
				t.Errorf("EnvelopeOverlaps(100, 0, ValidAt=%d) = %v, want %v", c.validAt, got, c.want)
			}
		})
	}
}

func TestEnvelopeOverlaps_IntervalClosedEnvelope(t *testing.T) {
	t.Parallel()
	// Envelope [100, 200). Overlap rule: from < end AND (to==0 OR to > start).
	cases := []struct {
		name       string
		start, end types.Instant
		want       bool
	}{
		{"query entirely before envelope", 1, 100, false},
		{"query touches envelope start (half-open, no overlap)", 50, 100, false},
		{"query overlaps envelope start", 50, 150, true},
		{"query entirely inside envelope", 120, 180, true},
		{"query overlaps envelope end", 150, 250, true},
		{"query touches envelope end (half-open, no overlap)", 200, 300, false},
		{"query entirely after envelope", 300, 400, false},
		{"query exactly matches envelope", 100, 200, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EnvelopeOverlaps(100, 200, storepkg.QueryOpts{ValidStart: c.start, ValidEnd: c.end})
			if got != c.want {
				t.Errorf("EnvelopeOverlaps(100, 200, [%d,%d)) = %v, want %v", c.start, c.end, got, c.want)
			}
		})
	}
}

func TestEnvelopeOverlaps_IntervalOpenEndedEnvelope(t *testing.T) {
	t.Parallel()
	// Envelope [100, +inf).
	cases := []struct {
		name       string
		start, end types.Instant
		want       bool
	}{
		{"query entirely before envelope", 1, 100, false},
		{"query overlaps envelope start", 50, 150, true},
		{"query far in the future still overlaps", 1_000_000, 2_000_000, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := EnvelopeOverlaps(100, 0, storepkg.QueryOpts{ValidStart: c.start, ValidEnd: c.end})
			if got != c.want {
				t.Errorf("EnvelopeOverlaps(100, 0, [%d,%d)) = %v, want %v", c.start, c.end, got, c.want)
			}
		})
	}
}

func TestEnvelopeOverlaps_OnlyOneIntervalBoundSetIsNoFilter(t *testing.T) {
	t.Parallel()
	// Both ValidStart AND ValidEnd must be set for interval filtering;
	// setting only one falls back to "no filter" (always true) — the
	// EnvelopeOverlaps-level mirror of MatchesTemporalFilter's documented
	// convention.
	cases := []storepkg.QueryOpts{
		{ValidStart: 500}, // ValidEnd unset
		{ValidEnd: 500},   // ValidStart unset
	}
	for _, opts := range cases {
		if !EnvelopeOverlaps(100, 200, opts) {
			t.Errorf("EnvelopeOverlaps(100, 200, %+v) = false, want true (only one interval bound set)", opts)
		}
	}
}

func TestEnvelopeOverlaps_ValidAtTakesPrecedenceOverInterval(t *testing.T) {
	t.Parallel()
	// Envelope [100, 200). ValidAt=150 is inside; the interval [300,400)
	// would not overlap at all — ValidAt must win.
	opts := storepkg.QueryOpts{ValidAt: 150, ValidStart: 300, ValidEnd: 400}
	if !EnvelopeOverlaps(100, 200, opts) {
		t.Error("EnvelopeOverlaps should prefer ValidAt over interval when both are set")
	}
}
