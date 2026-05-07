package storeutil

import (
	"testing"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
