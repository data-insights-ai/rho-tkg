package store

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/snowflake"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// EntityValidFrom derives the effective valid-from time for an entity.
// Uses explicit ValidFrom if set on TemporalMetadata, otherwise derives
// from the snowflake ID via the package-level snowflake.Layout.
func EntityValidFrom(id snowflake.ID, tm *types.TemporalMetadata) types.Instant {
	if tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return types.Instant(snowflakepkg.Layout.CreatedAt(id).UnixMilli())
}

// MatchesTemporalFilter evaluates whether an entity passes the temporal
// filter in opts. Returns true when no filter is set (zero values).
//
// ValidAt takes precedence over interval (ValidStart/ValidEnd).
// Both ValidStart AND ValidEnd must be set for interval filtering;
// setting only one is treated as no filter (returns true).
//
// Point-in-time: from <= t AND (to == 0 OR to > t)
// Interval:      from < end AND (to == 0 OR to > start)
func MatchesTemporalFilter(id snowflake.ID, tm *types.TemporalMetadata, opts QueryOpts) bool {
	if opts.ValidAt != 0 {
		return matchesPointInTime(id, tm, opts.ValidAt)
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		return matchesInterval(id, tm, opts.ValidStart, opts.ValidEnd)
	}
	return true // no filter
}

// matchesPointInTime checks: from <= t AND (to == 0 OR to > t).
func matchesPointInTime(id snowflake.ID, tm *types.TemporalMetadata, t types.Instant) bool {
	from := EntityValidFrom(id, tm)
	if from > t {
		return false
	}
	if tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// matchesInterval checks: from < end AND (to == 0 OR to > start).
func matchesInterval(id snowflake.ID, tm *types.TemporalMetadata, start, end types.Instant) bool {
	from := EntityValidFrom(id, tm)
	if from >= end {
		return false
	}
	if tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > start
	}
	return true
}
