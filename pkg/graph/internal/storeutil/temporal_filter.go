package storeutil

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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

// EnvelopeOverlaps reports whether a node's valid-time ENVELOPE [from, to) (to == 0
// meaning open-ended) could satisfy the valid-time filter in opts. It is the B4
// candidate-prune predicate: because the envelope is a SOUND SUPERSET of every
// version's interval, a false result proves NO version of the node can match the
// filter, so the candidate may be dropped without loading its chain; a true result
// only means SOME version MIGHT match (the chain resolver decides precisely). Only
// a point (ValidAt) or interval (ValidStart/ValidEnd) filter constrains — with
// neither, returns true (nothing to prune on).
func EnvelopeOverlaps(from, to types.Instant, opts storepkg.QueryOpts) bool {
	if opts.ValidAt != 0 {
		return from <= opts.ValidAt && (to == 0 || to > opts.ValidAt)
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		return from < opts.ValidEnd && (to == 0 || to > opts.ValidStart)
	}
	return true
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
func MatchesTemporalFilter(id snowflake.ID, tm *types.TemporalMetadata, opts storepkg.QueryOpts) bool {
	if opts.ValidAt != 0 {
		return MatchesPointInTime(id, tm, opts.ValidAt)
	}
	if opts.ValidStart > 0 && opts.ValidEnd > 0 {
		if opts.ValidStart >= opts.ValidEnd {
			return false
		}
		return MatchesInterval(id, tm, opts.ValidStart, opts.ValidEnd)
	}
	return true // no filter
}

// HasTemporalFilter reports whether opts contains an active temporal filter.
func HasTemporalFilter(opts storepkg.QueryOpts) bool {
	return opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
}

// MatchesPointInTime checks: from <= t AND (to == 0 OR to > t), with from
// derived via EntityValidFrom (explicit ValidFrom, else snowflake fallback).
//
// This function and MatchesInterval are the CANONICAL entity-level temporal
// predicates. The store backends use them for query push-down and the core
// graph layer delegates its nodeValidFrom/relValidFrom/validity helpers here
// — the semantics must never be redefined elsewhere (lesson 17: a fix behind
// one door must not miss the other).
func MatchesPointInTime(id snowflake.ID, tm *types.TemporalMetadata, t types.Instant) bool {
	from := EntityValidFrom(id, tm)
	if from > t {
		return false
	}
	if tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > t
	}
	return true
}

// MatchesInterval checks: from < end AND (to == 0 OR to > start), with from
// derived via EntityValidFrom. See MatchesPointInTime for the canonical-
// predicate contract.
func MatchesInterval(id snowflake.ID, tm *types.TemporalMetadata, start, end types.Instant) bool {
	from := EntityValidFrom(id, tm)
	if from >= end {
		return false
	}
	if tm != nil && tm.ValidTo != 0 {
		return tm.ValidTo > start
	}
	return true
}
