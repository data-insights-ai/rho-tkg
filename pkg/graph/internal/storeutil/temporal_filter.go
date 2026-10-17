package storeutil

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EntityValidFrom is THE start rule for a row (backlog 62):
// types.TemporalMetadata.EffectiveValidFrom with the ID's mint instant — its
// explicit ValidFrom, else its UntimedValidFrom. Every valid-time door — the
// chain resolver, the store push-down, the temporal index envelopes, the
// column filters — takes a row's start from here, so a create and a later
// version without a valid-from start on one clock. Config.UntimedValidStart
// needs no branch here (backlog 73): under UntimedUnbounded the create stores
// an untimed first version with ValidFrom = types.UnboundedValidFrom, an
// explicit start this rule returns as is.
func EntityValidFrom(id snowflake.ID, tm *types.TemporalMetadata) types.Instant {
	return tm.EffectiveValidFrom(SnowflakeInstant(id))
}

// UntimedValidFrom is types.TemporalMetadata.UntimedValidFrom with the ID's
// mint instant: the start of a row without a valid-from (its TxFrom, legacy
// UpdatedAt, the mint). The chain resolver uses it directly where it must
// ignore an inherited legacy ValidFrom.
func UntimedValidFrom(id snowflake.ID, tm *types.TemporalMetadata) types.Instant {
	return tm.UntimedValidFrom(SnowflakeInstant(id))
}

// NodeValidFrom is EntityValidFrom for a materialized node
// (types.Node.EffectiveValidFrom): allocation-free on a frozen row.
func NodeValidFrom(n *types.Node) types.Instant { return n.EffectiveValidFrom() }

// RelValidFrom is NodeValidFrom for a relationship.
func RelValidFrom(r *types.Relationship) types.Instant { return r.EffectiveValidFrom() }

// SnowflakeInstant returns the IMMUTABLE mint-time of a snowflake ID as a
// types.Instant (Unix ms). Unlike EntityValidFrom it never consults temporal
// metadata — this is the age boundary retention purge (ADR-0008 ByAge) tests,
// deliberately independent of a caller-set ValidFrom or a backfilled TxFrom so a
// backfilled fact cannot dodge (or be silently caught by) the purge predicate.
func SnowflakeInstant(id snowflake.ID) types.Instant {
	return types.Instant(idlayout.MintInstantMillis(id))
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
	if opts.HasValidAt() {
		return IntervalCovers(from, to, opts.ValidAt)
	}
	if opts.HasValidInterval() {
		return IntervalMeets(from, to, opts.ValidStart, opts.ValidEnd)
	}
	return true
}

// IntervalCovers reports whether the valid interval [from, to) (to == 0:
// open ended) holds at the instant at: from <= at and (to open or to > at).
// The one point rule of the point filters and the point resolvers.
func IntervalCovers(from, to, at types.Instant) bool {
	return from <= at && (to == 0 || to > at)
}

// IntervalMeets reports whether the valid interval [from, to) (to == 0: open
// ended, the stored rows' convention) meets the half-open window [start, end):
// from < end and (to open or to > start). start and end are real instants
// (types.UnboundedValidFrom and math.MaxInt64 are the far ends; 0 is the
// instant 0, not "unset"). The one overlap rule of the interval filters, the
// chain resolvers' interval kind and the windowed effective reads.
func IntervalMeets(from, to, start, end types.Instant) bool {
	return from < end && (to == 0 || to > start)
}

// MatchesTemporalFilter evaluates whether an entity passes the temporal
// filter in opts. Returns true when no filter is set (zero values).
//
// Presence is QueryOpts.HasValidAt / HasValidInterval (0 is an ordinary
// instant under a presence flag). ValidAt takes precedence over the interval;
// an unflagged lone ValidStart or ValidEnd is no filter (returns true).
//
// Point-in-time: from <= t AND (to == 0 OR to > t)
// Interval:      from < end AND (to == 0 OR to > start)
func MatchesTemporalFilter(id snowflake.ID, tm *types.TemporalMetadata, opts storepkg.QueryOpts) bool {
	return MatchesTemporalFilterFrom(EntityValidFrom(id, tm), tm, opts)
}

// MatchesTemporalFilterFrom is MatchesTemporalFilter with the row's effective
// valid-from already resolved (from), for a caller that resolved it without
// copying the row's metadata (NodeValidFrom / RelValidFrom).
func MatchesTemporalFilterFrom(from types.Instant, tm *types.TemporalMetadata, opts storepkg.QueryOpts) bool {
	var to types.Instant
	if tm != nil {
		to = tm.ValidTo
	}
	if opts.HasValidAt() {
		return IntervalCovers(from, to, opts.ValidAt)
	}
	if opts.HasValidInterval() {
		if opts.ValidStart >= opts.ValidEnd {
			return false
		}
		return IntervalMeets(from, to, opts.ValidStart, opts.ValidEnd)
	}
	return true // no filter
}

// MatchesPointInTime checks: from <= t AND (to == 0 OR to > t), with from
// derived via EntityValidFrom.
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
