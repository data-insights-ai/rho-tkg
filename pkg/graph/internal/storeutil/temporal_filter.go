package storeutil

import (
	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/internal/idlayout"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// EntityValidFrom is THE start rule for a row (backlog 62): its explicit
// ValidFrom, else its UntimedValidFrom. Every valid-time door — the chain
// resolver, the store push-down, the temporal index envelopes, the column
// filters — takes a row's start from here, so a create and a later version
// without a valid-from start on one clock. Config.UntimedValidStart needs no
// branch here (backlog 73): under UntimedUnbounded the create stores an
// untimed first version with ValidFrom = types.UnboundedValidFrom, an
// explicit start this rule returns as is.
func EntityValidFrom(id snowflake.ID, tm *types.TemporalMetadata) types.Instant {
	if tm != nil && tm.ValidFrom != 0 {
		return tm.ValidFrom
	}
	return UntimedValidFrom(id, tm)
}

// UntimedValidFrom is the start of a row without a valid-from: its TxFrom (the
// commit instant, or a backfilled tkg_tx_from), for a create and a later
// version alike. A legacy row without a TxFrom falls back to its UpdatedAt
// (a later version), then to the ID's mint instant. The chain resolver uses it
// directly where it must ignore an inherited legacy ValidFrom.
func UntimedValidFrom(id snowflake.ID, tm *types.TemporalMetadata) types.Instant {
	if tm != nil {
		if tm.TxFrom != 0 {
			return tm.TxFrom
		}
		if tm.UpdatedAt != 0 {
			return tm.UpdatedAt
		}
	}
	return types.Instant(idlayout.MintInstantMillis(id))
}

// NodeValidFrom is EntityValidFrom for a materialized node without copying
// its metadata (Temporal() copies on a frozen row): one allocation-free read
// of ValidFrom and TxFrom, the metadata only for a legacy row without a TxFrom.
func NodeValidFrom(n *types.Node) types.Instant {
	if vf, _, _ := n.ValidRange(); vf != 0 {
		return vf
	}
	if tx, _, _ := n.TxStamps(); tx != 0 {
		return tx
	}
	return UntimedValidFrom(n.ID().SnowflakeID(), n.Temporal())
}

// RelValidFrom is NodeValidFrom for a relationship.
func RelValidFrom(r *types.Relationship) types.Instant {
	if vf, _, _ := r.ValidRange(); vf != 0 {
		return vf
	}
	if tx, _, _ := r.TxStamps(); tx != 0 {
		return tx
	}
	return UntimedValidFrom(r.ID().SnowflakeID(), r.Temporal())
}

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
		return from <= opts.ValidAt && (to == 0 || to > opts.ValidAt)
	}
	if opts.HasValidInterval() {
		return from < opts.ValidEnd && (to == 0 || to > opts.ValidStart)
	}
	return true
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
		return from <= opts.ValidAt && (to == 0 || to > opts.ValidAt)
	}
	if opts.HasValidInterval() {
		if opts.ValidStart >= opts.ValidEnd {
			return false
		}
		return from < opts.ValidEnd && (to == 0 || to > opts.ValidStart)
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
