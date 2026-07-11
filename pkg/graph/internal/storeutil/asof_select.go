package storeutil

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// TemporalRow is the minimal view SelectAsOf needs over one version of an
// entity: its version ordinal and its temporal metadata. Both *types.Node and
// *types.Relationship satisfy it, so SelectAsOf is the ONE implementation of the
// as-of selection rule shared by the memory backend, the badger native
// reverse-scan (via an equivalence test), and the core resolution seam — no
// backend re-implements the rule (killing the cross-backend-divergence bug
// class: the as-of version-order divergence and the commit-window drop).
type TemporalRow interface {
	Version() uint32
	Temporal() *types.TemporalMetadata
}

// SelectAsOf implements THE canonical as-of (transaction-time belief-state)
// SELECTION rule over a version chain: it returns the newest belief recorded by
// pin, or (zero, false) when the entity is ABSENT at pin.
//
// The rule, in one place:
//
//   - Candidate: a version with 0 < TxFrom <= pin (recorded-by-then; lesson 43
//     keeps TxTo out of the candidacy test — superseded is not un-recorded).
//   - Newest belief: among candidates, the one with the highest VERSION ordinal.
//     Recency is by VERSION, not by TxFrom: an Update derives its TxFrom via
//     validInstantAfter and can bump it ABOVE a later append-only cascade row's
//     plain now() stamp, so version order — allocation order — is authoritative
//     (this is exactly what the badger native reverse-scan relies on, visiting
//     history newest-version-first).
//   - Retraction: if that decisive newest belief was itself retracted
//     (TxTo != 0 && TxTo <= pin) or hard-deleted (DeletedAt != 0 &&
//     DeletedAt <= pin) by the pin, the entity is ABSENT. The selector must
//     NEVER fall through to an older still-open row (lesson 62): an append-only
//     cascade can demote a prior current to history WITHOUT stamping its TxTo, so
//     a hard delete that tombstones the corrected tile would otherwise resurrect
//     the open-TxTo genesis.
//
// SelectAsOf is pure selection: it does NOT normalize the survivor to its
// then-visible state (TxTo / DeletedAt rewinding) — that is the caller's
// concern, applied to a copy where required.
func SelectAsOf[T TemporalRow](versions []T, pin types.Instant) (T, bool) {
	var best T
	found := false
	for _, v := range versions {
		tm := v.Temporal()
		if tm == nil || tm.TxFrom == 0 || tm.TxFrom > pin {
			continue
		}
		if !found || v.Version() > best.Version() {
			best, found = v, true
		}
	}
	if !found || retractedAtTxTime(best.Temporal(), pin) {
		var zero T
		return zero, false
	}
	return best, true
}

// retractedAtTxTime reports whether the decisive newest-belief version was
// already superseded or hard-deleted by pin. A hard Delete stamps TxTo ==
// DeletedAt in place, so the TxTo clause already covers deletes; the DeletedAt
// clause is a defensive restatement of the target semantics that can never
// diverge (DeletedAt is only ever set alongside an equal TxTo).
func retractedAtTxTime(tm *types.TemporalMetadata, pin types.Instant) bool {
	if tm == nil {
		return false
	}
	if tm.TxTo != 0 && tm.TxTo <= pin {
		return true
	}
	return tm.DeletedAt != 0 && tm.DeletedAt <= pin
}
