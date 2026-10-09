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

// SelectAsOf is SelectAsOfWithCurrent for an entity without a live current
// row in the candidate set (history rows only).
func SelectAsOf[T TemporalRow](history []T, pin types.Instant) (T, bool) {
	var zero T
	return SelectAsOfWithCurrent(history, zero, false, pin)
}

// SelectAsOfWithCurrent implements THE canonical as-of (transaction-time
// belief-state) SELECTION rule over an entity's chain: it returns the newest
// row recorded by pin, or (zero, false) when the entity is ABSENT at pin.
// history is in any order; current is the live current row when hasCurrent.
//
// The rule, in one place:
//
//   - Candidate: a row with 0 < TxFrom <= pin (recorded-by-then; lesson 43
//     keeps TxTo out of the candidacy test — superseded is not un-recorded).
//   - Newest: among candidates, the one with the highest VERSION ordinal.
//     Versions are allocated one above the chain's highest in write order
//     (core version_alloc.go), so the highest version is the row written last.
//     Recency is by version, not by TxFrom: an Update derives its TxFrom via
//     validInstantAfter and can bump it above a later write's plain now()
//     stamp (lesson 62).
//   - Current arm: a live current row that is a candidate and not retracted
//     (TxTo == 0) answers unless a history row ABOVE its version was recorded
//     after it and by the pin — the rows a bounded SetVersionInterval appends
//     while the current row keeps the store's current slot (backlog 18). That
//     row is then the newest row recorded by the pin, before AND after a later
//     write supersedes the current row, so the answer at a pin never depends
//     on later writes. History rows above the current version recorded
//     before it (a re-imported ID's earlier life) never answer for it.
//   - Retraction: if the newest row was superseded or hard-deleted by the pin
//     (TxTo != 0 && TxTo <= pin, or DeletedAt != 0 && DeletedAt <= pin) the
//     entity is ABSENT; the selector never falls through to an older still-open
//     row (lesson 62).
//   - A tombstone ends its life (handover 2a): a cascade row can sit ABOVE the
//     tombstoned row (written while that row kept the current slot), so the
//     newest row need not be the deleted one. The highest-version row below the
//     newest that carries a TxTo is the row that held the current slot when
//     the newest was written (rows in between never held it, so they carry no
//     TxTo); if it is a tombstone deleted after the newest row was recorded
//     and by the pin, the entity is ABSENT.
//
// SelectAsOfWithCurrent is pure selection: it does NOT normalize the survivor
// to its then-visible state (TxTo / DeletedAt rewinding) — that is the
// caller's concern, applied to a copy where required.
func SelectAsOfWithCurrent[T TemporalRow](history []T, current T, hasCurrent bool, pin types.Instant) (T, bool) {
	var zero T
	if hasCurrent {
		if ctm := current.Temporal(); ctm != nil && ctm.TxFrom > 0 && ctm.TxFrom <= pin && ctm.TxTo == 0 {
			best, above := current, false
			for _, h := range history {
				tm := h.Temporal()
				if tm == nil || h.Version() <= best.Version() || tm.TxFrom <= ctm.TxFrom || tm.TxFrom > pin {
					continue
				}
				best, above = h, true
			}
			if above && retractedAtTxTime(best.Temporal(), pin) {
				return zero, false
			}
			return best, true
		}
	}

	var best T
	found := false
	for _, v := range history {
		tm := v.Temporal()
		if tm == nil || tm.TxFrom == 0 || tm.TxFrom > pin {
			continue
		}
		if !found || v.Version() > best.Version() {
			best, found = v, true
		}
	}
	if !found || retractedAtTxTime(best.Temporal(), pin) {
		return zero, false
	}
	if lifeEndedAtTxTime(history, best, pin) {
		return zero, false
	}
	return best, true
}

// lifeEndedAtTxTime reports whether the row that held the current slot when
// newest was recorded — the highest-version row below newest carrying a TxTo —
// is a tombstone deleted after newest was recorded and by pin.
func lifeEndedAtTxTime[T TemporalRow](history []T, newest T, pin types.Instant) bool {
	var slot *types.TemporalMetadata
	var slotVersion uint32
	for _, h := range history {
		tm := h.Temporal()
		if tm == nil || tm.TxTo == 0 || h.Version() >= newest.Version() {
			continue
		}
		if slot == nil || h.Version() > slotVersion {
			slot, slotVersion = tm, h.Version()
		}
	}
	if slot == nil {
		return false
	}
	return slot.DeletedAt != 0 && slot.DeletedAt > newest.Temporal().TxFrom && slot.DeletedAt <= pin
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
