package storeutil

import (
	"cmp"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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
//     at or after it and by the pin — the rows a bounded SetVersionInterval
//     appends while the current row keeps the store's current slot (backlog
//     18; "at" covers chains written before v4.46, whose cascade numbered its
//     pieces above the resumption that took the slot, in the same write).
//     That row is then the newest row recorded by the pin, before AND after a
//     later write supersedes the current row, so the answer at a pin never
//     depends on later writes. History rows above the current version
//     recorded before it (a re-imported ID's earlier life) never answer for it.
//   - One write, one answer: among the rows recorded together with the newest
//     row (the run of versions below it sharing its TxFrom), the highest one
//     whose own valid interval is open at the pin answers (openInWrite). A
//     cascade written before v4.46 numbered the resumption that took the
//     current slot below its pieces; this keeps it (= Get) the answer.
//   - Retraction: if the newest row was superseded or hard-deleted by the pin
//     (TxTo != 0 && TxTo <= pin, or DeletedAt != 0 && DeletedAt <= pin) the
//     entity is ABSENT; the selector never falls through to an older still-open
//     row (lesson 62).
//   - A tombstone ends its life (handover 2a): a cascade row can sit ABOVE the
//     tombstoned row (written while that row kept the current slot), so the
//     newest row need not be the deleted one. The highest-version row below the
//     newest that carries a TxTo is the row that held the current slot when
//     the newest was written (rows in between never held it, so they carry no
//     TxTo; a TxTo below its row's TxFrom is a copied pre-v4.46 stamp and is
//     passed over); if it is a tombstone deleted after the newest row was
//     recorded and by the pin, the entity is ABSENT.
//
// SelectAsOfWithCurrent is pure selection: it does NOT normalize the survivor
// to its then-visible state (TxTo / DeletedAt rewinding) — that is the
// caller's concern, applied to a copy where required.
func SelectAsOfWithCurrent[T TemporalRow](history []T, current T, hasCurrent bool, pin types.Instant) (T, bool) {
	var zero T
	// desc: history newest version first (the order the badger scan visits).
	desc := slices.Clone(history)
	slices.SortStableFunc(desc, func(a, b T) int { return cmp.Compare(b.Version(), a.Version()) })

	if hasCurrent {
		if ctm := current.Temporal(); ctm != nil && ctm.TxFrom > 0 && ctm.TxFrom <= pin && ctm.TxTo == 0 {
			// seq: the rows above the current version, newest first, then the
			// current row. The first row recorded at or after the current one
			// and by the pin outranks it.
			seq := make([]T, 0, len(desc)+1)
			for _, h := range desc {
				if h.Version() > current.Version() {
					seq = append(seq, h)
				}
			}
			seq = append(seq, current)
			for i, h := range seq[:len(seq)-1] {
				tm := h.Temporal()
				if tm == nil || tm.TxFrom < ctm.TxFrom || tm.TxFrom > pin {
					continue
				}
				ans := openInWrite(seq, i, pin)
				if retractedAtTxTime(ans.Temporal(), pin) {
					return zero, false
				}
				return ans, true
			}
			return current, true
		}
	}

	for i, h := range desc {
		tm := h.Temporal()
		if tm == nil || tm.TxFrom == 0 || tm.TxFrom > pin {
			continue
		}
		ans := openInWrite(desc, i, pin)
		if retractedAtTxTime(ans.Temporal(), pin) || lifeEndedAtTxTime(desc, i, pin) {
			return zero, false
		}
		return ans, true
	}
	return zero, false
}

// openInWrite picks the answer among the rows written together with the newest
// row seq[i]: the run seq[i], seq[i+1], ... (newest first) that shares its
// TxFrom. The highest-version row of the run whose own valid interval is open
// at pin answers; with none, seq[i] does. A cascade written before v4.46
// numbered the resumption that took the current slot BELOW its pieces in the
// same write; this keeps answering that resumption (the current row, as Get
// does) instead of a bounded piece. A cascade written since numbers the slot
// row last, so the newest row of the write already is it.
func openInWrite[T TemporalRow](seq []T, i int, pin types.Instant) T {
	tx := seq[i].Temporal().TxFrom
	for _, r := range seq[i:] {
		tm := r.Temporal()
		if tm == nil || tm.TxFrom != tx {
			break
		}
		if OwnOpenAtTxTime(tm, pin) {
			return r
		}
	}
	return seq[i]
}

// OwnOpenAtTxTime reports whether a row's own valid interval is open as
// believed at pin: ValidTo 0, or a ValidTo a delete recorded after pin wrote
// (ValidTo == DeletedAt > pin, the normalization of lesson 60).
func OwnOpenAtTxTime(tm *types.TemporalMetadata, pin types.Instant) bool {
	return tm.ValidTo == 0 || (tm.DeletedAt != 0 && tm.DeletedAt > pin && tm.ValidTo == tm.DeletedAt)
}

// lifeEndedAtTxTime reports whether the row that held the current slot when
// the newest row desc[i] was recorded — the first lower row (desc is newest
// first) carrying a TxTo at or after its own TxFrom — is a tombstone deleted
// after desc[i] was recorded and by pin. A TxTo below its row's TxFrom is a
// stamp a cascade written before v4.46 copied from its source row (backlog 14):
// that row never held the slot, so it is passed over.
func lifeEndedAtTxTime[T TemporalRow](desc []T, i int, pin types.Instant) bool {
	newest := desc[i].Temporal().TxFrom
	for _, h := range desc[i+1:] {
		tm := h.Temporal()
		if tm == nil || tm.TxTo == 0 || tm.TxTo < tm.TxFrom {
			continue
		}
		return tm.DeletedAt != 0 && tm.DeletedAt > newest && tm.DeletedAt <= pin
	}
	return false
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
