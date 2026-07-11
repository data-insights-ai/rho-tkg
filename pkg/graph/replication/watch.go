package replication

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

const (
	// watchInitialBackoff is the poll interval Watch starts at — and resets to
	// — the instant it has just delivered a record. It is also the interval
	// used for the very first idle wait after catching up to the tip of the
	// feed.
	watchInitialBackoff = 25 * time.Millisecond

	// watchMaxBackoff is the ceiling the idle poll interval doubles toward
	// (25, 50, 100, 200, 400, 500, 500, ... ms) while the feed stays caught up.
	watchMaxBackoff = 500 * time.Millisecond
)

// Watch returns a Go 1.23 iter.Seq2 over committed change-log records with
// LSN >= fromLSN, in STRICTLY ASCENDING LSN order. It is a live-tailing
// convenience layer over ForEachChange for `for rec, err := range
// api.Watch(ctx, cursor)` callers — ForEachChange remains the right choice
// for a single bounded batch read; Watch is for a long-lived consumer that
// wants to keep observing new records as they commit.
//
// Cursor tracking: Watch maintains the next-LSN cursor internally as
// lastSeen+1 (lastSeen starts at fromLSN-1, or at 0 when fromLSN==0 — "from
// the beginning", since real LSNs are >= 1) and pulls batches via
// ForEachChange(lastSeen, ...), the same "LSN > afterLSN" contract documented
// on store.ChangeFeedCapability. Every record actually yielded advances
// lastSeen to that record's LSN before the next pull, so a single Watch call
// never redelivers an LSN it has already yielded.
//
// Tailing: once a pull returns no new records, Watch is caught up and polls
// again after a ctx-aware backoff. The backoff starts at 25ms and doubles
// while the feed stays idle, capped at 500ms; it resets to 25ms the instant
// any record is delivered (a burst of writes is drained promptly, a quiet
// period is polled cheaply). The wait is always a time.Timer selected against
// ctx.Done() — Watch never calls time.Sleep, so cancellation during a
// backoff wait is immediate rather than bounded by the remaining sleep.
//
// Termination — Watch stops for exactly one of four reasons and no others:
//  1. ctx is cancelled or its deadline expires. The iterator simply stops
//     without yielding an error. This is the NORMAL shutdown path for a live
//     tail (the caller is done watching), not a failure — a consumer that
//     wants to detect cancellation checks ctx itself, the same way it does
//     for any other ctx-driven loop.
//  2. The caller's range loop breaks, i.e. the yield callback returns false.
//     Watch stops promptly (mid-batch breaks are honored — it does not
//     finish draining the in-flight ForEachChange batch first).
//  3. The change-log is not active: the store has no change-feed capability
//     at all (e.g. tiered, or any store lacking store.ChangeFeedCapability),
//     OR it implements store.ChangeLogStatusCapability and reports the log as
//     OFF (present but disabled — e.g. badger/memory opened without
//     ChangeLog/WithChangeLog). Watch checks this ONCE, before the very first
//     pull, mirroring the exact fail-closed discipline io.API.Watermark and
//     ExportSince use (see (*Core).changeLogActive) — a present-but-disabled
//     log must never look like a caught-up tail that simply has nothing new
//     yet. Watch yields (store.ChangeRecord{}, store.ErrCapabilityNotSupported)
//     — assert with errors.Is — exactly once and stops; ForEachChange is never
//     called.
//  4. The underlying feed returns an error from ForEachChange during a LATER
//     pull (the log was active at the check above but a subsequent read
//     failed). Watch yields (store.ChangeRecord{}, err) exactly once and then
//     stops; it never retries a feed error.
//
// Delivery is at-least-once ACROSS separate Watch calls: a consumer that
// persists its own cursor (e.g. the last LSN it processed) and later resumes
// Watch(ctx, cursor+1) after a restart may observe a record again if its
// cursor was not durably advanced past that record before the restart.
// WITHIN one Watch call, delivery is exactly-once — the internal cursor only
// moves forward past records already yielded.
//
// Physical-redo-log caveat: the change-log is a MUTATION-LEVEL physical redo
// log, not a logical/transactional one — see the doc comment on
// store.ChangeFeedCapability and CLAUDE.md's change-log invariants. Since
// v4.10.1 a rolled-back transaction or batch is diverted before commit and
// therefore emits nothing to the feed, but two separately-committed
// mutations (e.g. a delete followed by a later, distinct re-create) each
// still produce their own record — Watch delivers exactly what was
// committed, in commit order, nothing more and nothing less.
func (a *API) Watch(ctx context.Context, fromLSN uint64) iter.Seq2[store.ChangeRecord, error] {
	return func(yield func(store.ChangeRecord, error) bool) {
		ops, err := a.ready()
		if err != nil {
			yield(store.ChangeRecord{}, err)
			return
		}
		if !ops.ChangeLogActive() {
			yield(store.ChangeRecord{}, fmt.Errorf("graph: watch: %w (no change-log)", store.ErrCapabilityNotSupported))
			return
		}

		var lastSeen uint64
		if fromLSN > 0 {
			lastSeen = fromLSN - 1
		}

		backoff := watchInitialBackoff
		for {
			if ctx.Err() != nil {
				return
			}

			produced := false
			stopped := false
			walkErr := ops.ForEachChange(lastSeen, func(rec store.ChangeRecord) bool {
				if ctx.Err() != nil {
					stopped = true
					return false
				}
				if !yield(rec, nil) {
					stopped = true
					return false
				}
				lastSeen = rec.LSN
				produced = true
				return true
			})
			if walkErr != nil {
				yield(store.ChangeRecord{}, walkErr)
				return
			}
			if stopped {
				return
			}
			if produced {
				// More may already be available (a burst); loop back
				// immediately at the reset (fast) backoff instead of waiting.
				backoff = watchInitialBackoff
				continue
			}

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			backoff *= 2
			if backoff > watchMaxBackoff {
				backoff = watchMaxBackoff
			}
		}
	}
}
