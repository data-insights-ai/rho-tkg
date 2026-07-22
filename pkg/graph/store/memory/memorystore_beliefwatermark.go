package memory

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Belief-watermark sidecar (memory arm) — BACKLOG 10c.
//
// See store.NodeBeliefWatermarkCapability's doc comment for the full
// rationale: this restores a SAFE version of the current-row-only fast path
// BACKLOG 10b removed from nodeAtLockedTx/relAtLockedTx, gated on an explicit
// per-entity invariant instead of an assumption a bounded cascade correction
// could silently break.
//
// LAZY, nil until the first consultation builds it from ms.nodes/ms.rels +
// ms.nodeHistory/ms.relHistory (mirrors the labelTxMembers precedent in this
// same package); maintained incrementally thereafter at every door that
// persists a current or history row with a TxFrom. Never decreases — bumping
// with an OLD, already-recorded TxFrom (e.g. a transaction rollback
// restoring prior state verbatim) is a safe no-op.

// bumpNodeBeliefWatermarkLocked records txFrom as a new lower bound for id's
// watermark if it exceeds what's already recorded. No-op until the sidecar is
// built. Caller holds ms.mu (write).
func (ms *Store) bumpNodeBeliefWatermarkLocked(id types.NodeID, txFrom types.Instant) {
	if ms.nodeBeliefWatermark == nil {
		return // not built yet — the lazy build will capture current state
	}
	if prev, ok := ms.nodeBeliefWatermark[id]; !ok || txFrom > prev {
		ms.nodeBeliefWatermark[id] = txFrom
	}
}

// bumpRelBeliefWatermarkLocked mirrors bumpNodeBeliefWatermarkLocked for
// relationships. Caller holds ms.mu (write).
func (ms *Store) bumpRelBeliefWatermarkLocked(id types.RelID, txFrom types.Instant) {
	if ms.relBeliefWatermark == nil {
		return
	}
	if prev, ok := ms.relBeliefWatermark[id]; !ok || txFrom > prev {
		ms.relBeliefWatermark[id] = txFrom
	}
}

// No cleanup/drop helper: snowflake IDs are never reused, so a stale
// watermark entry for a since-purged entity can never be misattributed to a
// different entity later — it is simply never looked up again (a hard-deleted
// entity's watermark would only matter if current still existed, and it
// doesn't). Leaving it in the map costs one int64 per ever-created entity,
// the same bounded-per-entity trade-off this codebase already accepts for
// other lifetime sidecars — deliberately not wired into every delete/purge
// door to avoid a whole class of "did I get the cleanup right" risk for a
// gain that is not worth it here.

// ensureNodeBeliefWatermarkBuiltLocked builds the watermark sidecar from
// current + history node state on first use. Caller holds ms.mu (write).
func (ms *Store) ensureNodeBeliefWatermarkBuiltLocked() {
	if ms.nodeBeliefWatermark != nil {
		return
	}
	built := make(map[types.NodeID]types.Instant, len(ms.nodes))
	ms.nodeBeliefWatermark = built
	for id, n := range ms.nodes {
		ms.bumpNodeBeliefWatermarkLocked(id, nodeTxFrom(n))
	}
	for id, versions := range ms.nodeHistory {
		for _, n := range versions {
			ms.bumpNodeBeliefWatermarkLocked(id, nodeTxFrom(n))
		}
	}
}

// ensureRelBeliefWatermarkBuiltLocked mirrors ensureNodeBeliefWatermarkBuiltLocked.
func (ms *Store) ensureRelBeliefWatermarkBuiltLocked() {
	if ms.relBeliefWatermark != nil {
		return
	}
	built := make(map[types.RelID]types.Instant, len(ms.rels))
	ms.relBeliefWatermark = built
	for id, r := range ms.rels {
		ms.bumpRelBeliefWatermarkLocked(id, relTxFrom(r))
	}
	for id, versions := range ms.relHistory {
		for _, r := range versions {
			ms.bumpRelBeliefWatermarkLocked(id, relTxFrom(r))
		}
	}
}

// NodeBeliefWatermark implements store.NodeBeliefWatermarkCapability.
func (ms *Store) NodeBeliefWatermark(id types.NodeID) (types.Instant, bool) {
	if ms == nil {
		return 0, false
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false
	}
	ms.ensureNodeBeliefWatermarkBuiltLocked()
	tx, ok := ms.nodeBeliefWatermark[id]
	return tx, ok
}

// RelBeliefWatermark implements store.RelBeliefWatermarkCapability.
func (ms *Store) RelBeliefWatermark(id types.RelID) (types.Instant, bool) {
	if ms == nil {
		return 0, false
	}
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, false
	}
	ms.ensureRelBeliefWatermarkBuiltLocked()
	tx, ok := ms.relBeliefWatermark[id]
	return tx, ok
}
