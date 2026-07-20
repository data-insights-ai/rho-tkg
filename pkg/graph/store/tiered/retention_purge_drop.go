package tiered

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldShardDrainSpinLimit bounds the drain wait so a wedged in-flight request cannot
// stall a purge forever. On timeout the shard is RE-LINKED and left to the row-scan
// path (safe — the fast-drop is a pure optimization). ~5s at 1ms/spin. Reused by
// Close()'s drains (BACKLOG 19n, drainActiveReqsBounded in tieredstore.go) — same
// bound, same "a wedged counter must not stall forever" rationale.
const coldShardDrainSpinLimit = 5000

// ErrDrainTimeout is returned (wrapped) when a bounded active-request drain hits
// coldShardDrainSpinLimit without reaching zero — signals a checkin leak elsewhere
// rather than normal in-flight traffic finishing late.
var ErrDrainTimeout = errors.New("graph: active-request drain timed out")

// drainActiveReqsBounded spin-waits (1ms/iteration) for load() to reach <= 0, up to
// coldShardDrainSpinLimit iterations (~5s). Returns true if drained, false on
// timeout — the caller decides what to do (report and proceed; a wedged counter
// must never make a lifecycle method like Close() hang forever, unlike the purge
// protocol which can safely fall back to a slower path instead).
func drainActiveReqsBounded(load func() int64) bool {
	for i := 0; i < coldShardDrainSpinLimit; i++ {
		if load() <= 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// fastDropEligibleShards physically DROPS wholly-aged-out single-label event shards
// (ADR-0008 R4 optimization) instead of row-scanning them, for a ByAge purge with the
// change-log DISABLED. It returns the accumulated drop result; the dropped shards are
// removed from ts.eventShards, so the caller's row-scan fan-out (forEachOpenShard)
// naturally skips them.
//
// A drop replaces the per-row cascade + flush with one directory removal. Correctness
// under concurrency rests on a DRAIN PROTOCOL (dropOneShard): a shard is unlinked from
// routing, its in-flight requests drained, and only THEN is its cross-shard residue
// collected + swept and the directory removed — so no concurrent write can leave
// un-swept residue (a new edge to a purged-window node fails endpoint validation once
// the shard is unlinked; an in-flight one is captured by the drain). Disabled under
// change-log (dropping a shard would destroy its 0x09 log segment → replica LSN gap).
func (ts *Store) fastDropEligibleShards(labelToken uint16, before types.Instant) (storecontract.RetentionPurgeResult, error) {
	var total storecontract.RetentionPurgeResult
	if ts.logEnabled || before <= 0 {
		return total, nil // change-log on (log-segment loss) → row-scan
	}

	// dropOneShard mutates ts.eventShards (the same map Close() ranges while
	// tearing shards down). Every other topology-mutating operation
	// (RotateHotShard, Clear, index creation) excludes Close() via
	// beginSequentialStoreWideOperation/lifecycleMu; the drop path must too,
	// or Close() and a drop's map mutations race unsynchronized (BACKLOG 19a).
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return total, err
	}
	defer releaseLifecycle()

	beforeTime := time.UnixMilli(int64(before))

	// Snapshot the candidate event shards whose entire window is below the boundary
	// (every entity minted before it — ByAge). Never the hot shard (write-active, open
	// window). timeEnd is exclusive, so timeEnd <= before ⇒ all mint-times < before.
	ts.mu.RLock()
	var candidates []*EventShard
	for _, es := range ts.eventShards {
		if es == ts.hotShard || es.timeEnd.IsZero() {
			continue
		}
		if !es.timeEnd.After(beforeTime) {
			candidates = append(candidates, es)
		}
	}
	ts.mu.RUnlock()

	for _, es := range candidates {
		res, ok, err := ts.dropOneShard(es, labelToken)
		if err != nil {
			return total, err
		}
		if ok {
			total.NodesPurged += res.NodesPurged
			total.RelsPurged += res.RelsPurged
			total.PurgedNodeIDs = append(total.PurgedNodeIDs, res.PurgedNodeIDs...)
		}
	}
	return total, nil
}

// dropOneShard runs the drain protocol for one candidate. ok=false (no error) means
// NOT dropped (multi-label, or the drain timed out) — the caller row-scans it instead.
func (ts *Store) dropOneShard(es *EventShard, labelToken uint16) (storecontract.RetentionPurgeResult, bool, error) {
	var zero storecontract.RetentionPurgeResult

	// (1) Single-label check (a shard holding another label is not droppable by a
	// single-label purge). Open the shard read-pinned for the check; this holds a
	// checkout, so do it BEFORE the drain-unlink.
	store, err := es.checkoutStore(ts)
	if err != nil {
		return zero, false, err
	}
	onlyLabel, _, _, err := store.CollectShardDropResidue(labelToken)
	es.checkinStore()
	if err != nil {
		return zero, false, err
	}
	if !onlyLabel {
		return zero, false, nil // other labels present — row-scan handles it
	}

	// (2) Unlink from routing so no NEW checkout can reach this shard (a concurrent
	// edge to a purged-window node now fails endpoint validation on the hot-shard
	// fallback instead of adding un-swept residue here).
	ts.mu.Lock()
	if ts.eventShards[es.name] != es {
		ts.mu.Unlock()
		return zero, false, nil // rotated/removed concurrently — skip
	}
	delete(ts.eventShards, es.name)
	ts.mu.Unlock()

	// (3) Drain in-flight requests. No new checkouts can arrive (unlinked), so
	// activeReqs only decreases. On timeout, RE-LINK and fall back to row-scan.
	drained := false
	for i := 0; i < coldShardDrainSpinLimit; i++ {
		if es.activeReqs.Load() == 0 {
			drained = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !drained {
		ts.mu.Lock()
		ts.eventShards[es.name] = es // re-link — the row scan will handle it
		ts.mu.Unlock()
		return zero, false, nil
	}

	// (4) Collect residue from the now-quiescent shard (all in-flight edges committed,
	// no new ones possible) and sweep the cross-shard part on survivor shards.
	store, err = es.checkoutStore(ts)
	if err != nil {
		return zero, false, err
	}
	stillOnlyLabel, nodeIDs, rels, cerr := store.CollectShardDropResidue(labelToken)
	es.checkinStore()
	if cerr != nil {
		return zero, false, cerr
	}
	// Re-verify single-label eligibility (BACKLOG 19e): a write that started
	// and finished entirely between step (1)'s check and step (2)'s unlink is
	// invisible to the drain (it was never "in-flight" at unlink time), so a
	// concurrent AddNodeLabelToken adding a FOREIGN label in that narrow
	// window would otherwise go undetected — the shard would be dropped with
	// live data under a label this purge was never authorized to remove. This
	// second, POST-drain result is authoritative; re-link and fall back to
	// row-scan instead of silently discarding it.
	if !stillOnlyLabel {
		ts.mu.Lock()
		ts.eventShards[es.name] = es
		ts.mu.Unlock()
		return zero, false, nil
	}
	dropSet := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		dropSet[id] = struct{}{}
	}
	relsSwept, serr := ts.sweepDroppedShardResidue(rels, dropSet)
	if serr != nil {
		return zero, false, serr
	}

	// (5) Physically drop: close the store, then durably commit the catalog
	// removal BEFORE deleting the directory (not after). This ordering means a
	// crash/failure before the catalog commit leaves the directory AND the
	// catalog untouched — consistent and safely retryable, mirroring
	// RotateHotShard's snapshot/restore discipline on a failed Save. A
	// crash/failure after the catalog commits but before RemoveAll finishes
	// leaves only a harmless orphaned, unreferenced directory (reclaimable by
	// a future cleanup pass) instead of a catalog entry pointing at a
	// directory that no longer exists — which would otherwise brick the
	// shard's reopen for a warm shard, or leave a permanent zombie catalog
	// entry for a cold one (BACKLOG 19b).
	es.shardMu.Lock()
	if es.store != nil {
		if closeErr := es.store.Close(); closeErr != nil {
			es.shardMu.Unlock()
			return zero, false, closeErr
		}
		es.store = nil
	}
	es.shardMu.Unlock()

	snapshot := ts.catalog.snapshotShards()
	ts.catalog.RemoveShard(es.name)
	if serr := ts.catalog.Save(); serr != nil {
		ts.catalog.restoreShards(snapshot)
		// The directory is still intact (RemoveAll hasn't run) and the
		// catalog rollback means it's still logically present too — reopen
		// (checkoutStore's fast path for a non-cold shard assumes es.store is
		// never nil, so relinking alone would leave every subsequent checkout
		// returning a nil store) and re-link so the shard stays reachable,
		// instead of being silently stranded in memory despite its data
		// surviving on disk.
		es.shardMu.Lock()
		store, reopenErr := ts.openBadgerStoreWithRecovery(es.path)
		if reopenErr == nil {
			es.store = store
		}
		es.shardMu.Unlock()
		ts.mu.Lock()
		ts.eventShards[es.name] = es
		ts.mu.Unlock()
		if reopenErr != nil {
			return zero, false, fmt.Errorf("graph: reopen shard %s after failed drop: %w (drop error: %v)", es.name, reopenErr, serr)
		}
		return zero, false, serr
	}
	if rerr := os.RemoveAll(filepath.Join(ts.dataDir, es.path)); rerr != nil {
		return zero, false, rerr
	}

	return storecontract.RetentionPurgeResult{
		NodesPurged:   len(nodeIDs),
		RelsPurged:    relsSwept,
		PurgedNodeIDs: nodeIDs,
	}, true, nil
}

// sweepDroppedShardResidue removes, on each SURVIVING endpoint's shard, the residue a
// dropped shard's cross-shard edges leave behind (mirrors the row-scan phase-2 sweep):
// an endpoint in dropSet is on the dropped shard (gone with the directory); the OTHER
// endpoint's shard holds the residue (a full-local rel or an orphan in-leg), removed
// via PurgeRelationshipByInfo. Returns the count of distinct rels swept.
func (ts *Store) sweepDroppedShardResidue(rels []storecontract.PurgedRel, dropSet map[types.NodeID]struct{}) (int, error) {
	return ts.sweepRelResidue(rels, func(endpoint types.NodeID) bool {
		_, gone := dropSet[endpoint]
		return gone // this endpoint is on the dropped shard — nothing to clean there
	})
}

// sweepRelResidue is the shared cross-shard residue-sweep body BACKLOG 19m
// factors out of sweepDroppedShardResidue (whole-shard fast-drop) and
// purgeNodesFanOut's Phase 2 (row-scan purge, retention_purge.go) — the two
// were independently near-duplicated, a future fix to one likely to miss
// the other. Dedupes rels by ID (an internal rel between two purged nodes,
// or a self-loop, would otherwise be swept once per endpoint), then for
// each endpoint whose shard MIGHT hold residue, purges it. isKnownGone lets
// a caller with a precomputed "already known dropped" set (the fast-drop
// path's dropSet) skip a redundant GetNode call for those endpoints; a
// caller with no such set (the row-scan path) passes a func that always
// returns false, relying solely on GetNode's ErrNodeNotFound to detect a
// purged endpoint. Returns the count of unique rels processed.
func (ts *Store) sweepRelResidue(rels []storecontract.PurgedRel, isKnownGone func(types.NodeID) bool) (int, error) {
	seen := make(map[types.RelID]struct{}, len(rels))
	swept := 0
	for _, pr := range rels {
		if _, ok := seen[pr.ID]; ok {
			continue
		}
		seen[pr.ID] = struct{}{}
		swept++
		for _, endpoint := range [2]types.NodeID{pr.StartID, pr.EndID} {
			if isKnownGone(endpoint) {
				continue
			}
			// Surviving endpoint: its shard may hold residue. Skip if the endpoint is
			// itself already gone (a GetNode miss), else route + purge the residue.
			if _, gerr := ts.GetNode(endpoint); gerr != nil {
				if errors.Is(gerr, ErrNodeNotFound) {
					continue
				}
				return swept, gerr
			}
			shard, checkin, serr := ts.shardForNodeIDChecked(endpoint)
			if serr != nil {
				return swept, serr
			}
			e := shard.PurgeRelationshipByInfo(pr)
			checkin()
			if e != nil {
				return swept, e
			}
		}
	}
	return swept, nil
}
