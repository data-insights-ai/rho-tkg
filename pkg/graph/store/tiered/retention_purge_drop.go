package tiered

import (
	"errors"
	"fmt"
	"os"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldShardDrainSpinLimit bounds the drain wait (~5s at 1ms/spin) so a wedged
// request cannot stall a purge; on timeout the shard is re-linked and left to
// the row-scan path. Also bounds Close()'s drains (BACKLOG 19n,
// drainActiveReqsBounded).
const coldShardDrainSpinLimit = 5000

// ErrDrainTimeout is returned (wrapped) when a bounded active-request drain
// hits coldShardDrainSpinLimit: signals a checkin leak, not late traffic.
var ErrDrainTimeout = errors.New("graph: active-request drain timed out")

// drainSpinLimit is coldShardDrainSpinLimit unless a test lowered it
// (drainSpinsForTest), so a drain-timeout test waits milliseconds, not 5s.
func (ts *Store) drainSpinLimit() int64 {
	if n := ts.drainSpinsForTest.Load(); n > 0 {
		return n
	}
	return coldShardDrainSpinLimit
}

// drainActiveReqsBounded spin-waits (1ms) for load() <= 0, up to
// ts.drainSpinLimit() iterations. Returns false on timeout; the caller
// decides (Close must never hang on a wedged counter).
func (ts *Store) drainActiveReqsBounded(load func() int64) bool {
	for i := int64(0); i < ts.drainSpinLimit(); i++ {
		if load() <= 0 {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

// fastDropEligibleShards physically DROPS wholly-aged-out single-label event
// shards (ADR-0008 R4) instead of row-scanning them, for a ByAge purge with the
// change-log DISABLED (a drop would destroy the shard's 0x09 log segment and
// leave a replica LSN gap). Dropped shards leave ts.eventShards, so the
// row-scan fan-out skips them. Safety rests on the DRAIN PROTOCOL
// (dropOneShard): unlink from routing, drain in-flight requests, only then
// collect and sweep cross-shard residue and remove the directory. Returns the
// accumulated drop result.
func (ts *Store) fastDropEligibleShards(labelToken uint16, before types.Instant) (storecontract.RetentionPurgeResult, error) {
	var total storecontract.RetentionPurgeResult
	if ts.logEnabled || before <= 0 {
		return total, nil // change-log on (log-segment loss) → row-scan
	}

	// dropOneShard mutates ts.eventShards, which Close() ranges; like every
	// topology mutation it must exclude Close() via
	// beginSequentialStoreWideOperation/lifecycleMu (BACKLOG 19a).
	releaseLifecycle, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		return total, err
	}
	defer releaseLifecycle()

	beforeTime := time.UnixMilli(int64(before))

	// Candidates: event shards whose whole window is below the boundary
	// (timeEnd exclusive). Never the write-active hot shard.
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

	// (1) Single-label check, read-pinned; holds a checkout, so BEFORE the
	// drain-unlink.
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

	// (2) Unlink from routing so no NEW checkout reaches the shard. Once unlinked, a new
	// edge to a purged-window node fails endpoint validation on the hot-shard fallback,
	// so it adds no unswept residue; the drain captures an in-flight one.
	ts.mu.Lock()
	if ts.eventShards[es.name] != es {
		ts.mu.Unlock()
		return zero, false, nil // rotated/removed concurrently — skip
	}
	delete(ts.eventShards, es.name)
	ts.reindexShardsLocked()
	ts.mu.Unlock()

	// (3) Drain in-flight requests. No new checkouts can arrive (unlinked), so
	// activeReqs only decreases. On timeout, RE-LINK and fall back to row-scan.
	drained := false
	for i := int64(0); i < ts.drainSpinLimit(); i++ {
		if es.activeReqs.Load() == 0 {
			drained = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !drained {
		ts.mu.Lock()
		ts.eventShards[es.name] = es // re-link — the row scan will handle it
		ts.reindexShardsLocked()
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
	// Re-verify single-label eligibility (BACKLOG 19e): a write that began and
	// finished between step (1) and the unlink is invisible to the drain, so a
	// concurrent AddNodeLabelToken adding a FOREIGN label would go undetected.
	// This post-drain result is authoritative: re-link and fall back to row-scan.
	if !stillOnlyLabel {
		ts.mu.Lock()
		ts.eventShards[es.name] = es
		ts.reindexShardsLocked()
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

	// (5) Physically drop: close the store, then commit the catalog removal
	// BEFORE deleting the directory. A failure before the commit leaves directory
	// and catalog untouched (retryable); one after it leaves only an orphaned
	// directory, never a catalog entry for a missing directory (BACKLOG 19b).
	es.shardMu.Lock()
	if es.store != nil {
		if closeErr := es.store.Close(); closeErr != nil {
			es.shardMu.Unlock()
			return zero, false, closeErr
		}
		es.clearStoreLocked(ts)
	}
	es.shardMu.Unlock()

	dir := ts.shardDir(es.path) // before the entry goes: it names the shard's volume
	snapshot := ts.catalog.snapshotShards()
	ts.catalog.RemoveShard(es.name)
	if serr := ts.catalog.Save(); serr != nil {
		ts.catalog.restoreShards(snapshot)
		// Directory intact and catalog rolled back: reopen (checkoutStore assumes
		// es.store is non-nil for non-cold shards) and re-link so the shard stays
		// reachable.
		es.shardMu.Lock()
		store, reopenErr := ts.openShardStoreWithRecovery(es.path, es.currentTier() == TierCold)
		if reopenErr == nil {
			es.setStoreLocked(ts, store)
		}
		es.shardMu.Unlock()
		ts.mu.Lock()
		ts.eventShards[es.name] = es
		ts.reindexShardsLocked()
		ts.mu.Unlock()
		if reopenErr != nil {
			return zero, false, fmt.Errorf("graph: reopen shard %s after failed drop: %w (drop error: %v)", es.name, reopenErr, serr)
		}
		return zero, false, serr
	}
	ts.foldDroppedAccesses(es)
	if rerr := os.RemoveAll(dir); rerr != nil {
		return zero, false, rerr
	}

	return storecontract.RetentionPurgeResult{
		NodesPurged:   len(nodeIDs),
		RelsPurged:    relsSwept,
		PurgedNodeIDs: nodeIDs,
	}, true, nil
}

// sweepDroppedShardResidue removes, on each SURVIVING endpoint's shard, the
// residue a dropped shard's cross-shard edges leave (an endpoint in dropSet is
// gone with the directory), via PurgeRelationshipByInfo. Returns the count of
// distinct rels swept.
func (ts *Store) sweepDroppedShardResidue(rels []storecontract.PurgedRel, dropSet map[types.NodeID]struct{}) (int, error) {
	return ts.sweepRelResidue(rels, func(endpoint types.NodeID) bool {
		_, gone := dropSet[endpoint]
		return gone // this endpoint is on the dropped shard — nothing to clean there
	})
}

// sweepRelResidue is the residue-sweep body shared by sweepDroppedShardResidue
// and purgeNodesFanOut's Phase 2 (retention_purge.go) (BACKLOG 19m). It dedupes
// rels by ID (internal rels and self-loops would repeat per endpoint), then
// purges at each endpoint whose shard might hold residue. isKnownGone lets the
// fast-drop path skip GetNode for dropSet endpoints; the row-scan path passes
// func always false and relies on ErrNodeNotFound. Returns the unique rel
// count.
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
