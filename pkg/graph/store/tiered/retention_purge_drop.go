package tiered

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldShardDrainSpinLimit bounds the drain wait (~5s at 1ms/spin) so a wedged
// request cannot stall a purge; on timeout the shard is re-linked and left to
// the row-scan path. Also bounds Close()'s drains
// (drainActiveReqsBounded).
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

// fastDropEligibleShards physically DROPS wholly-aged-out event shards that
// drop.collect finds droppable (ADR-0008 R4: one label's nodes; RHO-35: the
// purged types' relationships) instead of row-scanning them, for a ByAge purge with the
// change-log DISABLED (a drop would destroy the shard's 0x09 log segment and
// leave a replica LSN gap). Dropped shards leave ts.eventShards, so the
// row-scan fan-out skips them. Safety rests on the DRAIN PROTOCOL
// (dropOneShard): unlink from routing, drain in-flight requests, only then
// collect and sweep cross-shard residue and remove the directory. Returns the
// accumulated drop result.
func (ts *Store) fastDropEligibleShards(drop shardDrop, before types.Instant) (storecontract.RetentionPurgeResult, error) {
	var total storecontract.RetentionPurgeResult
	if ts.logEnabled || before <= 0 {
		return total, nil // change-log on (log-segment loss) → row-scan
	}

	// dropOneShard mutates ts.eventShards, which Close() ranges; like every
	// topology mutation it must exclude Close() via
	// beginSequentialStoreWideOperation/lifecycleMu.
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
		res, ok, err := ts.dropOneShard(es, drop)
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
// NOT dropped (drop.collect declined, or the drain timed out) — the caller row-scans it instead.
func (ts *Store) dropOneShard(es *EventShard, drop shardDrop) (storecontract.RetentionPurgeResult, bool, error) {
	var zero storecontract.RetentionPurgeResult

	// (1) Eligibility check (drop.collect), read-pinned; holds a checkout, so BEFORE the
	// drain-unlink.
	store, err := es.checkoutStore(ts)
	if err != nil {
		return zero, false, err
	}
	onlyLabel, _, _, err := drop.collect(store)
	es.checkinStore()
	if err != nil {
		return zero, false, err
	}
	if !onlyLabel {
		return zero, false, nil // not droppable — row-scan handles it
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
	// The name and directory stay reserved until the directory is gone: a
	// window shard made meanwhile (ADR-0013) takes another (makeWindowShardLocked).
	if ts.droppingShards == nil {
		ts.droppingShards = make(map[string]struct{})
	}
	ts.droppingShards[es.name] = struct{}{}
	ts.mu.Unlock()
	// One exit restores routing on every pre-commit failure. Once the catalog
	// removal commits, keep the name reserved until its directory is gone.
	committed, removed := false, false
	defer func() {
		ts.mu.Lock()
		defer ts.mu.Unlock()
		if !committed {
			ts.eventShards[es.name] = es
			ts.reindexShardsLocked()
		}
		if !committed || removed {
			delete(ts.droppingShards, es.name)
		}
	}()

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
		return zero, false, nil
	}

	// (4) Collect residue from the now-quiescent shard (all in-flight edges committed,
	// no new ones possible) and sweep the cross-shard part on survivor shards.
	store, err = es.checkoutStore(ts)
	if err != nil {
		return zero, false, err
	}
	stillOnlyLabel, nodeIDs, rels, cerr := drop.collect(store)
	es.checkinStore()
	if cerr != nil {
		return zero, false, cerr
	}
	// Re-verify eligibility: a write that began and
	// finished between step (1) and the unlink is invisible to the drain, so a
	// concurrent AddNodeLabelToken adding a FOREIGN label would go undetected.
	// This post-drain result is authoritative: re-link and fall back to row-scan.
	if !stillOnlyLabel {
		return zero, false, nil
	}
	relsSwept, serr := drop.sweep(nodeIDs, rels)
	if serr != nil {
		return zero, false, serr
	}

	// (5) Physically drop: close the store, then commit the catalog removal
	// BEFORE deleting the directory. A failure before the commit leaves directory
	// and catalog untouched (retryable); one after it leaves only an orphaned
	// directory, never a catalog entry for a missing directory.
	es.shardMu.Lock()
	// A handle swap (the sweep, a move) that began before the unlink reopens
	// the store outside shardMu: wait for it, so its handle is the one closed
	// here and not one installed after the directory is gone.
	es.waitMoveLocked()
	if es.store != nil {
		if closeErr := es.closeStoreLocked(ts, "drop: close shard"); closeErr != nil {
			// Detached and fenced; catalog entry and directory stay, so a
			// reopen reads the rows. A warm shard is cold in memory (its next
			// access opens it), and the shard is linked back after shardMu
			// is released (lock order: ts.mu is never taken under shardMu).
			es.coldInMemoryLocked()
			es.shardMu.Unlock()
			return zero, false, closeErr
		}
	}
	// The catalog entry leaves under the lock of the close: from here the
	// shard is owed to nobody, so no settle step of the handle sweep (queued
	// by that close, or earlier by a metered read's refusal) opens its
	// directory again.
	// The same Save lists it in Dropped, so a kill before its directory is
	// gone is finished at the next writable open (finishDrops).
	dropped := DroppedShard{Name: es.name, Volume: ts.volumeOf(es.path), Path: es.path} // before the entry goes: it names the shard's volume
	dir := ts.shardDir(es.path)
	snapshot := ts.catalog.snapshotShards()
	ts.catalog.dropShard(dropped)
	es.shardMu.Unlock()
	ts.dropStep("unlocked")

	if serr := ts.catalog.Save(); serr != nil {
		ts.catalog.restoreShards(snapshot)
		ts.catalog.forgetDropped(es.name)
		ts.dropStep("restored")
		// Directory intact and catalog rolled back: reopen (checkoutStore assumes
		// es.store is non-nil for non-cold shards) and re-link so the shard stays
		// reachable.
		es.shardMu.Lock()
		es.waitMoveLocked() // a settle step that began on the restored entry
		// The close above cleared the mark of a cold shard; this handle is
		// writable again, so the mark returns with it (saved by the next save
		// that succeeds), and what such a step sealed on the entry meanwhile
		// goes: the counts, and the no-rows mark of a shard it found empty,
		// which would keep the open shard out of every fan-out.
		es.dropCachedCounts(ts)
		es.noRows.Store(false)
		ts.catalog.SetShardRecover(es.name, true)
		store, reopenErr := ts.openShardStoreWithRecovery(es.path, es.currentTier() == TierCold, false)
		if reopenErr == nil {
			es.setStoreLocked(ts, store)
		} else {
			es.coldInMemoryLocked()
		}
		es.shardMu.Unlock()
		if reopenErr != nil {
			return zero, false, fmt.Errorf("graph: reopen shard %s after failed drop: %w (drop error: %v)", es.name, reopenErr, serr)
		}
		return zero, false, serr
	}
	committed = true
	ts.foldDroppedAccesses(es)
	if h := ts.dropRemoveHook; h != nil {
		h()
	}
	if rerr := os.RemoveAll(dir); rerr != nil {
		// The logical removal committed. Report it even if physical cleanup
		// failed; otherwise a retry can never report these purged IDs.
		slog.Error("tiered: dropped shard directory remains; name reserved", "shard", es.name, "dir", dir, "err", rerr)
	} else {
		removed = true
		ts.catalog.forgetDropped(es.name)
		if serr := ts.catalog.Save(); serr != nil {
			// Still listed: the next writable open finds the directory gone.
			slog.Warn("tiered: dropped shard still listed in the catalog", "shard", es.name, "err", serr)
		}
	}

	return storecontract.RetentionPurgeResult{
		NodesPurged:   len(nodeIDs),
		RelsPurged:    relsSwept,
		PurgedNodeIDs: nodeIDs,
	}, true, nil
}

// shardDrop is what one whole-shard drop decides and cleans: collect reports,
// on the shard, whether it is droppable and the node IDs and relationship
// descriptors that go with it; sweep removes their residue on the other
// shards and returns the distinct relationships removed. The node purge drops
// a shard of one label (nodeShardDrop), the relationship purge one of the
// purged types' relationships alone (relShardDrop).
type shardDrop struct {
	collect func(*BadgerStore) (ok bool, nodeIDs []types.NodeID, rels []storecontract.PurgedRel, err error)
	sweep   func(nodeIDs []types.NodeID, rels []storecontract.PurgedRel) (int, error)
}

// nodeShardDrop drops a shard holding only nodes of labelToken and their
// relationships; a cross-shard edge's residue is swept on its surviving
// endpoint's shard (an endpoint on the dropped shard is gone with it).
func (ts *Store) nodeShardDrop(labelToken uint16) shardDrop {
	return shardDrop{
		collect: func(s *BadgerStore) (bool, []types.NodeID, []storecontract.PurgedRel, error) {
			return s.CollectShardDropResidue(labelToken)
		},
		sweep: func(nodeIDs []types.NodeID, rels []storecontract.PurgedRel) (int, error) {
			dropSet := make(map[types.NodeID]struct{}, len(nodeIDs))
			for _, id := range nodeIDs {
				dropSet[id] = struct{}{}
			}
			return ts.sweepRelResidue(rels, func(endpoint types.NodeID) bool {
				_, gone := dropSet[endpoint]
				return gone
			})
		},
	}
}

// relShardDrop drops a shard holding only relationships of typeTokens minted
// before `before` (BadgerStore.CollectRelShardDropResidue); each one's legs
// elsewhere go as PurgeRelsByTypeBefore's phase 2 removes them.
func (ts *Store) relShardDrop(typeTokens []uint16, before types.Instant) shardDrop {
	return shardDrop{
		collect: func(s *BadgerStore) (bool, []types.NodeID, []storecontract.PurgedRel, error) {
			ok, rels, err := s.CollectRelShardDropResidue(typeTokens, before)
			return ok, nil, rels, err
		},
		sweep: func(_ []types.NodeID, rels []storecontract.PurgedRel) (int, error) {
			return ts.sweepRelLegResidue(rels)
		},
	}
}

// sweepRelLegResidue removes, on each endpoint's shard, the legs a purged
// relationship left where its row did not live (BadgerStore.PurgeRelLegResidue).
// Shared by PurgeRelsByTypeBefore's phase 2 and the relationship shard drop.
// Returns the distinct relationships named.
func (ts *Store) sweepRelLegResidue(rels []storecontract.PurgedRel) (int, error) {
	ids := make(map[types.RelID]struct{}, len(rels))
	for _, pr := range rels {
		ids[pr.ID] = struct{}{}
		for _, endpoint := range [2]types.NodeID{pr.StartID, pr.EndID} {
			shard, checkin, err := ts.shardForNodeIDChecked(endpoint)
			if err != nil {
				return len(ids), err
			}
			err = shard.PurgeRelLegResidue(pr)
			checkin()
			if err != nil {
				return len(ids), err
			}
		}
	}
	return len(ids), nil
}

// relLeg is one relationship and endpoint pair: a relationship whose history
// names other endpoints is listed once per pair, and each pair has residue.
type relLeg struct {
	id         types.RelID
	start, end types.NodeID
}

// sweepRelResidue is the residue-sweep body of the node shard drop and
// purgeNodesFanOut's Phase 2 (retention_purge.go). It dedupes rels by
// relationship and endpoint pair (internal rels and self-loops repeat per
// endpoint), then purges at each endpoint whose shard might hold residue.
// isKnownGone lets the fast-drop path skip GetNode for dropped endpoints; the
// row-scan path passes func always false and relies on ErrNodeNotFound.
// Returns the distinct relationship count.
func (ts *Store) sweepRelResidue(rels []storecontract.PurgedRel, isKnownGone func(types.NodeID) bool) (int, error) {
	seen := make(map[relLeg]struct{}, len(rels))
	ids := make(map[types.RelID]struct{}, len(rels))
	for _, pr := range rels {
		leg := relLeg{id: pr.ID, start: pr.StartID, end: pr.EndID}
		if _, ok := seen[leg]; ok {
			continue
		}
		seen[leg] = struct{}{}
		ids[pr.ID] = struct{}{}
		swept := len(ids)
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
	return len(ids), nil
}

// dropStep runs the test-only drop hook at step.
func (ts *Store) dropStep(step string) {
	if h := ts.dropHook; h != nil {
		h(step)
	}
}

// finishDrops removes, at a writable open, the directories of the shards a
// retention drop took out of the catalog before a kill or a failed removal
// (ShardCatalog.Dropped), and saves the catalog without them. A directory it
// cannot remove stays listed and its name stays reserved (droppingShards), so
// no window shard adopts it. It never fails the store: what it cannot finish
// it logs.
func (ts *Store) finishDrops() {
	dropped := ts.catalog.droppedShards()
	if len(dropped) == 0 {
		return
	}
	for _, d := range dropped {
		root, ok := ts.volumeDir(d.Volume)
		err := fmt.Errorf("volume %q not configured", d.Volume)
		if ok {
			err = os.RemoveAll(filepath.Join(root, d.Path))
		}
		if err != nil {
			slog.Error("tiered: dropped shard directory remains; name reserved", "shard", d.Name, "err", err)
			ts.mu.Lock()
			if ts.droppingShards == nil {
				ts.droppingShards = make(map[string]struct{})
			}
			ts.droppingShards[d.Name] = struct{}{}
			ts.mu.Unlock()
			continue
		}
		ts.catalog.forgetDropped(d.Name)
	}
	if err := ts.catalog.Save(); err != nil {
		slog.Warn("tiered: catalog not saved after finishing drops", "err", err)
	}
}
