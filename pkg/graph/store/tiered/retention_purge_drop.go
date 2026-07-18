package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldShardDrainSpinLimit bounds the drain wait so a wedged in-flight request cannot
// stall a purge forever. On timeout the shard is RE-LINKED and left to the row-scan
// path (safe — the fast-drop is a pure optimization). ~5s at 1ms/spin.
const coldShardDrainSpinLimit = 5000

// fastDropEligibleShards physically DROPS wholly-aged-out single-label event shards
// (ADR-0008 R4 optimization) instead of row-scanning them, for a ByAge purge with the
// change-log DISABLED. It returns the accumulated result and the set of dropped shard
// names, so the caller's row-scan fan-out skips them.
//
// A drop replaces the per-row cascade + flush with one directory removal. Correctness
// under concurrency rests on a DRAIN PROTOCOL (dropOneShard): a shard is unlinked from
// routing, its in-flight requests drained, and only THEN is its cross-shard residue
// collected + swept and the directory removed — so no concurrent write can leave
// un-swept residue (a new edge to a purged-window node fails endpoint validation once
// the shard is unlinked; an in-flight one is captured by the drain). Disabled under
// change-log (dropping a shard would destroy its 0x09 log segment → replica LSN gap).
func (ts *Store) fastDropEligibleShards(labelToken uint16, before types.Instant) (storecontract.RetentionPurgeResult, map[string]struct{}, error) {
	var total storecontract.RetentionPurgeResult
	dropped := make(map[string]struct{})
	if ts.logEnabled || before <= 0 {
		return total, dropped, nil // change-log on (log-segment loss) → row-scan
	}
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
			return total, dropped, err
		}
		if ok {
			total.NodesPurged += res.NodesPurged
			total.RelsPurged += res.RelsPurged
			total.PurgedNodeIDs = append(total.PurgedNodeIDs, res.PurgedNodeIDs...)
			dropped[es.name] = struct{}{}
		}
	}
	return total, dropped, nil
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
	_, nodeIDs, rels, cerr := store.CollectShardDropResidue(labelToken)
	es.checkinStore()
	if cerr != nil {
		return zero, false, cerr
	}
	dropSet := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		dropSet[id] = struct{}{}
	}
	relsSwept, serr := ts.sweepDroppedShardResidue(rels, dropSet)
	if serr != nil {
		return zero, false, serr
	}

	// (5) Physically drop: close the store, remove the directory, remove the catalog
	// entry, persist. The shard is unreachable + quiescent, so this is unobserved.
	es.shardMu.Lock()
	if es.store != nil {
		if closeErr := es.store.Close(); closeErr != nil {
			es.shardMu.Unlock()
			return zero, false, closeErr
		}
		es.store = nil
	}
	es.shardMu.Unlock()
	if rerr := os.RemoveAll(filepath.Join(ts.dataDir, es.path)); rerr != nil {
		return zero, false, rerr
	}
	ts.catalog.RemoveShard(es.name)
	if serr := ts.catalog.Save(); serr != nil {
		return zero, false, serr
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
	seen := make(map[types.RelID]struct{}, len(rels))
	swept := 0
	for _, pr := range rels {
		if _, ok := seen[pr.ID]; ok {
			continue
		}
		seen[pr.ID] = struct{}{}
		swept++
		for _, endpoint := range [2]types.NodeID{pr.StartID, pr.EndID} {
			if _, gone := dropSet[endpoint]; gone {
				continue // this endpoint is on the dropped shard — nothing to clean there
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
