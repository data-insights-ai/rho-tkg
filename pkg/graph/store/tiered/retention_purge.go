package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RetentionPurgeCapability          = (*Store)(nil)
	_ storecontract.RetentionPurgeByValidToCapability = (*Store)(nil)
	_ storecontract.RangePurgeLogCapability           = (*Store)(nil)
)

// PurgeNodesByLabelBefore hard-removes aged-out nodes of a label across every tiered
// shard (ADR-0008 R4, ByAge). See purgeNodesFanOut for the split-write mechanism.
func (ts *Store) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.RetentionPurgeResult{}, err
	}
	// Fast path (ADR-0008 R4 optimization): physically DROP wholly-aged-out
	// single-label event shards instead of row-scanning them (see fastDropEligibleShards
	// — ByAge + change-log-off only). Runs on the first chunked call; subsequent calls
	// find those shards already gone and the row scan drains the rest.
	dropResult, err := ts.fastDropEligibleShards(labelToken, before)
	if err != nil {
		return storecontract.RetentionPurgeResult{}, err
	}
	scanResult, err := ts.purgeNodesFanOut(func(shard *BadgerStore) (storecontract.RetentionPurgeResult, error) {
		return shard.PurgeNodesByLabelBefore(labelToken, before, chunk)
	})
	if err != nil {
		return scanResult, err
	}
	scanResult.NodesPurged += dropResult.NodesPurged
	scanResult.RelsPurged += dropResult.RelsPurged
	scanResult.PurgedNodeIDs = append(scanResult.PurgedNodeIDs, dropResult.PurgedNodeIDs...)
	return scanResult, nil
}

// PurgeNodesByLabelValidToBefore hard-removes nodes whose world-time validity ended
// before the boundary across every tiered shard (ADR-0008 R5, ByValidTo). Same
// split-write cross-shard mechanism as ByAge (see purgeNodesFanOut) — only the
// per-shard predicate differs; each shard applies its own mutable-predicate
// re-confirm under its write lock.
func (ts *Store) PurgeNodesByLabelValidToBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	return ts.purgeNodesFanOut(func(shard *BadgerStore) (storecontract.RetentionPurgeResult, error) {
		return shard.PurgeNodesByLabelValidToBefore(labelToken, before, chunk)
	})
}

// purgeNodesFanOut is the shared cross-shard purge (ADR-0008 R4). Tiered uses a
// SPLIT-WRITE cross-shard adjacency layout — a rel's entity + out-leg live on the
// START node's shard, its in-leg on the END node's shard — so unlike sharded
// (co-located legs) a per-shard label purge leaves a residue on a survivor's shard
// for any cross-shard edge. Two phases:
//
//	(1) Fan out `perShard` over ref + archive + every event shard (forEachOpenShard).
//	    Each shard removes its qualifying nodes, their CO-LOCATED rel parts + history,
//	    and reports the rels it TOUCHED.
//	(2) For each touched rel, clean its residue on the SURVIVING endpoint's shard.
//	    The residue always lands there (a purged endpoint's shard self-cleaned in
//	    phase 1): a survivor→purged rel leaves its entity+out-leg on the survivor's
//	    shard (a full-local delete), a purged→survivor rel leaves an orphan in-leg
//	    (an orphan-index purge). PurgeRelationshipByInfo dispatches on which.
//
// Each per-shard step is atomic; the whole is crash-recoverable-not-atomic (a
// re-run finishes; the graph watermark makes any residue invisible). Recordless —
// the graph layer owns the single ChangeRangePurge (see LogRangePurge).
func (ts *Store) purgeNodesFanOut(perShard func(shard *BadgerStore) (storecontract.RetentionPurgeResult, error)) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := ts.checkOpen(); err != nil {
		return zero, err
	}

	total := storecontract.RetentionPurgeResult{}
	var purgedIDs []types.NodeID
	var purgedRels []storecontract.PurgedRel
	perr := ts.forEachOpenShard(func(shard *BadgerStore) error {
		res, e := perShard(shard)
		if e != nil {
			return e
		}
		total.NodesPurged += res.NodesPurged
		total.RelsPurged += res.RelsPurged
		if res.More {
			total.More = true
		}
		purgedIDs = append(purgedIDs, res.PurgedNodeIDs...)
		purgedRels = append(purgedRels, res.PurgedRels...)
		return nil
	})
	if perr != nil {
		return total, perr
	}

	// Phase 2: cross-shard rel-residue sweep on the SURVIVING endpoint's shard
	// (BACKLOG 19m: shared body with sweepDroppedShardResidue via
	// sweepRelResidue, retention_purge_drop.go). No precomputed "known gone"
	// set here — a purged endpoint's shard already cleaned itself in phase 1,
	// so GetNode's ErrNodeNotFound alone is what detects it.
	if _, err := ts.sweepRelResidue(purgedRels, func(types.NodeID) bool { return false }); err != nil {
		return total, err
	}

	total.PurgedNodeIDs = purgedIDs
	return total, nil
}

// LogRangePurge appends ONE store-global ChangeRangePurge record on the reference
// shard (ADR-0008 R4/R3). The refShard shares the store-global LSN allocator and
// is part of the merged feed, so the record lands in the total order exactly like
// the sharded backend's anchor-shard emit. No-op when the change-log is disabled.
func (ts *Store) LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ref, release, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	defer release()
	return ref.LogRangePurge(labelToken, before, mode)
}
