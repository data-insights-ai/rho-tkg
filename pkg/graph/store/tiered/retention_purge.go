package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RangePurgeLogCapability  = (*Store)(nil)
)

// PurgeNodesByLabelBefore hard-removes aged-out nodes of a label across every
// tiered shard (ADR-0008 R4). Tiered uses a SPLIT-WRITE cross-shard adjacency
// layout — a rel's entity + out-leg live on the START node's shard, its in-leg on
// the END node's shard — so unlike sharded (co-located legs) a per-shard label
// purge leaves a residue on a survivor's shard for any cross-shard edge. Two phases:
//
//	(1) Fan out the per-shard label purge over ref + archive + every event shard
//	    (forEachOpenShard). Each shard removes its below-boundary nodes, their
//	    CO-LOCATED rel parts + history, and reports the rels it TOUCHED.
//	(2) For each touched rel, clean its residue on the SURVIVING endpoint's shard.
//	    The residue always lands there (a purged endpoint's shard self-cleaned in
//	    phase 1): a survivor→purged rel leaves its entity+out-leg on the survivor's
//	    shard (a full-local delete), a purged→survivor rel leaves an orphan in-leg
//	    (an orphan-index purge). PurgeRelationshipByInfo dispatches on which.
//
// Each per-shard step is atomic; the whole is crash-recoverable-not-atomic (a
// re-run finishes; the graph watermark makes any residue invisible). Recordless —
// the graph layer owns the single ChangeRangePurge (see LogRangePurge).
func (ts *Store) PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := ts.checkOpen(); err != nil {
		return zero, err
	}

	total := storecontract.RetentionPurgeResult{}
	var purgedIDs []types.NodeID
	var purgedRels []storecontract.PurgedRel
	perr := ts.forEachOpenShard(func(shard *BadgerStore) error {
		res, e := shard.PurgeNodesByLabelBefore(labelToken, before, chunk)
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

	// Phase 2: cross-shard rel-residue sweep on the SURVIVING endpoint's shard.
	seen := make(map[types.RelID]struct{}, len(purgedRels))
	for _, pr := range purgedRels {
		if _, ok := seen[pr.ID]; ok {
			continue // a rel touched from both endpoints appears twice
		}
		seen[pr.ID] = struct{}{}
		for _, endpoint := range [2]types.NodeID{pr.StartID, pr.EndID} {
			// Only a SURVIVING endpoint's shard can hold residue — a purged
			// endpoint's shard already cleaned itself in phase 1. Skipping the
			// purged endpoint also avoids a wasted orphan scan on its shard.
			if _, gerr := ts.GetNode(endpoint); gerr != nil {
				if errors.Is(gerr, ErrNodeNotFound) {
					continue
				}
				return total, gerr
			}
			shard, checkin, serr := ts.shardForNodeIDChecked(endpoint)
			if serr != nil {
				return total, serr
			}
			e := shard.PurgeRelationshipByInfo(pr)
			checkin()
			if e != nil {
				return total, e
			}
		}
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
