package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelRetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RelRangePurgeLogCapability  = (*Store)(nil)
)

// PurgeRelsByTypeBefore hard-removes aged-out relationships of a type across
// every tiered shard (backlog 21). Two phases:
//
//	(1) Each shard (ref, archive, every event shard, cold ones opened for the
//	    call) purges the relationships whose row or history it holds: row,
//	    type key, both local legs, history, ex-adjacency legs, sidecars.
//	(2) The split-write layout keeps a cross-shard relationship's in-leg on
//	    its END node's shard (and a delete's ex-adjacency legs on either
//	    endpoint's shard), where phase 1 does not look: for every purged
//	    relationship, each endpoint's shard drops those legs
//	    (PurgeRelLegResidue, a no-op where the row still lives).
//
// Each per-shard step is atomic; the whole is crash-recoverable-not-atomic (a
// rerun finishes; the per-type watermark makes any residue unreadable).
// Recordless: the graph layer owns the single ChangeRelRangePurge.
func (ts *Store) PurgeRelsByTypeBefore(typeToken uint16, before types.Instant, chunk int) (storecontract.RetentionPurgeResult, error) {
	var zero storecontract.RetentionPurgeResult
	if err := ts.checkOpen(); err != nil {
		return zero, err
	}
	total := storecontract.RetentionPurgeResult{}
	if err := ts.forEachOpenShard(func(shard *BadgerStore) error {
		res, e := shard.PurgeRelsByTypeBefore(typeToken, before, chunk)
		if e != nil {
			return e
		}
		total.RelsPurged += res.RelsPurged
		total.More = total.More || res.More
		total.PurgedRels = append(total.PurgedRels, res.PurgedRels...)
		return nil
	}); err != nil {
		return total, err
	}
	for _, pr := range total.PurgedRels {
		for _, endpoint := range [2]types.NodeID{pr.StartID, pr.EndID} {
			shard, checkin, err := ts.shardForNodeIDChecked(endpoint)
			if err != nil {
				return total, err
			}
			err = shard.PurgeRelLegResidue(pr)
			checkin()
			if err != nil {
				return total, err
			}
		}
	}
	return total, nil
}

// LogRelRangePurge appends ONE store-global ChangeRelRangePurge record on the
// reference shard (backlog 21), as LogRangePurge does for the node purge.
func (ts *Store) LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ref, release, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	defer release()
	return ref.LogRelRangePurge(typeToken, before, mode)
}
