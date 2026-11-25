package tiered

import (
	"slices"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var (
	_ storecontract.RelRetentionPurgeCapability = (*Store)(nil)
	_ storecontract.RelRangePurgeLogCapability  = (*Store)(nil)
	_ storecontract.RelShardDropCapability      = (*Store)(nil)
)

// PurgeRelsByTypeBefore hard-removes aged-out relationships of a type across
// every tiered shard. Two phases:
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
	if err := ts.checkWritable(); err != nil {
		return storecontract.RetentionPurgeResult{}, err
	}
	var zero storecontract.RetentionPurgeResult
	if err := ts.checkOpen(); err != nil {
		return zero, err
	}
	total := storecontract.RetentionPurgeResult{}
	if err := ts.forEachShard(func(shard *BadgerStore) error {
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
	if _, err := ts.sweepRelLegResidue(total.PurgedRels); err != nil {
		return total, err
	}
	return total, nil
}

// DropRelShardsBefore is store.RelShardDropCapability: it drops whole every
// rotated event shard whose window ends at or before `before` and that holds
// only relationships of typeTokens minted before it (Config.RelsByMint puts
// such shards there: entity nodes stay on the reference shard), by the drop
// protocol of the node purge (dropOneShard), and sweeps their legs on the
// endpoints' shards. The hot shard, a shard holding any node or another
// type, and every shard while the change-log is on are left to
// PurgeRelsByTypeBefore. A read-only store refuses with ErrReadOnly.
func (ts *Store) DropRelShardsBefore(typeTokens []uint16, before types.Instant) (storecontract.RetentionPurgeResult, error) {
	if err := ts.checkWritable(); err != nil {
		return storecontract.RetentionPurgeResult{}, err
	}
	if err := ts.checkOpen(); err != nil {
		return storecontract.RetentionPurgeResult{}, err
	}
	if len(typeTokens) == 0 {
		return storecontract.RetentionPurgeResult{}, nil
	}
	return ts.fastDropEligibleShards(ts.relShardDrop(slices.Clone(typeTokens), before), before)
}

// LogRelRangePurge appends ONE store-global ChangeRelRangePurge record on the
// reference shard, as LogRangePurge does for the node purge.
func (ts *Store) LogRelRangePurge(typeToken uint16, before types.Instant, mode uint8) error {
	if err := ts.checkWritable(); err != nil {
		return err
	}
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
