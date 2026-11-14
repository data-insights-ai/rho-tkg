package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

var _ storecontract.HistoryEpochMeteredCapability = (*Store)(nil)

// NodeHistoryEpoch returns the largest of the tokens of the shards
// GetNodeHistory reads for nid (walkNodeHistoryShards) and the store's
// shard-set token (histTopoEpoch). Every token comes from one increasing
// process-wide counter (storeutil.NextHistoryEpoch), so any change hands out
// a token above every earlier one and the largest moves: a history change on a
// walked shard (its token), a shard reopened (a new store: fresh tokens), a
// shard joining or leaving the walk through the shard set (rotation, a
// retention drop, Clear: the shard-set token, drawn under ts.mu before the new
// set is visible). Archive / restore move rows between walked shards with
// history writes on both.
func (ts *Store) NodeHistoryEpoch(nid types.NodeID) (uint64, error) {
	return ts.nodeHistoryEpochCharged(nid, nil)
}

// NodeHistoryEpochMetered is NodeHistoryEpoch charging each closed shard's
// open to t before it (store.HistoryEpochMeteredCapability; nil: none; R2-5
// HEADREAD).
func (ts *Store) NodeHistoryEpochMetered(nid types.NodeID, t *storecontract.ReadTally) (uint64, error) {
	return ts.nodeHistoryEpochCharged(nid, t)
}

// nodeHistoryEpochCharged is NodeHistoryEpoch charging each closed shard's
// open to opens before it (nil: none): the walk's own routing charges it, as
// for HasNodeHistoryMetered.
func (ts *Store) nodeHistoryEpochCharged(nid types.NodeID, opens *storecontract.ReadTally) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	topo := ts.histTopoEpoch.Load()
	e, err := walkNodeHistoryShards(ts, nid, opens, func(s *BadgerStore) (uint64, error) { return s.NodeHistoryEpoch(nid) }, maxEpoch)
	return max(topo, e), err
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (ts *Store) RelHistoryEpoch(rid types.RelID) (uint64, error) {
	return ts.relHistoryEpochCharged(rid, nil)
}

// RelHistoryEpochMetered is RelHistoryEpoch charging each closed shard's open
// to t before it (store.HistoryEpochMeteredCapability; nil: none).
func (ts *Store) RelHistoryEpochMetered(rid types.RelID, t *storecontract.ReadTally) (uint64, error) {
	return ts.relHistoryEpochCharged(rid, t)
}

// relHistoryEpochCharged is nodeHistoryEpochCharged for relationships.
func (ts *Store) relHistoryEpochCharged(rid types.RelID, opens *storecontract.ReadTally) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return 0, err
	}
	topo := ts.histTopoEpoch.Load()
	e, err := walkRelHistoryShards(ts, rid, opens, func(s *BadgerStore) (uint64, error) { return s.RelHistoryEpoch(rid) }, maxEpoch)
	return max(topo, e), err
}

func maxEpoch(a, b uint64) uint64 { return max(a, b) }
