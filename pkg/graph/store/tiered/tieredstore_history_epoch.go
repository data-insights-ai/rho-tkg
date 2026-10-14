package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

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
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	topo := ts.histTopoEpoch.Load()
	e, err := walkNodeHistoryShards(ts, nid, func(s *BadgerStore) (uint64, error) { return s.NodeHistoryEpoch(nid) }, maxEpoch)
	return max(topo, e), err
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (ts *Store) RelHistoryEpoch(rid types.RelID) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return 0, err
	}
	topo := ts.histTopoEpoch.Load()
	e, err := walkRelHistoryShards(ts, rid, func(s *BadgerStore) (uint64, error) { return s.RelHistoryEpoch(rid) }, maxEpoch)
	return max(topo, e), err
}

func maxEpoch(a, b uint64) uint64 { return max(a, b) }
