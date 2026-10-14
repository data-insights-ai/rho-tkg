package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

// NodeHistoryEpoch returns the largest token of the shards GetNodeHistory reads
// for nid (walkNodeHistoryShards). Tokens come from one increasing
// process-wide counter, so a change on any of those shards hands out a token
// above every earlier one; a shard leaving the walk leaves the rows of the
// others, unchanged since their tokens (store.HistoryEpochCapability).
func (ts *Store) NodeHistoryEpoch(nid types.NodeID) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	return walkNodeHistoryShards(ts, nid, func(s *BadgerStore) (uint64, error) { return s.NodeHistoryEpoch(nid) }, maxEpoch)
}

// RelHistoryEpoch is NodeHistoryEpoch for relationships.
func (ts *Store) RelHistoryEpoch(rid types.RelID) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return 0, err
	}
	return walkRelHistoryShards(ts, rid, func(s *BadgerStore) (uint64, error) { return s.RelHistoryEpoch(rid) }, maxEpoch)
}

func maxEpoch(a, b uint64) uint64 { return max(a, b) }
