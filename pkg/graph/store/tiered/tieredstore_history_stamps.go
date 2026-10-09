package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// shardStamps is one shard's (or the merged) history-stamps answer.
type shardStamps struct {
	from, to types.Instant
	has      bool
}

func mergeShardStamps(a, b shardStamps) shardStamps {
	from, to := storecontract.FoldTxStamps(a.from, a.to, b.from, b.to, 0)
	return shardStamps{from: from, to: to, has: a.has || b.has}
}

// NodeHistoryStamps returns the fold of GetNodeHistory(id)'s rows
// (store.HistoryStampsCapability): it walks exactly the shards
// GetNodeHistory reads (walkNodeHistoryShards, the HasNodeHistory walk) and
// folds each shard's answer. Open shards answer from their RAM sidecar; a
// cold shard is opened the way GetNodeHistory opens it and, opened with
// HistoryPresenceProbeOnly, reads the ID's rows instead of building one.
func (ts *Store) NodeHistoryStamps(nid types.NodeID) (types.Instant, types.Instant, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, 0, false, err
	}
	got, err := walkNodeHistoryShards(ts, nid, func(s *BadgerStore) (shardStamps, error) {
		f, t, has, err := s.NodeHistoryStamps(nid)
		return shardStamps{from: f, to: t, has: has}, err
	}, mergeShardStamps)
	if err != nil {
		return 0, 0, false, err
	}
	return got.from, got.to, got.has, nil
}

// RelHistoryStamps is NodeHistoryStamps for relationships, walking the shards
// GetRelHistory reads.
func (ts *Store) RelHistoryStamps(rid types.RelID) (types.Instant, types.Instant, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return 0, 0, false, err
	}
	got, err := walkRelHistoryShards(ts, rid, func(s *BadgerStore) (shardStamps, error) {
		f, t, has, err := s.RelHistoryStamps(rid)
		return shardStamps{from: f, to: t, has: has}, err
	}, mergeShardStamps)
	if err != nil {
		return 0, 0, false, err
	}
	return got.from, got.to, got.has, nil
}
