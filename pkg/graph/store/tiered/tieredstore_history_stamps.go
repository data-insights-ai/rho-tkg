package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryStampsCapability = (*Store)(nil)

// shardStamps is one shard's (or the merged) history-stamps answer; shards
// counts the shards that hold a history row of the ID.
type shardStamps struct {
	from, to types.Instant
	shards   int
}

func mergeShardStamps(a, b shardStamps) shardStamps {
	from, to := storecontract.FoldTxStamps(a.from, a.to, b.from, b.to, 0)
	return shardStamps{from: from, to: to, shards: a.shards + b.shards}
}

func shardAnswer(f, t types.Instant, has bool, err error) (shardStamps, error) {
	if !has {
		return shardStamps{}, err
	}
	return shardStamps{from: f, to: t, shards: 1}, err
}

// NodeHistoryStamps returns the fold of GetNodeHistory(id)'s rows
// (store.HistoryStampsCapability). It walks exactly the shards GetNodeHistory
// reads (walkNodeHistoryShards, the HasNodeHistory walk) and asks each shard's
// sidecar; a cold shard is opened the way GetNodeHistory opens it and, opened
// with HistoryPresenceProbeOnly, reads the ID's rows instead of building one.
// When one shard holds the ID's history its answer is the fold. When two or
// more do, GetNodeHistory keeps ONE copy per version (the first source's,
// mergeNodeHistorySources) and a fold of the shards' answers would count the
// dropped copies (a version reused across shards: old collided chains,
// backlog 26), so the rows are read through GetNodeHistory and folded — the
// same rows, exact on every shape, paid only by entities whose history spans
// shards.
func (ts *Store) NodeHistoryStamps(nid types.NodeID) (types.Instant, types.Instant, bool, error) {
	return ts.NodeHistoryStampsMetered(nid, nil)
}

// NodeHistoryStampsMetered is NodeHistoryStamps charging each closed shard's
// open to t before it, the whole-chain fold included (through a metered read
// snapshot, which counts its decodes too; store.NodeStampsMeteredCapability;
// nil: none).
func (ts *Store) NodeHistoryStampsMetered(nid types.NodeID, t *storecontract.ReadTally) (types.Instant, types.Instant, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, 0, false, err
	}
	got, err := walkNodeHistoryShards(ts, nid, t, func(s *BadgerStore) (shardStamps, error) {
		return shardAnswer(s.NodeHistoryStamps(nid))
	}, mergeShardStamps)
	if err != nil {
		return 0, 0, false, err
	}
	if got.shards <= 1 {
		return got.from, got.to, got.shards == 1, nil
	}
	var rows []*types.Node
	if t == nil {
		rows, err = ts.GetNodeHistory(nid)
	} else {
		snap := ts.NewReadSnapshot()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(t)
		rows, err = snap.GetNodeHistory(nid)
		snap.Close()
	}
	if err != nil {
		return 0, 0, false, err
	}
	var from, to types.Instant
	for _, r := range rows {
		f, t, d := r.TxStamps()
		from, to = storecontract.FoldTxStamps(from, to, f, t, d)
	}
	return from, to, len(rows) > 0, nil
}

// RelHistoryStamps is NodeHistoryStamps for relationships, walking the shards
// GetRelHistory reads and folding GetRelHistory when two or more hold rows.
func (ts *Store) RelHistoryStamps(rid types.RelID) (types.Instant, types.Instant, bool, error) {
	return ts.RelHistoryStampsMetered(rid, nil)
}

// RelHistoryStampsMetered is RelHistoryStamps charging each closed shard's open
// to t before it, the whole-chain fold included (through a metered read
// snapshot, which counts its decodes too; store.RelStampsMeteredCapability;
// nil: none).
func (ts *Store) RelHistoryStampsMetered(rid types.RelID, t *storecontract.ReadTally) (types.Instant, types.Instant, bool, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, 0, false, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return 0, 0, false, err
	}
	got, err := walkRelHistoryShards(ts, rid, t, func(s *BadgerStore) (shardStamps, error) {
		return shardAnswer(s.RelHistoryStamps(rid))
	}, mergeShardStamps)
	if err != nil {
		return 0, 0, false, err
	}
	if got.shards <= 1 {
		return got.from, got.to, got.shards == 1, nil
	}
	var rows []*types.Relationship
	if t == nil {
		rows, err = ts.GetRelHistory(rid)
	} else {
		snap := ts.NewReadSnapshot()
		snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(t)
		rows, err = snap.GetRelHistory(rid)
		snap.Close()
	}
	if err != nil {
		return 0, 0, false, err
	}
	var from, to types.Instant
	for _, r := range rows {
		f, t, d := r.TxStamps()
		from, to = storecontract.FoldTxStamps(from, to, f, t, d)
	}
	return from, to, len(rows) > 0, nil
}
