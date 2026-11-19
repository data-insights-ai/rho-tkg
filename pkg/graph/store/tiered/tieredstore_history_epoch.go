package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryEpochCapability = (*Store)(nil)

// NodeHistoryEpoch returns the largest of the tokens of the shards
// GetNodeHistory reads for nid (walkNodeHistoryShards) and, when that walk
// reads an event shard, the store's shard-set token (histTopoEpoch). Every
// token comes from one increasing process-wide counter
// (storeutil.NextHistoryEpoch), so any change hands out a token above every
// earlier one and the largest moves: a history change on a walked shard (its
// token), a shard reopened (a new store: fresh tokens), a shard joining or
// leaving the walk through the shard set (rotation, a retention drop, Clear:
// the shard-set token, drawn under ts.mu before the new set is visible).
// Archive / restore move rows between walked shards with history writes on
// both.
//
// A walk that reads only the reference shard and the archive (an entity
// living on the reference shard) cannot gain or lose a shard through the
// event shards' set, so the shard-set token is left out of its answer
// (CUTCOST: every rotation moved the token of every reference entity, and the
// next read of each folded its whole chain). A Clear drops the reference
// shard's own tokens.
//
// It never opens a shard (routeIfOpen; R2-5 HEADREAD): a token is good only
// while the handle that issued it stays open, so when the walk reaches a
// closed event shard or an archive not yet opened no kept token can match and
// the answer is 0, no token (store.HistoryEpochCapability).
func (ts *Store) NodeHistoryEpoch(nid types.NodeID) (uint64, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return 0, err
	}
	topo := ts.histTopoEpoch.Load()
	event := false
	e, err := walkNodeHistoryShardsMode(ts, nid, routeIfOpen, nil, func(s *BadgerStore) (uint64, error) {
		event = event || !ts.referenceSide(s)
		return s.NodeHistoryEpoch(nid)
	}, maxEpoch)
	return openEpoch(topo, event, e, err)
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
	event := false
	e, err := walkRelHistoryShardsMode(ts, rid, routeIfOpen, nil, func(s *BadgerStore) (uint64, error) {
		event = event || !ts.referenceSide(s)
		return s.RelHistoryEpoch(rid)
	}, maxEpoch)
	return openEpoch(topo, event, e, err)
}

// referenceSide reports whether s is the reference shard or the archive: the
// shards the event shards' set never adds to or takes from a walk.
func (ts *Store) referenceSide(s *BadgerStore) bool {
	return s == ts.refShard || s == ts.refArchive.Load()
}

// openEpoch is the token of a routeIfOpen walk: 0 when it reached a shard it
// would have had to open; the walk's own token when it read no event shard
// (event), else the larger of it and the shard-set token.
func openEpoch(topo uint64, event bool, e uint64, err error) (uint64, error) {
	if errors.Is(err, errShardNotOpen) {
		return 0, nil
	}
	if !event {
		return e, err
	}
	return max(topo, e), err
}

func maxEpoch(a, b uint64) uint64 { return max(a, b) }
