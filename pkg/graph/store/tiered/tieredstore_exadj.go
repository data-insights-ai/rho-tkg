package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.ExAdjacencyCapability = (*Store)(nil)

// Ex-adjacency on tiered (store.ExAdjacencyCapability, backlog 50). A node's
// adjacency lives on the shard that holds the node: the out-leg of a
// relationship on its start node's shard (with the entity), the in-leg on its
// end node's shard. The shard that drops a leg of a deleted relationship
// (deleteRelByInfo, DeleteRelEntityAndOut, DeleteRelIncoming) records its
// ex-adjacency there — a move between shards (archive, restore) or a create
// rollback deletes nothing and records none (Remove*Placement) — and
// a history row with other endpoints than the live row records each leg on its
// endpoint's shard (noteHistoryRowLegs). A node may have left the shard it was
// on (deleted, archived), so a read unions every shard the node can have
// lived on: the reference shard, the archive, and the event shard of its
// mint time.

// exAdjShards returns the shards nid's adjacency can have lived on, pinned;
// release checks them in.
func (ts *Store) exAdjShards(nid types.NodeID) (shards []*BadgerStore, release func(), err error) {
	var checkins []func()
	release = func() {
		for _, c := range checkins {
			c()
		}
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, release, err
	}
	shards, checkins = append(shards, ref), append(checkins, refCheckin)
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, release, err
	}
	checkins = append(checkins, archiveCheckin)
	if archive != nil {
		shards = append(shards, archive)
	}
	event, eventCheckin, err := ts.timestampToEventShardEntry(nid.SnowflakeID()).checkoutStoreForRead(ts)
	if err != nil {
		return nil, release, err
	}
	shards, checkins = append(shards, event), append(checkins, eventCheckin)
	return shards, release, nil
}

// ForEachExAdjacentRelID implements store.ExAdjacencyCapability.
func (ts *Store) ForEachExAdjacentRelID(nid types.NodeID, incoming bool, fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	ids, err := ts.exAdjRelIDs(nid, incoming)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// exAdjRelIDs collects nid's ex-adjacency from every shard it can have lived
// on (the shards are checked in before the caller's callback runs).
func (ts *Store) exAdjRelIDs(nid types.NodeID, incoming bool) ([]types.RelID, error) {
	shards, release, err := ts.exAdjShards(nid)
	defer release()
	if err != nil {
		return nil, err
	}
	var ids []types.RelID
	for _, s := range shards {
		if err := s.ForEachExAdjacentRelID(nid, incoming, func(id types.RelID) bool { ids = append(ids, id); return true }); err != nil {
			return nil, err
		}
	}
	return ids, nil
}

// NoteExAdjacentRel implements store.ExAdjacencyCapability: each leg on its
// endpoint's shard.
func (ts *Store) NoteExAdjacentRel(id types.RelID, start, end types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipIndexEntry(start, end, 1, id); err != nil {
		return err
	}
	for _, leg := range []struct {
		node     types.NodeID
		incoming bool
	}{{start, false}, {end, true}} {
		shard, checkin, err := ts.shardForNodeIDChecked(leg.node)
		if err != nil {
			return err
		}
		err = shard.NoteExAdjacentLeg(leg.node, leg.incoming, id)
		checkin()
		if err != nil {
			return err
		}
	}
	return nil
}

// noteHistoryRowLegs records the legs of history row r of rid on its endpoints'
// shards unless the live row names the same endpoints (the rel's own shard has
// already recorded them locally when they differ; this covers an endpoint on
// another shard).
func (ts *Store) noteHistoryRowLegs(rid types.RelID, r *types.Relationship) error {
	cur, err := ts.GetRelationship(rid)
	switch {
	case err == nil && cur.StartNodeID() == r.StartNodeID() && cur.EndNodeID() == r.EndNodeID():
		return nil
	case err != nil && !errors.Is(err, storecontract.ErrRelNotFound):
		return err
	}
	return ts.NoteExAdjacentRel(rid, r.StartNodeID(), r.EndNodeID())
}
