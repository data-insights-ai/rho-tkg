package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CurrentStampsCapability = (*Store)(nil)

// NodeCurrentStamps returns the stamps and version of nid's current row
// (store.CurrentStampsCapability) from the shard GetNode / LendNode read
// (shardForNodeIDChecked: the reference shard, the archive, else the ID's
// event shard, a cold one opened for the read), through that shard's
// capability: no row decode on a cache miss, no copy. Only that one shard is
// asked: a stale copy of the row on another shard (left by an archive or a
// move) is not folded in, exactly as the lend door does not read it.
func (ts *Store) NodeCurrentStamps(nid types.NodeID) (storecontract.RowStamps, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return storecontract.RowStamps{}, err
	}
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	defer checkin()
	return store.NodeCurrentStamps(nid)
}

// RelCurrentStamps is NodeCurrentStamps for relationships, routed as
// GetRelationship / LendRelationship route (routeRelRow: reference shard,
// archive, the ID's event shard, then every other); the first shard holding
// the row answers.
func (ts *Store) RelCurrentStamps(rid types.RelID) (storecontract.RowStamps, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return storecontract.RowStamps{}, err
	}
	return routeRelRow(ts, rid, relCurrentStampsRow)
}

// nodeCurrentStampsRow is nodeRowLive returning the row's stamps, read
// without decoding or copying the row: absent when the shard's index does not
// hold the ID or its entity row is gone.
func nodeCurrentStampsRow(store *BadgerStore, id types.NodeID) (storecontract.RowStamps, bool, error) {
	if store == nil || !store.HasNodeID(id.SnowflakeID()) {
		return storecontract.RowStamps{}, false, nil
	}
	s, err := store.NodeCurrentStamps(id)
	if err != nil {
		if errors.Is(err, ErrNodeNotFound) {
			return storecontract.RowStamps{}, false, nil
		}
		return storecontract.RowStamps{}, false, err
	}
	return s, true, nil
}

// relCurrentStampsRow is relationshipRow / lendRelationshipRow returning the
// row's stamps: absent when the shard's index does not hold the ID or its
// entity row is gone.
func relCurrentStampsRow(store *BadgerStore, id types.RelID) (storecontract.RowStamps, bool, error) {
	if store == nil || !store.HasRelID(id.SnowflakeID()) {
		return storecontract.RowStamps{}, false, nil
	}
	s, err := store.RelCurrentStamps(id)
	if err != nil {
		if errors.Is(err, ErrRelNotFound) {
			return storecontract.RowStamps{}, false, nil
		}
		return storecontract.RowStamps{}, false, err
	}
	return s, true, nil
}
