package tiered

import (
	"errors"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.CurrentStampsCapability = (*Store)(nil)

// NodeCurrentStamps returns the stamps and version of nid's current row
// (store.CurrentStampsCapability) from the shard GetNode / LendNode read
// (shardForNodeIDChecked), without row decode or copy. Only that shard is
// asked: a stale copy left on another shard by a move is not folded in.
func (ts *Store) NodeCurrentStamps(nid types.NodeID) (storecontract.RowStamps, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return storecontract.RowStamps{}, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return storecontract.RowStamps{}, err
	}
	defer checkin()
	return store.NodeCurrentStamps(nid)
}

// RelCurrentStamps is NodeCurrentStamps for relationships, routed like
// GetRelationship (routeRelRow); the first shard holding the row answers.
func (ts *Store) RelCurrentStamps(rid types.RelID) (storecontract.RowStamps, error) {
	if err := ts.checkOpen(); err != nil {
		return storecontract.RowStamps{}, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return storecontract.RowStamps{}, err
	}
	move := ts.lockMoveRead() // route and read see the row on one shard
	defer move.unlock()
	return routeRelRow(ts, rid, relCurrentStampsRow)
}

// nodeCurrentStampsRow is nodeRowLive returning the row's stamps without
// decode or copy; absent when the index or entity row is gone.
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
// row's stamps; absent when the index or entity row is gone.
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
