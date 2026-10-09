package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// HasNodeHistory reports whether GetNodeHistory(id) would return a row
// (store.HistoryPresenceCapability). It walks exactly the shards
// GetNodeHistory reads, in the same order and with the same errors, and asks
// each one HasNodeHistory instead of reading its rows: the owning shard; for a
// node live on the reference shard also the archive; for an archived node also
// the reference shard; for a node live nowhere every history shard. Cold shards
// are opened the way GetNodeHistory opens them (checkoutStoreForRead) — no new
// open path — and answer by a per-ID key probe instead of building their RAM
// set (a cold shard opens with badger Config.HistoryPresenceProbeOnly).
func (ts *Store) HasNodeHistory(nid types.NodeID) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return false, err
	}
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return false, err
	}
	defer checkin()
	has, err := store.HasNodeHistory(nid)
	if err != nil {
		return false, err
	}

	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return false, err
	}
	if liveHere && !isArchive {
		if store == ts.refShard {
			return ts.historyPresentWithArchive(store, has, func(s *BadgerStore) (bool, error) { return s.HasNodeHistory(nid) })
		}
		return has, nil
	}
	if isArchive {
		return ts.historyPresentWithReference(store, has, func(s *BadgerStore) (bool, error) { return s.HasNodeHistory(nid) })
	}
	return ts.historyPresentAnywhere(store, has, func(s *BadgerStore) (bool, error) { return s.HasNodeHistory(nid) })
}

// HasRelHistory is HasNodeHistory for relationships, walking the shards
// GetRelHistory reads (a cross-shard relationship's history lives with the
// relationship row's shard).
func (ts *Store) HasRelHistory(rid types.RelID) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return false, err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
	if err != nil {
		return false, err
	}
	defer checkin()
	has, err := shard.HasRelHistory(rid)
	if err != nil {
		return false, err
	}

	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return false, err
	}
	if liveHere && !isArchive {
		if shard == ts.refShard {
			return ts.historyPresentWithArchive(shard, has, func(s *BadgerStore) (bool, error) { return s.HasRelHistory(rid) })
		}
		return has, nil
	}
	if isArchive {
		return ts.historyPresentWithReference(shard, has, func(s *BadgerStore) (bool, error) { return s.HasRelHistory(rid) })
	}
	return ts.historyPresentAnywhere(shard, has, func(s *BadgerStore) (bool, error) { return s.HasRelHistory(rid) })
}

// historyPresentWithArchive mirrors nodeHistoryWithArchive /
// relHistoryWithArchive: the owner's answer, or the archive's.
func (ts *Store) historyPresentWithArchive(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return false, err
	}
	if archive == nil {
		return has, nil
	}
	defer archiveCheckin()
	if archive == skip {
		return has, nil
	}
	archiveHas, err := ask(archive)
	if err != nil {
		return false, err
	}
	return has || archiveHas, nil
}

// historyPresentWithReference mirrors nodeHistoryWithReference /
// relHistoryWithReference: the archive owner's answer, or the reference shard's.
func (ts *Store) historyPresentWithReference(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return false, err
	}
	defer refCheckin()
	if ref == skip {
		return has, nil
	}
	refHas, err := ask(ref)
	if err != nil {
		return false, err
	}
	return has || refHas, nil
}

// historyPresentAnywhere mirrors the deleted-entity fan-out of
// GetNodeHistory / GetRelHistory: every history shard is asked (the fan-out
// does not stop early, so a shard error surfaces exactly as History's does).
func (ts *Store) historyPresentAnywhere(skip *BadgerStore, has bool, ask func(*BadgerStore) (bool, error)) (bool, error) {
	err := ts.forEachHistoryShard(skip, func(candidate *BadgerStore) (bool, error) {
		candidateHas, err := ask(candidate)
		if err != nil {
			return false, err
		}
		has = has || candidateHas
		return false, nil
	})
	if err != nil {
		return false, err
	}
	return has, nil
}
