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
	return walkNodeHistoryShards(ts, nid, func(s *BadgerStore) (bool, error) { return s.HasNodeHistory(nid) }, orPresence)
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
	return walkRelHistoryShards(ts, rid, func(s *BadgerStore) (bool, error) { return s.HasRelHistory(rid) }, orPresence)
}

func orPresence(a, b bool) bool { return a || b }

// walkNodeHistoryShards asks every shard GetNodeHistory reads for node nid, in
// its order and with its errors, and merges the answers: the owning shard;
// with the archive for a node live on the reference shard; with the reference
// shard for an archived node; with every history shard for a node live
// nowhere. HasNodeHistory and NodeHistoryStamps share it, so the presence and
// the stamps of one ID always come from the same rows.
func walkNodeHistoryShards[T any](ts *Store, nid types.NodeID, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return zero, err
	}
	defer checkin()
	acc, err := ask(store)
	if err != nil {
		return zero, err
	}
	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return zero, err
	}
	if liveHere && !isArchive {
		if store == ts.refShard {
			return historyWalkWithArchive(ts, store, acc, ask, merge)
		}
		return acc, nil
	}
	if isArchive {
		return historyWalkWithReference(ts, store, acc, ask, merge)
	}
	return historyWalkAnywhere(ts, store, acc, ask, merge)
}

// walkRelHistoryShards is walkNodeHistoryShards for relationships, walking
// the shards GetRelHistory reads.
func walkRelHistoryShards[T any](ts *Store, rid types.RelID, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
	if err != nil {
		return zero, err
	}
	defer checkin()
	acc, err := ask(shard)
	if err != nil {
		return zero, err
	}
	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return zero, err
	}
	if liveHere && !isArchive {
		if shard == ts.refShard {
			return historyWalkWithArchive(ts, shard, acc, ask, merge)
		}
		return acc, nil
	}
	if isArchive {
		return historyWalkWithReference(ts, shard, acc, ask, merge)
	}
	return historyWalkAnywhere(ts, shard, acc, ask, merge)
}

// historyWalkWithArchive mirrors nodeHistoryWithArchive /
// relHistoryWithArchive: the owner's answer merged with the archive's.
func historyWalkWithArchive[T any](ts *Store, skip *BadgerStore, acc T, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return zero, err
	}
	if archive == nil {
		return acc, nil
	}
	defer archiveCheckin()
	if archive == skip {
		return acc, nil
	}
	other, err := ask(archive)
	if err != nil {
		return zero, err
	}
	return merge(acc, other), nil
}

// historyWalkWithReference mirrors nodeHistoryWithReference /
// relHistoryWithReference: the archive owner's answer merged with the
// reference shard's.
func historyWalkWithReference[T any](ts *Store, skip *BadgerStore, acc T, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return zero, err
	}
	defer refCheckin()
	if ref == skip {
		return acc, nil
	}
	other, err := ask(ref)
	if err != nil {
		return zero, err
	}
	return merge(acc, other), nil
}

// historyWalkAnywhere mirrors the deleted-entity fan-out of GetNodeHistory /
// GetRelHistory: every history shard is asked (the fan-out does not stop
// early, so a shard error surfaces exactly as History's does).
func historyWalkAnywhere[T any](ts *Store, skip *BadgerStore, acc T, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	err := ts.forEachHistoryShard(skip, func(candidate *BadgerStore) (bool, error) {
		other, err := ask(candidate)
		if err != nil {
			return false, err
		}
		acc = merge(acc, other)
		return false, nil
	})
	if err != nil {
		return zero, err
	}
	return acc, nil
}
