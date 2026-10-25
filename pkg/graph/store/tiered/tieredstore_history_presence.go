package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var _ storecontract.HistoryPresenceCapability = (*Store)(nil)

// HasNodeHistory reports whether GetNodeHistory(id) would return a row
// (store.HistoryPresenceCapability). It walks exactly the shards GetNodeHistory
// reads, in the same order with the same errors, asking each HasNodeHistory
// instead of reading rows: the owning shard; plus the archive for a node live
// on the reference shard; plus the reference shard for an archived node; every
// history shard for a node live nowhere. Cold shards open via
// checkoutStoreForRead and answer by a per-ID key probe (badger
// Config.HistoryPresenceProbeOnly).
func (ts *Store) HasNodeHistory(nid types.NodeID) (bool, error) {
	return ts.hasNodeHistoryVia(nid, directReads)
}

func (ts *Store) hasNodeHistoryVia(nid types.NodeID, via shardVia) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return false, err
	}
	return walkNodeHistoryShards(ts, nid, func(s *BadgerStore) (bool, error) { return via(s).HasNodeHistory(nid) }, orPresence)
}

// HasRelHistory is HasNodeHistory for relationships, walking the shards
// GetRelHistory reads.
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

// walkNodeHistoryShards asks every shard GetNodeHistory reads for nid, in its
// order and with its errors, and merges the answers (same shard rules as
// HasNodeHistory). HasNodeHistory and NodeHistoryStamps share it so presence
// and stamps of one ID come from the same rows. Liveness is read through the
// shard's current-row stamps (backlog 42), the same answer as GetNodeHistory
// except that a current row with a decodable temporal block but undecodable
// body is live here. No moveMu: moves leave history on its shard and the walk
// falls back to every shard (TestRefArchiveReadersSeeMovedEntityOnce).
func walkNodeHistoryShards[T any](ts *Store, nid types.NodeID, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	store, checkin, isArchive, err := ts.shardForNodeIDReadWithArchive(nid)
	if err != nil {
		return zero, err
	}
	defer checkin()
	acc, err := ask(store)
	if err != nil {
		return zero, err
	}
	_, liveHere, err := nodeCurrentStampsRow(store, nid)
	if err != nil {
		return zero, err
	}
	liveHere, isArchive = ts.nodeLivesRoute(nid, liveHere, isArchive)
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

// walkRelHistoryShards is walkNodeHistoryShards for relationships. No moveMu
// (TestRefArchiveReadersSeeMovedEntityOnce).
func walkRelHistoryShards[T any](ts *Store, rid types.RelID, ask func(*BadgerStore) (T, error), merge func(T, T) T) (T, error) {
	var zero T
	shard, checkin, isArchive, probe, err := ts.routeRelShardFor(rid, true)
	if err != nil {
		return zero, err
	}
	defer checkin()
	acc, err := ask(shard)
	if err != nil {
		return zero, err
	}
	if probe.err != nil {
		return zero, probe.err
	}
	liveHere := probe.live // the route read the row: no second read
	liveHere, isArchive = ts.relLivesRoute(rid, liveHere, isArchive)
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
	ts.refArchiveGapHook()
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
// relHistoryWithReference: the archive owner's answer merged with the reference
// shard's.
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
// GetRelHistory: every shard is asked, no early stop, so errors surface as in
// History.
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
