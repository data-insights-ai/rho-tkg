package tiered

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- History reads ---

func (ts *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	n, err := shard.GetNodeVersion(nid, version)
	if err == nil || !errors.Is(err, ErrVersionNotFound) {
		return n, err
	}

	// If the live entity is on this shard, the local ErrVersionNotFound is
	// authoritative — no need to wake other shards (incl. cold ones).
	// Skip the optimisation when the live entity sits on refArchive: ArchiveNode
	// only migrates the current entity, so pre-archive history versions remain
	// on refShard and must be discovered via the fan-out below.
	if shard.HasNodeID(nid.SnowflakeID()) && shard != ts.refArchive.Load() {
		return nil, ErrVersionNotFound
	}

	var found *types.Node
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		n, err := candidate.GetNodeVersion(nid, version)
		if errors.Is(err, ErrVersionNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		found = n
		return true, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	if found != nil {
		return found, nil
	}
	return nil, ErrVersionNotFound
}

func (ts *Store) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := store.GetNodeHistory(nid)
	if err != nil || len(history) > 0 {
		return history, err
	}

	// If the live entity is on this shard, an empty history here is
	// authoritative — the deleted-entity fan-out is unnecessary and would
	// needlessly wake cold shards.
	// Skip the optimisation when the live entity sits on refArchive: ArchiveNode
	// only migrates the current entity, so pre-archive history versions remain
	// on refShard and must be discovered via the fan-out below.
	if store.HasNodeID(nid.SnowflakeID()) && store != ts.refArchive.Load() {
		return nil, nil
	}

	var found []*types.Node
	searchErr := ts.forEachHistoryShard(store, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetNodeHistory(nid)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			return false, nil
		}
		found = history
		return true, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return found, nil
}

func (ts *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	r, err := shard.GetRelVersion(rid, version)
	if err == nil || !errors.Is(err, ErrVersionNotFound) {
		return r, err
	}

	// If the live rel entity is on this shard, the local ErrVersionNotFound is
	// authoritative — no need to wake other shards.
	// Skip the optimisation when the live rel sits on refArchive: ArchiveNode
	// only migrates the current entity, so pre-archive rel history versions
	// remain on refShard and must be discovered via the fan-out below.
	if shard.HasRelID(rid.SnowflakeID()) && shard != ts.refArchive.Load() {
		return nil, ErrVersionNotFound
	}

	var found *types.Relationship
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		r, err := candidate.GetRelVersion(rid, version)
		if errors.Is(err, ErrVersionNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		found = r
		return true, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	if found != nil {
		return found, nil
	}
	return nil, ErrVersionNotFound
}

func (ts *Store) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := shard.GetRelHistory(rid)
	if err != nil || len(history) > 0 {
		return history, err
	}

	// If the live rel entity is on this shard, an empty history here is
	// authoritative — skip the deleted-rel fan-out so cold shards are not
	// needlessly opened.
	// Skip the optimisation when the live rel sits on refArchive: pre-archive
	// rel history remains on refShard, so fall through to the fan-out below.
	if shard.HasRelID(rid.SnowflakeID()) && shard != ts.refArchive.Load() {
		return nil, nil
	}

	var found []*types.Relationship
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetRelHistory(rid)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			return false, nil
		}
		found = history
		return true, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return found, nil
}

// AllNodeHistoryIDs returns the IDs of all nodes that have version history
// entries across the reference shard, the reference archive (when present),
// and every event shard.
//
// Implemented as a thin wrapper over AllNodeHistoryIDsFrom — pages 64K IDs at
// a time so the in-flight ID slice stays bounded even on graphs with deep
// history. Eliminates the parallel-merge transient (~400MB on 52-shard
// year-long graphs) that the previous goroutine fan-out produced.
func (ts *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	const pageSize = 65536
	var (
		all   []types.NodeID
		after types.NodeID
	)
	for {
		page, err := ts.AllNodeHistoryIDsFrom(after, pageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		if len(page) < pageSize {
			break
		}
		after = page[len(page)-1]
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all, nil
}

// AllRelHistoryIDs returns the IDs of all relationships with version history
// entries. See AllNodeHistoryIDs for the implementation rationale.
func (ts *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	const pageSize = 65536
	var (
		all   []types.RelID
		after types.RelID
	)
	for {
		page, err := ts.AllRelHistoryIDsFrom(after, pageSize)
		if err != nil {
			return nil, err
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		if len(page) < pageSize {
			break
		}
		after = page[len(page)-1]
	}
	if len(all) == 0 {
		return nil, nil
	}
	return all, nil
}

// AllNodeHistoryIDsFrom is the bounded-RAM, cursor-paginated history-ID scan
// for TieredStore.
//
// Implementation strategy:
//
//   - Probe the reference shard, the reference archive (if any), and each
//     event shard SEQUENTIALLY via checkout/checkin. Only one shard's
//     iterator is open at any time.
//   - Each per-shard call passes (after, limit) through to the underlying
//     BadgerStore.AllNodeHistoryIDsFrom, so each shard returns at most
//     `limit` IDs. The dedup `seen` set is therefore bounded by the IDs
//     returned in the current call (across shards), not by the total
//     graph size.
//   - Accumulated IDs are sorted ascending after merging and trimmed to
//     `limit`. Cross-shard duplicates (same ID in archive and event shard,
//     possible after archive-then-update) collapse to one occurrence.
//
// The previous parallel goroutine fan-out has been removed: it held all
// shard slices in RAM simultaneously and dedup'd via a final map keyed on
// the entire population — both transients have been eliminated.
func (ts *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	seen := make(map[snowflake.ID]struct{})
	var raw []snowflake.ID

	addAll := func(ids []types.NodeID) {
		for _, id := range ids {
			r := id.SnowflakeID()
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			raw = append(raw, r)
		}
	}

	// Reference shard.
	refIDs, err := ts.refShard.AllNodeHistoryIDsFrom(after, limit)
	if err != nil {
		return nil, err
	}
	addAll(refIDs)

	// Reference archive (lazy-open + close-race safe via checkoutArchive).
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return nil, archiveErr
	}
	if archive != nil {
		archiveIDs, err := archive.AllNodeHistoryIDsFrom(after, limit)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		addAll(archiveIDs)
	}

	// Event shards — sequential checkout/checkin.
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return nil, err
		}
		ids, scanErr := store.AllNodeHistoryIDsFrom(after, limit)
		es.checkinStore()
		if scanErr != nil {
			return nil, scanErr
		}
		addAll(ids)
	}

	if len(raw) == 0 {
		return nil, nil
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i] < raw[j] })
	if limit > 0 && limit < len(raw) {
		raw = raw[:limit]
	}
	return rawToNodeIDs(raw), nil
}

// AllRelHistoryIDsFrom is the relationship-history equivalent of
// AllNodeHistoryIDsFrom. Same sequential checkout/checkin walk, same
// bounded-RAM dedup contract.
func (ts *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	seen := make(map[snowflake.ID]struct{})
	var raw []snowflake.ID

	addAll := func(ids []types.RelID) {
		for _, id := range ids {
			r := id.SnowflakeID()
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			raw = append(raw, r)
		}
	}

	refIDs, err := ts.refShard.AllRelHistoryIDsFrom(after, limit)
	if err != nil {
		return nil, err
	}
	addAll(refIDs)

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return nil, archiveErr
	}
	if archive != nil {
		archiveIDs, err := archive.AllRelHistoryIDsFrom(after, limit)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		addAll(archiveIDs)
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return nil, err
		}
		ids, scanErr := store.AllRelHistoryIDsFrom(after, limit)
		es.checkinStore()
		if scanErr != nil {
			return nil, scanErr
		}
		addAll(ids)
	}

	if len(raw) == 0 {
		return nil, nil
	}
	sort.Slice(raw, func(i, j int) bool { return raw[i] < raw[j] })
	if limit > 0 && limit < len(raw) {
		raw = raw[:limit]
	}
	return rawToRelIDs(raw), nil
}

// forEachHistoryShard probes shards that may own history after the live entity
// index has been deleted. Reference entity history lives on the reference
// shard even when timestamp fallback selects an event shard; cross-shard
// relationship history lives with the relationship entity shard.
//
// Probes refShard, refArchive (if present), and ALL event shards (hot, warm,
// and cold). Cold shards must be included: a cross-shard event→event
// relationship's history is written to the start-node's home shard, which may
// have transitioned warm→cold via `ColdAfter` demotion after the relationship
// was deleted. Skipping cold shards here would silently lose deleted-rel
// history once the start-node's shard ages out. The lazy-open cost is paid
// once per cold shard per process; subsequent probes are cheap Badger Seeks.
func (ts *Store) forEachHistoryShard(skip *BadgerStore, fn func(*BadgerStore) (bool, error)) error {
	if ts.refShard != skip {
		stop, err := fn(ts.refShard)
		if err != nil || stop {
			return err
		}
	}

	// Pin the archive via checkoutArchive (incrementing archiveActiveReqs)
	// so the callback sees a stable handle even if Close / closeIdleShards
	// races. checkoutArchive lazy-opens on cold start when the catalog
	// records an archive but the pointer is nil. The checkin must wrap
	// the callback invocation so the pin is released on every exit path.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil && archive != skip {
		stop, err := fn(archive)
		archiveCheckin()
		if err != nil || stop {
			return err
		}
	} else if archive != nil {
		archiveCheckin()
	}

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		if store == skip {
			es.checkinStore()
			continue
		}
		stop, cbErr := fn(store)
		es.checkinStore()
		if cbErr != nil {
			return cbErr
		}
		if stop {
			return nil
		}
	}
	return nil
}

func (ts *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachNodeHistoryID(func(id types.NodeID) bool {
		if !fn(id) {
			stopped = true
			return false
		}
		return true
	}); err != nil {
		return err
	}
	if stopped {
		return nil
	}

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		err := archive.ForEachNodeHistoryID(func(id types.NodeID) bool {
			if !fn(id) {
				stopped = true
				return false
			}
			return true
		})
		archiveCheckin()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		err = store.ForEachNodeHistoryID(func(id types.NodeID) bool {
			if !fn(id) {
				stopped = true
				return false
			}
			return true
		})
		es.checkinStore()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}

func (ts *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachRelHistoryID(func(id types.RelID) bool {
		if !fn(id) {
			stopped = true
			return false
		}
		return true
	}); err != nil {
		return err
	}
	if stopped {
		return nil
	}

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		err := archive.ForEachRelHistoryID(func(id types.RelID) bool {
			if !fn(id) {
				stopped = true
				return false
			}
			return true
		})
		archiveCheckin()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		err = store.ForEachRelHistoryID(func(id types.RelID) bool {
			if !fn(id) {
				stopped = true
				return false
			}
			return true
		})
		es.checkinStore()
		if err != nil {
			return err
		}
		if stopped {
			return nil
		}
	}
	return nil
}
