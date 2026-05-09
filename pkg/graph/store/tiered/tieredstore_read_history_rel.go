package tiered

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Relationship-history reads (R5-F9 split out from tieredstore_read_history.go).
// Mirror layout of the node-history methods left in tieredstore_read_history.go.

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
