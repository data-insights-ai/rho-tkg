package tiered

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationship-history reads.
// Mirror layout of the node-history methods in tieredstore_read_history.go.

func (ts *Store) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
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
	// Reference relationships are the exception: archive/restore can leave
	// history in refArchive while the restored live row is back on refShard.
	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return nil, err
	}
	if liveHere && !isArchive {
		if shard == ts.refShard {
			archive, archiveCheckin, archiveErr := ts.checkoutArchive()
			if archiveErr != nil {
				return nil, archiveErr
			}
			if archive != nil {
				r, err := archive.GetRelVersion(rid, version)
				archiveCheckin()
				if err == nil || !errors.Is(err, ErrVersionNotFound) {
					return r, err
				}
			}
		}
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
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := shard.GetRelHistory(rid)
	if err != nil {
		return nil, err
	}

	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return nil, err
	}
	if liveHere && !isArchive {
		if shard == ts.refShard {
			return ts.relHistoryWithArchive(rid, shard, history)
		}
		return history, nil
	}
	if isArchive {
		return ts.relHistoryWithReference(rid, shard, history)
	}

	sources := relHistorySourcesFrom(shard, history)
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetRelHistory(rid)
		if err != nil {
			return false, err
		}
		appendRelHistorySource(&sources, candidate, history)
		return false, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return mergeRelHistorySources(sources), nil
}

func (ts *Store) RelHistoryVersionsFrom(rid types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := shard.RelHistoryVersionsFrom(rid, startVersion, limit)
	if err != nil {
		return nil, err
	}

	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return nil, err
	}
	if liveHere && !isArchive {
		if shard == ts.refShard {
			sources := relHistorySourcesFrom(shard, history)
			archive, archiveCheckin, err := ts.checkoutArchive()
			if err != nil {
				return nil, err
			}
			if archive != nil && archive != shard {
				archiveHistory, err := archive.RelHistoryVersionsFrom(rid, startVersion, limit)
				archiveCheckin()
				if err != nil {
					return nil, err
				}
				appendRelHistorySource(&sources, archive, archiveHistory)
			} else if archive != nil {
				archiveCheckin()
			}
			return trimRelHistoryPage(mergeRelHistorySources(sources), limit), nil
		}
		return history, nil
	}
	if isArchive {
		sources := relHistorySourcesFrom(shard, history)
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return nil, err
		}
		if ref != shard {
			refHistory, err := ref.RelHistoryVersionsFrom(rid, startVersion, limit)
			refCheckin()
			if err != nil {
				return nil, err
			}
			appendRelHistorySource(&sources, ref, refHistory)
		} else {
			refCheckin()
		}
		return trimRelHistoryPage(mergeRelHistorySources(sources), limit), nil
	}

	sources := relHistorySourcesFrom(shard, history)
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.RelHistoryVersionsFrom(rid, startVersion, limit)
		if err != nil {
			return false, err
		}
		appendRelHistorySource(&sources, candidate, history)
		return false, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return trimRelHistoryPage(mergeRelHistorySources(sources), limit), nil
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
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}
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

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refIDs, err := ref.AllRelHistoryIDsFrom(after, limit)
	refCheckin()
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
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return nil, err
		}
		ids, scanErr := store.AllRelHistoryIDsFrom(after, limit)
		release()
		if scanErr != nil {
			return nil, scanErr
		}
		addAll(ids)
	}

	if len(raw) == 0 {
		return nil, nil
	}
	storeutil.SortSnowflakeIDs(raw)
	if limit > 0 && limit < len(raw) {
		raw = raw[:limit]
	}
	return rawToRelIDs(raw), nil
}
func (ts *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	return ts.ForEachRelHistoryIDByDepth(DepthAll, fn)
}

func (ts *Store) ForEachRelHistoryIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := validateDepth(depth); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	if depth == DepthAll {
		return ts.forEachRelHistoryIDAll(fn)
	}
	return ts.forEachRelHistoryIDByDepth(depth, fn)
}

func (ts *Store) forEachRelHistoryIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error {
	maxID, err := ts.maxRelHistoryIDByDepth(depth)
	if err != nil {
		return err
	}
	if maxID == 0 {
		return nil
	}
	maxRaw := maxID.SnowflakeID()

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}

	ids := make([]types.RelID, 0)
	var archiveProbeErr error
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		if archive != nil {
			archiveCheckin()
		}
		return err
	}
	if err := ref.ForEachRelHistoryID(func(id types.RelID) bool {
		if archive != nil {
			// Restored reference relationships can have archive history, but
			// current ref-shard ownership makes them eligible for hot/warm depth.
			refLive, err := relationshipRowLive(ref, id)
			if err != nil {
				archiveProbeErr = err
				return false
			}
			if !refLive {
				archiveLive, err := relationshipRowLive(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if archiveLive {
					return true
				}
				hasHistory, err := archiveHasRelHistoryID(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if hasHistory {
					return true
				}
			}
		}
		if id.SnowflakeID() > maxRaw {
			return true
		}
		ids = append(ids, id)
		return true
	}); err != nil {
		refCheckin()
		if archive != nil {
			archiveCheckin()
		}
		return err
	}
	refCheckin()
	if archiveProbeErr != nil {
		if archive != nil {
			archiveCheckin()
		}
		return archiveProbeErr
	}
	if archive != nil {
		archiveCheckin()
	}
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(depth)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		ids = ids[:0]
		err = store.ForEachRelHistoryID(func(id types.RelID) bool {
			if id.SnowflakeID() > maxRaw {
				return true
			}
			ids = append(ids, id)
			return true
		})
		release()
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !fn(id) {
				return nil
			}
		}
	}
	return nil
}

func (ts *Store) forEachRelHistoryIDAll(fn func(types.RelID) bool) error {
	maxID, err := ts.maxRelHistoryIDAll()
	if err != nil {
		return err
	}
	if maxID == 0 {
		return nil
	}
	maxRaw := maxID.SnowflakeID()

	var after types.RelID
	for {
		ids, err := ts.AllRelHistoryIDsFrom(after, 1024)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			if id.SnowflakeID() > maxRaw {
				return nil
			}
			if !fn(id) {
				return nil
			}
		}
		after = ids[len(ids)-1]
		if after.SnowflakeID() >= maxRaw {
			return nil
		}
	}
}

func (ts *Store) maxRelHistoryIDByDepth(depth ShardDepth) (types.RelID, error) {
	if depth == DepthAll {
		return ts.maxRelHistoryIDAll()
	}
	var max types.RelID
	observe := func(id types.RelID) {
		if id.SnowflakeID() > max.SnowflakeID() {
			max = id
		}
	}

	var archive *BadgerStore
	var archiveCheckin func()
	if depth != DepthAll {
		var archiveErr error
		archive, archiveCheckin, archiveErr = ts.checkoutArchive()
		if archiveErr != nil {
			return 0, archiveErr
		}
		defer archiveCheckin()
	}

	var archiveProbeErr error
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	err = ref.ForEachRelHistoryID(func(id types.RelID) bool {
		if archive != nil {
			refLive, err := relationshipRowLive(ref, id)
			if err != nil {
				archiveProbeErr = err
				return false
			}
			if !refLive {
				archiveLive, err := relationshipRowLive(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if archiveLive {
					return true
				}
				hasHistory, err := archiveHasRelHistoryID(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if hasHistory {
					return true
				}
			}
		}
		observe(id)
		return true
	})
	refCheckin()
	if err != nil {
		return 0, err
	}
	if archiveProbeErr != nil {
		return 0, archiveProbeErr
	}

	if depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return 0, archiveErr
		}
		if archive != nil {
			err := archive.ForEachRelHistoryID(func(id types.RelID) bool {
				observe(id)
				return true
			})
			archiveCheckin()
			if err != nil {
				return 0, err
			}
		}
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(depth)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return 0, err
		}
		err = store.ForEachRelHistoryID(func(id types.RelID) bool {
			observe(id)
			return true
		})
		release()
		if err != nil {
			return 0, err
		}
	}
	return max, nil
}

func (ts *Store) maxRelHistoryIDAll() (types.RelID, error) {
	var max types.RelID
	observe := func(id types.RelID) {
		if id.SnowflakeID() > max.SnowflakeID() {
			max = id
		}
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	id, err := ref.MaxRelHistoryID()
	refCheckin()
	if err != nil {
		return 0, err
	}
	observe(id)

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		id, err := archive.MaxRelHistoryID()
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		observe(id)
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()
	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return 0, err
		}
		id, scanErr := store.MaxRelHistoryID()
		release()
		if scanErr != nil {
			return 0, scanErr
		}
		observe(id)
	}
	return max, nil
}

func archiveHasRelHistoryID(archive *BadgerStore, id types.RelID) (bool, error) {
	raw := id.SnowflakeID()
	after := types.RelID(0)
	if raw > 0 {
		after = types.RelID(raw - 1)
	}
	ids, err := archive.AllRelHistoryIDsFrom(after, 1)
	if err != nil {
		return false, err
	}
	return len(ids) > 0 && ids[0] == id, nil
}

type relHistorySource struct {
	store   *BadgerStore
	history []*types.Relationship
}

func relHistorySourcesFrom(store *BadgerStore, history []*types.Relationship) []relHistorySource {
	if len(history) == 0 {
		return nil
	}
	return []relHistorySource{{store: store, history: history}}
}

func appendRelHistorySource(sources *[]relHistorySource, store *BadgerStore, history []*types.Relationship) {
	if len(history) == 0 {
		return
	}
	*sources = append(*sources, relHistorySource{store: store, history: history})
}

func (ts *Store) relHistoryWithArchive(rid types.RelID, skip *BadgerStore, history []*types.Relationship) ([]*types.Relationship, error) {
	sources := relHistorySourcesFrom(skip, history)
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, err
	}
	if archive != nil && archive != skip {
		archiveHistory, err := archive.GetRelHistory(rid)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		appendRelHistorySource(&sources, archive, archiveHistory)
	} else if archive != nil {
		archiveCheckin()
	}
	return mergeRelHistorySources(sources), nil
}

func (ts *Store) relHistoryWithReference(rid types.RelID, skip *BadgerStore, history []*types.Relationship) ([]*types.Relationship, error) {
	sources := relHistorySourcesFrom(skip, history)
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	if ref != skip {
		refHistory, err := ref.GetRelHistory(rid)
		refCheckin()
		if err != nil {
			return nil, err
		}
		appendRelHistorySource(&sources, ref, refHistory)
	} else {
		refCheckin()
	}
	return mergeRelHistorySources(sources), nil
}

func mergeRelHistorySources(sources []relHistorySource) []*types.Relationship {
	if len(sources) == 0 {
		return nil
	}
	if len(sources) == 1 {
		return sources[0].history
	}
	total := 0
	for _, source := range sources {
		total += len(source.history)
	}
	if total == 0 {
		return nil
	}
	out := make([]*types.Relationship, 0, total)
	for _, source := range sources {
		out = append(out, source.history...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Version() < out[j].Version() })
	deduped := out[:0]
	var last uint32
	for i, r := range out {
		if i > 0 && r.Version() == last {
			continue
		}
		deduped = append(deduped, r)
		last = r.Version()
	}
	return deduped
}

func trimRelHistoryPage(history []*types.Relationship, limit int) []*types.Relationship {
	if limit > 0 && len(history) > limit {
		return history[:limit]
	}
	return history
}

// ForEachDeletedRelID is the relationship counterpart of ForEachDeletedNodeID.
// Streaming probe inside the history-iteration callback; shard locks/checkouts
// are released across callback invocations so ts.GetRelationship re-routes
// safely. Yields only IDs whose current row is absent.
func (ts *Store) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	return ts.ForEachDeletedRelIDByDepth(DepthAll, fn)
}

func (ts *Store) ForEachDeletedRelIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := validateDepth(depth); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	var probeErr error
	err := ts.ForEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
		_, err := ts.GetRelationship(id)
		if err == nil {
			return true // live — skip
		}
		if !errors.Is(err, ErrRelNotFound) {
			probeErr = err
			return false
		}
		return fn(id)
	})
	if probeErr != nil {
		return probeErr
	}
	return err
}
