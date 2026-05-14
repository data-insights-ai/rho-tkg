package tiered

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- History reads ---

func (ts *Store) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	shard, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
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
	// Reference entities are the exception: archive/restore can leave history
	// in refArchive while the restored live row is back on refShard.
	liveHere, err := nodeRowLive(shard, nid)
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
				n, err := archive.GetNodeVersion(nid, version)
				archiveCheckin()
				if err == nil || !errors.Is(err, ErrVersionNotFound) {
					return n, err
				}
			}
		}
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
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := store.GetNodeHistory(nid)
	if err != nil {
		return nil, err
	}

	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return nil, err
	}
	if liveHere && !isArchive {
		if store == ts.refShard {
			return ts.nodeHistoryWithArchive(nid, store, history)
		}
		return history, nil
	}
	if isArchive {
		return ts.nodeHistoryWithReference(nid, store, history)
	}

	sources := nodeHistorySourcesFrom(store, history)
	searchErr := ts.forEachHistoryShard(store, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetNodeHistory(nid)
		if err != nil {
			return false, err
		}
		appendNodeHistorySource(&sources, candidate, history)
		return false, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return mergeNodeHistorySources(sources), nil
}

func (ts *Store) NodeHistoryVersionsFrom(nid types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateHistoryPageLimit(limit); err != nil {
		return nil, err
	}
	store, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := store.NodeHistoryVersionsFrom(nid, startVersion, limit)
	if err != nil {
		return nil, err
	}

	liveHere, err := nodeRowLive(store, nid)
	if err != nil {
		return nil, err
	}
	if liveHere && !isArchive {
		if store == ts.refShard {
			sources := nodeHistorySourcesFrom(store, history)
			archive, archiveCheckin, err := ts.checkoutArchive()
			if err != nil {
				return nil, err
			}
			if archive != nil && archive != store {
				archiveHistory, err := archive.NodeHistoryVersionsFrom(nid, startVersion, limit)
				archiveCheckin()
				if err != nil {
					return nil, err
				}
				appendNodeHistorySource(&sources, archive, archiveHistory)
			} else if archive != nil {
				archiveCheckin()
			}
			return trimNodeHistoryPage(mergeNodeHistorySources(sources), limit), nil
		}
		return history, nil
	}
	if isArchive {
		sources := nodeHistorySourcesFrom(store, history)
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return nil, err
		}
		if ref != store {
			refHistory, err := ref.NodeHistoryVersionsFrom(nid, startVersion, limit)
			refCheckin()
			if err != nil {
				return nil, err
			}
			appendNodeHistorySource(&sources, ref, refHistory)
		} else {
			refCheckin()
		}
		return trimNodeHistoryPage(mergeNodeHistorySources(sources), limit), nil
	}

	sources := nodeHistorySourcesFrom(store, history)
	searchErr := ts.forEachHistoryShard(store, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.NodeHistoryVersionsFrom(nid, startVersion, limit)
		if err != nil {
			return false, err
		}
		appendNodeHistorySource(&sources, candidate, history)
		return false, nil
	})
	if searchErr != nil {
		return nil, searchErr
	}
	return trimNodeHistoryPage(mergeNodeHistorySources(sources), limit), nil
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
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}
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
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refIDs, err := ref.AllNodeHistoryIDsFrom(after, limit)
	refCheckin()
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
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return nil, err
		}
		ids, scanErr := store.AllNodeHistoryIDsFrom(after, limit)
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
	return rawToNodeIDs(raw), nil
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
// history once the start-node's shard ages out. Cold shards opened only for
// this read are closed after their callback returns so broad scans do not
// accumulate one Badger handle per historical shard.
func (ts *Store) forEachHistoryShard(skip *BadgerStore, fn func(*BadgerStore) (bool, error)) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	if ref != skip {
		stop, err := fn(ref)
		refCheckin()
		if err != nil || stop {
			return err
		}
	} else {
		refCheckin()
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
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		if store == skip {
			release()
			continue
		}
		stop, cbErr := fn(store)
		release()
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
	return ts.ForEachNodeHistoryIDByDepth(DepthAll, fn)
}

func (ts *Store) ForEachNodeHistoryIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error {
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
		return ts.forEachNodeHistoryIDAll(fn)
	}
	return ts.forEachNodeHistoryIDByDepth(depth, fn)
}

func (ts *Store) forEachNodeHistoryIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error {
	maxID, err := ts.maxNodeHistoryIDByDepth(depth)
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

	ids := make([]types.NodeID, 0)
	var archiveProbeErr error
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		if archive != nil {
			archiveCheckin()
		}
		return err
	}
	if err := ref.ForEachNodeHistoryID(func(id types.NodeID) bool {
		if archive != nil {
			// Restored reference nodes can have archive history, but current
			// ref-shard ownership makes them eligible for hot/warm depth.
			refLive, err := nodeRowLive(ref, id)
			if err != nil {
				archiveProbeErr = err
				return false
			}
			if !refLive {
				archiveLive, err := nodeRowLive(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if archiveLive {
					return true
				}
				hasHistory, err := archiveHasNodeHistoryID(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if hasHistory {
					return true
				}
			}
		}
		// Bound observation to the maxRaw snapshot computed before this scan
		// started. A concurrent writer can add a new history ID between the
		// maxNodeHistoryIDByDepth call and this iteration; without this prune,
		// the late ID would be observed here but not reflected in maxRaw, and
		// callers that pair this iteration with maxRaw-based reasoning would
		// see a result set that exceeds the snapshot upper bound.
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
		err = store.ForEachNodeHistoryID(func(id types.NodeID) bool {
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

func (ts *Store) forEachNodeHistoryIDAll(fn func(types.NodeID) bool) error {
	maxID, err := ts.maxNodeHistoryIDAll()
	if err != nil {
		return err
	}
	if maxID == 0 {
		return nil
	}
	maxRaw := maxID.SnowflakeID()

	var after types.NodeID
	for {
		ids, err := ts.AllNodeHistoryIDsFrom(after, 1024)
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

func (ts *Store) maxNodeHistoryIDByDepth(depth ShardDepth) (types.NodeID, error) {
	if depth == DepthAll {
		return ts.maxNodeHistoryIDAll()
	}
	var max types.NodeID
	observe := func(id types.NodeID) {
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
	err = ref.ForEachNodeHistoryID(func(id types.NodeID) bool {
		if archive != nil {
			refLive, err := nodeRowLive(ref, id)
			if err != nil {
				archiveProbeErr = err
				return false
			}
			if !refLive {
				archiveLive, err := nodeRowLive(archive, id)
				if err != nil {
					archiveProbeErr = err
					return false
				}
				if archiveLive {
					return true
				}
				hasHistory, err := archiveHasNodeHistoryID(archive, id)
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
			err := archive.ForEachNodeHistoryID(func(id types.NodeID) bool {
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
		err = store.ForEachNodeHistoryID(func(id types.NodeID) bool {
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

func (ts *Store) maxNodeHistoryIDAll() (types.NodeID, error) {
	var max types.NodeID
	observe := func(id types.NodeID) {
		if id.SnowflakeID() > max.SnowflakeID() {
			max = id
		}
	}

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	id, err := ref.MaxNodeHistoryID()
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
		id, err := archive.MaxNodeHistoryID()
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
		id, scanErr := store.MaxNodeHistoryID()
		release()
		if scanErr != nil {
			return 0, scanErr
		}
		observe(id)
	}
	return max, nil
}

func archiveHasNodeHistoryID(archive *BadgerStore, id types.NodeID) (bool, error) {
	raw := id.SnowflakeID()
	after := types.NodeID(0)
	if raw > 0 {
		after = types.NodeID(raw - 1)
	}
	ids, err := archive.AllNodeHistoryIDsFrom(after, 1)
	if err != nil {
		return false, err
	}
	return len(ids) > 0 && ids[0] == id, nil
}

type nodeHistorySource struct {
	store   *BadgerStore
	history []*types.Node
}

func nodeHistorySourcesFrom(store *BadgerStore, history []*types.Node) []nodeHistorySource {
	if len(history) == 0 {
		return nil
	}
	return []nodeHistorySource{{store: store, history: history}}
}

func appendNodeHistorySource(sources *[]nodeHistorySource, store *BadgerStore, history []*types.Node) {
	if len(history) == 0 {
		return
	}
	*sources = append(*sources, nodeHistorySource{store: store, history: history})
}

func (ts *Store) nodeHistoryWithArchive(nid types.NodeID, skip *BadgerStore, history []*types.Node) ([]*types.Node, error) {
	sources := nodeHistorySourcesFrom(skip, history)
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return nil, err
	}
	if archive != nil && archive != skip {
		archiveHistory, err := archive.GetNodeHistory(nid)
		archiveCheckin()
		if err != nil {
			return nil, err
		}
		appendNodeHistorySource(&sources, archive, archiveHistory)
	} else if archive != nil {
		archiveCheckin()
	}
	return mergeNodeHistorySources(sources), nil
}

func (ts *Store) nodeHistoryWithReference(nid types.NodeID, skip *BadgerStore, history []*types.Node) ([]*types.Node, error) {
	sources := nodeHistorySourcesFrom(skip, history)
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	if ref != skip {
		refHistory, err := ref.GetNodeHistory(nid)
		refCheckin()
		if err != nil {
			return nil, err
		}
		appendNodeHistorySource(&sources, ref, refHistory)
	} else {
		refCheckin()
	}
	return mergeNodeHistorySources(sources), nil
}

func mergeNodeHistorySources(sources []nodeHistorySource) []*types.Node {
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
	out := make([]*types.Node, 0, total)
	for _, source := range sources {
		out = append(out, source.history...)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Version() < out[j].Version() })
	deduped := out[:0]
	var last uint32
	for i, n := range out {
		if i > 0 && n.Version() == last {
			continue
		}
		deduped = append(deduped, n)
		last = n.Version()
	}
	return deduped
}

func trimNodeHistoryPage(history []*types.Node, limit int) []*types.Node {
	if limit > 0 && len(history) > limit {
		return history[:limit]
	}
	return history
}

// ForEachDeletedNodeID iterates IDs that have history rows but no current
// row. Streaming: probes ts.GetNode INSIDE the history-iteration callback —
// shard locks/checkouts are released across callback invocations (see the
// pagination shape in forEachNodeHistoryIDAll / forEachNodeHistoryIDByDepth),
// so the GetNode probe re-routes safely without holding the iterator's shard
// handles. Yields only IDs whose current row is absent; live entities are
// skipped without buffering the full history set.
func (ts *Store) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	return ts.ForEachDeletedNodeIDByDepth(DepthAll, fn)
}

func (ts *Store) ForEachDeletedNodeIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error {
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
	err := ts.ForEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
		_, err := ts.GetNode(id)
		if err == nil {
			return true // live — skip
		}
		if !errors.Is(err, ErrNodeNotFound) {
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
