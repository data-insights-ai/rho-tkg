package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Bulk queries ---

func (ts *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refNodes, err := ref.AllNodes(stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}

	// refArchive parity: archived nodes are still GetNode-addressable, so
	// public bulk scans must include them. Otherwise an archived entity
	// silently disappears from AllNodes / similar APIs even though point
	// lookups still find it. checkoutArchive lazy-opens on cold start
	// (catalog-archive-shard-but-pointer-nil).
	//
	// Depth gating: archive is the coldest tier of reference data;
	// including it in DepthHot/DepthWarm would surface entities the
	// caller asked to exclude. Only DepthAll requests archive content.
	// refShard is queried for all Depth values per existing semantics
	// (reference data is not Depth-tiered, only event data is).
	var archiveNodes []*types.Node
	if opts.Depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			archiveNodes, err = archive.AllNodes(stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
		}
	}

	type result struct {
		nodes []*types.Node
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].nodes, results[i].err = store.AllNodes(stripDepth(opts))
	})

	var slices [][]*types.Node
	if len(refNodes) > 0 {
		slices = append(slices, refNodes)
	}
	if len(archiveNodes) > 0 {
		slices = append(slices, archiveNodes)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.nodes) > 0 {
			slices = append(slices, r.nodes)
		}
	}

	merged := mergeNodeSlices(slices)
	return applyNodePagination(merged, opts), nil
}

// --- Counts ---

func (ts *Store) NodeCount() (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	n, err := ref.NodeCount()
	refCheckin()
	if err != nil {
		return 0, err
	}
	total += n

	// refArchive parity: archived nodes count toward the public total.
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		an, err := archive.NodeCount()
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		total += an
	}

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].count, results[i].err = store.NodeCount()
	})
	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		total += r.count
	}
	return total, nil
}

func (ts *Store) NodeCountByLabel(token uint16) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, err
	}

	total := 0
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	n, err := ref.NodeCountByLabel(token)
	refCheckin()
	if err != nil {
		return 0, err
	}
	total += n

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		an, err := archive.NodeCountByLabel(token)
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		total += an
	}

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].count, results[i].err = store.NodeCountByLabel(token)
	})
	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		total += r.count
	}
	return total, nil
}

func (ts *Store) NodeCountByLabelAndPropertyKey(token uint16, propertyKey string) (int, error) {
	if err := ts.checkOpen(); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return 0, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propertyKey); err != nil {
		return 0, err
	}

	total := 0
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return 0, err
	}
	n, err := ref.NodeCountByLabelAndPropertyKey(token, propertyKey)
	refCheckin()
	if err != nil {
		return 0, err
	}
	total += n

	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return 0, archiveErr
	}
	if archive != nil {
		an, err := archive.NodeCountByLabelAndPropertyKey(token, propertyKey)
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		total += an
	}

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		results[i].count, results[i].err = store.NodeCountByLabelAndPropertyKey(token, propertyKey)
	})
	for _, r := range results {
		if r.err != nil {
			return 0, r.err
		}
		total += r.count
	}
	return total, nil
}

// --- ID enumeration ---

func (ts *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	if err := ts.checkOpen(); err != nil {
		return nil, err
	}
	if err := validateQueryOpts(opts); err != nil {
		return nil, err
	}
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refTyped, err := ref.AllNodeIDs(stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}
	refIDs := refTyped

	// refArchive parity: AllNodeIDs must surface archived nodes whenever
	// AllNodes / GetNode see them. Depth-gated to DepthAll — archive is
	// the coldest tier of reference data; DepthHot/DepthWarm callers
	// asked to exclude it. Same policy as AllNodes.
	var archiveIDs []types.NodeID
	if opts.Depth == DepthAll {
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return nil, archiveErr
		}
		if archive != nil {
			typed, err := archive.AllNodeIDs(stripDepth(opts))
			archiveCheckin()
			if err != nil {
				return nil, err
			}
			archiveIDs = typed
		}
	}

	type result struct {
		ids []types.NodeID
		err error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			results[i].err = err
			return
		}
		defer release()
		typed, err := store.AllNodeIDs(stripDepth(opts))
		results[i].ids = typed
		results[i].err = err
	})

	var slices [][]types.NodeID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}
	if len(archiveIDs) > 0 {
		slices = append(slices, archiveIDs)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.ids) > 0 {
			slices = append(slices, r.ids)
		}
	}

	merged := mergeNodeIDSlices(slices)
	return applyNodeIDPagination(merged, opts), nil
}

// --- ForEach iterators ---
// Sequential shard iteration — one shard at a time, no goroutines, no ID merge.
// This avoids materializing IDs from every shard at once while keeping callbacks
// outside Tiered shard checkouts.

func (ts *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	ids := make([]types.NodeID, 0)
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	if err := ref.ForEachNodeID(func(id types.NodeID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		refCheckin()
		return err
	}
	refCheckin()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}

	// Archive shard (cold-start safe + Close race-free via checkoutArchive).
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		ids = ids[:0]
		err := archive.ForEachNodeID(func(id types.NodeID) bool {
			ids = append(ids, id)
			return true
		})
		archiveCheckin()
		if err != nil {
			return err
		}
		for _, id := range ids {
			if !fn(id) {
				return nil
			}
		}
	}

	// Event shards — sequential, depth=all.
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			return err
		}
		ids = ids[:0]
		err = store.ForEachNodeID(func(id types.NodeID) bool {
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
