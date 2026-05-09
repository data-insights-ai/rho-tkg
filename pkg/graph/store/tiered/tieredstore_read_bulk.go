package tiered

import (
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Bulk queries ---

func (ts *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refNodes, err := ts.refShard.AllNodes(stripDepth(opts))
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
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *EventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].nodes, results[i].err = store.AllNodes(stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

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
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.NodeCount()
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

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.NodeCount()
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (ts *Store) NodeCountByLabel(token uint16) (int, error) {
	if ts.ontology.ClassifyByToken(token) == ClassReference {
		n, err := ts.refShard.NodeCountByLabel(token)
		if err != nil {
			return 0, err
		}
		archive, archiveCheckin, archiveErr := ts.checkoutArchive()
		if archiveErr != nil {
			return 0, archiveErr
		}
		if archive == nil {
			return n, nil
		}
		an, err := archive.NodeCountByLabel(token)
		archiveCheckin()
		if err != nil {
			return 0, err
		}
		return n + an, nil
	}
	// Event label: sum across all event shards.
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.NodeCountByLabel(token)
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// --- ID enumeration ---

func (ts *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refTyped, err := ts.refShard.AllNodeIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}
	refIDs := nodeIDsToRaw(refTyped)

	// refArchive parity: AllNodeIDs must surface archived nodes whenever
	// AllNodes / GetNode see them. Depth-gated to DepthAll — archive is
	// the coldest tier of reference data; DepthHot/DepthWarm callers
	// asked to exclude it. Same policy as AllNodes.
	var archiveIDs []snowflake.ID
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
			archiveIDs = nodeIDsToRaw(typed)
		}
	}

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *EventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			typed, err := store.AllNodeIDs(stripDepth(opts))
			results[i].ids = nodeIDsToRaw(typed)
			results[i].err = err
		}(i, es)
	}
	wg.Wait()

	var slices [][]snowflake.ID
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

	merged := mergeIDSlices(slices)
	paginated := applyIDPagination(merged, opts)
	return rawToNodeIDs(paginated), nil
}

// --- ForEach iterators ---
// Sequential shard iteration — one shard at a time, no goroutines, no mergeIDSlices.
// This eliminates the O(N) per-shard slice allocations that cause OOM on large graphs.

func (ts *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachNodeID(func(id types.NodeID) bool {
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

	// Archive shard (cold-start safe + Close race-free via checkoutArchive).
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		err := archive.ForEachNodeID(func(id types.NodeID) bool {
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

	// Event shards — sequential, depth=all.
	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		err = store.ForEachNodeID(func(id types.NodeID) bool {
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

