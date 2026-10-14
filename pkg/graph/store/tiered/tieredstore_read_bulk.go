package tiered

import (
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Bulk queries ---

// AllNodes returns every node across every shard (reference, archive at
// DepthAll, event shards at opts.Depth). Memory caveat (BACKLOG 19j): NOT a
// streaming door; every shard's full result is materialized concurrently, so
// peak RAM is O(total nodes), and opts.Limit/opts.After only trim the merged
// slice afterwards. The graph layer's ForEachByLabel/IterByLabel do not help
// (tiered has no streaming capability). ForEachNodeID is O(one-shard) but
// yields bare IDs without depth filtering. Prefer label-scoped queries.
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

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return nil, err
	}
	refNodes, err := ref.AllNodes(stripDepth(opts))
	refCheckin()
	if err != nil {
		return nil, err
	}

	// refArchive parity: archived nodes stay GetNode-addressable, so bulk scans
	// include them, but only at DepthAll (the coldest tier; DepthHot/DepthWarm
	// callers excluded it). checkoutArchive lazy-opens on cold start. refShard is
	// queried for every Depth (reference data is not Depth-tiered).
	var archiveNodes []*types.Node
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
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
	move.unlock()

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
	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
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
	ts.refArchiveGapHook()
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
	move.unlock()

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		// A closed shard answers from the counts it recorded on its way out,
		// rather than being reopened to add up numbers it already knew.
		if n, ok := es.countFromCache(func(c *shardCounts) int { return c.nodes }); ok {
			results[i].count = n
			return
		}
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
	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
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

	ts.refArchiveGapHook()
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
	move.unlock()

	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	type result struct {
		count int
		err   error
	}
	results := make([]result, len(eventShards))
	queryEventShards(eventShards, func(i int, es *EventShard) {
		// A closed shard answers from the counts it recorded on its way out,
		// rather than being reopened to add up numbers it already knew. This is the fold AllLabelCounts runs once per label, so a reopen here was paid |labels| times over.
		if n, ok := es.countFromCache(func(c *shardCounts) int { return c.byLabel[token] }); ok {
			results[i].count = n
			return
		}
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
	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
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

	ts.refArchiveGapHook()
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
	move.unlock()

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

// AllNodeIDs has AllNodes's memory caveat (BACKLOG 19j): every shard's ID set
// is materialized before pagination. ForEachNodeID is the O(one-shard)
// streaming alternative (no depth filtering).
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

	move := ts.lockMoveRead() // a node moved mid-read would show twice or not at all
	defer move.unlock()
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

	// refArchive parity: archived nodes surface at DepthAll only, as in AllNodes.
	var archiveIDs []types.NodeID
	if opts.Depth == DepthAll {
		ts.refArchiveGapHook()
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
	move.unlock()

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
// Sequential, one shard at a time, no goroutines or ID merge; callbacks run
// outside shard checkouts.

func (ts *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	// Reference and archive IDs are collected under one moveMu read hold and
	// emitted after it (fn may call back), so a moved entity is emitted once.
	ids := make([]types.NodeID, 0)
	collect := func(id types.NodeID) bool {
		ids = append(ids, id)
		return true
	}
	move := ts.lockMoveRead()
	defer move.unlock()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	err = ref.ForEachNodeID(collect)
	refCheckin()
	if err != nil {
		return err
	}

	// Archive shard (cold-start safe + Close race-free via checkoutArchive).
	ts.refArchiveGapHook()
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return archiveErr
	}
	if archive != nil {
		err := archive.ForEachNodeID(collect)
		archiveCheckin()
		if err != nil {
			return err
		}
	}
	move.unlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}

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
