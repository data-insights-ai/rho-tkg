package graph

import (
	"errors"
	"sort"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// stripDepth returns opts with the Depth field cleared.
// Used when forwarding to single-shard queries (Depth is TieredStore-level).
// All other fields — Limit, After, ValidAt, ValidStart, ValidEnd — are preserved
// so per-shard calls respect pagination and temporal filters.
func stripDepth(opts QueryOpts) QueryOpts {
	opts.Depth = 0
	return opts
}

// Internal slice converters bridging typed entity IDs and raw snowflake.ID
// for cross-shard merge helpers (mergeIDSlices, applyIDPagination).
func nodeIDsToRaw(ids []types.NodeID) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]snowflake.ID, len(ids))
	for i, id := range ids {
		out[i] = id.SnowflakeID()
	}
	return out
}

func relIDsToRaw(ids []types.RelID) []snowflake.ID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]snowflake.ID, len(ids))
	for i, id := range ids {
		out[i] = id.SnowflakeID()
	}
	return out
}

func rawToNodeIDs(ids []snowflake.ID) []types.NodeID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out
}

func rawToRelIDs(ids []snowflake.ID) []types.RelID {
	if len(ids) == 0 {
		return nil
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out
}

// --- Entity reads ---
// O(1) shard resolution: ref probe + timestamp extraction.

func (ts *TieredStore) GetNode(nid types.NodeID) (*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.GetNode(nid)
}

func (ts *TieredStore) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	shard, err := ts.shardForRelID(rid)
	if err != nil {
		return nil, err
	}
	return shard.GetRelationship(rid)
}

func (ts *TieredStore) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	var result []*types.Node
	for _, id := range ids {
		n, err := ts.GetNode(id)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, nil
}

func (ts *TieredStore) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	var result []*types.Relationship
	for _, id := range ids {
		r, err := ts.GetRelationship(id)
		if errors.Is(err, ErrRelNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, nil
}

// --- Label/type queries ---

func (ts *TieredStore) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	if ts.ontology.ClassifyByToken(token) == ClassReference {
		return ts.refShard.NodesByLabel(token, stripDepth(opts))
	}
	// Event label: fan out across all event shards matching depth.
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
		err   error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].nodes, results[i].err = store.NodesByLabel(token, stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Node
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

func (ts *TieredStore) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refRels, err := ts.refShard.RelationshipsByType(token, stripDepth(opts))
	if err != nil {
		return nil, err
	}

	type result struct {
		rels []*types.Relationship
		err  error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].rels, results[i].err = store.RelationshipsByType(token, stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Relationship
	if len(refRels) > 0 {
		slices = append(slices, refRels)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.rels) > 0 {
			slices = append(slices, r.rels)
		}
	}

	merged := mergeRelSlices(slices)
	return applyRelPagination(merged, opts), nil
}

// --- Bulk queries ---

func (ts *TieredStore) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refNodes, err := ts.refShard.AllNodes(stripDepth(opts))
	if err != nil {
		return nil, err
	}

	type result struct {
		nodes []*types.Node
		err   error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
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

func (ts *TieredStore) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refRels, err := ts.refShard.AllRelationships(stripDepth(opts))
	if err != nil {
		return nil, err
	}

	type result struct {
		rels []*types.Relationship
		err  error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].rels, results[i].err = store.AllRelationships(stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Relationship
	if len(refRels) > 0 {
		slices = append(slices, refRels)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.rels) > 0 {
			slices = append(slices, r.rels)
		}
	}

	merged := mergeRelSlices(slices)
	return applyRelPagination(merged, opts), nil
}

// --- Adjacency queries ---

func (ts *TieredStore) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	// Entity + out/ are co-located in the node's shard.
	shard, err := ts.shardForNodeID(nid)
	if err != nil {
		return nil, err
	}
	return shard.OutgoingRelationships(nid, typeToken)
}

// OutgoingRelationshipsForNodes batches outgoing relationship queries across shards.
// Groups nodeIDs by shard, delegates per-shard, and merges results.
func (ts *TieredStore) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	// Partition nodeIDs by shard.
	shardBuckets := make(map[*BadgerStore][]types.NodeID)
	for _, id := range nodeIDs {
		shard, err := ts.shardForNodeID(id)
		if err != nil {
			return nil, err
		}
		shardBuckets[shard] = append(shardBuckets[shard], id)
	}

	// Delegate per-shard and merge.
	result := make(map[types.NodeID][]*types.Relationship, len(nodeIDs))
	for shard, bucket := range shardBuckets {
		m, err := shard.OutgoingRelationshipsForNodes(bucket, typeToken)
		if err != nil {
			return nil, err
		}
		for nid, rels := range m {
			result[nid] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

func (ts *TieredStore) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	nodeID := nid.SnowflakeID()

	// Get relIDs from the node's shard inIdx.
	shard, err := ts.shardForNodeID(nid)
	if err != nil {
		return nil, err
	}
	relIDs := shard.incomingRelIDs(nodeID, typeToken)

	if len(relIDs) == 0 {
		return nil, nil
	}

	// Fetch each rel entity via shard resolution (relID timestamp -> O(1) per entity).
	result := make([]*types.Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		relShard, err := ts.shardForRelID(types.RelID(relID))
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(types.RelID(relID))
		if errors.Is(err, ErrRelNotFound) {
			continue // orphan from partial failure
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result, nil
}

// IncomingRelationshipsForNodes batches incoming relationship queries for multiple
// nodes. For each node, relIDs come from the node's shard inIdx; relationship
// entities are fetched via cross-shard resolution (relID timestamp -> shard).
func (ts *TieredStore) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: collect relIDs per node from each node's shard inIdx.
	type relRef struct {
		nodeID snowflake.ID
		relID  snowflake.ID
	}
	var refs []relRef
	seen := make(map[snowflake.ID]struct{}, len(typedNodeIDs))

	for _, tnid := range typedNodeIDs {
		nid := tnid.SnowflakeID()
		if _, dup := seen[nid]; dup {
			continue
		}
		seen[nid] = struct{}{}

		shard, err := ts.shardForNodeID(tnid)
		if err != nil {
			return nil, err
		}
		relIDs := shard.incomingRelIDs(nid, typeToken)
		for _, rid := range relIDs {
			refs = append(refs, relRef{nodeID: nid, relID: rid})
		}
	}

	if len(refs) == 0 {
		return nil, nil
	}

	// Phase 2: fetch each rel entity via shard resolution.
	result := make(map[types.NodeID][]*types.Relationship, len(seen))
	for _, ref := range refs {
		relShard, err := ts.shardForRelID(types.RelID(ref.relID))
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(types.RelID(ref.relID))
		if errors.Is(err, ErrRelNotFound) {
			continue // orphan from partial failure
		}
		if err != nil {
			return nil, err
		}
		key := types.NodeID(ref.nodeID)
		result[key] = append(result[key], r)
	}

	// Sort per-node slices for deterministic output.
	for nid := range result {
		sortRelsByID(result[nid])
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// --- Counts ---

func (ts *TieredStore) NodeCount() (int, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.NodeCount()
	if err != nil {
		return 0, err
	}
	total += n

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

func (ts *TieredStore) RelationshipCount() (int, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.RelationshipCount()
	if err != nil {
		return 0, err
	}
	total += n

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.RelationshipCount()
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (ts *TieredStore) NodeCountByLabel(token uint16) (int, error) {
	if ts.ontology.ClassifyByToken(token) == ClassReference {
		return ts.refShard.NodeCountByLabel(token)
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

func (ts *TieredStore) RelCountByType(token uint16) (int, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	total := 0
	n, err := ts.refShard.RelCountByType(token)
	if err != nil {
		return 0, err
	}
	total += n

	for _, es := range eventShards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return 0, err
		}
		n, err := store.RelCountByType(token)
		es.checkinStore()
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// --- Property indexes ---

func (ts *TieredStore) NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error) {
	if ts.ontology.ClassifyByToken(labelToken) == ClassReference {
		return ts.refShard.NodesByLabelAndProperty(labelToken, key, value, stripDepth(opts))
	}
	// Event label: fan out across all event shards matching depth.
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	type result struct {
		nodes []*types.Node
		err   error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			results[i].nodes, results[i].err = store.NodesByLabelAndProperty(labelToken, key, value, stripDepth(opts))
		}(i, es)
	}
	wg.Wait()

	var slices [][]*types.Node
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

// --- ID enumeration ---

func (ts *TieredStore) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refTyped, err := ts.refShard.AllNodeIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}
	refIDs := nodeIDsToRaw(refTyped)

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
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

func (ts *TieredStore) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refTyped, err := ts.refShard.AllRelIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}
	refIDs := relIDsToRaw(refTyped)

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			typed, err := store.AllRelIDs(stripDepth(opts))
			results[i].ids = relIDsToRaw(typed)
			results[i].err = err
		}(i, es)
	}
	wg.Wait()

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
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
	return rawToRelIDs(paginated), nil
}

// --- History reads ---

func (ts *TieredStore) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	shard, err := ts.shardForNodeID(nid)
	if err != nil {
		return nil, err
	}
	return shard.GetNodeVersion(nid, version)
}

func (ts *TieredStore) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.GetNodeHistory(nid)
}

func (ts *TieredStore) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	shard, err := ts.shardForRelID(rid)
	if err != nil {
		return nil, err
	}
	return shard.GetRelVersion(rid, version)
}

func (ts *TieredStore) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	shard, err := ts.shardForRelID(rid)
	if err != nil {
		return nil, err
	}
	return shard.GetRelHistory(rid)
}

func (ts *TieredStore) AllNodeHistoryIDs() ([]types.NodeID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refTyped, err := ts.refShard.AllNodeHistoryIDs()
	if err != nil {
		return nil, err
	}
	refIDs := nodeIDsToRaw(refTyped)

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			typed, err := store.AllNodeHistoryIDs()
			results[i].ids = nodeIDsToRaw(typed)
			results[i].err = err
		}(i, es)
	}
	wg.Wait()

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.ids) > 0 {
			slices = append(slices, r.ids)
		}
	}

	return rawToNodeIDs(mergeIDSlices(slices)), nil
}

func (ts *TieredStore) AllRelHistoryIDs() ([]types.RelID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refTyped, err := ts.refShard.AllRelHistoryIDs()
	if err != nil {
		return nil, err
	}
	refIDs := relIDsToRaw(refTyped)

	type result struct {
		ids []snowflake.ID
		err error
	}
	results := make([]result, len(eventShards))
	var wg sync.WaitGroup
	for i, es := range eventShards {
		wg.Add(1)
		go func(i int, es *eventShard) {
			defer wg.Done()
			store, err := es.checkoutStore(ts)
			if err != nil {
				results[i].err = err
				return
			}
			defer es.checkinStore()
			typed, err := store.AllRelHistoryIDs()
			results[i].ids = relIDsToRaw(typed)
			results[i].err = err
		}(i, es)
	}
	wg.Wait()

	var slices [][]snowflake.ID
	if len(refIDs) > 0 {
		slices = append(slices, refIDs)
	}
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		if len(r.ids) > 0 {
			slices = append(slices, r.ids)
		}
	}

	return rawToRelIDs(mergeIDSlices(slices)), nil
}

// --- ForEach iterators ---
// Sequential shard iteration — one shard at a time, no goroutines, no mergeIDSlices.
// This eliminates the O(N) per-shard slice allocations that cause OOM on large graphs.

func (ts *TieredStore) ForEachNodeID(fn func(types.NodeID) bool) error {
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

	// Archive shard (if open).
	ts.archiveMu.Lock()
	archive := ts.refArchive
	ts.archiveMu.Unlock()
	if archive != nil {
		if err := archive.ForEachNodeID(func(id types.NodeID) bool {
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

func (ts *TieredStore) ForEachRelID(fn func(types.RelID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachRelID(func(id types.RelID) bool {
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

	ts.archiveMu.Lock()
	archive := ts.refArchive
	ts.archiveMu.Unlock()
	if archive != nil {
		if err := archive.ForEachRelID(func(id types.RelID) bool {
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
	}

	ts.mu.RLock()
	shards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	for _, es := range shards {
		store, err := es.checkoutStore(ts)
		if err != nil {
			return err
		}
		err = store.ForEachRelID(func(id types.RelID) bool {
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

func (ts *TieredStore) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
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

	ts.archiveMu.Lock()
	archive := ts.refArchive
	ts.archiveMu.Unlock()
	if archive != nil {
		if err := archive.ForEachNodeHistoryID(func(id types.NodeID) bool {
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

func (ts *TieredStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
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

	ts.archiveMu.Lock()
	archive := ts.refArchive
	ts.archiveMu.Unlock()
	if archive != nil {
		if err := archive.ForEachRelHistoryID(func(id types.RelID) bool {
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

// --- Merge helpers ---
// Standard k-way merge of pre-sorted slices. For Phase 3a with 2 shards,
// this is a simple 2-way merge.

func mergeNodeSlices(slices [][]*types.Node) []*types.Node {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	// 2-way merge for the common case.
	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Node, 0, total)

	// Flatten and sort (simple approach for small shard counts).
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result
}

func mergeRelSlices(slices [][]*types.Relationship) []*types.Relationship {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]*types.Relationship, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result
}

func mergeIDSlices(slices [][]snowflake.ID) []snowflake.ID {
	if len(slices) == 0 {
		return nil
	}
	if len(slices) == 1 {
		return slices[0]
	}

	total := 0
	for _, s := range slices {
		total += len(s)
	}
	result := make([]snowflake.ID, 0, total)
	for _, s := range slices {
		result = append(result, s...)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i] < result[j]
	})
	return result
}

// --- Pagination helpers ---
// Apply After/Limit from QueryOpts to already-merged, sorted slices.

func applyNodePagination(nodes []*types.Node, opts QueryOpts) []*types.Node {
	if opts.After != 0 {
		i := sort.Search(len(nodes), func(i int) bool {
			return nodes[i].ID().SnowflakeID() > opts.After
		})
		nodes = nodes[i:]
	}
	if opts.Limit > 0 && len(nodes) > opts.Limit {
		nodes = nodes[:opts.Limit]
	}
	if len(nodes) == 0 {
		return nil
	}
	return nodes
}

func applyRelPagination(rels []*types.Relationship, opts QueryOpts) []*types.Relationship {
	if opts.After != 0 {
		i := sort.Search(len(rels), func(i int) bool {
			return rels[i].ID().SnowflakeID() > opts.After
		})
		rels = rels[i:]
	}
	if opts.Limit > 0 && len(rels) > opts.Limit {
		rels = rels[:opts.Limit]
	}
	if len(rels) == 0 {
		return nil
	}
	return rels
}

func applyIDPagination(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.After != 0 {
		i := sort.Search(len(ids), func(i int) bool {
			return ids[i] > opts.After
		})
		ids = ids[i:]
	}
	if opts.Limit > 0 && len(ids) > opts.Limit {
		ids = ids[:opts.Limit]
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}
