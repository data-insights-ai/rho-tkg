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

// --- Entity reads ---
// O(1) shard resolution: ref probe + timestamp extraction.

func (ts *TieredStore) GetNode(id snowflake.ID) (*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.GetNode(id)
}

func (ts *TieredStore) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return shard.GetRelationship(id)
}

func (ts *TieredStore) GetNodesByIDs(ids []snowflake.ID) ([]*types.Node, error) {
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

func (ts *TieredStore) GetRelationshipsByIDs(ids []snowflake.ID) ([]*types.Relationship, error) {
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

func (ts *TieredStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	// Entity + out/ are co-located in the node's shard.
	shard, err := ts.shardForNodeID(nodeID)
	if err != nil {
		return nil, err
	}
	return shard.OutgoingRelationships(nodeID, typeToken)
}

// OutgoingRelationshipsForNodes batches outgoing relationship queries across shards.
// Groups nodeIDs by shard, delegates per-shard, and merges results.
func (ts *TieredStore) OutgoingRelationshipsForNodes(nodeIDs []snowflake.ID, typeToken uint16) (map[snowflake.ID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	// Partition nodeIDs by shard.
	shardBuckets := make(map[*BadgerStore][]snowflake.ID)
	for _, id := range nodeIDs {
		shard, err := ts.shardForNodeID(id)
		if err != nil {
			return nil, err
		}
		shardBuckets[shard] = append(shardBuckets[shard], id)
	}

	// Delegate per-shard and merge.
	result := make(map[snowflake.ID][]*types.Relationship, len(nodeIDs))
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

func (ts *TieredStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	// Get relIDs from the node's shard inIdx.
	shard, err := ts.shardForNodeID(nodeID)
	if err != nil {
		return nil, err
	}
	relIDs := shard.incomingRelIDs(nodeID, typeToken)

	if len(relIDs) == 0 {
		return nil, nil
	}

	// Fetch each rel entity via checked shard resolution so cross-shard rels
	// stored on shards that have aged to cold remain reachable.
	result := make([]*types.Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		relShard, checkin, err := ts.shardForRelIDChecked(relID)
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(relID)
		checkin()
		if errors.Is(err, ErrRelNotFound) {
			continue // orphan from partial failure
		}
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
	})
	return result, nil
}

// IncomingRelationshipsForNodes batches incoming relationship queries for multiple
// nodes. For each node, relIDs come from the node's shard inIdx; relationship
// entities are fetched via cross-shard resolution (relID timestamp -> shard).
func (ts *TieredStore) IncomingRelationshipsForNodes(nodeIDs []snowflake.ID, typeToken uint16) (map[snowflake.ID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	// Phase 1: collect relIDs per node from each node's shard inIdx.
	type relRef struct {
		nodeID snowflake.ID
		relID  snowflake.ID
	}
	var refs []relRef
	seen := make(map[snowflake.ID]struct{}, len(nodeIDs))

	for _, nid := range nodeIDs {
		if _, dup := seen[nid]; dup {
			continue
		}
		seen[nid] = struct{}{}

		shard, err := ts.shardForNodeID(nid)
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

	// Phase 2: fetch each rel entity via checked shard resolution so cross-shard
	// rels stored on shards that have aged to cold remain reachable.
	result := make(map[snowflake.ID][]*types.Relationship, len(seen))
	for _, ref := range refs {
		relShard, checkin, err := ts.shardForRelIDChecked(ref.relID)
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(ref.relID)
		checkin()
		if errors.Is(err, ErrRelNotFound) {
			continue // orphan from partial failure
		}
		if err != nil {
			return nil, err
		}
		result[ref.nodeID] = append(result[ref.nodeID], r)
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

func (ts *TieredStore) AllNodeIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllNodeIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}

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
			results[i].ids, results[i].err = store.AllNodeIDs(stripDepth(opts))
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
	return applyIDPagination(merged, opts), nil
}

func (ts *TieredStore) AllRelIDs(opts QueryOpts) ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(opts.Depth)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllRelIDs(stripDepth(opts))
	if err != nil {
		return nil, err
	}

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
			results[i].ids, results[i].err = store.AllRelIDs(stripDepth(opts))
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
	return applyIDPagination(merged, opts), nil
}

// --- History reads ---

func (ts *TieredStore) GetNodeVersion(id snowflake.ID, version uint32) (*types.Node, error) {
	shard, checkin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	n, err := shard.GetNodeVersion(id, version)
	if err == nil || !errors.Is(err, ErrVersionNotFound) {
		return n, err
	}

	// If the live entity is on this shard, the local ErrVersionNotFound is
	// authoritative — no need to wake other shards (incl. cold ones).
	if shard.hasNodeID(id) {
		return nil, ErrVersionNotFound
	}

	var found *types.Node
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		n, err := candidate.GetNodeVersion(id, version)
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

func (ts *TieredStore) GetNodeHistory(id snowflake.ID) ([]*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := store.GetNodeHistory(id)
	if err != nil || len(history) > 0 {
		return history, err
	}

	// If the live entity is on this shard, an empty history here is
	// authoritative — the deleted-entity fan-out is unnecessary and would
	// needlessly wake cold shards.
	if store.hasNodeID(id) {
		return nil, nil
	}

	var found []*types.Node
	searchErr := ts.forEachHistoryShard(store, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetNodeHistory(id)
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

func (ts *TieredStore) GetRelVersion(id snowflake.ID, version uint32) (*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	r, err := shard.GetRelVersion(id, version)
	if err == nil || !errors.Is(err, ErrVersionNotFound) {
		return r, err
	}

	// If the live rel entity is on this shard, the local ErrVersionNotFound is
	// authoritative — no need to wake other shards.
	if shard.hasRelID(id) {
		return nil, ErrVersionNotFound
	}

	var found *types.Relationship
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		r, err := candidate.GetRelVersion(id, version)
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

func (ts *TieredStore) GetRelHistory(id snowflake.ID) ([]*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(id)
	if err != nil {
		return nil, err
	}
	defer checkin()
	history, err := shard.GetRelHistory(id)
	if err != nil || len(history) > 0 {
		return history, err
	}

	// If the live rel entity is on this shard, an empty history here is
	// authoritative — skip the deleted-rel fan-out so cold shards are not
	// needlessly opened.
	if shard.hasRelID(id) {
		return nil, nil
	}

	var found []*types.Relationship
	searchErr := ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetRelHistory(id)
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

func (ts *TieredStore) AllNodeHistoryIDs() ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllNodeHistoryIDs()
	if err != nil {
		return nil, err
	}

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
			results[i].ids, results[i].err = store.AllNodeHistoryIDs()
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

	return mergeIDSlices(slices), nil
}

func (ts *TieredStore) AllRelHistoryIDs() ([]snowflake.ID, error) {
	ts.mu.RLock()
	eventShards := ts.eventShardSnapshot(DepthAll)
	ts.mu.RUnlock()

	refIDs, err := ts.refShard.AllRelHistoryIDs()
	if err != nil {
		return nil, err
	}

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
			results[i].ids, results[i].err = store.AllRelHistoryIDs()
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

	return mergeIDSlices(slices), nil
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
func (ts *TieredStore) forEachHistoryShard(skip *BadgerStore, fn func(*BadgerStore) (bool, error)) error {
	if ts.refShard != skip {
		stop, err := fn(ts.refShard)
		if err != nil || stop {
			return err
		}
	}

	archive := ts.refArchive.Load()
	if archive == nil && ts.hasArchiveShard() {
		if err := ts.ensureRefArchive(); err != nil {
			return err
		}
		archive = ts.refArchive.Load()
	}
	if archive != nil && archive != skip {
		stop, err := fn(archive)
		if err != nil || stop {
			return err
		}
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

// --- ForEach iterators ---
// Sequential shard iteration — one shard at a time, no goroutines, no mergeIDSlices.
// This eliminates the O(N) per-shard slice allocations that cause OOM on large graphs.

func (ts *TieredStore) ForEachNodeID(fn func(snowflake.ID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachNodeID(func(id snowflake.ID) bool {
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
	archive := ts.refArchive.Load()
	if archive != nil {
		if err := archive.ForEachNodeID(func(id snowflake.ID) bool {
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
		err = store.ForEachNodeID(func(id snowflake.ID) bool {
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

func (ts *TieredStore) ForEachRelID(fn func(snowflake.ID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachRelID(func(id snowflake.ID) bool {
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

	archive := ts.refArchive.Load()
	if archive != nil {
		if err := archive.ForEachRelID(func(id snowflake.ID) bool {
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
		err = store.ForEachRelID(func(id snowflake.ID) bool {
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

func (ts *TieredStore) ForEachNodeHistoryID(fn func(snowflake.ID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachNodeHistoryID(func(id snowflake.ID) bool {
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

	archive := ts.refArchive.Load()
	if archive != nil {
		if err := archive.ForEachNodeHistoryID(func(id snowflake.ID) bool {
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
		err = store.ForEachNodeHistoryID(func(id snowflake.ID) bool {
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

func (ts *TieredStore) ForEachRelHistoryID(fn func(snowflake.ID) bool) error {
	stopped := false
	if err := ts.refShard.ForEachRelHistoryID(func(id snowflake.ID) bool {
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

	archive := ts.refArchive.Load()
	if archive != nil {
		if err := archive.ForEachRelHistoryID(func(id snowflake.ID) bool {
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
		err = store.ForEachRelHistoryID(func(id snowflake.ID) bool {
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
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
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
		return result[i].InternalID().SnowflakeID() < result[j].InternalID().SnowflakeID()
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
			return nodes[i].InternalID().SnowflakeID() > opts.After
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
			return rels[i].InternalID().SnowflakeID() > opts.After
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
