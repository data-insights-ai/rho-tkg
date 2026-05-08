package tiered

import (
	"errors"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Entity reads ---
// O(1) shard resolution: ref probe + timestamp extraction.

func (ts *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return store.GetNode(nid)
}

func (ts *Store) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return shard.GetRelationship(rid)
}

func (ts *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
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

func (ts *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
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

// --- Adjacency queries ---

func (ts *Store) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	// Entity + out/ are co-located in the node's shard. Use the checked
	// resolver so a cold owner stays pinned for the duration of the read.
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	defer checkin()
	return shard.OutgoingRelationships(nid, typeToken)
}

// OutgoingRelationshipsForNodes batches outgoing relationship queries across shards.
// Groups nodeIDs by shard, delegates per-shard, and merges results.
//
// Each owner shard is checked out via shardForNodeIDChecked so cold shards
// remain pinned for the per-shard delegated read.
func (ts *Store) OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(nodeIDs) == 0 {
		return nil, nil
	}

	// Partition nodeIDs by shard. Track checkin functions so each owner shard
	// can be released once we are done with it.
	shardBuckets := make(map[*BadgerStore][]types.NodeID)
	checkins := make(map[*BadgerStore][]func())
	releaseAll := func() {
		for _, fns := range checkins {
			for _, fn := range fns {
				fn()
			}
		}
	}
	for _, id := range nodeIDs {
		shard, checkin, err := ts.shardForNodeIDChecked(id)
		if err != nil {
			releaseAll()
			return nil, err
		}
		shardBuckets[shard] = append(shardBuckets[shard], id)
		checkins[shard] = append(checkins[shard], checkin)
	}
	defer releaseAll()

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

func (ts *Store) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	// Get relIDs from the node's shard inIdx. The owner is checked out so a
	// cold node shard cannot be closed mid-read.
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return nil, err
	}
	relIDs := shard.IncomingRelIDs(nid.SnowflakeID(), typeToken)
	checkin()

	if len(relIDs) == 0 {
		return nil, nil
	}

	// Fetch each rel entity via checked shard resolution so cross-shard rels
	// stored on shards that have aged to cold remain reachable.
	result := make([]*types.Relationship, 0, len(relIDs))
	for _, relID := range relIDs {
		rid := types.RelID(relID)
		relShard, checkin, err := ts.shardForRelIDChecked(rid)
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(rid)
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
		return result[i].ID().SnowflakeID() < result[j].ID().SnowflakeID()
	})
	return result, nil
}

// IncomingRelationshipsForNodes batches incoming relationship queries for multiple
// nodes. For each node, relIDs come from the node's shard inIdx; relationship
// entities are fetched via cross-shard resolution (relID timestamp -> shard).
func (ts *Store) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
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

		// Check out the node's owner shard so a cold owner stays pinned for
		// the inIdx scan; release immediately afterwards because the rel
		// fetch below uses its own checkout against the rel-owner shard.
		shard, checkin, err := ts.shardForNodeIDChecked(tnid)
		if err != nil {
			return nil, err
		}
		relIDs := shard.IncomingRelIDs(nid, typeToken)
		checkin()
		for _, rid := range relIDs {
			refs = append(refs, relRef{nodeID: nid, relID: rid})
		}
	}

	if len(refs) == 0 {
		return nil, nil
	}

	// Phase 2: fetch each rel entity via checked shard resolution so cross-shard
	// rels stored on shards that have aged to cold remain reachable.
	result := make(map[types.NodeID][]*types.Relationship, len(seen))
	for _, ref := range refs {
		relShard, checkin, err := ts.shardForRelIDChecked(types.RelID(ref.relID))
		if err != nil {
			return nil, err
		}
		r, err := relShard.GetRelationship(types.RelID(ref.relID))
		checkin()
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
		storepkg.SortRelsByID(result[nid])
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}
