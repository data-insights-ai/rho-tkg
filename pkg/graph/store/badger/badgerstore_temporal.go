// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"errors"
	"fmt"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// filterNodeIDsByTemporalPeek removes IDs that don't match the temporal filter
// using Peek (zero allocation for cache hits). Cache misses are kept as candidates
// to be post-filtered after GetNode.
func (bs *Store) filterNodeIDsByTemporalPeek(ids []types.NodeID, opts QueryOpts) []types.NodeID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := make([]types.NodeID, 0, len(ids))
	for _, nid := range ids {
		id := nid.SnowflakeID()
		v, status := bs.nodeCache.Peek(id)
		switch status {
		case indexpkg.CacheHit:
			if storepkg.MatchesTemporalFilter(id, v.Temporal(), opts) {
				filtered = append(filtered, nid)
			}
		case indexpkg.CacheDeleted:
			// skip — entity is deleted
		case indexpkg.CacheMiss:
			// Keep as candidate — will be post-filtered after GetNode.
			filtered = append(filtered, nid)
		}
	}
	return filtered
}

// filterRelIDsByTemporalPeek removes IDs that don't match the temporal filter
// using Peek. Cache misses are kept as candidates.
func (bs *Store) filterRelIDsByTemporalPeek(ids []types.RelID, opts QueryOpts) []types.RelID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := make([]types.RelID, 0, len(ids))
	for _, rid := range ids {
		id := rid.SnowflakeID()
		v, status := bs.relCache.Peek(id)
		switch status {
		case indexpkg.CacheHit:
			if storepkg.MatchesTemporalFilter(id, v.Temporal(), opts) {
				filtered = append(filtered, rid)
			}
		case indexpkg.CacheDeleted:
			// skip
		case indexpkg.CacheMiss:
			filtered = append(filtered, rid)
		}
	}
	return filtered
}

// fetchNodesWithTemporalFilter fetches nodes by ID and post-filters for temporal
// match. Cache-miss candidates that were speculatively included are filtered here.
func (bs *Store) fetchNodesWithTemporalFilter(ids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	nodes := make([]*types.Node, 0, len(ids))
	for _, nid := range ids {
		id := nid.SnowflakeID()
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: query node %d: %w", id, err)
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id, n.Temporal(), opts) {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// fetchRelsWithTemporalFilter fetches relationships by ID and post-filters for
// temporal match.
func (bs *Store) fetchRelsWithTemporalFilter(ids []types.RelID, opts QueryOpts) ([]*types.Relationship, error) {
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	rels := make([]*types.Relationship, 0, len(ids))
	for _, rid := range ids {
		id := rid.SnowflakeID()
		r, err := bs.GetRelationship(rid)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", id, err)
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(id, r.Temporal(), opts) {
			continue
		}
		rels = append(rels, r)
	}
	return rels, nil
}

// collectNodeLabelTokens returns all label token values from a node.
func collectNodeLabelTokens(n *types.Node) []uint16 {
	tokens := n.AllLabelTokens()
	result := make([]uint16, len(tokens))
	for i, t := range tokens {
		result[i] = t.Value()
	}
	return result
}
