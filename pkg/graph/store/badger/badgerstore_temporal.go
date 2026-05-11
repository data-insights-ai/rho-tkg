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

// fetchNodesWithTemporalFilterPage fetches sorted node IDs after the cursor and
// stops only after Limit temporally valid rows have been collected. Cache-miss
// IDs from filterNodeIDsByTemporalPeek must not consume page slots before their
// metadata has been read from Badger.
func (bs *Store) fetchNodesWithTemporalFilterPage(ids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	nodes := make([]*types.Node, 0, capForLimit(opts.Limit))
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
		if opts.Limit > 0 && len(nodes) >= opts.Limit {
			break
		}
	}
	if len(nodes) == 0 {
		return nil, nil
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

// fetchRelsWithTemporalFilterPage is the relationship counterpart to
// fetchNodesWithTemporalFilterPage.
func (bs *Store) fetchRelsWithTemporalFilterPage(ids []types.RelID, opts QueryOpts) ([]*types.Relationship, error) {
	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	rels := make([]*types.Relationship, 0, capForLimit(opts.Limit))
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
		if opts.Limit > 0 && len(rels) >= opts.Limit {
			break
		}
	}
	if len(rels) == 0 {
		return nil, nil
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
