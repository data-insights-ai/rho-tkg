// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index for fast filtering when one exists and a temporal filter is set.
// Store never returns an error.
func (ms *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.labelIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	// Temporal index fast path: use interval index if one exists for this label.
	// When a temporal query is requested (ValidAt or interval), the index result
	// is always authoritative — nil means 0 matches, not "index not consulted."
	// We must not fall through to the full label scan in that case.
	if ti, ok := ms.temporalIndexes[token]; ok {
		// temporalIndex returns raw snowflake.IDs (Tier D handoff).
		var rawIDs []snowflake.ID
		temporalQuery := false
		if opts.ValidAt != 0 {
			rawIDs = ti.QueryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			rawIDs = ti.QueryOverlap(opts.ValidStart, opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			rawIDs = storepkg.PaginateIDs(rawIDs, opts.After, opts.Limit)
			if len(rawIDs) == 0 {
				return nil, nil
			}
			result := make([]*types.Node, 0, len(rawIDs))
			for _, id := range rawIDs {
				if n, ok := ms.nodes[types.NodeID(id)]; ok {
					result = append(result, n.DeepCopy())
				}
			}
			return result, nil
		}
	}

	// Standard path: collect and sort IDs before fetching entities.
	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter: read in-memory entity pointer (no deep-copy).
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	return result, nil
}

// RelationshipsByType returns relationships with the given type token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Store never returns an error.
func (ms *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	return result, nil
}

// NodeCount returns the number of stored nodes.
// Store never returns an error.
func (ms *Store) NodeCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.nodes), nil
}

// RelationshipCount returns the number of stored relationships.
// Store never returns an error.
func (ms *Store) RelationshipCount() (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.rels), nil
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
// Store never returns an error.
func (ms *Store) NodeCountByLabel(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.labelIdx[token]), nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
// Store never returns an error.
func (ms *Store) RelCountByType(token uint16) (int, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return len(ms.typeIdx[token]), nil
}

// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy.
func (ms *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}
	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy.
func (ms *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}
	ids := make([]types.RelID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.nodes {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachRelID(fn func(types.RelID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.rels {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachNodeHistoryID iterates over all node IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.nodeHistory {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	for id := range ms.relHistory {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
// Thin wrapper that delegates to AllNodeHistoryIDsFrom(0, 0).
func (ms *Store) AllNodeHistoryIDs() ([]types.NodeID, error) {
	return ms.AllNodeHistoryIDsFrom(types.NodeID(0), 0)
}

// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
// Thin wrapper that delegates to AllRelHistoryIDsFrom(0, 0).
func (ms *Store) AllRelHistoryIDs() ([]types.RelID, error) {
	return ms.AllRelHistoryIDsFrom(types.RelID(0), 0)
}

// AllNodeHistoryIDsFrom returns the IDs of nodes with version history, sorted
// ascending, starting strictly after `after`. limit ≤ 0 returns all remaining.
func (ms *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodeHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.NodeID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateNodeIDs(ids, types.EntityID(after), limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelHistoryIDsFrom returns the IDs of relationships with version history,
// sorted ascending, starting strictly after `after`. limit ≤ 0 returns all remaining.
func (ms *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.relHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.RelID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = storepkg.PaginateRelIDs(ids, types.EntityID(after), limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// --- Bulk queries ---

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.nodes) == 0 {
		return nil, nil
	}

	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterNodeIDsByTemporal(ids, opts)

	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	return result, nil
}

// AllRelationships returns all stored relationships, with optional pagination and temporal filtering.
// Results are sorted by snowflake.ID for deterministic output.
func (ms *Store) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if len(ms.rels) == 0 {
		return nil, nil
	}

	ids := make([]types.RelID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter.
	ids = ms.filterRelIDsByTemporal(ids, opts)

	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	return result, nil
}

// GetNodesByIDs returns nodes matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (ms *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			result = append(result, n.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (ms *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			result = append(result, r.DeepCopy())
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(result)
	return result, nil
}

// --- Temporal filtering helpers ---

// filterNodeIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy, no allocation per entity.
// Caller must hold ms.mu (read or write).
func (ms *Store) filterNodeIDsByTemporal(ids []types.NodeID, opts QueryOpts) []types.NodeID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		n, ok := ms.nodes[id]
		if !ok {
			continue
		}
		if storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// filterRelIDsByTemporal removes IDs that don't match the temporal filter in opts.
// Reads the in-memory entity pointer directly — no deep-copy.
// Caller must hold ms.mu (read or write).
func (ms *Store) filterRelIDsByTemporal(ids []types.RelID, opts QueryOpts) []types.RelID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := ids[:0] // reuse backing array
	for _, id := range ids {
		r, ok := ms.rels[id]
		if !ok {
			continue
		}
		if storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
			filtered = append(filtered, id)
		}
	}
	return filtered
}
