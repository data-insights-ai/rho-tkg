// Package memory provides memory.Store — the thread-safe in-memory
// implementation of the pkg/graph/store.Store interface. Used as the
// default backend by pkg/graph and also as a building block in tests.
package memory

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func errNilIterationCallback() error {
	return fmt.Errorf("%w: nil iteration callback", ErrInvalidStoreMutation)
}

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index for fast filtering when one exists and a temporal filter is set.
// Store never returns an error.
func (ms *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

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
			if len(rawIDs) == 0 {
				return nil, nil
			}
			ids := storepkg.ToNodeIDs(rawIDs)
			storepkg.SortNodeIDs(ids)
			return ms.nodesByLabelFromIDs(token, ids, opts), nil
		}
	}

	if hfi, ok := ms.hfIndexes[token]; ok {
		var ids []types.NodeID
		temporalQuery := false
		if opts.ValidAt != 0 {
			ids = hfi.CandidatesUpTo(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			ids = hfi.CandidatesUpTo(opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			if len(ids) == 0 {
				return nil, nil
			}
			ids = uniqueNodeIDs(ids)
			storepkg.SortNodeIDs(ids)
			return ms.nodesByLabelFromIDs(token, ids, opts), nil
		}
	}

	// Standard path: collect and sort IDs before fetching entities.
	ids := make([]types.NodeID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	storepkg.SortNodeIDs(ids)

	return ms.nodesByLabelFromIDs(token, ids, opts), nil
}

func (ms *Store) nodesByLabelFromIDs(token uint16, ids []types.NodeID, opts QueryOpts) []*types.Node {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	capHint := len(ids)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := make([]*types.Node, 0, capHint)
	for _, id := range ids {
		if n, ok := ms.nodes[id]; ok {
			if !n.HasLabelTokenRaw(token) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n.DeepCopy())
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// RelationshipsByType returns relationships with the given type token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Store never returns an error.
func (ms *Store) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelTypeToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

	set := ms.typeIdx[token]
	if len(set) == 0 {
		return nil, nil
	}

	ids := make([]types.RelID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	storepkg.SortRelIDs(ids)

	ids = storepkg.PaginateRelIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	capHint := len(ids)
	if opts.Limit > 0 && opts.Limit < capHint {
		capHint = opts.Limit
	}
	result := make([]*types.Relationship, 0, capHint)
	for _, id := range ids {
		if r, ok := ms.rels[id]; ok {
			if !r.HasTypeTokenRaw(token) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
				continue
			}
			result = append(result, r.DeepCopy())
			if opts.Limit > 0 && len(result) >= opts.Limit {
				break
			}
		}
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// NodeCount returns the number of stored nodes.
// Store never returns an error.
func (ms *Store) NodeCount() (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return len(ms.nodes), nil
}

// RelationshipCount returns the number of stored relationships.
// Store never returns an error.
func (ms *Store) RelationshipCount() (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	return len(ms.rels), nil
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
// Store never returns an error.
func (ms *Store) NodeCountByLabel(token uint16) (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	if token == 0 {
		return 0, storecontract.ValidateLabelToken(token)
	}
	return len(ms.labelIdx[token]), nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
// Store never returns an error.
func (ms *Store) RelCountByType(token uint16) (int, error) {
	if ms == nil {
		return 0, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	if err := ms.checkOpenLocked(); err != nil {
		return 0, err
	}
	if token == 0 {
		return 0, storecontract.ValidateRelTypeToken(token)
	}
	return len(ms.typeIdx[token]), nil
}

// AllNodeIDs returns the IDs of all current nodes, with optional temporal
// filtering and pagination. Non-temporal calls return only IDs without entity
// deserialization; temporal calls inspect current entity metadata.
func (ms *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

	if len(ms.nodes) == 0 {
		return nil, nil
	}
	ids := ms.collectNodeIDsByTemporalLocked(opts)
	storepkg.SortNodeIDs(ids)
	ids = storepkg.PaginateNodeIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelIDs returns the IDs of all current relationships, with optional
// temporal filtering and pagination. Non-temporal calls return only IDs without
// entity deserialization; temporal calls inspect current entity metadata.
func (ms *Store) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

	if len(ms.rels) == 0 {
		return nil, nil
	}
	ids := ms.collectRelIDsByTemporalLocked(opts)
	storepkg.SortRelIDs(ids)
	ids = storepkg.PaginateRelIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id := range ms.nodes {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachRelID(fn func(types.RelID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0, len(ms.rels))
	for id := range ms.rels {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachNodeHistoryID iterates over all node IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.NodeID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
// Iteration stops early if fn returns false. No ordering guarantee.
func (ms *Store) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachDeletedNodeID iterates IDs that have history rows but no current
// row — exactly the set the graph layer needs to fold onto narrow indexed
// candidates for history-aware queries. Iteration stops early if fn
// returns false. No ordering guarantee.
func (ms *Store) ForEachDeletedNodeID(fn func(types.NodeID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.NodeID, 0)
	for id := range ms.nodeHistory {
		if _, ok := ms.nodes[id]; ok {
			continue
		}
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// ForEachDeletedRelID is the relationship counterpart of ForEachDeletedNodeID.
func (ms *Store) ForEachDeletedRelID(fn func(types.RelID) bool) error {
	if ms == nil {
		return ErrNilStore
	}
	ms.mu.RLock()
	if err := ms.checkOpenLocked(); err != nil {
		ms.mu.RUnlock()
		return err
	}
	if fn == nil {
		ms.mu.RUnlock()
		return errNilIterationCallback()
	}
	ids := make([]types.RelID, 0)
	for id := range ms.relHistory {
		if _, ok := ms.rels[id]; ok {
			continue
		}
		ids = append(ids, id)
	}
	ms.mu.RUnlock()
	for _, id := range ids {
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
// ascending, starting strictly after `after`. limit 0 returns all remaining.
func (ms *Store) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}

	if len(ms.nodeHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.NodeID, 0, len(ms.nodeHistory))
	for id := range ms.nodeHistory {
		ids = append(ids, id)
	}
	storepkg.SortNodeIDs(ids)
	ids = storepkg.PaginateNodeIDs(ids, types.EntityID(after), limit)
	if len(ids) == 0 {
		return nil, nil
	}
	return ids, nil
}

// AllRelHistoryIDsFrom returns the IDs of relationships with version history,
// sorted ascending, starting strictly after `after`. limit 0 returns all remaining.
func (ms *Store) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidatePagination(types.EntityID(after), limit); err != nil {
		return nil, err
	}

	if len(ms.relHistory) == 0 {
		return nil, nil
	}
	ids := make([]types.RelID, 0, len(ms.relHistory))
	for id := range ms.relHistory {
		ids = append(ids, id)
	}
	storepkg.SortRelIDs(ids)
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
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

	if len(ms.nodes) == 0 {
		return nil, nil
	}

	ids := ms.collectNodeIDsByTemporalLocked(opts)
	storepkg.SortNodeIDs(ids)

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
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}

	if len(ms.rels) == 0 {
		return nil, nil
	}

	ids := ms.collectRelIDsByTemporalLocked(opts)
	storepkg.SortRelIDs(ids)

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

// GetNodesByIDs returns nodes for every requested ID.
// Missing IDs return ErrNodeNotFound. Results are sorted by snowflake.ID.
func (ms *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}

	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, ok := ms.nodes[id]
		if !ok {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, ErrNodeNotFound)
		}
		result = append(result, n.DeepCopy())
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(result)
	return result, nil
}

// GetRelationshipsByIDs returns relationships for every requested ID.
// Missing IDs return ErrRelNotFound. Results are sorted by snowflake.ID.
func (ms *Store) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if ms == nil {
		return nil, ErrNilStore
	}
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	if err := ms.checkOpenLocked(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateRelID(id); err != nil {
			return nil, err
		}
	}

	result := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, ok := ms.rels[id]
		if !ok {
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, ErrRelNotFound)
		}
		result = append(result, r.DeepCopy())
	}
	if len(result) == 0 {
		return nil, nil
	}
	storepkg.SortRelsByID(result)
	return result, nil
}

// --- Temporal filtering helpers ---

// collectNodeIDsByTemporalLocked collects current node IDs, applying active
// temporal filters before sorting so large temporal scans sort only candidates
// that can appear in the page. Caller must hold ms.mu (read or write).
func (ms *Store) collectNodeIDsByTemporalLocked(opts QueryOpts) []types.NodeID {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	ids := make([]types.NodeID, 0, len(ms.nodes))
	for id, n := range ms.nodes {
		if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), n.Temporal(), opts) {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}

// collectRelIDsByTemporalLocked is the relationship counterpart to
// collectNodeIDsByTemporalLocked. Caller must hold ms.mu (read or write).
func (ms *Store) collectRelIDsByTemporalLocked(opts QueryOpts) []types.RelID {
	hasTemporal := storepkg.HasTemporalFilter(opts)
	ids := make([]types.RelID, 0, len(ms.rels))
	for id, r := range ms.rels {
		if hasTemporal && !storepkg.MatchesTemporalFilter(id.SnowflakeID(), r.Temporal(), opts) {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	return ids
}
