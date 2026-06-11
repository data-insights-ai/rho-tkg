package badger

import (
	"errors"
	"fmt"
	"log/slog"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Node-side query methods (R5-F9 split out from badgerstore_node.go).

func (bs *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(token); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()

	// Temporal index fast path: avoids iterating the full label set when a
	// temporal index exists and a temporal filter is active.
	// When a temporal query is requested, the index result is always authoritative
	// — nil means 0 matches, not "index not consulted." Do not fall through to
	// the full label scan in that case.
	if ti, ok := bs.temporalIndexes[token]; ok && !ti.Building {
		var ids []snowflake.ID
		temporalQuery := false
		if opts.ValidAt != 0 {
			ids = ti.QueryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			ids = ti.QueryOverlap(opts.ValidStart, opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			bs.idxMu.RUnlock()
			if len(ids) == 0 {
				return nil, nil
			}
			nids := storepkg.ToNodeIDs(ids)
			storepkg.SortNodeIDs(nids)
			return bs.fetchNodesByLabelIDs(token, nids, opts)
		}
	}

	if hfi, ok := bs.hfIndexes[token]; ok && !hfi.IsBuilding() {
		var nids []types.NodeID
		temporalQuery := false
		if opts.ValidAt != 0 {
			nids = hfi.CandidatesUpTo(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			nids = hfi.CandidatesUpTo(opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			bs.idxMu.RUnlock()
			if len(nids) == 0 {
				return nil, nil
			}
			nids = uniqueNodeIDs(nids)
			storepkg.SortNodeIDs(nids)
			return bs.fetchNodesByLabelIDs(token, nids, opts)
		}
	}

	nids, idErr := bs.labelNodeIDsSnapshotLocked(token)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}
	if len(nids) == 0 {
		return nil, nil
	}

	storepkg.SortNodeIDs(nids)

	// Temporal pre-filter via Peek (zero allocation for cache hits).
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

	return bs.fetchNodesByLabelIDs(token, nids, opts)
}

func (bs *Store) fetchNodesByLabelIDs(token uint16, ids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := storepkg.HasTemporalFilter(opts)
	nodes := make([]*types.Node, 0, capForLimit(opts.Limit))
	for _, nid := range ids {
		id := nid.SnowflakeID()
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned index entry
			}
			return nil, fmt.Errorf("graph: query node %d: %w", id, err)
		}
		if !n.HasLabelTokenRaw(token) {
			continue
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

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Snapshot nodeIDs under lock, sort + paginate, then fetch via GetNode.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	if len(nids) == 0 {
		return nil, nil
	}

	storepkg.SortNodeIDs(nids)

	// Temporal pre-filter via Peek.
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

	if storepkg.HasTemporalFilter(opts) {
		return bs.fetchNodesWithTemporalFilterPage(nids, opts)
	}
	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(nids, opts)
}

// GetNodesByIDs returns nodes for every requested ID.
// Missing IDs return ErrNodeNotFound. Results are sorted by snowflake.ID.
func (bs *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
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

	if unique, ok := uniqueNodeIDsPreserveOrderIfDuplicate(ids); ok {
		return bs.getNodesByIDsWithDuplicates(ids, unique)
	}

	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.prefetchNode(id)
		if err != nil {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, err)
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(nodes)
	return nodes, nil
}

func (bs *Store) getNodesByIDsWithDuplicates(ids, unique []types.NodeID) ([]*types.Node, error) {
	found := make(map[types.NodeID]*types.Node, len(unique))
	for _, id := range unique {
		n, err := bs.prefetchNode(id)
		if err != nil {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, err)
		}
		found[id] = n
	}

	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n := found[id]
		if n == nil {
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, ErrNodeNotFound)
		}
		nodes = append(nodes, n)
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	storepkg.SortNodesByID(nodes)
	return nodes, nil
}

func uniqueNodeIDsPreserveOrderIfDuplicate(ids []types.NodeID) ([]types.NodeID, bool) {
	if len(ids) < 2 {
		return nil, false
	}
	if len(ids) <= 32 {
		for i, id := range ids {
			for _, prev := range ids[:i] {
				if id == prev {
					return uniqueNodeIDsPreserveOrder(ids), true
				}
			}
		}
		return nil, false
	}

	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			return uniqueNodeIDsPreserveOrder(ids), true
		}
		seen[id] = struct{}{}
	}
	return nil, false
}

func uniqueNodeIDsPreserveOrder(ids []types.NodeID) []types.NodeID {
	unique := make([]types.NodeID, 0, len(ids))
	seen := make(map[types.NodeID]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	return unique
}

// NodesByLabelAndProperty returns nodes with the given label token and property
// value, applying temporal/depth query options before cursor pagination.
func (bs *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateLabelToken(labelToken); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateIndexPropertyKey(propKey); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	if err := types.ValidatePropertyValue(value); err != nil {
		return nil, fmt.Errorf("graph: nodes by label and property value: %w", err)
	}
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	// Snapshot matching IDs under RLock, then release before entity I/O.
	bs.idxMu.RLock()
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}

	if idx, ok := bs.propertyIndexes[key]; ok && idx.Mutated == nil {
		// Indexed path: snapshot matching IDs.
		nids := idx.NodeIDs(value)
		if len(nids) == 0 {
			bs.idxMu.RUnlock()
			return nil, nil
		}
		bs.idxMu.RUnlock()

		storepkg.SortNodeIDs(nids)

		// Temporal pre-filter via Peek.
		nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

		return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, targetKey, nids, opts)
	}

	// Fallback: snapshot label IDs, release lock, then scan properties.
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}
	if len(nids) == 0 {
		return nil, nil
	}

	// Sort label IDs, apply cursor skip, scan in order for property matches.
	storepkg.SortNodeIDs(nids)
	return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, targetKey, nids, opts)
}

func (bs *Store) fetchNodesByLabelPropertyIDs(labelToken uint16, propKey, targetKey string, nids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)
	if len(nids) == 0 {
		return nil, nil
	}

	hasTemporal := storepkg.HasTemporalFilter(opts)
	var result []*types.Node
	for _, nid := range nids {
		n, err := bs.prefetchNodeScan(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned index entry
			}
			return nil, err
		}
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		if valueKey, found := n.IndexablePropertyValueKey(propKey); found && valueKey == targetKey {
			if hasTemporal && !storepkg.MatchesTemporalFilter(nid.SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			result = append(result, n)
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

// AllNodeIDs returns the IDs of all current nodes, with optional temporal
// filtering and pagination. Non-temporal calls return only IDs without entity
// deserialization; temporal calls fetch current entity metadata. O(N) in
// nodeIDs map size.
func (bs *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateQueryOpts(opts); err != nil {
		return nil, err
	}
	bs.idxMu.RLock()
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	if len(nids) == 0 {
		return nil, nil
	}
	storepkg.SortNodeIDs(nids)
	if storepkg.HasTemporalFilter(opts) {
		var err error
		nids, err = bs.filterNodeIDsByTemporalFetch(nids, opts)
		if err != nil {
			return nil, err
		}
	}
	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}
	return nids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return errNilIterationCallback()
	}
	bs.idxMu.RLock()
	ids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()
	for _, id := range ids {
		if !fn(id) {
			return nil
		}
	}
	return nil
}

// prefetchNode retrieves a node from cache or Badger WITHOUT holding idxMu.
// Used as a pre-fetch step before acquiring idxMu.Lock() in write operations
// (DeleteNode, ReplaceNode, RemoveNodeLabelToken) to avoid holding the global
// write lock during slow disk I/O on cache misses.
//
// Callers MUST re-verify node existence under idxMu.Lock() after calling this
// (TOCTOU guard). The returned node may be stale if a concurrent delete occurred
// between the pre-fetch and the write lock acquisition — the re-verify catches this.
//
// Safety: nodeCache has its own internal mutex; db.View opens a read-only Badger
// transaction — neither requires idxMu. Dirty (unflushed) nodes are always retained
// in the LRU (soft capacity never evicts dirty entries), so a newly Put node that
// has not yet been flushed to Badger will always be found in the cache.
