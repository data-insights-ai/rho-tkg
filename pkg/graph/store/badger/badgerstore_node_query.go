package badger

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

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

	// Temporal index fast path: with a temporal filter the index result is
	// authoritative (nil means 0 matches), never fall through to the label scan.
	if ti, ok := bs.temporalIndexes[token]; ok && !ti.Building {
		var ids []snowflake.ID
		temporalQuery := false
		if opts.HasValidAt() {
			ids = ti.QueryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.HasValidInterval() {
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
		if opts.HasValidAt() {
			nids = hfi.CandidatesUpTo(opts.ValidAt)
			temporalQuery = true
		} else if opts.HasValidInterval() {
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

	// NoSort drops the O(n log n) sort; keyset pagination (After > 0) needs it.
	if !opts.NoSort || opts.After != 0 {
		storepkg.SortNodeIDs(nids)
	}

	// Temporal pre-filter via Peek.
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

	return bs.fetchNodesByLabelIDs(token, nids, opts)
}

func (bs *Store) fetchNodesByLabelIDs(token uint16, ids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	ids = storepkg.PaginateNodeIDs(ids, opts.After, 0)
	if len(ids) == 0 {
		return nil, nil
	}
	hasTemporal := opts.HasValidTimeFilter()

	// Unbounded large scans decode badger misses across cores (decode is the serial
	// floor); Limit'd scans keep the serial early-stopping door. Label and temporal
	// filters apply post-decode, as in the serial callback.
	if opts.Limit == 0 && len(ids) >= parallelDecodeMinIDs {
		all, err := bs.collectNodesBulkParallel(ids)
		if err != nil {
			return nil, err
		}
		nodes := make([]*types.Node, 0, len(all))
		for _, n := range all {
			if !n.HasLabelTokenRaw(token) {
				continue
			}
			if hasTemporal && !storepkg.MatchesTemporalFilter(bs.validAxis.Get(), n.ID().SnowflakeID(), n.Temporal(), opts) {
				continue
			}
			nodes = append(nodes, n)
		}
		if len(nodes) == 0 {
			return nil, nil
		}
		return nodes, nil
	}

	nodes := make([]*types.Node, 0, capForLimit(opts.Limit))
	err := bs.forEachNodeBulk(ids, func(n *types.Node) bool {
		if !n.HasLabelTokenRaw(token) {
			return true // continue
		}
		if hasTemporal && !storepkg.MatchesTemporalFilter(bs.validAxis.Get(), n.ID().SnowflakeID(), n.Temporal(), opts) {
			return true
		}
		nodes = append(nodes, n)
		return opts.Limit == 0 || len(nodes) < opts.Limit // stop once Limit reached
	})
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	return nodes, nil
}

// forEachNodeBulk materializes the given ascending node IDs through ONE
// forward-seeking badger iterator (prefetch off, see scanSnapshot.seekAfterMiss)
// instead of a read transaction per node. fn sees each present node in ID order
// (frozen, cache hits served without promotion); false stops. Absent nodes
// (deleted, orphaned index entry) are skipped. Misses read through a
// scanSnapshot that re-pins after a flush, so a row committed and evicted
// mid-scan is never read from a stale snapshot.
func (bs *Store) forEachNodeBulk(ids []types.NodeID, fn func(*types.Node) bool) error {
	snap := newScanSnapshot(bs, bs.nodeCache.FlushEpoch)
	defer snap.close()
	for _, nid := range ids {
		id := nid.SnowflakeID()
		// Cache first, no promote: a full scan must not lock per row or evict hot entries.
		v, status := bs.nodeCache.GetNoPromote(id)
		switch status {
		case indexpkg.CacheHit:
			if !fn(v) {
				return nil
			}
			continue
		case indexpkg.CacheDeleted:
			continue // tombstone — skip, as an orphaned index entry
		}
		// Miss: decode via the shared iterator (opened lazily).
		item := snap.seekAfterMiss(storepkg.NodeKey(id))
		if item == nil {
			continue // deleted / orphaned index entry
		}
		var decoded *types.Node
		derr := item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := storepkg.SafeUnmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n, err := bs.decodeNodeWireForKey(w, id)
			if err != nil {
				return fmt.Errorf("graph: decode node: %w", err)
			}
			decoded = n
			return nil
		})
		if derr != nil {
			return derr
		}
		decoded.Freeze() // shared frozen scan row
		if !fn(decoded) {
			return nil
		}
	}
	return nil
}

// AllNodes returns all stored nodes sorted by snowflake.ID, with optional
// pagination and temporal filtering.
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

	if opts.HasValidTimeFilter() {
		return bs.fetchNodesWithTemporalFilterPage(nids, opts)
	}
	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(nids, opts)
}

// GetNodesByIDs returns nodes for every requested ID, sorted by snowflake.ID;
// a missing ID returns ErrNodeNotFound.
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

// NodesByLabelAndProperty returns nodes with the label token and property value,
// applying temporal/depth options before cursor pagination.
func (bs *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	return bs.nodesByLabelPropertyKeys(labelToken, propKey, opts, func() ([]string, error) {
		return storepkg.ValueIndexKeys(value, "nodes by label and property")
	})
}

var _ storecontract.PropertyKeysLookupCapability = (*Store)(nil)

// NodesByLabelAndPropertyKeys implements
// storecontract.PropertyKeysLookupCapability: one paged walk over the keys'
// posting lists (index.PostingsOfKeys; on disk, each key's prefix scan,
// united), or one label scan matching any key (round 11).
func (bs *Store) NodesByLabelAndPropertyKeys(labelToken uint16, propKey string, valueKeys []string, opts QueryOpts) ([]*types.Node, error) {
	return bs.nodesByLabelPropertyKeys(labelToken, propKey, opts, func() ([]string, error) { return valueKeys, nil })
}

// nodesByLabelPropertyKeys is the body of both doors; keysOf runs after the
// store, token and option checks (a single value is validated there).
func (bs *Store) nodesByLabelPropertyKeys(labelToken uint16, propKey string, opts QueryOpts, keysOf func() ([]string, error)) ([]*types.Node, error) {
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
	targetKeys, err := keysOf()
	if err != nil {
		return nil, err
	}
	if targetKeys = storepkg.DistinctIndexKeys(targetKeys); len(targetKeys) == 0 {
		return nil, nil
	}

	// Snapshot matching IDs under RLock, release before entity I/O.
	bs.idxMu.RLock()
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}

	if idx, ok := bs.propertyIndexes[key]; ok && idx.Mutated == nil {
		// Indexed path.
		elements := idx.Elements()
		if !bs.propIdxOnDisk {
			// The keys' sorted posting lists, paged from opts.After in
			// batches (backlog 72): no copy or sort of the whole key.
			ids := idx.PostingsOfKeys(targetKeys)
			empty := ids.Len() == 0
			bs.idxMu.RUnlock()
			if empty {
				return nil, nil
			}
			return pagePostings(bs, ids, opts, func(batch []snowflake.ID, page QueryOpts) ([]*types.Node, error) {
				nids := bs.filterNodeIDsByTemporalPeek(storepkg.ToNodeIDs(batch), page)
				return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, targetKeys, elements, nids, page)
			})
		}
		var nids []types.NodeID
		for _, targetKey := range targetKeys {
			found, dErr := bs.propertyIndexDiskLookupLocked(propKey, targetKey)
			if dErr != nil {
				bs.idxMu.RUnlock()
				return nil, dErr
			}
			nids = append(nids, found...)
		}
		bs.idxMu.RUnlock()
		if len(nids) == 0 {
			return nil, nil
		}

		storepkg.SortNodeIDs(nids)
		nids = slices.Compact(nids)

		// Temporal pre-filter via Peek.
		nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

		return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, targetKeys, elements, nids, opts)
	}

	// Fallback: label scan + property filter, with the declared index's match
	// while it is still building (an Elements index means "contains").
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	scanElements := bs.propertyIndexes[key].Elements()
	nids, idErr := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if idErr != nil {
		return nil, idErr
	}
	if len(nids) == 0 {
		return nil, nil
	}

	// Sorted scan for property matches.
	storepkg.SortNodeIDs(nids)
	return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, targetKeys, scanElements, nids, opts)
}

// NodesByLabelContainingProperty answers an Elements-index lookup (backlog 63) on a
// store holding no copy of the index: the label's nodes whose propKey carries
// value's key as a scalar or list element, filtered while the rows are fetched.
// No in-tree caller since v4.58.0 (tiered builds the index on every shard and
// scans a shard lacking its copy itself); kept for API compatibility.
func (bs *Store) NodesByLabelContainingProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
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
	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}
	bs.idxMu.RLock()
	nids, err := bs.labelNodeIDsSnapshotLocked(labelToken)
	bs.idxMu.RUnlock()
	if err != nil || len(nids) == 0 {
		return nil, err
	}
	storepkg.SortNodeIDs(nids)
	return bs.fetchNodesByLabelPropertyIDs(labelToken, propKey, []string{targetKey}, true, nids, opts)
}

func (bs *Store) fetchNodesByLabelPropertyIDs(labelToken uint16, propKey string, targetKeys []string, elements bool, nids []types.NodeID, opts QueryOpts) ([]*types.Node, error) {
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0)
	if len(nids) == 0 {
		return nil, nil
	}

	hasTemporal := opts.HasValidTimeFilter()
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
		if storepkg.NodeHasAnyIndexKey(n, propKey, elements, targetKeys) {
			if hasTemporal && !storepkg.MatchesTemporalFilter(bs.validAxis.Get(), nid.SnowflakeID(), n.Temporal(), opts) {
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

// AllNodeIDs returns the IDs of all current nodes with optional temporal filtering
// and pagination; non-temporal calls deserialize no entities. O(N).
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
	if opts.HasValidTimeFilter() {
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

// ForEachNodeID calls fn for each current node ID; false stops. Unordered.
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

// prefetchNode retrieves a node from cache or Badger WITHOUT holding idxMu, so
// write paths avoid holding the global write lock during disk I/O on a miss.
// Callers MUST re-verify existence under idxMu.Lock() (TOCTOU): the node may be
// stale after a concurrent delete. Safe without idxMu: the cache has its own
// mutex, and dirty nodes are never evicted, so an unflushed Put is always found.
