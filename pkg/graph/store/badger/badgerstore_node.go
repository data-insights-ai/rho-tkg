// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// PutNode stores a node with its label index entries.
// Updates in-memory state immediately; Badger write is queued for async flush.
// Returns ErrNodeExists if a node with the same ID already exists.
func (bs *Store) PutNode(n *types.Node) error {
	nid := n.InternalID()
	id := nid.SnowflakeID() // LRU is keyed by raw snowflake.ID (Tier D — see lru.go).

	w := storepkg.NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	bs.idxMu.Lock()

	// Check for duplicate.
	if _, exists := bs.nodeIDs[nid]; exists {
		bs.idxMu.Unlock()
		return ErrNodeExists
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, n.DeepCopy())
	bs.nodeIDs[nid] = struct{}{}

	// Build write ops.
	ops := []writeOp{{opType: writeOpSet, key: storepkg.NodeKey(id), value: data}}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if bs.labelIdx[tv] == nil {
			bs.labelIdx[tv] = make(map[types.NodeID]struct{})
		}
		bs.labelIdx[tv][nid] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tv, id)})
		bs.getOrCreateLabelCounter(tv).Add(1)
	}

	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, n, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Cache-first: checks LRU cache, then nodeIDs (O(1) existence check),
// then falls through to Badger only if the node is confirmed to exist.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) GetNode(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	// Check cache first.
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v.DeepCopy(), nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Short-circuit: nodeIDs is the authoritative set of all node IDs.
	// Avoids opening a Badger transaction for non-existent nodes.
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Cache miss, node exists — read from Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = storepkg.WireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean (evictable).
	bs.nodeCache.LoadClean(id, n)
	return n.DeepCopy(), nil
}

// DeleteNode removes a node and its label index entries.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) DeleteNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	// Pre-fetch node state before acquiring the write lock to avoid holding
	// idxMu.Lock() during Badger disk I/O on cache misses (B3: lock scope rule).
	// prefetchNode checks the cache and falls through to db.View without any lock.
	n, err := bs.prefetchNode(nid)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	// TOCTOU guard: re-verify existence after acquiring write lock.
	// A concurrent delete may have removed the node between prefetchNode and here.
	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Build delete ops using pre-fetched node (labels needed for index cleanup).
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, nid)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, nid)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (bs *Store) ReplaceNode(n *types.Node) error {
	nid := n.InternalID()
	id := nid.SnowflakeID()

	w := storepkg.NodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes: remove old entries, add new.
	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		// Pre-fetch failed (concurrent delete between prefetch and write lock, or
		// cache miss on a just-opened store) — brute-force purge to avoid orphans.
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}
	bs.nodeCache.Put(id, n.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, n, id)
	bs.appendOps(writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// RemoveNodeLabelToken removes tok from the label index for id and persists updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw) and have its
// version bumped. Version history must be written by the caller before this call.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes using pre-fetched old node state.
	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	// Remove tok from the in-memory label index.
	if set, ok := bs.labelIdx[tok]; ok {
		delete(set, nid)
		if len(set) == 0 {
			delete(bs.labelIdx, tok)
		}
	}
	bs.getOrCreateLabelCounter(tok).Add(-1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	// Queue: set node data + delete label index entry.
	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	id := nid.SnowflakeID()
	w := storepkg.NodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[nid]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	if old != nil {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[types.NodeID]struct{})
		bs.labelIdx[tok] = set
	}
	set[nid] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	bs.appendOps(
		writeOp{opType: writeOpSet, key: storepkg.NodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index fast path when available and a temporal filter is set.
func (bs *Store) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()

	// Temporal index fast path: avoids iterating the full label set when a
	// temporal index exists and a temporal filter is active.
	// When a temporal query is requested, the index result is always authoritative
	// — nil means 0 matches, not "index not consulted." Do not fall through to
	// the full label scan in that case.
	if ti, ok := bs.temporalIndexes[token]; ok {
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
			ids = storepkg.PaginateIDs(ids, opts.After, opts.Limit)
			if len(ids) == 0 {
				return nil, nil
			}
			return bs.fetchNodesWithTemporalFilter(storepkg.ToNodeIDs(ids), opts)
		}
	}

	set := bs.labelIdx[token]
	nids := make([]types.NodeID, 0, len(set))
	for id := range set {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	if len(nids) == 0 {
		return nil, nil
	}

	sort.Slice(nids, func(i, j int) bool { return nids[i].SnowflakeID() < nids[j].SnowflakeID() })

	// Temporal pre-filter via Peek (zero allocation for cache hits).
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(nids, opts)
}

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Snapshot nodeIDs under lock, sort + paginate, then fetch via GetNode.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	if len(nids) == 0 {
		return nil, nil
	}

	sort.Slice(nids, func(i, j int) bool { return nids[i].SnowflakeID() < nids[j].SnowflakeID() })

	// Temporal pre-filter via Peek.
	nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(nids, opts)
}

// GetNodesByIDs returns nodes matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (bs *Store) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(id)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
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

// DeleteNodeCascade atomically removes a node and all connected relationships.
// Phases 1+2 (preflight + in-memory mutations) run under idxMu write lock.
// Version history is preserved — temporal queries can still reconstruct past state.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *Store) DeleteNodeCascade(nid types.NodeID) error {
	_, corruptErr, err := bs.cascadeDeleteLocked(nid)
	if err != nil {
		return err
	}
	if corruptErr == nil && bs.syncWrites {
		return bs.flush()
	}
	return corruptErr
}

// cascadeDeleteInner performs Phases 1+2 of DeleteNodeCascade.
// Caller MUST hold bs.idxMu.Lock(). All ops are appended to pending under the same lock
// so that the caller can append additional ops (e.g. tombstone history) before releasing.
// Returns (toDelete, corruptErr, fatalErr):
//   - fatalErr != nil: aborted with no mutations applied.
//   - corruptErr != nil: cleanup completed but node data was unreadable (indexes brute-force purged).
//   - Otherwise: clean success.
func (bs *Store) cascadeDeleteInner(nid types.NodeID) ([]RelDeleteInfo, error, error) {
	id := nid.SnowflakeID()
	if _, exists := bs.nodeIDs[nid]; !exists {
		return nil, nil, ErrNodeNotFound
	}

	// Collect all connected relIDs (dedup self-loops).
	relIDs := make(map[types.RelID]struct{})
	for relID := range bs.outIdx[nid] {
		relIDs[relID] = struct{}{}
	}
	for relID := range bs.inIdx[nid] {
		relIDs[relID] = struct{}{}
	}

	// Phase 1 — Preflight: read all relationship metadata before any mutations.
	// If any read fails (corruption), we abort without partial state changes.
	toDelete := make([]RelDeleteInfo, 0, len(relIDs))
	for relID := range relIDs {
		r, err := bs.getRelLocked(relID)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // tolerate already-deleted rels
			}
			return nil, nil, fmt.Errorf("graph: cascade read relationship: %w", err)
		}
		toDelete = append(toDelete, RelDeleteInfo{
			ID:      relID.SnowflakeID(),
			RelType: r.TypeToken().Value(),
			StartID: r.StartNodeID().SnowflakeID(),
			EndID:   r.EndNodeID().SnowflakeID(),
		})
	}

	// Phase 2 — Apply: all mutations use pre-read data, no reads, cannot fail.
	for _, info := range toDelete {
		bs.deleteRelByInfo(info)
	}

	// Get node data for label cleanup.
	n, err := bs.getNodeLocked(nid)
	if err != nil {
		// Node was in nodeIDs but can't be loaded (data corruption or cache miss
		// with closed DB). Still proceed with cleanup — scrub labelIdx by scanning
		// ALL label sets to prevent orphaned index entries (perma-leak).
		// O(L) where L is total distinct labels — bounded, corruption-only path.
		ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}
		for tok, set := range bs.labelIdx {
			if _, exists := set[nid]; exists {
				delete(set, nid)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
				ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
				bs.getOrCreateLabelCounter(tok).Add(-1)
			}
		}
		// Property, temporal, and vector indexes: node data unavailable, brute-force purge.
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		indexpkg.PurgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		indexpkg.PurgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)

		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		bs.appendOps(ops...)
		bs.nodeCount.Add(-1)
		return toDelete, fmt.Errorf("graph: cascade completed with corrupt node data: %w", err), nil
	}

	// Build delete ops for node.
	ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, nid)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
	indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
	indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, nid)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)

	return toDelete, nil, nil
}

// cascadeDeleteLocked acquires idxMu.Lock() and delegates to cascadeDeleteInner.
// Used by DeleteNodeCascade — same contract as before the refactor.
func (bs *Store) cascadeDeleteLocked(nid types.NodeID) ([]RelDeleteInfo, error, error) {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	return bs.cascadeDeleteInner(nid)
}

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (bs *Store) PutNodesBatch(nodes []*types.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Pre-serialize all nodes outside the lock.
	type nodeData struct {
		nid  types.NodeID
		id   snowflake.ID
		data []byte
	}
	serialized := make([]nodeData, len(nodes))
	for i, n := range nodes {
		w := storepkg.NodeToWire(n)
		data, err := msgpack.Marshal(w)
		if err != nil {
			return fmt.Errorf("graph: marshal node: %w", err)
		}
		nid := n.InternalID()
		serialized[i] = nodeData{nid: nid, id: nid.SnowflakeID(), data: data}
	}

	bs.idxMu.Lock()

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, nd := range serialized {
		if _, exists := bs.nodeIDs[nd.nid]; exists {
			bs.idxMu.Unlock()
			return ErrNodeExists
		}
		if _, exists := seen[nd.nid]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate node ID %d in batch", nd.id)
		}
		seen[nd.nid] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(nodes)*3) // entity + avg ~2 label indexes
	for i, n := range nodes {
		nd := serialized[i]

		bs.nodeCache.Put(nd.id, n.DeepCopy())
		bs.nodeIDs[nd.nid] = struct{}{}

		ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.NodeKey(nd.id), value: nd.data})
		for _, tok := range n.AllLabelTokens() {
			tv := tok.Value()
			if bs.labelIdx[tv] == nil {
				bs.labelIdx[tv] = make(map[types.NodeID]struct{})
			}
			bs.labelIdx[tv][nd.nid] = struct{}{}
			ops = append(ops, writeOp{opType: writeOpSet, key: storepkg.LabelIndexKey(tv, nd.id)})
			bs.getOrCreateLabelCounter(tv).Add(1)
		}
		indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, nd.id)
		indexpkg.AddNodeToTemporalIndexes(bs.temporalIndexes, n, nd.id)
		indexpkg.AddNodeToVectorIndexes(bs.vectorIndexes, n, nd.id)
	}

	bs.appendOps(ops...)
	bs.nodeCount.Add(int64(len(nodes)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read node data for label cleanup.
// Phase 2: remove from cache, indexes, queue delete ops.
// Missing ID → ErrNodeNotFound, zero mutations. Nil/empty input → nil error.
func (bs *Store) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if len(typedIDs) == 0 {
		return nil
	}

	bs.idxMu.Lock()

	// Phase 1: validate — all must exist + pre-read for label cleanup.
	nodeData := make([]*types.Node, len(typedIDs))
	for i, nid := range typedIDs {
		if _, exists := bs.nodeIDs[nid]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		n, err := bs.getNodeLocked(nid)
		if err != nil {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: batch read node %d: %w", nid.SnowflakeID(), err)
		}
		nodeData[i] = n
	}

	// Phase 2: apply — all validated, safe to mutate.
	for i, nid := range typedIDs {
		n := nodeData[i]
		id := nid.SnowflakeID()

		ops := []writeOp{{opType: writeOpDelete, key: storepkg.NodeKey(id)}}

		allTokens := collectNodeLabelTokens(n)
		for _, tok := range allTokens {
			ops = append(ops, writeOp{opType: writeOpDelete, key: storepkg.LabelIndexKey(tok, id)})
			if set, exists := bs.labelIdx[tok]; exists {
				delete(set, nid)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
			}
			bs.getOrCreateLabelCounter(tok).Add(-1)
		}

		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
		indexpkg.RemoveNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
		indexpkg.RemoveNodeFromVectorIndexes(bs.vectorIndexes, n, id)
		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, nid)
		bs.appendOps(ops...)
	}

	bs.nodeCount.Add(-int64(len(typedIDs)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// loadNodeFromBadger reads and unmarshals a node within an existing Badger transaction.
// Does not interact with the LRU cache. Used during loadIndexes where the cache is
// not yet populated and concurrent access has not started.
func (bs *Store) loadNodeFromBadger(txn *badgerv4.Txn, id snowflake.ID) (*types.Node, error) {
	item, err := txn.Get(storepkg.NodeKey(id))
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var n *types.Node
	err = item.Value(func(val []byte) error {
		var w storepkg.NodeWire
		if err := msgpack.Unmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal node: %w", err)
		}
		n = storepkg.WireToNode(w)
		return nil
	})
	return n, err
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional temporal filtering. Uses the property index if one exists;
// falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *Store) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	// Snapshot matching IDs under RLock, then release before entity I/O.
	bs.idxMu.RLock()
	key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propKey}

	if idx, ok := bs.propertyIndexes[key]; ok {
		// Indexed path: snapshot matching IDs.
		// propertyIndex.entries returns map[snowflake.ID]struct{} (off-limits file);
		// wrap as types.NodeID at the boundary.
		matchSet := idx.Lookup(value)
		if len(matchSet) == 0 {
			bs.idxMu.RUnlock()
			return nil, nil
		}
		nids := make([]types.NodeID, 0, len(matchSet))
		for id := range matchSet {
			nids = append(nids, types.NodeID(id))
		}
		bs.idxMu.RUnlock()

		sort.Slice(nids, func(i, j int) bool { return nids[i].SnowflakeID() < nids[j].SnowflakeID() })

		// Temporal pre-filter via Peek.
		nids = bs.filterNodeIDsByTemporalPeek(nids, opts)

		nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
		if len(nids) == 0 {
			return nil, nil
		}

		return bs.fetchNodesWithTemporalFilter(nids, opts)
	}

	// Fallback: snapshot label IDs, release lock, then scan properties.
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	labelIDs := bs.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		bs.idxMu.RUnlock()
		return nil, nil
	}

	nids := make([]types.NodeID, 0, len(labelIDs))
	for id := range labelIDs {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	targetKey := indexpkg.PropertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	// Sort label IDs, apply cursor skip, scan in order for property matches.
	sort.Slice(nids, func(i, j int) bool { return nids[i].SnowflakeID() < nids[j].SnowflakeID() })
	nids = storepkg.PaginateNodeIDs(nids, opts.After, 0) // apply cursor, not limit yet
	if len(nids) == 0 {
		return nil, nil
	}

	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	var result []*types.Node
	for _, nid := range nids {
		n, err := bs.GetNode(nid)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned index entry
			}
			return nil, err
		}
		if v, found := n.GetProperty(propKey); found {
			if indexpkg.PropertyValueKey(v) == targetKey {
				if hasTemporal && !storepkg.MatchesTemporalFilter(nid.SnowflakeID(), n.Temporal(), opts) {
					continue
				}
				result = append(result, n)
				if opts.Limit > 0 && len(result) >= opts.Limit {
					break
				}
			}
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy. O(N) in nodeIDs map size.
func (bs *Store) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	bs.idxMu.RLock()
	nids := make([]types.NodeID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nids = append(nids, id)
	}
	bs.idxMu.RUnlock()

	if len(nids) == 0 {
		return nil, nil
	}
	sort.Slice(nids, func(i, j int) bool { return nids[i].SnowflakeID() < nids[j].SnowflakeID() })
	nids = storepkg.PaginateNodeIDs(nids, opts.After, opts.Limit)
	if len(nids) == 0 {
		return nil, nil
	}
	return nids, nil
}

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *Store) ForEachNodeID(fn func(types.NodeID) bool) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for id := range bs.nodeIDs {
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
func (bs *Store) prefetchNode(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v, nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Cache miss — check existence before incurring Badger I/O.
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Node exists in-memory but not in cache (flushed + evicted from LRU).
	// Read from Badger without holding any lock.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = storepkg.WireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}

// getNodeLocked retrieves a node from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *Store) getNodeLocked(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	if status == indexpkg.CacheHit {
		return v, nil
	}
	if status == indexpkg.CacheDeleted {
		return nil, ErrNodeNotFound
	}

	// Cache miss — read from Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(storepkg.NodeKey(id))
		if err == badgerv4.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w storepkg.NodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = storepkg.WireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}
