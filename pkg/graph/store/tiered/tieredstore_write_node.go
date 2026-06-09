package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Node write operations ---
// Nodes are single-shard. Reference nodes route by primary label; event nodes
// route by ID timestamp so caller-supplied/backfilled IDs remain readable via
// shardForNodeIDChecked after a successful create.

func (ts *Store) nodeIDExistsForCreate(n *types.Node) (bool, error) {
	if err := ts.checkOpen(); err != nil {
		return false, err
	}
	id := n.ID()
	raw := id.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return false, err
	}
	if ref.HasNodeID(raw) {
		refCheckin()
		return true, nil
	}
	refCheckin()

	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return false, err
	}
	if archive != nil {
		exists := archive.HasNodeID(raw)
		archiveCheckin()
		if exists {
			return true, nil
		}
	}

	if ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) != ClassReference {
		return false, nil
	}
	if !ts.idOutsideHotShardWindow(raw) {
		ts.mu.RLock()
		hotES := ts.hotShard
		ts.mu.RUnlock()
		hot, err := hotES.checkoutStore(ts)
		if err != nil {
			return false, err
		}
		defer hotES.checkinStore()
		return hot.HasNodeID(raw), nil
	}

	eventOwner, eventCheckin, err := ts.shardForNodeIDChecked(id)
	if err != nil {
		return false, err
	}
	defer eventCheckin()
	return eventOwner.HasNodeID(raw), nil
}

func (ts *Store) shardForNodeCreate(n *types.Node) (*BadgerStore, func(), error) {
	if err := ts.checkOpen(); err != nil {
		return nil, nil, err
	}
	if ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
		return ts.checkoutRefShard()
	}
	es := ts.timestampToEventShardEntry(n.ID().SnowflakeID())
	store, err := es.checkoutStore(ts)
	if err != nil {
		return nil, nil, err
	}
	return store, func() { es.checkinStore() }, nil
}

func (ts *Store) PutNode(n *types.Node) error {
	return ts.putNode(n, true)
}

func (ts *Store) PutNodeGeneratedID(n *types.Node, proof generatedcreate.Proof) error {
	if !proof.Valid() {
		return ts.PutNode(n)
	}
	return ts.putNode(n, false)
}

func (ts *Store) putNode(n *types.Node, checkGlobalDuplicate bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	if err := ts.checkRotation(); err != nil {
		return err
	}
	ts.nodeCreateMu.Lock()
	defer ts.nodeCreateMu.Unlock()

	if checkGlobalDuplicate {
		exists, err := ts.nodeIDExistsForCreate(n)
		if err != nil {
			return err
		}
		if exists {
			return ErrNodeExists
		}
	}

	shard, checkin, err := ts.shardForNodeCreate(n)
	if err != nil {
		return err
	}
	defer checkin()

	id := n.ID().SnowflakeID()
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, n, id)
	if err != nil {
		return err
	}
	if err := shard.PutNode(n); err != nil {
		return err
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) ReplaceNode(n *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(n); err != nil {
		return err
	}
	id := n.ID().SnowflakeID()
	store, checkin, err := ts.shardForNodeIDChecked(n.ID())
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, n); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, n); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, n, id)
	if err != nil {
		return err
	}
	if err := store.ReplaceNode(n); err != nil {
		return err
	}
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) DeleteNode(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	out, err := ts.OutgoingRelationships(nid, 0)
	if err != nil {
		return err
	}
	in, err := ts.IncomingRelationships(nid, 0)
	if err != nil {
		return err
	}
	if len(out) != 0 || len(in) != 0 {
		return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, nid)
	}
	if err := store.DeleteNode(types.NodeID(id)); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, updatedNode, id)
	if err != nil {
		return err
	}
	if err := store.RemoveNodeLabelToken(types.NodeID(id), tok, updatedNode); err != nil {
		return err
	}
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelRemoval(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, updatedNode, id)
	if err != nil {
		return err
	}
	if err := store.RemoveNodeLabelTokenWithHistory(types.NodeID(id), tok, updatedNode, prevVersion, prevState); err != nil {
		return err
	}
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, updatedNode, id)
	if err != nil {
		return err
	}
	if err := store.AddNodeLabelToken(types.NodeID(id), tok, updatedNode); err != nil {
		return err
	}
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(nid, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevVersion, prevState); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, err := store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeLabelAddition(old, updatedNode, tok); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, updatedNode, id)
	if err != nil {
		return err
	}
	if err := store.AddNodeLabelTokenWithHistory(types.NodeID(id), tok, updatedNode, prevVersion, prevState); err != nil {
		return err
	}
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) PutNodesBatch(nodes []*types.Node) error {
	return ts.putNodesBatch(nodes, true)
}

func (ts *Store) PutNodesBatchGeneratedID(nodes []*types.Node, proof generatedcreate.Proof) error {
	if !proof.Valid() {
		return ts.PutNodesBatch(nodes)
	}
	return ts.putNodesBatch(nodes, false)
}

func (ts *Store) putNodesBatch(nodes []*types.Node, checkGlobalDuplicate bool) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(nodes) == 0 {
		return nil
	}
	if err := ts.checkRotation(); err != nil {
		return err
	}
	ts.nodeCreateMu.Lock()
	defer ts.nodeCreateMu.Unlock()

	seen := make(map[types.NodeID]struct{}, len(nodes))
	for _, n := range nodes {
		if err := storecontract.ValidateNodeWrite(n); err != nil {
			return err
		}
		id := n.ID()
		if _, ok := seen[id]; ok {
			return ErrNodeExists
		}
		seen[id] = struct{}{}
		if checkGlobalDuplicate {
			exists, err := ts.nodeIDExistsForCreate(n)
			if err != nil {
				return err
			}
			if exists {
				return ErrNodeExists
			}
		}
	}

	// Partition nodes by create shard.
	type nodeBucket struct {
		shard *BadgerStore
		nodes []*types.Node
	}
	bucketIndexes := make(map[*BadgerStore]int)
	buckets := make([]nodeBucket, 0)
	var checkins []func()
	releaseAll := func() {
		for _, checkin := range checkins {
			checkin()
		}
	}
	for _, n := range nodes {
		shard, checkin, err := ts.shardForNodeCreate(n)
		if err != nil {
			releaseAll()
			return err
		}
		checkins = append(checkins, checkin)
		idx, ok := bucketIndexes[shard]
		if !ok {
			idx = len(buckets)
			bucketIndexes[shard] = idx
			buckets = append(buckets, nodeBucket{shard: shard})
		}
		buckets[idx].nodes = append(buckets[idx].nodes, n)
	}
	defer releaseAll()

	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	vectorUpdates := make([][]indexpkg.NodeVectorIndexUpdate, len(nodes))
	for i, n := range nodes {
		updates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, n, n.ID().SnowflakeID())
		if err != nil {
			return err
		}
		vectorUpdates[i] = updates
	}

	var written []nodeBucket
	rollbackBuckets := func(extra nodeBucket) error {
		var rollbackErrs []error
		for _, bucket := range append(written, extra) {
			for _, n := range bucket.nodes {
				if rbErr := bucket.shard.DeleteNode(n.ID()); rbErr != nil && !errors.Is(rbErr, ErrNodeNotFound) {
					rollbackErrs = append(rollbackErrs, rbErr)
				}
			}
		}
		return errors.Join(rollbackErrs...)
	}
	for _, bucket := range buckets {
		if err := bucket.shard.PutNodesBatch(bucket.nodes); err != nil {
			if rbErr := rollbackBuckets(bucket); rbErr != nil {
				return fmt.Errorf("tiered: put nodes: %w (rollback failed: %v)", err, rbErr)
			}
			return fmt.Errorf("tiered: put nodes (writes rolled back): %w", err)
		}
		written = append(written, bucket)
	}
	for i, n := range nodes {
		if err := indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates[i], n.ID().SnowflakeID()); err != nil {
			return err
		}
	}
	return nil
}

func (ts *Store) DeleteNodesBatch(ids []types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		if err := storecontract.ValidateNodeID(id); err != nil {
			return err
		}
	}
	ids = uniqueNodeIDs(ids)

	// Partition by actual shard. Use the checked variant so a cold owner
	// stays pinned during the per-shard batch write below.
	shardBuckets := make(map[*BadgerStore][]types.NodeID)
	var checkins []func()
	releaseAll := func() {
		for _, fn := range checkins {
			fn()
		}
	}
	for _, id := range ids {
		shard, checkin, err := ts.shardForNodeIDChecked(id)
		if err != nil {
			releaseAll()
			return err
		}
		shardBuckets[shard] = append(shardBuckets[shard], id)
		checkins = append(checkins, checkin)
	}
	defer releaseAll()

	// Preflight every shard before mutating any bucket. A later shard rejecting
	// a connected node must not leave an earlier shard's unconnected node
	// deleted.
	for shard, bucket := range shardBuckets {
		for _, id := range bucket {
			rawID := id.SnowflakeID()
			if len(shard.OutgoingRelIDs(rawID)) != 0 || len(shard.IncomingRelIDs(rawID, 0)) != 0 {
				return fmt.Errorf("%w: node %d has connected relationships", ErrInvalidStoreMutation, id)
			}
		}
	}

	// Pre-read nodes before deletion for Store-level vector index cleanup and
	// rollback if a later shard bucket fails.
	type nodeEntry struct {
		shard *BadgerStore
		id    snowflake.ID
		old   *types.Node
	}
	entries := make([]nodeEntry, 0, len(ids))
	nodesByID := make(map[types.NodeID]*types.Node, len(ids))
	for shard, bucket := range shardBuckets {
		for _, id := range bucket {
			old, err := shard.GetNode(id)
			if err != nil {
				return err
			}
			entries = append(entries, nodeEntry{shard: shard, id: id.SnowflakeID(), old: old})
			nodesByID[id] = old
		}
	}

	deleted := make([]deletedNodeBucket, 0, len(shardBuckets))
	rollbackDeleted := func(err error) error {
		if rbErr := rollbackDeletedNodeBuckets(deleted); rbErr != nil {
			return fmt.Errorf("%w (node batch rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("%w (prior node deletes rolled back)", err)
	}

	for shard, bucket := range shardBuckets {
		if err := shard.DeleteNodesBatch(bucket); err != nil {
			if !errors.Is(err, ErrNodeNotFound) {
				deleted = appendMissingNodeBucket(deleted, shard, bucket, nodesByID)
			}
			return rollbackDeleted(err)
		}
		deleted = append(deleted, deletedNodeBucket{
			shard: shard,
			nodes: nodesForIDs(bucket, nodesByID),
		})
	}
	// Update Store-level vector indexes. The shard-level DeleteNodesBatch updates
	// per-shard bs.vectorIndexes; ts.vectorIndexes is separate and must be kept in sync.
	ts.vectorIdxMu.Lock()
	for _, e := range entries {
		if e.old != nil {
			indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, e.old, e.id)
		} else {
			indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, e.id)
		}
	}
	ts.vectorIdxMu.Unlock()
	return nil
}

type deletedNodeBucket struct {
	shard *BadgerStore
	nodes []*types.Node
}

func nodesForIDs(ids []types.NodeID, nodesByID map[types.NodeID]*types.Node) []*types.Node {
	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if n := nodesByID[id]; n != nil {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

func appendMissingNodeBucket(dst []deletedNodeBucket, shard *BadgerStore, ids []types.NodeID, nodesByID map[types.NodeID]*types.Node) []deletedNodeBucket {
	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		if shard.HasNodeID(id.SnowflakeID()) {
			continue
		}
		if n := nodesByID[id]; n != nil {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		return dst
	}
	return append(dst, deletedNodeBucket{shard: shard, nodes: nodes})
}

func rollbackDeletedNodeBuckets(buckets []deletedNodeBucket) error {
	var rollbackErrs []error
	for i := len(buckets) - 1; i >= 0; i-- {
		bucket := buckets[i]
		if len(bucket.nodes) == 0 {
			continue
		}
		if err := bucket.shard.PutNodesBatch(bucket.nodes); err != nil {
			rollbackErrs = append(rollbackErrs, fmt.Errorf("restore %d nodes: %w", len(bucket.nodes), err))
		}
	}
	return errors.Join(rollbackErrs...)
}

func uniqueNodeIDs(ids []types.NodeID) []types.NodeID {
	seen := make(map[types.NodeID]struct{}, len(ids))
	out := make([]types.NodeID, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
