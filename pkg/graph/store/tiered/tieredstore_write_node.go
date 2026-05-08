package tiered

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Node write operations ---
// Nodes are single-shard: routed entirely by primary label classification.

func (ts *Store) PutNode(n *types.Node) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}
	shard := ts.shardForNode(n.PrimaryLabelToken().Value())
	if err := shard.PutNode(n); err != nil {
		return err
	}
	id := n.ID().SnowflakeID()
	ts.vectorIdxMu.Lock()
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, n, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) ReplaceNode(n *types.Node) error {
	id := n.ID().SnowflakeID()
	store, checkin, err := ts.shardForNodeIDChecked(n.ID())
	if err != nil {
		return err
	}
	defer checkin()
	// Read old state for accurate vector index removal.
	old, _ := store.GetNode(types.NodeID(id))
	if err := store.ReplaceNode(n); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, n, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) DeleteNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	// Read node before deletion for vector index maintenance.
	old, _ := store.GetNode(types.NodeID(id))
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
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	// Read old state for accurate vector index removal.
	old, _ := store.GetNode(types.NodeID(id))
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := store.RemoveNodeLabelToken(types.NodeID(id), tok, updatedNode); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	// Read old state for accurate vector index removal.
	old, _ := store.GetNode(types.NodeID(id))
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := store.RemoveNodeLabelTokenWithHistory(types.NodeID(id), tok, updatedNode, prevVersion, prevState); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	old, _ := store.GetNode(types.NodeID(id))
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := store.AddNodeLabelToken(types.NodeID(id), tok, updatedNode); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	id := nid.SnowflakeID()

	store, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	// Read old state for accurate vector index removal.
	old, _ := store.GetNode(types.NodeID(id))
	if err := ts.ensurePrimaryLabelClassUnchanged(old, updatedNode); err != nil {
		return err
	}
	if err := store.AddNodeLabelTokenWithHistory(types.NodeID(id), tok, updatedNode, prevVersion, prevState); err != nil {
		return err
	}
	ts.vectorIdxMu.Lock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) PutNodesBatch(nodes []*types.Node) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	// Partition nodes by shard.
	refNodes := make([]*types.Node, 0)
	evtNodes := make([]*types.Node, 0)
	for _, n := range nodes {
		if ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
			refNodes = append(refNodes, n)
		} else {
			evtNodes = append(evtNodes, n)
		}
	}
	if len(refNodes) > 0 {
		if err := ts.refShard.PutNodesBatch(refNodes); err != nil {
			return err
		}
	}
	if len(evtNodes) > 0 {
		ts.mu.RLock()
		hot := ts.hotShard.store
		ts.mu.RUnlock()
		if err := hot.PutNodesBatch(evtNodes); err != nil {
			// Best-effort rollback: remove ref shard writes to prevent silent partial state.
			// If rollback itself fails, the store is already in a broken state — log and return
			// the original error so the caller knows to expect inconsistency.
			for _, n := range refNodes {
				_ = ts.refShard.DeleteNode(n.ID())
			}
			return fmt.Errorf("tiered: put hot nodes (ref writes rolled back best-effort): %w", err)
		}
	}
	// Update Store-level vector indexes. The shard-level PutNodesBatch updates
	// the per-shard bs.vectorIndexes; ts.vectorIndexes is separate and must be kept in sync.
	ts.vectorIdxMu.Lock()
	for _, n := range nodes {
		indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, n, n.ID().SnowflakeID())
	}
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) DeleteNodesBatch(ids []types.NodeID) error {
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

	// Pre-read nodes before deletion for Store-level vector index cleanup.
	// GetNode failure (shard closed, corruption) is non-fatal: nil triggers purge fallback.
	type nodeEntry struct {
		id  snowflake.ID
		old *types.Node
	}
	entries := make([]nodeEntry, 0, len(ids))
	for shard, bucket := range shardBuckets {
		for _, id := range bucket {
			old, _ := shard.GetNode(id)
			entries = append(entries, nodeEntry{id: id.SnowflakeID(), old: old})
		}
	}

	for shard, bucket := range shardBuckets {
		if err := shard.DeleteNodesBatch(bucket); err != nil {
			return err
		}
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
