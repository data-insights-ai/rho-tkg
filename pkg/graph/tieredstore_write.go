package graph

import (
	"errors"
	"fmt"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Node write operations ---
// Nodes are single-shard: routed entirely by primary label classification.

func (ts *TieredStore) PutNode(n *types.Node) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}
	shard := ts.shardForNode(n.PrimaryLabelToken().Value())
	if err := shard.PutNode(n); err != nil {
		return err
	}
	id := n.ID().SnowflakeID()
	ts.vectorIdxMu.Lock()
	addNodeToVectorIndexes(ts.vectorIndexes, n, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) ReplaceNode(n *types.Node) error {
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	addNodeToVectorIndexes(ts.vectorIndexes, n, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) DeleteNode(nid types.NodeID) error {
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	addNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	addNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	addNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
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
		removeNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
	addNodeToVectorIndexes(ts.vectorIndexes, updatedNode, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *TieredStore) PutNodesBatch(nodes []*types.Node) error {
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
	return nil
}

func (ts *TieredStore) DeleteNodesBatch(ids []types.NodeID) error {
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
	for shard, bucket := range shardBuckets {
		if err := shard.DeleteNodesBatch(bucket); err != nil {
			return err
		}
	}
	return nil
}

// --- Relationship write operations ---
// Relationships may be cross-shard when start and end nodes live in different shards.
// After rotation, two event entities may live in different shards (warm vs hot).
// We use shard-based routing (shardForNodeIDChecked) instead of class-based routing
// to correctly handle E→E cross-shard relationships, and pin cold owners for the
// duration of the write.

func (ts *TieredStore) PutRelationship(r *types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	relID := r.ID().SnowflakeID()

	// Resolve actual shards — not class. Two event entities may be in different shards.
	// Use the checked variants so cold owner shards stay pinned during the
	// split-write below.
	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return err
	}
	defer endCheckin()

	if startShard == endShard {
		// Same shard: delegate entirely.
		return startShard.PutRelationship(r)
	}

	// Cross-shard: verify endpoints exist.
	entityShard := startShard // entity + out/
	inShard := endShard       // in/

	if !entityShard.hasNodeID(startID) {
		return ErrNodeNotFound
	}
	if !inShard.hasNodeID(endID) {
		return ErrNodeNotFound
	}

	// Split-write ordering per spec §12.
	if endShard == ts.refShard {
		// E→R: reference shard (in/) first — critical path for SOC queries.
		if err := inShard.putRelIncoming(endID, startID, relType, relID); err != nil {
			return err
		}
		if err := entityShard.putRelEntityAndOut(r); err != nil {
			// Roll back the in/ write so the partial state isn't left visible.
			info := relDeleteInfo{id: relID, relType: relType, startID: startID, endID: endID}
			if rbErr := inShard.deleteRelIncoming(info); rbErr != nil {
				return fmt.Errorf("tiered: put cross-shard relationship entity failed after in/ write: %w (rollback in/ failed: %v)", err, rbErr)
			}
			return err
		}
		return nil
	}
	// R→E or E→E(cross-shard): entity shard first.
	if err := entityShard.putRelEntityAndOut(r); err != nil {
		return err
	}
	if err := inShard.putRelIncoming(endID, startID, relType, relID); err != nil {
		// Roll back the entity/out write so the partial state isn't left visible.
		if _, rbErr := entityShard.deleteRelEntityAndOut(relID); rbErr != nil {
			return fmt.Errorf("tiered: put cross-shard relationship in/ failed after entity write: %w (rollback entity/out failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func (ts *TieredStore) ReplaceRelationship(r *types.Relationship) error {
	shard, checkin, err := ts.shardForRelIDChecked(r.ID())
	if err != nil {
		return err
	}
	defer checkin()
	return shard.ReplaceRelationship(r)
}

func (ts *TieredStore) DeleteRelationship(rid types.RelID) error {
	id := rid.SnowflakeID()

	// Find which shard owns the entity. Use the checked variant so a cold
	// owner stays pinned until the writes below complete.
	entityShard, entityCheckin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return err
	}
	defer entityCheckin()

	// Check if this is a cross-shard relationship by reading the rel metadata.
	r, err := entityShard.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}

	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return err
	}
	defer endCheckin()

	if startShard == endShard {
		// Same shard: delegate entirely.
		return entityShard.DeleteRelationship(types.RelID(id))
	}

	// Cross-shard: delete entity+out from entity shard, in/ from in shard.
	// On failure of the second leg, restore the entity+out write so a re-read
	// still observes the rel rather than leaving a phantom in/ entry on the
	// end-node shard.
	info, err := entityShard.deleteRelEntityAndOut(id)
	if err != nil {
		return err
	}

	inShard := endShard
	if err := inShard.deleteRelIncoming(info); err != nil {
		if rbErr := entityShard.putRelEntityAndOut(r); rbErr != nil {
			return fmt.Errorf("tiered: delete cross-shard relationship in/ failed after entity/out delete: %w (rollback entity/out failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func (ts *TieredStore) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	// Partition: same-shard rels can use batch, cross-shard use individual put.
	// Group by *BadgerStore pointer for batching. Owner shards are checked
	// out so cold owners stay pinned for the per-shard batch write below.
	shardBuckets := make(map[*BadgerStore][]*types.Relationship)
	var checkins []func()
	releaseAll := func() {
		for _, fn := range checkins {
			fn()
		}
	}

	for _, r := range rels {
		startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
		if err != nil {
			releaseAll()
			return err
		}
		endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
		if err != nil {
			startCheckin()
			releaseAll()
			return err
		}

		if startShard != endShard {
			// Cross-shard: individual put — release these checkins immediately
			// since PutRelationship pins again internally. There is a brief
			// unpinned window between checkin here and PutRelationship's
			// internal re-pin, but closeIdleShards cannot fire inside it:
			// each owner shard's lastAccess was just bumped by the prior
			// checkout (line above), and IdleTimeout >= 5min, so the window
			// is bounded by IdleTimeout and the activeReqs gap is microseconds.
			startCheckin()
			endCheckin()
			if err := ts.PutRelationship(r); err != nil {
				releaseAll()
				return err
			}
			continue
		}

		// Same-shard: hold the start-shard pin (covers both endpoints) until
		// the per-shard batch write below. The endpoint pin is redundant but
		// cheap; release it now so we don't leak duplicate references.
		endCheckin()
		shardBuckets[startShard] = append(shardBuckets[startShard], r)
		checkins = append(checkins, startCheckin)
	}
	defer releaseAll()

	// Write per-shard buckets, tracking committed batches for best-effort rollback.
	// If a later shard fails, previously committed shards are rolled back to prevent
	// silent partial state (e.g., rels in refShard with no corresponding hot-shard rels).
	type committedBatch struct {
		shard *BadgerStore
		rels  []*types.Relationship
	}
	var committed []committedBatch

	for shard, bucket := range shardBuckets {
		if err := shard.PutRelationshipsBatch(bucket); err != nil {
			// Best-effort rollback: delete rels written to previously committed shards.
			for _, cb := range committed {
				for _, r := range cb.rels {
					_ = cb.shard.DeleteRelationship(r.ID())
				}
			}
			return fmt.Errorf("tiered: put rels batch (prior shard writes rolled back best-effort): %w", err)
		}
		committed = append(committed, committedBatch{shard: shard, rels: bucket})
	}
	return nil
}

func (ts *TieredStore) DeleteRelationshipsBatch(ids []types.RelID) error {
	// Cross-shard aware: per-ID delete. Each relationship may be cross-shard
	// (entity+out/ in one shard, in/ in another), so DeleteRelationship handles
	// the split-delete logic correctly at the cost of one shard-resolution lookup
	// per ID. A future optimisation could partition same-shard rels and batch them;
	// for v3 workloads the per-ID path is acceptable.
	// TODO(v3.1.0): partition by shard for same-shard rels to allow store-level batching.
	for _, id := range ids {
		if err := ts.DeleteRelationship(id); err != nil {
			return err
		}
	}
	return nil
}

// --- Atomic replace + history ---

func (ts *TieredStore) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	shard, checkin, err := ts.shardForNodeIDChecked(current.ID())
	if err != nil {
		return err
	}
	defer checkin()
	return shard.ReplaceNodeWithHistory(current, prevVersion, prevState)
}

func (ts *TieredStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	// Resolve by rel ID — the start node is not authoritative for the rel's
	// home shard once the entity has been migrated (e.g., archived together
	// with its endpoints), and the start-node-keyed lookup also skips the
	// refArchive probe baked into shardForRelIDChecked. Pin the resolved
	// owner for the duration of the atomic replace+history write so a cold
	// owner cannot be closed out by closeIdleShards mid-write.
	shard, checkin, err := ts.shardForRelIDChecked(current.ID())
	if err != nil {
		return err
	}
	defer checkin()
	return shard.ReplaceRelWithHistory(current, prevVersion, prevState)
}

// --- Version history writes ---

func (ts *TieredStore) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	// Reference snapshots route to refShard regardless of timestamp; otherwise
	// fall back to id-based resolution with checkout/checkin so a cold owner
	// stays pinned for the write.
	if n != nil && ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
		return ts.refShard.PutNodeVersion(nid, version, n)
	}
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	return shard.PutNodeVersion(nid, version, n)
}

func (ts *TieredStore) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	id := nid.SnowflakeID()
	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()
	history, err := shard.GetNodeHistory(nid)
	if err != nil {
		return err
	}
	if len(history) > 0 {
		return shard.TruncateNodeHistory(nid, keepVersions)
	}

	// If the live entity is on this shard, the empty history is authoritative —
	// the truncate is a no-op and there is no need to fan out across shards.
	// Exception: when the live entity is on refArchive, pre-archive history may
	// still live on refShard, so fall through to the fan-out.
	if shard.hasNodeID(id) && shard != ts.refArchive.Load() {
		return shard.TruncateNodeHistory(nid, keepVersions)
	}

	truncated := false
	err = ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetNodeHistory(nid)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			return false, nil
		}
		truncated = true
		return true, candidate.TruncateNodeHistory(nid, keepVersions)
	})
	if err != nil {
		return err
	}
	if truncated {
		return nil
	}
	return shard.TruncateNodeHistory(nid, keepVersions)
}

func (ts *TieredStore) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	// Route by rel ID — consistent with ReplaceRelWithHistory above. The
	// previous start-node-keyed routing skipped the refArchive probe baked
	// into shardForRelIDChecked and diverged from the path that writes the
	// current rel state, so a rel that was archived together with its
	// start node had its history written on the wrong shard whenever the
	// caller hit this path (currently only ImportGraph; future history
	// writers must also land on the rel's home shard). Pin the resolved
	// owner so a cold owner cannot be closed mid-write.
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return err
	}
	defer checkin()
	return shard.PutRelVersion(rid, version, r)
}

func (ts *TieredStore) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	id := rid.SnowflakeID()
	shard, checkin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return err
	}
	defer checkin()
	history, err := shard.GetRelHistory(rid)
	if err != nil {
		return err
	}
	if len(history) > 0 {
		return shard.TruncateRelHistory(rid, keepVersions)
	}

	// If the live rel entity is on this shard, the empty history is
	// authoritative — the truncate is a no-op on this shard and there is no
	// need to fan out across shards.
	// Exception: when the live rel is on refArchive, pre-archive history may
	// still live on refShard, so fall through to the fan-out.
	if shard.hasRelID(id) && shard != ts.refArchive.Load() {
		return shard.TruncateRelHistory(rid, keepVersions)
	}

	truncated := false
	err = ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetRelHistory(rid)
		if err != nil {
			return false, err
		}
		if len(history) == 0 {
			return false, nil
		}
		truncated = true
		return true, candidate.TruncateRelHistory(rid, keepVersions)
	})
	if err != nil {
		return err
	}
	if truncated {
		return nil
	}
	// No shard owns history for this id — delegate to the originally resolved
	// shard so its NotFound/no-op semantics are surfaced consistently.
	return shard.TruncateRelHistory(rid, keepVersions)
}

// --- Cascade operations ---

func (ts *TieredStore) DeleteNodeCascade(nid types.NodeID) error {
	id := nid.SnowflakeID()

	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()

	// Collect all connected relIDs from this shard's outIdx + inIdx.
	outRels := shard.outgoingRelIDs(id)
	inRels := shard.incomingRelIDs(id, 0)

	// Deduplicate and delete each relationship (cross-shard aware).
	seen := make(map[snowflake.ID]struct{}, len(outRels)+len(inRels))
	for _, relID := range outRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		if err := ts.DeleteRelationship(types.RelID(relID)); err != nil {
			return err
		}
	}
	for _, relID := range inRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		if err := ts.DeleteRelationship(types.RelID(relID)); err != nil {
			return err
		}
	}

	// Delete the node itself.
	return shard.DeleteNode(types.NodeID(id))
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in one per-shard batch flush.
//
// Same shard: delegates to the entity shard (fully atomic within the shard).
// Cross-shard: delete the in/ leg first, then atomically tombstone+delete on
// the entity shard. Reversing the order vs the plain DeleteRelationship path
// makes rollback feasible — undoing a tombstone-history write on the entity
// shard would require removing the history record, which is harder than
// re-creating an in/ index entry on the end-node shard. If the entity-shard
// write fails, the in/ leg is restored via putRelIncoming so callers never
// observe a phantom delete-with-history that left a dangling in/ entry.
func (ts *TieredStore) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	id := rid.SnowflakeID()

	entityShard, entityCheckin, err := ts.shardForRelIDChecked(rid)
	if err != nil {
		return err
	}
	defer entityCheckin()

	r, err := entityShard.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}

	startShard, startCheckin, err := ts.shardForNodeIDChecked(r.StartNodeID())
	if err != nil {
		return err
	}
	defer startCheckin()
	endShard, endCheckin, err := ts.shardForNodeIDChecked(r.EndNodeID())
	if err != nil {
		return err
	}
	defer endCheckin()

	if startShard == endShard {
		// Same shard: fully atomic.
		return entityShard.DeleteRelWithHistory(types.RelID(id), prevVersion, tombstone)
	}

	// Cross-shard: delete in/ on end-node shard first so we can roll it back
	// if the entity-shard write fails. Restoring an in/ entry is symmetric
	// (putRelIncoming); restoring a tombstone-history write would require
	// reversing an atomic Badger batch that already advanced the version
	// chain, which is not part of the Store contract.
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	info := relDeleteInfo{
		id:      id,
		relType: relType,
		startID: startID,
		endID:   endID,
	}
	if err := endShard.deleteRelIncoming(info); err != nil {
		return err
	}
	if err := entityShard.DeleteRelWithHistory(types.RelID(id), prevVersion, tombstone); err != nil {
		// Roll back the in/ leg. If the rollback itself fails, surface both
		// errors so operators can run RunRepair to reconcile.
		if rbErr := endShard.putRelIncoming(endID, startID, relType, id); rbErr != nil {
			return fmt.Errorf("tiered: cross-shard DeleteRelWithHistory entity-shard write failed: %w (rollback of in/ entry on end shard failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

// DeleteNodeWithHistory atomically writes tombstone history for the node and all
// connected relationships, then cascade-deletes.
//
// Builds a tombMap for fast lookup, then iterates rels: uses DeleteRelWithHistory
// for rels in tombMap, falls back to DeleteRelationship for unexpected rels.
// Passes nil relTombstones to the shard-level DeleteNodeWithHistory (rels already
// handled above individually).
//
// Cross-shard atomicity is per-shard only (B7 limitation — same as DeleteNodeCascade).
func (ts *TieredStore) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	id := nid.SnowflakeID()

	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()

	// Build lookup map: relID → RelTombstone.
	tombMap := make(map[snowflake.ID]RelTombstone, len(relTombstones))
	for _, rt := range relTombstones {
		tombMap[rt.ID.SnowflakeID()] = rt
	}

	// Collect all connected relIDs and delete each with history.
	outRels := shard.outgoingRelIDs(id)
	inRels := shard.incomingRelIDs(id, 0)

	seen := make(map[snowflake.ID]struct{}, len(outRels)+len(inRels))
	for _, relID := range outRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		typedRelID := types.RelID(relID)
		if rt, ok := tombMap[relID]; ok {
			if err := ts.DeleteRelWithHistory(typedRelID, rt.PrevVersion, rt.Tombstone); err != nil {
				return err
			}
		} else {
			// Unexpected rel (not in tombstones) — fall back to plain delete.
			if err := ts.DeleteRelationship(typedRelID); err != nil {
				return err
			}
		}
	}
	for _, relID := range inRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		typedRelID := types.RelID(relID)
		if rt, ok := tombMap[relID]; ok {
			if err := ts.DeleteRelWithHistory(typedRelID, rt.PrevVersion, rt.Tombstone); err != nil {
				return err
			}
		} else {
			if err := ts.DeleteRelationship(typedRelID); err != nil {
				return err
			}
		}
	}

	// Delete the node itself with its tombstone. relTombstones=nil because
	// all rels were handled individually above.
	return shard.DeleteNodeWithHistory(types.NodeID(id), prevNodeVersion, nodeTombstone, nil)
}

// --- Property indexes ---

// ErrEventPropertyIndex is returned when attempting to create a property index
// on an event label in a TieredStore. Only reference entities support indexes.
var ErrEventPropertyIndex = errors.New("graph: property indexes only supported for reference entities in TieredStore")

// ErrPrimaryLabelClassMutation is returned when a label mutation would change a
// node's primary-label ontology class (reference vs event). Such a change would
// move the live entity to a different shard while leaving prior history on the
// original shard, fragmenting the version chain. Callers must instead delete
// and recreate the node under the new class.
var ErrPrimaryLabelClassMutation = errors.New("graph: label mutation would change primary-label class (reference<->event); not supported on TieredStore")

// ensurePrimaryLabelClassUnchanged compares the primary-label classes of the
// pre-mutation and post-mutation nodes and rejects the mutation if they differ.
// old may be nil (entity already gone) — in that case the check is skipped.
func (ts *TieredStore) ensurePrimaryLabelClassUnchanged(old, updated *types.Node) error {
	if old == nil || updated == nil {
		return nil
	}
	oldClass := ts.ontology.ClassifyByToken(old.PrimaryLabelToken().Value())
	newClass := ts.ontology.ClassifyByToken(updated.PrimaryLabelToken().Value())
	if oldClass != newClass {
		return ErrPrimaryLabelClassMutation
	}
	return nil
}

func (ts *TieredStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	if ts.ontology.ClassifyByToken(labelToken) != ClassReference {
		return ErrEventPropertyIndex
	}
	return ts.refShard.CreatePropertyIndex(labelToken, propertyKey)
}

func (ts *TieredStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	shard := ts.shardForNode(labelToken)
	return shard.DropPropertyIndex(labelToken, propertyKey)
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal index on nodes with the given label token
// across all shards (reference + all event shards). New hot shards created via
// rotation will also inherit the index.
func (ts *TieredStore) CreateTemporalIndex(labelToken uint16) error {
	ts.mu.RLock()
	shards, release, err := ts.allActiveShards()
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	defer release()

	for _, shard := range shards {
		if err := shard.CreateTemporalIndex(labelToken); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			return err
		}
	}

	ts.tempIdxMu.Lock()
	// Record label token if not already tracked.
	found := false
	for _, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			found = true
			break
		}
	}
	if !found {
		ts.tempIdxLabels = append(ts.tempIdxLabels, labelToken)
	}
	ts.tempIdxMu.Unlock()
	return nil
}

// DropTemporalIndex removes the temporal index for the given label token
// from all shards.
func (ts *TieredStore) DropTemporalIndex(labelToken uint16) error {
	ts.mu.RLock()
	shards, release, err := ts.allActiveShards()
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	defer release()

	var lastErr error
	found := false
	for _, shard := range shards {
		if err := shard.DropTemporalIndex(labelToken); err != nil {
			if !errors.Is(err, ErrTemporalIndexNotFound) {
				lastErr = err
			}
		} else {
			found = true
		}
	}
	if lastErr != nil {
		return lastErr
	}

	ts.tempIdxMu.Lock()
	for i, tok := range ts.tempIdxLabels {
		if tok == labelToken {
			ts.tempIdxLabels = append(ts.tempIdxLabels[:i], ts.tempIdxLabels[i+1:]...)
			break
		}
	}
	ts.tempIdxMu.Unlock()

	if !found {
		return ErrTemporalIndexNotFound
	}
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token across all shards (reference + all event shards).
// New hot shards created via rotation will NOT automatically inherit HFI — callers
// must re-call CreateHighFrequencyIndex after rotation if needed.
// Returns ErrTemporalIndexExists if any temporal index already exists for this label.
func (ts *TieredStore) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	ts.mu.RLock()
	shards, release, err := ts.allActiveShards()
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	defer release()

	for _, shard := range shards {
		if err := shard.CreateHighFrequencyIndex(labelToken, bucketSize); err != nil && !errors.Is(err, ErrTemporalIndexExists) {
			return err
		}
	}
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token
// from all shards. Returns ErrTemporalIndexNotFound if no index exists on any shard.
func (ts *TieredStore) DropHighFrequencyIndex(labelToken uint16) error {
	ts.mu.RLock()
	shards, release, err := ts.allActiveShards()
	ts.mu.RUnlock()
	if err != nil {
		return err
	}
	defer release()

	var lastErr error
	found := false
	for _, shard := range shards {
		if err := shard.DropHighFrequencyIndex(labelToken); err != nil {
			if !errors.Is(err, ErrTemporalIndexNotFound) {
				lastErr = err
			}
		} else {
			found = true
		}
	}
	if lastErr != nil {
		return lastErr
	}
	if !found {
		return ErrTemporalIndexNotFound
	}
	return nil
}

// CreateVectorIndex creates a vector similarity index spanning all shards.
// The index is maintained at the TieredStore level (not per-shard).
// Scans existing nodes across all shards to populate the index.
// Returns ErrVectorIndexExists on duplicate.
func (ts *TieredStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}

	ts.vectorIdxMu.Lock()
	if _, exists := ts.vectorIndexes[key]; exists {
		ts.vectorIdxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &vectorIndex{dims: dims, metric: metric}
	ts.vectorIndexes[key] = vi
	ts.vectorIdxMu.Unlock()

	// Populate from all existing nodes across all shards.
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		return err
	}
	for _, n := range nodes {
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		val, ok := n.GetProperty(propertyKey)
		if !ok {
			continue
		}
		vec, ok := toFloat32Slice(val)
		if !ok {
			continue
		}
		id := n.ID().SnowflakeID()
		_ = vi.add(id, vec)
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (ts *TieredStore) DropVectorIndex(labelToken uint16, propertyKey string) error {
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()

	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := ts.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(ts.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
func (ts *TieredStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	ts.vectorIdxMu.RLock()
	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	vi, exists := ts.vectorIndexes[key]
	ts.vectorIdxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	ids, err := vi.searchNearest(query, k)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := ts.GetNode(types.NodeID(id))
		if err != nil {
			continue // node may have been deleted concurrently
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// allActiveShards returns all currently open BadgerStores (refShard +
// refArchive + open event shards), and a release function the caller MUST
// invoke when done — it drops the refArchive pin obtained via
// checkoutArchive. Caller must hold ts.mu.RLock or ts.mu.Lock for the
// event-shard snapshot.
//
// refArchive is included whenever it is open (lazy-opens on first
// archived entity). Excluding it would silently skip archived reference
// entities from index/admin operations even though the archive holds
// indexed reference data with the same shape as refShard.
func (ts *TieredStore) allActiveShards() ([]*BadgerStore, func(), error) {
	shards := make([]*BadgerStore, 0, 2+len(ts.eventShards))
	shards = append(shards, ts.refShard)
	for _, es := range ts.eventShards {
		if es.store != nil {
			shards = append(shards, es.store)
		}
	}
	archive, archiveCheckin, archiveErr := ts.checkoutArchive()
	if archiveErr != nil {
		return nil, nil, archiveErr
	}
	if archive != nil {
		shards = append(shards, archive)
		return shards, archiveCheckin, nil
	}
	return shards, func() {}, nil
}

// --- Reference archive ---

// ErrNotReferenceEntity is returned when attempting to archive a non-reference entity.
var ErrNotReferenceEntity = errors.New("graph: entity is not a reference entity")

// ArchiveNode moves a reference node and all its relationships from refShard
// to refArchive. Only reference entities can be archived.
// The node must exist in refShard. Event nodes cannot be archived.
//
// Atomicity: this operation is NOT transactional across the two BadgerStores.
// On failure the rollback is best-effort (DeleteNodeCascade on the target).
// If both the primary write AND the rollback fail, data may exist in both
// stores simultaneously. The repair subsystem (tieredstore_repair.go) can
// detect and resolve this: a node present in both refShard and refArchive
// is flagged as a split-brain condition and corrected using the refShard copy.
func (ts *TieredStore) ArchiveNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	// 1. Verify node is in refShard.
	if !ts.refShard.hasNodeID(id) {
		return fmt.Errorf("graph: archive: %w", ErrNodeNotFound)
	}

	// 2. Lazy-open refArchive.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}
	archive := ts.refArchive.Load()

	// 3. Read node from refShard.
	node, err := ts.refShard.GetNode(types.NodeID(id))
	if err != nil {
		return fmt.Errorf("graph: archive read node: %w", err)
	}

	// 4. Read all outgoing + incoming rels from refShard.
	outIDs := ts.refShard.outgoingRelIDs(id)
	inIDs := ts.refShard.incomingRelIDs(id, 0)

	// Deduplicate and collect unique relIDs.
	seen := make(map[snowflake.ID]struct{}, len(outIDs)+len(inIDs))
	var relIDs []snowflake.ID
	for _, rid := range outIDs {
		if _, ok := seen[rid]; !ok {
			seen[rid] = struct{}{}
			relIDs = append(relIDs, rid)
		}
	}
	for _, rid := range inIDs {
		if _, ok := seen[rid]; !ok {
			seen[rid] = struct{}{}
			relIDs = append(relIDs, rid)
		}
	}

	// Read all relationship entities from refShard.
	var rels []*types.Relationship
	for _, rid := range relIDs {
		r, err := ts.refShard.GetRelationship(types.RelID(rid))
		if errors.Is(err, ErrRelNotFound) {
			continue // cross-shard entity, skip
		}
		if err != nil {
			return fmt.Errorf("graph: archive read rel %d: %w", rid, err)
		}
		rels = append(rels, r)
	}

	// 5. Write node + rels to refArchive.
	if err := archive.PutNode(node); err != nil {
		return fmt.Errorf("graph: archive write node: %w", err)
	}

	for _, r := range rels {
		// PutRelationship validates endpoint existence. If the other endpoint
		// isn't in the archive (partial archive), skip the rel — it will be
		// deleted by DeleteNodeCascade below.
		err := archive.PutRelationship(r)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			// Best-effort rollback: remove partially written data from archive.
			_ = archive.DeleteNodeCascade(types.NodeID(id))
			return fmt.Errorf("graph: archive write rel: %w", err)
		}
	}

	// 6. Delete from refShard (cascade deletes node + all rels in refShard).
	if err := ts.refShard.DeleteNodeCascade(types.NodeID(id)); err != nil {
		// Best-effort rollback: remove data from archive since source delete failed.
		_ = archive.DeleteNodeCascade(types.NodeID(id))
		return fmt.Errorf("graph: archive delete from ref: %w", err)
	}

	return nil
}

// RestoreNode moves a reference node and all its relationships from refArchive
// back to refShard. Reverse of ArchiveNode.
//
// Atomicity: same best-effort rollback guarantee as ArchiveNode — NOT
// transactional across two BadgerStores. If both the primary write and the
// rollback fail, the repair subsystem resolves the split-brain state.
func (ts *TieredStore) RestoreNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	// 1. Ensure archive is open.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}
	archive := ts.refArchive.Load()

	// 2. Verify node is in archive.
	if !archive.hasNodeID(id) {
		return fmt.Errorf("graph: restore: %w", ErrNodeNotFound)
	}

	// 3. Read node from archive.
	node, err := archive.GetNode(types.NodeID(id))
	if err != nil {
		return fmt.Errorf("graph: restore read node: %w", err)
	}

	// 4. Read all rels from archive.
	outIDs := archive.outgoingRelIDs(id)
	inIDs := archive.incomingRelIDs(id, 0)

	seen := make(map[snowflake.ID]struct{}, len(outIDs)+len(inIDs))
	var relIDs []snowflake.ID
	for _, rid := range outIDs {
		if _, ok := seen[rid]; !ok {
			seen[rid] = struct{}{}
			relIDs = append(relIDs, rid)
		}
	}
	for _, rid := range inIDs {
		if _, ok := seen[rid]; !ok {
			seen[rid] = struct{}{}
			relIDs = append(relIDs, rid)
		}
	}

	var rels []*types.Relationship
	for _, rid := range relIDs {
		r, err := archive.GetRelationship(types.RelID(rid))
		if errors.Is(err, ErrRelNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("graph: restore read rel %d: %w", rid, err)
		}
		rels = append(rels, r)
	}

	// 5. Write to refShard.
	if err := ts.refShard.PutNode(node); err != nil {
		return fmt.Errorf("graph: restore write node: %w", err)
	}

	for _, r := range rels {
		err := ts.refShard.PutRelationship(r)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			// Best-effort rollback: remove partially written data from refShard.
			_ = ts.refShard.DeleteNodeCascade(types.NodeID(id))
			return fmt.Errorf("graph: restore write rel: %w", err)
		}
	}

	// 6. Delete from archive.
	if err := archive.DeleteNodeCascade(types.NodeID(id)); err != nil {
		// Best-effort rollback: remove data from refShard since archive delete failed.
		_ = ts.refShard.DeleteNodeCascade(types.NodeID(id))
		return fmt.Errorf("graph: restore delete from archive: %w", err)
	}

	return nil
}
