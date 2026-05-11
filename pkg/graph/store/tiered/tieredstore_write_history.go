package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Atomic replace + history ---

func (ts *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if err := storecontract.ValidateNodeWrite(current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistorySnapshot(current.ID(), prevState); err != nil {
		return err
	}
	shard, checkin, err := ts.shardForNodeIDChecked(current.ID())
	if err != nil {
		return err
	}
	defer checkin()
	old, err := shard.GetNode(current.ID())
	if err != nil {
		return err
	}
	if err := ts.ensurePrimaryLabelClassUnchanged(old, current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeReplacement(old, prevState); err != nil {
		return err
	}
	id := current.ID().SnowflakeID()
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	if err := indexpkg.ValidateNodeVectorIndexes(ts.vectorIndexes, current, id); err != nil {
		return err
	}
	if err := shard.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return err
	}
	// Update Store-level vector indexes. The shard-level method updates
	// per-shard bs.vectorIndexes; ts.vectorIndexes is separate and must be kept in sync.
	indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	return indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, current, id)
}

func (ts *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistorySnapshot(current.ID(), prevState); err != nil {
		return err
	}
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

func (ts *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, n); err != nil {
		return err
	}
	if err := ts.checkOpen(); err != nil {
		return err
	}
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

func (ts *Store) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	shard, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
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
	if shard.HasNodeID(id) && !isArchive {
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

func (ts *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, r); err != nil {
		return err
	}
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

func (ts *Store) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return err
	}
	if err := storecontract.ValidateHistoryRetention(keepVersions); err != nil {
		return err
	}
	shard, checkin, isArchive, err := ts.shardForRelIDCheckedWithArchive(rid)
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
	if relationshipRowExists(shard, rid) && !isArchive {
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

func (ts *Store) DeleteNodeCascade(nid types.NodeID) error {
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()

	// Read node before deletion for Store-level vector index maintenance.
	// If this fails but the shard delete still removes the node, the error path
	// below purges by ID so stale TieredStore-level vector entries cannot survive.
	old, oldErr := shard.GetNode(types.NodeID(id))
	if oldErr != nil {
		old = nil
	}

	// Collect all connected relIDs from this shard's outIdx + inIdx.
	outRels := shard.OutgoingRelIDs(id)
	inRels := shard.IncomingRelIDs(id, 0)
	relSnapshots, err := ts.snapshotConnectedRelationships(outRels, inRels)
	if err != nil {
		return err
	}

	// Deduplicate and delete each relationship (cross-shard aware).
	seen := make(map[snowflake.ID]struct{}, len(outRels)+len(inRels))
	committedRels := make([]*types.Relationship, 0, len(relSnapshots))
	for _, relID := range outRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		snap := relSnapshots[relID]
		if snap == nil {
			continue
		}
		if err := ts.DeleteRelationship(types.RelID(relID)); err != nil {
			return ts.wrapPlainRelationshipRollbackError(err, committedRels)
		}
		committedRels = append(committedRels, snap)
	}
	for _, relID := range inRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		snap := relSnapshots[relID]
		if snap == nil {
			continue
		}
		if err := ts.DeleteRelationship(types.RelID(relID)); err != nil {
			return ts.wrapPlainRelationshipRollbackError(err, committedRels)
		}
		committedRels = append(committedRels, snap)
	}

	// Delete the node itself. Use the shard-level cascade path even after
	// live relationships were removed above so stale adjacency-only entries
	// are purged instead of making the final unconnected delete fail.
	if err := shard.DeleteNodeCascade(types.NodeID(id)); err != nil {
		ts.removeNodeFromVectorIndexesIfDeleted(shard, id, old)
		if _, getErr := shard.GetNode(types.NodeID(id)); getErr == nil {
			return ts.wrapPlainRelationshipRollbackError(err, committedRels)
		} else if !errors.Is(getErr, ErrNodeNotFound) {
			if rbErr := ts.rollbackPlainDeletedRelationships(committedRels); rbErr != nil {
				return fmt.Errorf("tiered: delete node cascade failed: %w (node state check failed: %v; relationship rollback failed: %v)", err, getErr, rbErr)
			}
			return fmt.Errorf("tiered: delete node cascade failed: %w (node state check failed: %v; relationship rollback attempted)", err, getErr)
		}
		return err
	}
	ts.removeNodeFromVectorIndexes(old, id)
	return nil
}

func (ts *Store) snapshotConnectedRelationships(outRels, inRels []snowflake.ID) (map[snowflake.ID]*types.Relationship, error) {
	snapshots := make(map[snowflake.ID]*types.Relationship, len(outRels)+len(inRels))
	for _, relID := range outRels {
		if _, ok := snapshots[relID]; ok {
			continue
		}
		rel, err := ts.GetRelationship(types.RelID(relID))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		snapshots[relID] = rel.DeepCopy()
	}
	for _, relID := range inRels {
		if _, ok := snapshots[relID]; ok {
			continue
		}
		rel, err := ts.GetRelationship(types.RelID(relID))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, err
		}
		snapshots[relID] = rel.DeepCopy()
	}
	return snapshots, nil
}

func (ts *Store) wrapPlainRelationshipRollbackError(err error, committed []*types.Relationship) error {
	if rbErr := ts.rollbackPlainDeletedRelationships(committed); rbErr != nil {
		return fmt.Errorf("%w (relationship rollback failed: %v)", err, rbErr)
	}
	return err
}

func (ts *Store) rollbackPlainDeletedRelationships(committed []*types.Relationship) error {
	var rollbackErrs []error
	for i := len(committed) - 1; i >= 0; i-- {
		if rbErr := ts.PutRelationship(committed[i]); rbErr != nil {
			rollbackErrs = append(rollbackErrs, fmt.Errorf("restore relationship %d: %w", committed[i].ID(), rbErr))
		}
	}
	return errors.Join(rollbackErrs...)
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
// write fails, the in/ leg is restored via PutRelIncoming so callers never
// observe a phantom delete-with-history that left a dangling in/ entry.
func (ts *Store) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	if err := storecontract.ValidateRelationshipHistorySnapshot(rid, tombstone); err != nil {
		return err
	}
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
	if err := storecontract.ValidateRelationshipReplacement(r, tombstone); err != nil {
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
	// (PutRelIncoming); restoring a tombstone-history write would require
	// reversing an atomic Badger batch that already advanced the version
	// chain, which is not part of the Store contract.
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	info := RelDeleteInfo{
		ID:      id,
		RelType: relType,
		StartID: startID,
		EndID:   endID,
	}
	if err := endShard.DeleteRelIncoming(info); err != nil {
		return err
	}
	if err := entityShard.DeleteRelWithHistory(types.RelID(id), prevVersion, tombstone); err != nil {
		// Roll back the in/ leg. If the rollback itself fails, surface both
		// errors so operators can run RunRepair to reconcile.
		if rbErr := endShard.PutRelIncoming(endID, startID, relType, id); rbErr != nil {
			return fmt.Errorf("tiered: cross-shard DeleteRelWithHistory entity-shard write failed: %w (rollback of in/ entry on end shard failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

// DeleteNodeWithHistory writes tombstone history for the node and all connected
// relationships, then deletes the live rows.
//
// Validates that relTombstones covers exactly the live relationships connected
// to the node, then deletes each with history before writing the node tombstone.
// Passes nil relTombstones to the shard-level DeleteNodeWithHistory because
// relationships are already handled individually above.
//
// Relationship deletes happen before the node tombstone write so endpoint
// constraints remain valid. If a later relationship delete or the node
// tombstone write fails before the node is actually removed, previously deleted
// relationships and their history are restored.
func (ts *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	if err := storecontract.ValidateNodeHistorySnapshot(nid, nodeTombstone); err != nil {
		return err
	}
	for _, rt := range relTombstones {
		if err := storecontract.ValidateRelationshipHistorySnapshot(rt.ID, rt.Tombstone); err != nil {
			return err
		}
	}
	id := nid.SnowflakeID()

	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()

	old, oldErr := shard.GetNode(types.NodeID(id))
	if oldErr == nil {
		if err := storecontract.ValidateNodeReplacement(old, nodeTombstone); err != nil {
			return err
		}
	} else {
		old = nil
	}

	// Collect all connected relIDs and validate tombstones before any delete.
	outRels := shard.OutgoingRelIDs(id)
	inRels := shard.IncomingRelIDs(id, 0)
	tombMap, liveRels, err := ts.validateDeleteNodeRelTombstones(nid, outRels, inRels, relTombstones)
	if err != nil {
		return err
	}

	seen := make(map[snowflake.ID]struct{}, len(outRels)+len(inRels))
	committedRels := make([]tieredDeletedRelSnapshot, 0, len(outRels)+len(inRels))
	for _, relID := range outRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		if _, live := liveRels[relID]; !live {
			continue
		}
		typedRelID := types.RelID(relID)
		snap, err := ts.deleteRelationshipForNodeWithHistory(typedRelID, tombMap)
		if err != nil {
			return ts.wrapDeleteNodeRelationshipRollbackError(err, committedRels)
		}
		committedRels = append(committedRels, snap)
	}
	for _, relID := range inRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		if _, live := liveRels[relID]; !live {
			continue
		}
		typedRelID := types.RelID(relID)
		snap, err := ts.deleteRelationshipForNodeWithHistory(typedRelID, tombMap)
		if err != nil {
			return ts.wrapDeleteNodeRelationshipRollbackError(err, committedRels)
		}
		committedRels = append(committedRels, snap)
	}

	// Delete the node itself with its tombstone. relTombstones=nil because
	// all rels were handled individually above.
	if err := shard.DeleteNodeWithHistory(types.NodeID(id), prevNodeVersion, nodeTombstone, nil); err != nil {
		ts.removeNodeFromVectorIndexesIfDeleted(shard, id, old)
		if _, getErr := shard.GetNode(types.NodeID(id)); getErr == nil {
			return ts.wrapDeleteNodeRelationshipRollbackError(err, committedRels)
		} else if !errors.Is(getErr, ErrNodeNotFound) {
			if rbErr := ts.rollbackDeletedRelationships(committedRels); rbErr != nil {
				return fmt.Errorf("tiered: delete node with history failed: %w (node state check failed: %v; relationship rollback failed: %v)", err, getErr, rbErr)
			}
			return fmt.Errorf("tiered: delete node with history failed: %w (node state check failed: %v; relationship rollback attempted)", err, getErr)
		}
		return err
	}
	ts.removeNodeFromVectorIndexes(old, id)
	return nil
}

func (ts *Store) validateDeleteNodeRelTombstones(nid types.NodeID, outRels, inRels []snowflake.ID, relTombstones []RelTombstone) (map[snowflake.ID]RelTombstone, map[snowflake.ID]struct{}, error) {
	connected := make(map[snowflake.ID]struct{}, len(outRels)+len(inRels))
	for _, relID := range outRels {
		connected[relID] = struct{}{}
	}
	for _, relID := range inRels {
		connected[relID] = struct{}{}
	}

	tombMap := make(map[snowflake.ID]RelTombstone, len(relTombstones))
	liveRels := make(map[snowflake.ID]struct{}, len(connected))
	for _, rt := range relTombstones {
		rawID := rt.ID.SnowflakeID()
		if _, dup := tombMap[rawID]; dup {
			return nil, nil, fmt.Errorf("%w: duplicate relationship tombstone %d", ErrInvalidStoreMutation, rt.ID)
		}
		tombMap[rawID] = rt
		if _, ok := connected[rawID]; !ok {
			return nil, nil, fmt.Errorf("%w: relationship tombstone %d is not connected to node %d", ErrInvalidStoreMutation, rt.ID, nid)
		}
		old, err := ts.GetRelationship(rt.ID)
		if err != nil {
			return nil, nil, err
		}
		if err := storecontract.ValidateRelationshipReplacement(old, rt.Tombstone); err != nil {
			return nil, nil, err
		}
		liveRels[rawID] = struct{}{}
	}

	for relID := range connected {
		if _, ok := liveRels[relID]; ok {
			continue
		}
		if _, err := ts.GetRelationship(types.RelID(relID)); err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, nil, err
		}
		return nil, nil, fmt.Errorf("%w: missing relationship tombstone %d", ErrInvalidStoreMutation, relID)
	}
	return tombMap, liveRels, nil
}

type tieredDeletedRelSnapshot struct {
	rel     *types.Relationship
	history []*types.Relationship
}

func (ts *Store) deleteRelationshipForNodeWithHistory(rid types.RelID, tombMap map[snowflake.ID]RelTombstone) (tieredDeletedRelSnapshot, error) {
	rt, ok := tombMap[rid.SnowflakeID()]
	if !ok {
		return tieredDeletedRelSnapshot{}, fmt.Errorf("%w: missing relationship tombstone %d", ErrInvalidStoreMutation, rid)
	}
	rel, err := ts.GetRelationship(rid)
	if err != nil {
		return tieredDeletedRelSnapshot{}, err
	}
	history, err := ts.GetRelHistory(rid)
	if err != nil {
		return tieredDeletedRelSnapshot{}, err
	}
	snap := tieredDeletedRelSnapshot{
		rel:     rel.DeepCopy(),
		history: deepCopyTieredRelHistory(history),
	}

	if err := ts.DeleteRelWithHistory(rid, rt.PrevVersion, rt.Tombstone); err != nil {
		return tieredDeletedRelSnapshot{}, err
	}
	return snap, nil
}

func (ts *Store) wrapDeleteNodeRelationshipRollbackError(err error, committed []tieredDeletedRelSnapshot) error {
	if rbErr := ts.rollbackDeletedRelationships(committed); rbErr != nil {
		return fmt.Errorf("%w (relationship rollback failed: %v)", err, rbErr)
	}
	return err
}

func (ts *Store) rollbackDeletedRelationships(committed []tieredDeletedRelSnapshot) error {
	var firstErr error
	capture := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for i := len(committed) - 1; i >= 0; i-- {
		snap := committed[i]
		capture(ts.PutRelationship(snap.rel))
		capture(ts.restoreRelHistorySnapshot(snap.rel.ID(), snap.history))
	}
	return firstErr
}

func (ts *Store) restoreRelHistorySnapshot(id types.RelID, history []*types.Relationship) error {
	if err := ts.TruncateRelHistory(id, 0); err != nil {
		return err
	}
	for _, r := range history {
		if err := ts.PutRelVersion(id, r.Version(), r); err != nil {
			return err
		}
	}
	return nil
}

func deepCopyTieredRelHistory(history []*types.Relationship) []*types.Relationship {
	if len(history) == 0 {
		return nil
	}
	out := make([]*types.Relationship, len(history))
	for i, r := range history {
		out[i] = r.DeepCopy()
	}
	return out
}

func (ts *Store) removeNodeFromVectorIndexes(old *types.Node, id snowflake.ID) {
	ts.vectorIdxMu.Lock()
	defer ts.vectorIdxMu.Unlock()
	if old != nil {
		indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	} else {
		indexpkg.PurgeNodeFromAllVectorIndexes(ts.vectorIndexes, id)
	}
}

func (ts *Store) removeNodeFromVectorIndexesIfDeleted(shard *BadgerStore, id snowflake.ID, old *types.Node) {
	if _, err := shard.GetNode(types.NodeID(id)); errors.Is(err, ErrNodeNotFound) {
		ts.removeNodeFromVectorIndexes(old, id)
	}
}
