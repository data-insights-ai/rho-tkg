package tiered

import (
	"errors"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Atomic replace + history ---

func (ts *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeWrite(current); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(current.ID(), prevVersion, prevState); err != nil {
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
	if err := storecontract.ValidateNodeLiveVersion(old, prevVersion); err != nil {
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
	vectorUpdates, err := indexpkg.PrepareNodeVectorIndexUpdates(ts.vectorIndexes, current, id)
	if err != nil {
		return err
	}
	if err := shard.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return err
	}
	// Update Store-level vector indexes. The shard-level method updates
	// per-shard bs.vectorIndexes; ts.vectorIndexes is separate and must be kept in sync.
	indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, old, id)
	return indexpkg.AddPreparedNodeToVectorIndexes(vectorUpdates, id)
}

func (ts *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipWrite(current); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(current.ID(), prevVersion, prevState); err != nil {
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
	old, err := shard.GetRelationship(current.ID())
	if err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipLiveVersion(old, prevVersion); err != nil {
		return err
	}
	return shard.ReplaceRelWithHistory(current, prevVersion, prevState)
}

// --- Version history writes ---

func (ts *Store) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, version, n); err != nil {
		return err
	}
	// Reference snapshots route to refShard regardless of timestamp; otherwise
	// fall back to id-based resolution with checkout/checkin so a cold owner
	// stays pinned for the write.
	if n != nil && ts.ontology.ClassifyByToken(n.PrimaryLabelToken().Value()) == ClassReference {
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return err
		}
		defer refCheckin()
		return ref.PutNodeVersion(nid, version, n)
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
	shard, checkin, isArchive, err := ts.shardForNodeIDCheckedWithArchive(nid)
	if err != nil {
		return err
	}
	defer checkin()
	history, err := shard.GetNodeHistory(nid)
	if err != nil {
		return err
	}
	liveHere, err := nodeRowLive(shard, nid)
	if err != nil {
		return err
	}

	if liveHere && !isArchive {
		if shard == ts.refShard {
			sources := nodeHistorySourcesFrom(shard, history)
			archive, archiveCheckin, err := ts.checkoutArchive()
			if err != nil {
				return err
			}
			if archive != nil {
				archiveHistory, err := archive.GetNodeHistory(nid)
				archiveCheckin()
				if err != nil {
					return err
				}
				appendNodeHistorySource(&sources, archive, archiveHistory)
			} else {
				archiveCheckin()
			}
			return truncateNodeHistorySources(nid, sources, keepVersions)
		}
		return shard.TruncateNodeHistory(nid, keepVersions)
	}
	if isArchive {
		sources := nodeHistorySourcesFrom(shard, history)
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return err
		}
		refHistory, err := ref.GetNodeHistory(nid)
		refCheckin()
		if err != nil {
			return err
		}
		appendNodeHistorySource(&sources, ref, refHistory)
		return truncateNodeHistorySources(nid, sources, keepVersions)
	}

	sources := nodeHistorySourcesFrom(shard, history)
	err = ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetNodeHistory(nid)
		if err != nil {
			return false, err
		}
		appendNodeHistorySource(&sources, candidate, history)
		return false, nil
	})
	if err != nil {
		return err
	}
	if len(sources) > 0 {
		return truncateNodeHistorySources(nid, sources, keepVersions)
	}
	return shard.TruncateNodeHistory(nid, keepVersions)
}

func (ts *Store) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, version, r); err != nil {
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
	liveHere, err := relationshipRowLive(shard, rid)
	if err != nil {
		return err
	}

	if liveHere && !isArchive {
		if shard == ts.refShard {
			sources := relHistorySourcesFrom(shard, history)
			archive, archiveCheckin, err := ts.checkoutArchive()
			if err != nil {
				return err
			}
			if archive != nil {
				archiveHistory, err := archive.GetRelHistory(rid)
				archiveCheckin()
				if err != nil {
					return err
				}
				appendRelHistorySource(&sources, archive, archiveHistory)
			} else {
				archiveCheckin()
			}
			return truncateRelHistorySources(rid, sources, keepVersions)
		}
		return shard.TruncateRelHistory(rid, keepVersions)
	}
	if isArchive {
		sources := relHistorySourcesFrom(shard, history)
		ref, refCheckin, err := ts.checkoutRefShard()
		if err != nil {
			return err
		}
		refHistory, err := ref.GetRelHistory(rid)
		refCheckin()
		if err != nil {
			return err
		}
		appendRelHistorySource(&sources, ref, refHistory)
		return truncateRelHistorySources(rid, sources, keepVersions)
	}

	sources := relHistorySourcesFrom(shard, history)
	err = ts.forEachHistoryShard(shard, func(candidate *BadgerStore) (bool, error) {
		history, err := candidate.GetRelHistory(rid)
		if err != nil {
			return false, err
		}
		appendRelHistorySource(&sources, candidate, history)
		return false, nil
	})
	if err != nil {
		return err
	}
	if len(sources) > 0 {
		return truncateRelHistorySources(rid, sources, keepVersions)
	}
	// No shard owns history for this id — delegate to the originally resolved
	// shard so its NotFound/no-op semantics are surfaced consistently.
	return shard.TruncateRelHistory(rid, keepVersions)
}

func truncateNodeHistorySources(nid types.NodeID, sources []nodeHistorySource, keepVersions int) error {
	if len(sources) == 0 {
		return nil
	}
	if len(sources) == 1 {
		return sources[0].store.TruncateNodeHistory(nid, keepVersions)
	}
	if keepVersions == 0 {
		for _, source := range sources {
			if err := source.store.TruncateNodeHistory(nid, 0); err != nil {
				return err
			}
		}
		return nil
	}

	keepBySource := nodeHistoryKeepCounts(sources, keepVersions)
	if len(keepBySource) == 0 {
		return nil
	}
	for i, source := range sources {
		if err := source.store.TruncateNodeHistory(nid, keepBySource[i]); err != nil {
			return err
		}
	}
	return nil
}

func nodeHistoryKeepCounts(sources []nodeHistorySource, keepVersions int) map[int]int {
	type versionSource struct {
		version uint32
		source  int
	}
	versions := make([]versionSource, 0)
	for sourceIdx, source := range sources {
		for _, n := range source.history {
			version := n.Version()
			versions = append(versions, versionSource{version: version, source: sourceIdx})
		}
	}
	if len(versions) <= keepVersions {
		return nil
	}
	sort.SliceStable(versions, func(i, j int) bool { return versions[i].version < versions[j].version })
	unique := versions[:0]
	var last uint32
	for i, entry := range versions {
		if i > 0 && entry.version == last {
			continue
		}
		unique = append(unique, entry)
		last = entry.version
	}
	versions = unique
	if len(versions) <= keepVersions {
		return nil
	}
	keepBySource := make(map[int]int, len(sources))
	for _, kept := range versions[len(versions)-keepVersions:] {
		keepBySource[kept.source]++
	}
	return keepBySource
}

func truncateRelHistorySources(rid types.RelID, sources []relHistorySource, keepVersions int) error {
	if len(sources) == 0 {
		return nil
	}
	if len(sources) == 1 {
		return sources[0].store.TruncateRelHistory(rid, keepVersions)
	}
	if keepVersions == 0 {
		for _, source := range sources {
			if err := source.store.TruncateRelHistory(rid, 0); err != nil {
				return err
			}
		}
		return nil
	}

	keepBySource := relHistoryKeepCounts(sources, keepVersions)
	if len(keepBySource) == 0 {
		return nil
	}
	for i, source := range sources {
		if err := source.store.TruncateRelHistory(rid, keepBySource[i]); err != nil {
			return err
		}
	}
	return nil
}

func relHistoryKeepCounts(sources []relHistorySource, keepVersions int) map[int]int {
	type versionSource struct {
		version uint32
		source  int
	}
	versions := make([]versionSource, 0)
	for sourceIdx, source := range sources {
		for _, r := range source.history {
			version := r.Version()
			versions = append(versions, versionSource{version: version, source: sourceIdx})
		}
	}
	if len(versions) <= keepVersions {
		return nil
	}
	sort.SliceStable(versions, func(i, j int) bool { return versions[i].version < versions[j].version })
	unique := versions[:0]
	var last uint32
	for i, entry := range versions {
		if i > 0 && entry.version == last {
			continue
		}
		unique = append(unique, entry)
		last = entry.version
	}
	versions = unique
	if len(versions) <= keepVersions {
		return nil
	}
	keepBySource := make(map[int]int, len(sources))
	for _, kept := range versions[len(versions)-keepVersions:] {
		keepBySource[kept.source]++
	}
	return keepBySource
}

// --- Cascade operations ---

func (ts *Store) DeleteNodeCascade(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
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
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rid, prevVersion, tombstone); err != nil {
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
	if err := storecontract.ValidateRelationshipLiveVersion(r, prevVersion); err != nil {
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
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeHistoryVersionSnapshot(nid, prevNodeVersion, nodeTombstone); err != nil {
		return err
	}
	for _, rt := range relTombstones {
		if err := storecontract.ValidateRelationshipHistoryVersionSnapshot(rt.ID, rt.PrevVersion, rt.Tombstone); err != nil {
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
		if err := storecontract.ValidateNodeLiveVersion(old, prevNodeVersion); err != nil {
			return err
		}
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
	//
	// Error semantics on partial failure:
	//   * shard reports error AND node is GONE (ErrNodeNotFound on the
	//     liveness check): the node delete actually succeeded in the
	//     underlying state but the call's response framing or async
	//     bookkeeping failed. We surface the original error to the caller
	//     so they can retry/log, but we do NOT roll back the relationship
	//     deletes — the final on-disk state (node + rels gone) is the
	//     intended outcome. Callers that retry should observe the operation
	//     as already-applied.
	//   * shard reports error AND node is STILL LIVE: full rollback of
	//     prior relationship deletes via wrapDeleteNodeRelationshipRollbackError.
	//   * shard reports error AND the liveness check itself fails (corruption
	//     or shard-level error): attempt rollback anyway and wrap both errors.
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
		if err := storecontract.ValidateRelationshipLiveVersion(old, rt.PrevVersion); err != nil {
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
