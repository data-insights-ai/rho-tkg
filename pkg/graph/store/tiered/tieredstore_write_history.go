package tiered

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Atomic replace + history ---

func (ts *Store) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	shard, checkin, err := ts.shardForNodeIDChecked(current.ID())
	if err != nil {
		return err
	}
	defer checkin()
	if err := shard.ReplaceNodeWithHistory(current, prevVersion, prevState); err != nil {
		return err
	}
	// Update Store-level vector indexes. The shard-level method updates
	// per-shard bs.vectorIndexes; ts.vectorIndexes is separate and must be kept in sync.
	id := current.ID().SnowflakeID()
	ts.vectorIdxMu.Lock()
	indexpkg.RemoveNodeFromVectorIndexes(ts.vectorIndexes, prevState, id)
	indexpkg.AddNodeToVectorIndexes(ts.vectorIndexes, current, id)
	ts.vectorIdxMu.Unlock()
	return nil
}

func (ts *Store) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
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
	if shard.HasNodeID(id) && shard != ts.refArchive.Load() {
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
	if shard.HasRelID(id) && shard != ts.refArchive.Load() {
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
	id := nid.SnowflakeID()

	shard, checkin, err := ts.shardForNodeIDChecked(nid)
	if err != nil {
		return err
	}
	defer checkin()

	// Read node before deletion for Store-level vector index maintenance.
	// GetNode failure (shard closed, corruption) is non-fatal: old==nil triggers
	// the purge fallback below, and the delete still proceeds.
	old, _ := shard.GetNode(types.NodeID(id))

	// Collect all connected relIDs from this shard's outIdx + inIdx.
	outRels := shard.OutgoingRelIDs(id)
	inRels := shard.IncomingRelIDs(id, 0)

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
	if err := shard.DeleteNode(types.NodeID(id)); err != nil {
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

// DeleteNodeWithHistory atomically writes tombstone history for the node and all
// connected relationships, then cascade-deletes.
//
// Builds a tombMap for fast lookup, then iterates rels: uses DeleteRelWithHistory
// for rels in tombMap, falls back to DeleteRelationship for unexpected rels.
// Passes nil relTombstones to the shard-level DeleteNodeWithHistory (rels already
// handled above individually).
//
// Cross-shard atomicity is per-shard only (B7 limitation — same as DeleteNodeCascade).
func (ts *Store) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
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
	outRels := shard.OutgoingRelIDs(id)
	inRels := shard.IncomingRelIDs(id, 0)

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

	// Read node before deletion for Store-level vector index maintenance.
	// GetNode failure (shard closed, corruption) is non-fatal: old==nil triggers
	// the purge fallback below, and the delete still proceeds.
	old, _ := shard.GetNode(types.NodeID(id))

	// Delete the node itself with its tombstone. relTombstones=nil because
	// all rels were handled individually above.
	if err := shard.DeleteNodeWithHistory(types.NodeID(id), prevNodeVersion, nodeTombstone, nil); err != nil {
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
