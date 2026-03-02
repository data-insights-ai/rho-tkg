package graph

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// --- Node write operations ---
// Nodes are single-shard: routed entirely by primary label classification.

func (ts *TieredStore) PutNode(n *types.Node) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}
	shard := ts.shardForNode(n.PrimaryLabelToken().Value())
	return shard.PutNode(n)
}

func (ts *TieredStore) ReplaceNode(n *types.Node) error {
	shard, err := ts.shardForNodeID(n.InternalID().SnowflakeID())
	if err != nil {
		return err
	}
	return shard.ReplaceNode(n)
}

func (ts *TieredStore) DeleteNode(id snowflake.ID) error {
	shard, err := ts.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.DeleteNode(id)
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
			return err
		}
	}
	return nil
}

func (ts *TieredStore) DeleteNodesBatch(ids []snowflake.ID) error {
	// Partition by actual shard using shardForNodeID.
	shardBuckets := make(map[*BadgerStore][]snowflake.ID)
	for _, id := range ids {
		shard, err := ts.shardForNodeID(id)
		if err != nil {
			return err
		}
		shardBuckets[shard] = append(shardBuckets[shard], id)
	}
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
// We use shard-based routing (shardForNodeID) instead of class-based routing
// to correctly handle E→E cross-shard relationships.

func (ts *TieredStore) PutRelationship(r *types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()
	relID := r.InternalID().SnowflakeID()

	// Resolve actual shards — not class. Two event entities may be in different shards.
	startShard, err := ts.shardForNodeID(startID)
	if err != nil {
		return err
	}
	endShard, err := ts.shardForNodeID(endID)
	if err != nil {
		return err
	}

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
		return entityShard.putRelEntityAndOut(r)
	}
	// R→E or E→E(cross-shard): entity shard first.
	if err := entityShard.putRelEntityAndOut(r); err != nil {
		return err
	}
	return inShard.putRelIncoming(endID, startID, relType, relID)
}

func (ts *TieredStore) ReplaceRelationship(r *types.Relationship) error {
	shard, err := ts.shardForRelID(r.InternalID().SnowflakeID())
	if err != nil {
		return err
	}
	return shard.ReplaceRelationship(r)
}

func (ts *TieredStore) DeleteRelationship(id snowflake.ID) error {
	// Find which shard owns the entity.
	entityShard, err := ts.shardForRelID(id)
	if err != nil {
		return err
	}

	// Check if this is a cross-shard relationship by reading the rel metadata.
	r, err := entityShard.GetRelationship(id)
	if err != nil {
		return err
	}

	startShard, err := ts.shardForNodeID(r.StartNodeID().SnowflakeID())
	if err != nil {
		return err
	}
	endShard, err := ts.shardForNodeID(r.EndNodeID().SnowflakeID())
	if err != nil {
		return err
	}

	if startShard == endShard {
		// Same shard: delegate entirely.
		return entityShard.DeleteRelationship(id)
	}

	// Cross-shard: delete entity+out from entity shard, in/ from in shard.
	info, err := entityShard.deleteRelEntityAndOut(id)
	if err != nil {
		return err
	}

	inShard := endShard
	return inShard.deleteRelIncoming(info)
}

func (ts *TieredStore) PutRelationshipsBatch(rels []*types.Relationship) error {
	if err := ts.checkRotation(); err != nil {
		return err
	}

	// Partition: same-shard rels can use batch, cross-shard use individual put.
	// Group by *BadgerStore pointer for batching.
	shardBuckets := make(map[*BadgerStore][]*types.Relationship)

	for _, r := range rels {
		startShard, err := ts.shardForNodeID(r.StartNodeID().SnowflakeID())
		if err != nil {
			return err
		}
		endShard, err := ts.shardForNodeID(r.EndNodeID().SnowflakeID())
		if err != nil {
			return err
		}

		if startShard != endShard {
			// Cross-shard: individual put.
			if err := ts.PutRelationship(r); err != nil {
				return err
			}
			continue
		}

		shardBuckets[startShard] = append(shardBuckets[startShard], r)
	}

	for shard, bucket := range shardBuckets {
		if err := shard.PutRelationshipsBatch(bucket); err != nil {
			return err
		}
	}
	return nil
}

func (ts *TieredStore) DeleteRelationshipsBatch(ids []snowflake.ID) error {
	// Cross-shard aware: per-ID delete.
	for _, id := range ids {
		if err := ts.DeleteRelationship(id); err != nil {
			return err
		}
	}
	return nil
}

// --- Atomic replace + history ---

func (ts *TieredStore) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	shard, err := ts.shardForNodeID(current.InternalID().SnowflakeID())
	if err != nil {
		return err
	}
	return shard.ReplaceNodeWithHistory(current, prevVersion, prevState)
}

func (ts *TieredStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	shard, err := ts.shardForRelID(current.InternalID().SnowflakeID())
	if err != nil {
		return err
	}
	return shard.ReplaceRelWithHistory(current, prevVersion, prevState)
}

// --- Version history writes ---

func (ts *TieredStore) PutNodeVersion(id snowflake.ID, version uint32, n *types.Node) error {
	shard, err := ts.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.PutNodeVersion(id, version, n)
}

func (ts *TieredStore) TruncateNodeHistory(id snowflake.ID, keepVersions int) error {
	shard, err := ts.shardForNodeID(id)
	if err != nil {
		return err
	}
	return shard.TruncateNodeHistory(id, keepVersions)
}

func (ts *TieredStore) PutRelVersion(id snowflake.ID, version uint32, r *types.Relationship) error {
	shard, err := ts.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.PutRelVersion(id, version, r)
}

func (ts *TieredStore) TruncateRelHistory(id snowflake.ID, keepVersions int) error {
	shard, err := ts.shardForRelID(id)
	if err != nil {
		return err
	}
	return shard.TruncateRelHistory(id, keepVersions)
}

// --- Cascade operations ---

func (ts *TieredStore) DeleteNodeCascade(id snowflake.ID) error {
	shard, err := ts.shardForNodeID(id)
	if err != nil {
		return err
	}

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
		if err := ts.DeleteRelationship(relID); err != nil {
			return err
		}
	}
	for _, relID := range inRels {
		if _, ok := seen[relID]; ok {
			continue
		}
		seen[relID] = struct{}{}
		if err := ts.DeleteRelationship(relID); err != nil {
			return err
		}
	}

	// Delete the node itself.
	return shard.DeleteNode(id)
}

// --- Property indexes ---

// ErrEventPropertyIndex is returned when attempting to create a property index
// on an event label in a TieredStore. Only reference entities support indexes.
var ErrEventPropertyIndex = errors.New("graph: property indexes only supported for reference entities in TieredStore")

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

// --- Reference archive ---

// ErrNotReferenceEntity is returned when attempting to archive a non-reference entity.
var ErrNotReferenceEntity = errors.New("graph: entity is not a reference entity")

// ArchiveNode moves a reference node and all its relationships from refShard
// to refArchive. Only reference entities can be archived.
// The node must exist in refShard. Event nodes cannot be archived.
func (ts *TieredStore) ArchiveNode(id snowflake.ID) error {
	// 1. Verify node is in refShard.
	if !ts.refShard.hasNodeID(id) {
		return fmt.Errorf("graph: archive: %w", ErrNodeNotFound)
	}

	// 2. Lazy-open refArchive.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}

	// 3. Read node from refShard.
	node, err := ts.refShard.GetNode(id)
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
		r, err := ts.refShard.GetRelationship(rid)
		if errors.Is(err, ErrRelNotFound) {
			continue // cross-shard entity, skip
		}
		if err != nil {
			return fmt.Errorf("graph: archive read rel %d: %w", rid, err)
		}
		rels = append(rels, r)
	}

	// 5. Write node + rels to refArchive.
	if err := ts.refArchive.PutNode(node); err != nil {
		return fmt.Errorf("graph: archive write node: %w", err)
	}
	archiveWritten := true

	for _, r := range rels {
		// PutRelationship validates endpoint existence. If the other endpoint
		// isn't in the archive (partial archive), skip the rel — it will be
		// deleted by DeleteNodeCascade below.
		err := ts.refArchive.PutRelationship(r)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			// Best-effort rollback: remove partially written data from archive.
			_ = ts.refArchive.DeleteNodeCascade(id)
			return fmt.Errorf("graph: archive write rel: %w", err)
		}
	}

	// 6. Delete from refShard (cascade deletes node + all rels in refShard).
	if err := ts.refShard.DeleteNodeCascade(id); err != nil {
		if archiveWritten {
			// Best-effort rollback: remove data from archive since source delete failed.
			_ = ts.refArchive.DeleteNodeCascade(id)
		}
		return fmt.Errorf("graph: archive delete from ref: %w", err)
	}

	return nil
}

// RestoreNode moves a reference node and all its relationships from refArchive
// back to refShard. Reverse of ArchiveNode.
func (ts *TieredStore) RestoreNode(id snowflake.ID) error {
	// 1. Ensure archive is open.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}

	// 2. Verify node is in archive.
	if !ts.refArchive.hasNodeID(id) {
		return fmt.Errorf("graph: restore: %w", ErrNodeNotFound)
	}

	// 3. Read node from archive.
	node, err := ts.refArchive.GetNode(id)
	if err != nil {
		return fmt.Errorf("graph: restore read node: %w", err)
	}

	// 4. Read all rels from archive.
	outIDs := ts.refArchive.outgoingRelIDs(id)
	inIDs := ts.refArchive.incomingRelIDs(id, 0)

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
		r, err := ts.refArchive.GetRelationship(rid)
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
	refWritten := true

	for _, r := range rels {
		err := ts.refShard.PutRelationship(r)
		if errors.Is(err, ErrNodeNotFound) {
			continue
		}
		if err != nil {
			// Best-effort rollback: remove partially written data from refShard.
			_ = ts.refShard.DeleteNodeCascade(id)
			return fmt.Errorf("graph: restore write rel: %w", err)
		}
	}

	// 6. Delete from archive.
	if err := ts.refArchive.DeleteNodeCascade(id); err != nil {
		if refWritten {
			// Best-effort rollback: remove data from refShard since archive delete failed.
			_ = ts.refShard.DeleteNodeCascade(id)
		}
		return fmt.Errorf("graph: restore delete from archive: %w", err)
	}

	return nil
}
