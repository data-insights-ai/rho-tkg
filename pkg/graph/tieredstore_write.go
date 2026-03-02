package graph

import (
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
	shard := ts.shardForNodeID(n.InternalID().SnowflakeID())
	return shard.ReplaceNode(n)
}

func (ts *TieredStore) DeleteNode(id snowflake.ID) error {
	shard := ts.shardForNodeID(id)
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
		shard := ts.shardForNodeID(id)
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
	startShard := ts.shardForNodeID(startID)
	endShard := ts.shardForNodeID(endID)

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
	shard := ts.shardForRelID(r.InternalID().SnowflakeID())
	return shard.ReplaceRelationship(r)
}

func (ts *TieredStore) DeleteRelationship(id snowflake.ID) error {
	// Find which shard owns the entity.
	entityShard := ts.shardForRelID(id)

	// Check if this is a cross-shard relationship by reading the rel metadata.
	r, err := entityShard.GetRelationship(id)
	if err != nil {
		return err
	}

	startShard := ts.shardForNodeID(r.StartNodeID().SnowflakeID())
	endShard := ts.shardForNodeID(r.EndNodeID().SnowflakeID())

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
		startShard := ts.shardForNodeID(r.StartNodeID().SnowflakeID())
		endShard := ts.shardForNodeID(r.EndNodeID().SnowflakeID())

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
	shard := ts.shardForNodeID(current.InternalID().SnowflakeID())
	return shard.ReplaceNodeWithHistory(current, prevVersion, prevState)
}

func (ts *TieredStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	shard := ts.shardForRelID(current.InternalID().SnowflakeID())
	return shard.ReplaceRelWithHistory(current, prevVersion, prevState)
}

// --- Version history writes ---

func (ts *TieredStore) PutNodeVersion(id snowflake.ID, version uint32, n *types.Node) error {
	shard := ts.shardForNodeID(id)
	return shard.PutNodeVersion(id, version, n)
}

func (ts *TieredStore) TruncateNodeHistory(id snowflake.ID, keepVersions int) error {
	shard := ts.shardForNodeID(id)
	return shard.TruncateNodeHistory(id, keepVersions)
}

func (ts *TieredStore) PutRelVersion(id snowflake.ID, version uint32, r *types.Relationship) error {
	shard := ts.shardForRelID(id)
	return shard.PutRelVersion(id, version, r)
}

func (ts *TieredStore) TruncateRelHistory(id snowflake.ID, keepVersions int) error {
	shard := ts.shardForRelID(id)
	return shard.TruncateRelHistory(id, keepVersions)
}

// --- Cascade operations ---

func (ts *TieredStore) DeleteNodeCascade(id snowflake.ID) error {
	shard := ts.shardForNodeID(id)

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

func (ts *TieredStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	shard := ts.shardForNode(labelToken)
	return shard.CreatePropertyIndex(labelToken, propertyKey)
}

func (ts *TieredStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	shard := ts.shardForNode(labelToken)
	return shard.DropPropertyIndex(labelToken, propertyKey)
}
