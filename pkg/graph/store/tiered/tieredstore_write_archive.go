package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Reference archive ---

// ErrNotReferenceEntity is returned when attempting to archive a non-reference entity.
var ErrNotReferenceEntity = errors.New("graph: entity is not a reference entity")

// ErrCrossShardArchiveRel is kept for errors.Is compatibility with older
// callers. Current archive/restore paths migrate relationship placement
// across the ref/archive boundary instead of returning this sentinel.
var ErrCrossShardArchiveRel = errors.New("graph: cross-shard archive relationship migration failed")

// ArchiveNode moves a reference node and all its relationships from refShard
// to refArchive. Only reference entities can be archived.
// The node must exist in refShard. Event nodes cannot be archived.
//
// Cross-store atomicity is implemented with explicit rollback of completed
// relationship placement moves and the temporary destination node write. If a
// rollback step itself fails, the returned error includes both the primary
// failure and the rollback failure.
func (ts *Store) ArchiveNode(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	// 1. Verify node is in refShard.
	if !ts.refShard.HasNodeID(id) {
		return fmt.Errorf("graph: archive: %w", ErrNodeNotFound)
	}

	// 2. Read node from refShard.
	node, err := ts.refShard.GetNode(types.NodeID(id))
	if err != nil {
		return fmt.Errorf("graph: archive read node: %w", err)
	}

	// 3. Open and pin refArchive. Relationship placement planning below needs
	// the target archive handle.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	defer archiveCheckin()
	if archive == nil {
		return fmt.Errorf("graph: archive: refArchive unexpectedly nil after ensureRefArchive")
	}

	// 4. Collect all unique rel IDs touching the node.
	outIDs := ts.refShard.OutgoingRelIDs(id)
	inIDs := ts.refShard.IncomingRelIDs(id, 0)
	relIDs := mergeUniqueRelIDs(outIDs, inIDs)
	moves, releaseMoves, err := ts.planRelationshipPlacementMoves(types.NodeID(id), relIDs, ts.refShard, archive)
	if err != nil {
		return err
	}
	defer releaseMoves()
	if err := ts.preflightArchiveNodeDestination(types.NodeID(id), archive, moves); err != nil {
		return fmt.Errorf("graph: archive destination preflight: %w", err)
	}

	// 5. Write node to refArchive, then move every touching relationship's
	// entity/out leg and incoming leg to the shards implied by the new
	// endpoint placement.
	if err := archive.PutNode(node); err != nil {
		return fmt.Errorf("graph: archive write node: %w", err)
	}

	var committed []relationshipPlacementMove
	for _, move := range moves {
		if err := migrateRelationshipPlacement(move); err != nil {
			rbErr := rollbackArchiveNodeState(committed, archive, types.NodeID(id))
			if rbErr != nil {
				return fmt.Errorf("graph: archive relationship placement: %w (rollback failed: %v)", err, rbErr)
			}
			return fmt.Errorf("graph: archive relationship placement: %w", err)
		}
		committed = append(committed, move)
	}

	// 6. Delete the node row from refShard. Relationship indexes have already
	// been moved; the cascade path is used here only to purge stale
	// adjacency-only entries that had no live relationship row to move.
	if err := ts.refShard.DeleteNodeCascade(types.NodeID(id)); err != nil {
		rbErr := rollbackArchiveNodeState(committed, archive, types.NodeID(id))
		if rbErr != nil {
			return fmt.Errorf("graph: archive delete from ref: %w (relationship rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("graph: archive delete from ref: %w", err)
	}

	return nil
}

// RestoreNode moves a reference node and all its relationships from refArchive
// back to refShard. Reverse of ArchiveNode.
//
// Cross-store atomicity follows ArchiveNode: completed relationship placement
// moves and the temporary destination node write are rolled back on failure, and
// rollback failures are included in the returned error.
func (ts *Store) RestoreNode(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()

	// 1. Ensure archive is open and pin against concurrent Close. Without
	// the pin, Close racing between Load and the reads/writes below could
	// free the archive BadgerStore under us.
	if err := ts.ensureRefArchive(); err != nil {
		return err
	}
	archive, archiveCheckin, err := ts.checkoutArchive()
	if err != nil {
		return err
	}
	defer archiveCheckin()
	if archive == nil {
		return fmt.Errorf("graph: restore: refArchive unexpectedly nil after ensureRefArchive")
	}

	// 2. Verify node is in archive.
	if !archive.HasNodeID(id) {
		return fmt.Errorf("graph: restore: %w", ErrNodeNotFound)
	}

	// 3. Read node from archive.
	node, err := archive.GetNode(types.NodeID(id))
	if err != nil {
		return fmt.Errorf("graph: restore read node: %w", err)
	}

	// 4. Read all relationship IDs touching the archived node.
	outIDs := archive.OutgoingRelIDs(id)
	inIDs := archive.IncomingRelIDs(id, 0)
	relIDs := mergeUniqueRelIDs(outIDs, inIDs)
	moves, releaseMoves, err := ts.planRelationshipPlacementMoves(types.NodeID(id), relIDs, archive, ts.refShard)
	if err != nil {
		return err
	}
	defer releaseMoves()
	if err := ts.preflightArchiveNodeDestination(types.NodeID(id), ts.refShard, moves); err != nil {
		return fmt.Errorf("graph: restore destination preflight: %w", err)
	}

	// 5. Write node to refShard, then move every touching relationship back
	// to the placement implied by the restored endpoint.
	if err := ts.refShard.PutNode(node); err != nil {
		return fmt.Errorf("graph: restore write node: %w", err)
	}

	var committed []relationshipPlacementMove
	for _, move := range moves {
		if err := migrateRelationshipPlacement(move); err != nil {
			rbErr := rollbackArchiveNodeState(committed, ts.refShard, types.NodeID(id))
			if rbErr != nil {
				return fmt.Errorf("graph: restore relationship placement: %w (rollback failed: %v)", err, rbErr)
			}
			return fmt.Errorf("graph: restore relationship placement: %w", err)
		}
		committed = append(committed, move)
	}

	// 6. Delete the node row from archive. Relationship placement has already
	// moved away from the archived endpoint; the cascade path is used here only
	// to purge stale adjacency-only entries that had no live relationship row.
	if err := archive.DeleteNodeCascade(types.NodeID(id)); err != nil {
		rbErr := rollbackArchiveNodeState(committed, ts.refShard, types.NodeID(id))
		if rbErr != nil {
			return fmt.Errorf("graph: restore delete from archive: %w (relationship rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("graph: restore delete from archive: %w", err)
	}

	return nil
}

type relationshipPlacementMove struct {
	rel       *types.Relationship
	oldEntity *BadgerStore
	oldIn     *BadgerStore
	newEntity *BadgerStore
	newIn     *BadgerStore
}

func mergeUniqueRelIDs(a, b []snowflake.ID) []snowflake.ID {
	seen := make(map[snowflake.ID]struct{}, len(a)+len(b))
	out := make([]snowflake.ID, 0, len(a)+len(b))
	for _, rid := range a {
		if _, ok := seen[rid]; ok {
			continue
		}
		seen[rid] = struct{}{}
		out = append(out, rid)
	}
	for _, rid := range b {
		if _, ok := seen[rid]; ok {
			continue
		}
		seen[rid] = struct{}{}
		out = append(out, rid)
	}
	return out
}

func (ts *Store) planRelationshipPlacementMoves(moving types.NodeID, relIDs []snowflake.ID, from, to *BadgerStore) ([]relationshipPlacementMove, func(), error) {
	var releases []func()
	releaseAll := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	endpointShard := func(endpoint types.NodeID, movingShard *BadgerStore) (*BadgerStore, error) {
		if endpoint == moving {
			return movingShard, nil
		}
		shard, checkin, err := ts.shardForNodeIDChecked(endpoint)
		if err != nil {
			return nil, err
		}
		releases = append(releases, checkin)
		return shard, nil
	}

	moves := make([]relationshipPlacementMove, 0, len(relIDs))
	for _, rawID := range relIDs {
		rid := types.RelID(rawID)
		rel, err := ts.GetRelationship(rid)
		if errors.Is(err, ErrRelNotFound) {
			continue
		}
		if err != nil {
			releaseAll()
			return nil, nil, fmt.Errorf("graph: read relationship %d: %w", rawID, err)
		}

		oldStart, err := endpointShard(rel.StartNodeID(), from)
		if err != nil {
			releaseAll()
			return nil, nil, err
		}
		oldEnd, err := endpointShard(rel.EndNodeID(), from)
		if err != nil {
			releaseAll()
			return nil, nil, err
		}
		newStart, err := endpointShard(rel.StartNodeID(), to)
		if err != nil {
			releaseAll()
			return nil, nil, err
		}
		newEnd, err := endpointShard(rel.EndNodeID(), to)
		if err != nil {
			releaseAll()
			return nil, nil, err
		}

		moves = append(moves, relationshipPlacementMove{
			rel:       rel,
			oldEntity: oldStart,
			oldIn:     oldEnd,
			newEntity: newStart,
			newIn:     newEnd,
		})
	}
	return moves, releaseAll, nil
}

func (ts *Store) preflightArchiveNodeDestination(nodeID types.NodeID, destination *BadgerStore, moves []relationshipPlacementMove) error {
	rawID := nodeID.SnowflakeID()
	if destination.HasNodeID(rawID) {
		return ErrNodeExists
	}
	for _, move := range moves {
		if move.oldEntity == move.newEntity {
			continue
		}
		if relationshipRowExists(move.newEntity, move.rel.ID()) {
			return ErrRelExists
		}
	}
	return ts.purgeOrRejectDestinationAdjacency(nodeID, destination)
}

func (ts *Store) purgeOrRejectDestinationAdjacency(nodeID types.NodeID, destination *BadgerStore) error {
	rawID := nodeID.SnowflakeID()
	for _, rawRelID := range mergeUniqueRelIDs(destination.OutgoingRelIDs(rawID), destination.IncomingRelIDs(rawID, 0)) {
		rid := types.RelID(rawRelID)
		if _, err := ts.GetRelationship(rid); err != nil {
			if errors.Is(err, ErrRelNotFound) {
				if purgeErr := destination.PurgeOrphanRelationshipIndexes(rid); purgeErr != nil {
					return purgeErr
				}
				continue
			}
			return fmt.Errorf("graph: destination relationship %d state: %w", rawRelID, err)
		}
		return fmt.Errorf("%w: destination already has relationship %d for node %d", ErrInvalidStoreMutation, rawRelID, nodeID)
	}
	return nil
}

func migrateRelationshipPlacement(move relationshipPlacementMove) error {
	rel := move.rel
	info := RelDeleteInfo{
		ID:      rel.ID().SnowflakeID(),
		RelType: rel.TypeToken().Value(),
		StartID: rel.StartNodeID().SnowflakeID(),
		EndID:   rel.EndNodeID().SnowflakeID(),
	}
	entityMoved := move.oldEntity != move.newEntity
	inMoved := move.oldIn != move.newIn
	if !entityMoved && !inMoved {
		return nil
	}

	wroteEntity := false
	wroteIncoming := false
	deletedOldIncoming := false
	rollbackNew := func() error {
		var rollbackErrs []error
		if wroteIncoming {
			if err := move.newIn.DeleteRelIncoming(info); err != nil {
				rollbackErrs = append(rollbackErrs, err)
			}
		}
		if wroteEntity {
			if _, err := move.newEntity.DeleteRelEntityAndOut(info.ID); err != nil {
				rollbackErrs = append(rollbackErrs, err)
			}
		}
		return errors.Join(rollbackErrs...)
	}

	if entityMoved {
		if err := move.newEntity.PutRelEntityAndOut(rel); err != nil {
			return err
		}
		wroteEntity = true
	}
	if inMoved {
		if err := move.newIn.PutRelIncoming(info.EndID, info.StartID, info.RelType, info.ID); err != nil {
			if rbErr := rollbackNew(); rbErr != nil {
				return fmt.Errorf("%w (rollback new placement failed: %v)", err, rbErr)
			}
			return err
		}
		wroteIncoming = true
		if err := move.oldIn.DeleteRelIncoming(info); err != nil {
			if rbErr := rollbackNew(); rbErr != nil {
				return fmt.Errorf("%w (rollback new placement failed: %v)", err, rbErr)
			}
			return err
		}
		deletedOldIncoming = true
	}
	if entityMoved {
		if _, err := move.oldEntity.DeleteRelEntityAndOut(info.ID); err != nil {
			var rollbackErrs []error
			if deletedOldIncoming {
				if rbErr := move.oldIn.PutRelIncoming(info.EndID, info.StartID, info.RelType, info.ID); rbErr != nil {
					rollbackErrs = append(rollbackErrs, rbErr)
				}
			}
			if rbErr := rollbackNew(); rbErr != nil {
				rollbackErrs = append(rollbackErrs, rbErr)
			}
			if rbErr := errors.Join(rollbackErrs...); rbErr != nil {
				return fmt.Errorf("%w (rollback placement failed: %v)", err, rbErr)
			}
			return err
		}
	}
	return nil
}

func rollbackArchiveNodeState(moves []relationshipPlacementMove, nodeStore *BadgerStore, nodeID types.NodeID) error {
	var rollbackErrs []error
	if err := rollbackRelationshipPlacementMoves(moves); err != nil {
		rollbackErrs = append(rollbackErrs, err)
	}
	if err := nodeStore.DeleteNode(nodeID); err != nil && !errors.Is(err, ErrNodeNotFound) {
		rollbackErrs = append(rollbackErrs, err)
	}
	return errors.Join(rollbackErrs...)
}

func rollbackRelationshipPlacementMoves(moves []relationshipPlacementMove) error {
	var rollbackErrs []error
	for i := len(moves) - 1; i >= 0; i-- {
		move := moves[i]
		reverse := relationshipPlacementMove{
			rel:       move.rel,
			oldEntity: move.newEntity,
			oldIn:     move.newIn,
			newEntity: move.oldEntity,
			newIn:     move.oldIn,
		}
		if err := migrateRelationshipPlacement(reverse); err != nil {
			rollbackErrs = append(rollbackErrs, err)
		}
	}
	return errors.Join(rollbackErrs...)
}
