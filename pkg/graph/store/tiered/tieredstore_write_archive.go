package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// --- Reference archive ---

// ErrNotReferenceEntity is returned when reference archive operations encounter
// a non-reference entity.
var ErrNotReferenceEntity = errors.New("graph: entity is not a reference entity")

// ErrCrossShardArchiveRel is kept for errors.Is compatibility; archive and
// restore migrate relationship placement instead of returning it.
var ErrCrossShardArchiveRel = errors.New("graph: cross-shard archive relationship migration failed")

// ArchiveNode moves a reference node and all its relationships from refShard
// to refArchive; the node must exist in refShard and be reference-class.
// Atomicity is explicit rollback of completed placement moves and the
// temporary destination write; a failed rollback is joined to the primary error.
//
// Concurrency: holds moveMu for the whole move, so reads see the node on
// exactly one shard. Writes route without moveMu: a direct tiered.Store user
// must not write the moving node or its rels; the graph layer fences this via
// c.mu (g.Tier().Archive takes it exclusively).
func (ts *Store) ArchiveNode(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.moveMu.Lock() // a reader of the reference shard and the archive must see the node on one shard (lockMoveRead)
	defer ts.moveMu.Unlock()
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	defer refCheckin()

	// 1. Verify node is in refShard.
	if !ref.HasNodeID(id) {
		return ts.archiveRefMissError(nid)
	}

	// 2. Read node from refShard.
	node, err := ref.GetNode(types.NodeID(id))
	if err != nil {
		return fmt.Errorf("graph: archive read node: %w", err)
	}
	if err := ts.requireReferenceArchiveNode("archive", node); err != nil {
		return err
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
	outIDs := ref.OutgoingRelIDs(id)
	inIDs := ref.IncomingRelIDs(id, 0)
	relIDs := mergeUniqueRelIDs(outIDs, inIDs)
	moves, releaseMoves, err := ts.planRelationshipPlacementMoves(types.NodeID(id), relIDs, ref, archive)
	if err != nil {
		return err
	}
	defer releaseMoves()
	if err := ts.checkAndCleanArchiveNodeDestination(types.NodeID(id), archive, moves); err != nil {
		return fmt.Errorf("graph: archive destination preflight: %w", err)
	}

	// 5. Write node to refArchive, then move every touching relationship's
	// entity/out and incoming legs to the shards implied by the new placement.
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

	// 6. Delete the node row; the cascade only purges stale adjacency-only entries
	// (rel indexes were already moved).
	if err := ref.DeleteNodeCascade(types.NodeID(id)); err != nil {
		rbErr := rollbackArchiveNodeState(committed, archive, types.NodeID(id))
		if rbErr != nil {
			return fmt.Errorf("graph: archive delete from ref: %w (relationship rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("graph: archive delete from ref: %w", err)
	}

	return nil
}

// RestoreNode moves a reference node and its relationships from refArchive
// back to refShard (reverse of ArchiveNode, same rollback and concurrency
// contract: reads fenced by moveMu, writes not).
func (ts *Store) RestoreNode(nid types.NodeID) error {
	if err := ts.checkOpen(); err != nil {
		return err
	}
	ts.moveMu.Lock() // a reader of the reference shard and the archive must see the node on one shard (lockMoveRead)
	defer ts.moveMu.Unlock()
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return err
	}
	id := nid.SnowflakeID()
	ref, refCheckin, err := ts.checkoutRefShard()
	if err != nil {
		return err
	}
	defer refCheckin()

	// 1. Ensure the archive is open and pin it so a racing Close cannot free it.
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
	if err := ts.requireReferenceArchiveNode("restore", node); err != nil {
		return err
	}

	// 4. Read all relationship IDs touching the archived node.
	outIDs := archive.OutgoingRelIDs(id)
	inIDs := archive.IncomingRelIDs(id, 0)
	relIDs := mergeUniqueRelIDs(outIDs, inIDs)
	moves, releaseMoves, err := ts.planRelationshipPlacementMoves(types.NodeID(id), relIDs, archive, ref)
	if err != nil {
		return err
	}
	defer releaseMoves()
	if err := ts.checkAndCleanArchiveNodeDestination(types.NodeID(id), ref, moves); err != nil {
		return fmt.Errorf("graph: restore destination preflight: %w", err)
	}

	// 5. Write node to refShard, then move every touching relationship back
	// to the placement implied by the restored endpoint.
	if err := ref.PutNode(node); err != nil {
		return fmt.Errorf("graph: restore write node: %w", err)
	}

	var committed []relationshipPlacementMove
	for _, move := range moves {
		if err := migrateRelationshipPlacement(move); err != nil {
			rbErr := rollbackArchiveNodeState(committed, ref, types.NodeID(id))
			if rbErr != nil {
				return fmt.Errorf("graph: restore relationship placement: %w (rollback failed: %v)", err, rbErr)
			}
			return fmt.Errorf("graph: restore relationship placement: %w", err)
		}
		committed = append(committed, move)
	}

	// 6. Delete the node row from the archive; the cascade only purges stale
	// adjacency-only entries (placement already moved).
	if err := archive.DeleteNodeCascade(types.NodeID(id)); err != nil {
		rbErr := rollbackArchiveNodeState(committed, ref, types.NodeID(id))
		if rbErr != nil {
			return fmt.Errorf("graph: restore delete from archive: %w (relationship rollback failed: %v)", err, rbErr)
		}
		return fmt.Errorf("graph: restore delete from archive: %w", err)
	}

	return nil
}

func (ts *Store) archiveRefMissError(nid types.NodeID) error {
	id := nid.SnowflakeID()
	eventShard := ts.timestampToEventShardEntry(id)
	eventStore, eventCheckin, err := eventShard.checkoutStoreForRead(ts)
	if err != nil {
		return err
	}
	defer eventCheckin()

	node, err := eventStore.GetNode(nid)
	if errors.Is(err, ErrNodeNotFound) {
		return fmt.Errorf("graph: archive: %w", ErrNodeNotFound)
	}
	if err != nil {
		return fmt.Errorf("graph: archive event probe: %w", err)
	}
	if err := ts.requireReferenceArchiveNode("archive", node); err != nil {
		return err
	}
	return fmt.Errorf("graph: archive: %w", ErrNodeNotFound)
}

func (ts *Store) requireReferenceArchiveNode(op string, node *types.Node) error {
	if node == nil {
		return fmt.Errorf("graph: %s: %w", op, ErrNodeNotFound)
	}
	if ts.ontology.ClassifyByToken(node.PrimaryLabelToken().Value()) != ClassReference {
		return fmt.Errorf("graph: %s: %w", op, ErrNotReferenceEntity)
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
	endpointPins := make(map[types.NodeID]*BadgerStore, len(relIDs))
	endpointShard := func(endpoint types.NodeID, movingShard *BadgerStore) (*BadgerStore, error) {
		if endpoint == moving {
			return movingShard, nil
		}
		if shard, ok := endpointPins[endpoint]; ok {
			return shard, nil
		}
		shard, checkin, err := ts.shardForNodeIDChecked(endpoint)
		if err != nil {
			return nil, err
		}
		endpointPins[endpoint] = shard
		releases = append(releases, checkin)
		return shard, nil
	}

	moves := make([]relationshipPlacementMove, 0, len(relIDs))
	for _, rawID := range relIDs {
		rid := types.RelID(rawID)
		rel, err := ts.getRelationshipChecked(rid) // moveMu is write-held: GetRelationship would read-lock it
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

// checkAndCleanArchiveNodeDestination validates that destination has no
// conflicting node/relationship rows for nodeID before Archive/Restore writes
// there. Unlike the read-only preflight* functions it can MUTATE destination:
// purgeOrRejectDestinationAdjacency purges a confirmed-orphaned index entry
// (pointing at a relationship that exists nowhere), which is idempotent and
// safe whether or not the move later aborts (BACKLOG 19o).
func (ts *Store) checkAndCleanArchiveNodeDestination(nodeID types.NodeID, destination *BadgerStore, moves []relationshipPlacementMove) error {
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
		if _, err := ts.getRelationshipChecked(rid); err != nil { // moveMu is write-held (ArchiveNode/RestoreNode)
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

// migrateRelationshipPlacement moves a live relationship's keys between
// shards; the removals record no ex-adjacency (Remove*Placement doors).
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
			if err := move.newIn.RemoveRelIncomingPlacement(info); err != nil {
				rollbackErrs = append(rollbackErrs, err)
			}
		}
		if wroteEntity {
			if _, err := move.newEntity.RemoveRelEntityAndOutPlacement(info.ID); err != nil {
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
		if err := move.oldIn.RemoveRelIncomingPlacement(info); err != nil {
			if rbErr := rollbackNew(); rbErr != nil {
				return fmt.Errorf("%w (rollback new placement failed: %v)", err, rbErr)
			}
			return err
		}
		deletedOldIncoming = true
	}
	if entityMoved {
		if _, err := move.oldEntity.RemoveRelEntityAndOutPlacement(info.ID); err != nil {
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
