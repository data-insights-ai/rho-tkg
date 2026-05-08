package tiered

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Reference archive ---

// ErrNotReferenceEntity is returned when attempting to archive a non-reference entity.
var ErrNotReferenceEntity = errors.New("graph: entity is not a reference entity")

// ErrCrossShardArchiveRel is returned by ArchiveNode when the node has at
// least one relationship whose other endpoint will not be on refArchive
// after the move. Archiving such a node would either drop the rel
// silently (cascade-on-refShard) or leave dangling adjacency entries on
// the partner shard. Callers must either delete the rel first or arrange
// for the partner endpoint to also live on refArchive.
//
// Self-loops are exempt: both endpoints are the archived node, so the
// rel is fully migrated by ArchiveNode.
var ErrCrossShardArchiveRel = errors.New("graph: cannot archive node with cross-shard relationship; archive both endpoints or delete the rel first")

// ArchiveNode moves a reference node and all its relationships from refShard
// to refArchive. Only reference entities can be archived.
// The node must exist in refShard. Event nodes cannot be archived.
//
// Cross-shard rel restriction (ErrCrossShardArchiveRel): a node may only
// be archived when every relationship touching it would be entirely
// resident on refArchive afterwards. In practice this means self-loops
// only — any rel to another node that is NOT being archived in the same
// call (refShard same-shard rel or event-shard cross-shard rel) is
// rejected up front. Otherwise we either silently lose the rel
// (cascade-on-refShard wipes both the entity and the partner's adjacency
// entries) or leave a dangling in/ or out/ entry on the partner shard.
// Proper cross-shard archival migration is a future feature; until then
// the caller must either delete the rel or arrange for both endpoints
// to be co-archived.
//
// Atomicity: this operation is NOT transactional across the two BadgerStores.
// On failure the rollback is best-effort (DeleteNodeCascade on the target).
// If both the primary write AND the rollback fail, data may exist in both
// stores simultaneously. The repair subsystem (tieredstore_repair.go) can
// detect and resolve this: a node present in both refShard and refArchive
// is flagged as a split-brain condition and corrected using the refShard copy.
func (ts *Store) ArchiveNode(nid types.NodeID) error {
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

	// 3. Collect all unique rel IDs touching the node.
	outIDs := ts.refShard.OutgoingRelIDs(id)
	inIDs := ts.refShard.IncomingRelIDs(id, 0)
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

	// 4. Pre-scan rels: classify each as self-loop (safe to migrate) or
	// cross-shard (reject). Doing this BEFORE any mutation keeps the
	// "fail loud" path side-effect free — no need for rollback paths to
	// undo a partial archive write.
	var rels []*types.Relationship
	for _, rid := range relIDs {
		r, err := ts.refShard.GetRelationship(types.RelID(rid))
		if errors.Is(err, ErrRelNotFound) {
			// Cross-shard rel where the entity lives on another shard
			// (refShard only carries the in/ entry). Archiving would
			// leave the in/ entry dangling on refShard.
			return fmt.Errorf("%w (rel %d entity lives on another shard)", ErrCrossShardArchiveRel, rid)
		}
		if err != nil {
			return fmt.Errorf("graph: archive read rel %d: %w", rid, err)
		}
		// Self-loop is the only safe case: both endpoints are the archived
		// node, so after the move the rel is fully resident on refArchive.
		if r.StartNodeID().SnowflakeID() != id || r.EndNodeID().SnowflakeID() != id {
			return fmt.Errorf("%w (rel %d: %d -> %d)", ErrCrossShardArchiveRel, rid, r.StartNodeID().SnowflakeID(), r.EndNodeID().SnowflakeID())
		}
		rels = append(rels, r)
	}

	// 5. Lazy-open refArchive (only after pre-scan passes — avoids wasteful
	// open + immediate close on rejected archive attempts) and pin against
	// concurrent Close. Without the pin, a Close racing between Load and
	// the writes below could free the archive BadgerStore under us
	// (segfault on PutNode / PutRelationship). checkoutArchive's
	// archiveActiveReqs increment makes Close wait for archiveCheckin().
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

	// 6. Write node + rels to refArchive.
	if err := archive.PutNode(node); err != nil {
		return fmt.Errorf("graph: archive write node: %w", err)
	}

	for _, r := range rels {
		// All surviving rels are self-loops on the archived node, so both
		// endpoints exist on archive after the PutNode above.
		if err := archive.PutRelationship(r); err != nil {
			_ = archive.DeleteNodeCascade(types.NodeID(id)) // best-effort rollback; returning primary error
			return fmt.Errorf("graph: archive write rel: %w", err)
		}
	}

	// 7. Delete from refShard (cascade deletes node + all rels in refShard).
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
func (ts *Store) RestoreNode(nid types.NodeID) error {
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

	// 4. Read all rels from archive.
	outIDs := archive.OutgoingRelIDs(id)
	inIDs := archive.IncomingRelIDs(id, 0)

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
