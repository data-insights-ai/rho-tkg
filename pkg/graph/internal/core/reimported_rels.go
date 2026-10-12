package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Relationships whose lives name different endpoints (backlog 50).
//
// A relationship ID may be deleted and re-imported with another start or end
// node (relationship_import.go); its earlier life's rows stay in history
// (backlog 38) and name the old endpoints. The adjacency doors gather their
// candidates from the live adjacency, the deleted IDs and the store's
// ReimportedRelCapability set (forEachRelOffAdjacencyIDByDepth), so every
// write door that makes one ID's rows name different endpoints notes the ID
// first:
//
//   - Rels.Import / GraphTx.ImportRel over a deleted life with other endpoints
//     (the history relLifeStart read);
//   - a replica's apply of a relationship put over a deleted life
//     (applyRelPutLocked) and IO.Import's current row over history rows, or
//     history row under a current row, with other endpoints.
//
// A re-import with the same endpoints is not noted: the live adjacency reaches
// every life. Graphs written before the set existed are covered by a one-time
// scan at open (backfillReimportedRelsBestEffort).

// reimportedRelsBuiltMeta marks a store whose re-imported set covers every
// relationship it holds (written by the open-time scan).
const reimportedRelsBuiltMeta = "rel_reimported_built"

// relRowsNameOther reports whether a row names another start or end node than
// start / end.
func relRowsNameOther(rows []*types.Relationship, start, end types.NodeID) bool {
	for _, r := range rows {
		if r != nil && (r.StartNodeID() != start || r.EndNodeID() != end) {
			return true
		}
	}
	return false
}

// noteRelLivesDiffer records id in the store's set before the row that makes
// its lives name different endpoints is written (a failed write leaves a
// harmless extra candidate). No-op on a store without the set.
func (c *Core) noteRelLivesDiffer(id types.RelID) error {
	if c.reimportedRels == nil {
		return nil
	}
	return c.reimportedRels.NoteReimportedRel(id)
}

// noteRelPutOverHistory notes r's ID when r, a current row about to be written
// for an ID without one, names other endpoints than a history row of the ID.
// A store with HistoryPresenceCapability answers "no history" without a read.
func (c *Core) noteRelPutOverHistory(r *types.Relationship) error {
	if c.reimportedRels == nil {
		return nil
	}
	if p, ok := c.store.(storepkg.HistoryPresenceCapability); ok {
		has, err := p.HasRelHistory(r.ID())
		if errors.Is(err, storepkg.ErrSlotNotLocal) || (err == nil && !has) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	history, err := c.getRelHistory(r.ID())
	if errors.Is(err, storepkg.ErrSlotNotLocal) {
		return nil
	}
	if err != nil {
		return err
	}
	if relRowsNameOther(history, r.StartNodeID(), r.EndNodeID()) {
		return c.noteRelLivesDiffer(r.ID())
	}
	return nil
}

// noteRelHistoryUnderCurrent notes h's ID when h, a history row about to be
// written, names other endpoints than the ID's current row.
func (c *Core) noteRelHistoryUnderCurrent(h *types.Relationship) error {
	if c.reimportedRels == nil {
		return nil
	}
	cur, err := c.getCurrentRelationship(h.ID())
	if errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrSlotNotLocal) {
		return nil
	}
	if err != nil {
		return err
	}
	if relRowsNameOther([]*types.Relationship{h}, cur.StartNodeID(), cur.EndNodeID()) {
		return c.noteRelLivesDiffer(h.ID())
	}
	return nil
}

// backfillReimportedRelsBestEffort makes the store's set cover a graph written
// before it existed: once per store (reimportedRelsBuiltMeta), every live
// relationship with history whose rows name other endpoints than its live row
// is noted (O(relationships with history), at open). A store that cannot run
// it (no MetaKV, read-only, a failed scan) leaves c.reimportedRels nil, so the
// adjacency doors fold every relationship with history: sound, as before.
func (c *Core) backfillReimportedRelsBestEffort() {
	if c.reimportedRels == nil {
		return
	}
	if c.metaKV == nil {
		c.reimportedRels = nil
		return
	}
	if v, err := c.metaKV.MetaGet(reimportedRelsBuiltMeta); err == nil && len(v) > 0 {
		return
	}
	if err := c.backfillReimportedRels(); err != nil {
		c.reimportedRels = nil
		return
	}
	if err := c.metaKV.MetaSet(reimportedRelsBuiltMeta, []byte{1}); err != nil {
		c.reimportedRels = nil
	}
}

func (c *Core) backfillReimportedRels() error {
	var ids []types.RelID
	if err := c.store.ForEachRelHistoryID(func(id types.RelID) bool {
		ids = append(ids, id)
		return true
	}); err != nil {
		return err
	}
	for _, id := range ids {
		cur, err := c.getCurrentRelationship(id)
		if errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrSlotNotLocal) {
			continue // deleted: the deleted IDs reach every life
		}
		if err != nil {
			return err
		}
		if err := c.noteRelPutOverHistory(cur); err != nil {
			return err
		}
	}
	return nil
}
