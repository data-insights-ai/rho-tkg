package core

import (
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The whole-unit pre-flight of a Batch or ingest group that carries
// caller-instant ops — node and relationship alike (DeleteNodeWithTx,
// UpdateNodeWithTx, DeleteRelationshipWithTx, UpdateRelationshipWithTx). One
// pre-flight for both kinds, because they interact: a node delete at t
// tombstones every relationship of the node at t, so a caller-instant op on
// one of those relationships, or a plain one, in the same unit is an op on the
// same entity. See batch_rel_withtx.go for why a refusal refuses the whole
// unit and why a caller-instant op must be the only op on its entity in the
// unit.

// callerTxUnit is the queued ops of one apply unit (a BatchBuilder, an ingest
// group) as the pre-flight reads them.
type callerTxUnit struct {
	nodes        []pendingNode
	rels         []pendingRel
	nodeUpdates  []pendingNodeUpdate
	relUpdates   []pendingRelUpdate
	nodeDeletes  []pendingNodeDelete
	relDeletes   []pendingRelDelete
	nodeCascades []pendingNodeCascade
	relCascades  []pendingRelCascade
}

func (b *BatchBuilder) callerTxUnit() callerTxUnit {
	return callerTxUnit{b.nodes, b.rels, b.nodeUpdates, b.relUpdates, b.nodeDeletes, b.relDeletes, b.nodeCascades, b.relCascades}
}

func (g *ingestGroup) callerTxUnit() callerTxUnit {
	return callerTxUnit{g.nodes, g.rels, g.nodeUpdates, g.relUpdates, g.nodeDeletes, g.relDeletes, g.nodeCascades, g.relCascades}
}

// hasCallerTx reports whether the unit carries any caller-instant op. A
// strong-mode ingest group that does is applied in a unit of its own
// (applyCommitGroup), so its refusal fails that group only.
func (u callerTxUnit) hasCallerTx() bool {
	for i := range u.relDeletes {
		if u.relDeletes[i].at != 0 {
			return true
		}
	}
	for i := range u.relUpdates {
		if u.relUpdates[i].update.temporal.txAt != 0 {
			return true
		}
	}
	for i := range u.nodeUpdates {
		if u.nodeUpdates[i].update.temporal.txAt != 0 {
			return true
		}
	}
	for i := range u.nodeDeletes {
		if u.nodeDeletes[i].at != 0 {
			return true
		}
	}
	return false
}

// nodeAdjacentRelIDs lists the relationships a delete of node id would
// cascade (deduplicated), read under the node's entity lock.
func (c *Core) nodeAdjacentRelIDs(id types.NodeID) ([]types.RelID, error) {
	c.entityLocks.LockEntity(id.SnowflakeID())
	defer c.entityLocks.UnlockEntity(id.SnowflakeID())
	out, err := c.store.OutgoingRelationships(id, 0)
	if err != nil {
		return nil, err
	}
	in, err := c.store.IncomingRelationships(id, 0)
	if err != nil {
		return nil, err
	}
	seen := make(map[types.RelID]struct{}, len(out)+len(in))
	ids := make([]types.RelID, 0, len(out)+len(in))
	for _, r := range append(append(make([]*types.Relationship, 0, len(out)+len(in)), out...), in...) {
		if _, ok := seen[r.ID()]; !ok {
			seen[r.ID()] = struct{}{}
			ids = append(ids, r.ID())
		}
	}
	return ids, nil
}

// precheckCallerTxOps is the pre-flight of a unit carrying caller-instant ops.
// It runs before any write of the unit and returns the first refusal as a
// BatchError naming the op, or nil. Per caller-instant op:
//
//   - it is the only op of the unit on its entity. Node ops count node creates,
//     updates, deletes and cascades; relationship ops count relationship
//     creates, updates, deletes, cascades AND the relationships every node
//     delete of the unit (plain or caller-instant) cascades. A caller-instant
//     node delete also refuses when one of its cascaded relationships carries
//     another op, or a relationship create of the unit ends at the node (that
//     relationship would be cascaded at t below its own TxFrom);
//   - the entity exists and the seam's own refusals pass, under the entity
//     locks the seam takes: checkNodeCallerUpdate, checkNodeCascadeCallerTx
//     (the node and every cascaded relationship), checkRelCallerUpdate,
//     checkRelCallerDelete.
//
// Under Batch.Execute's exclusive lock nothing lands between this check and
// the write. The concurrent ingest mode runs under the shared lock: a racing
// standalone write can still make the seam refuse one op afterwards (that op
// alone fails, as every concurrent-mode op is atomic per entity only).
func (c *Core) precheckCallerTxOps(u callerTxUnit) error {
	if !u.hasCallerTx() {
		return nil
	}
	nodeTouched := make(map[types.NodeID]int, len(u.nodes)+len(u.nodeUpdates)+len(u.nodeDeletes)+len(u.nodeCascades))
	for i := range u.nodes {
		nodeTouched[u.nodes[i].node.ID()]++
	}
	for i := range u.nodeUpdates {
		nodeTouched[u.nodeUpdates[i].id]++
	}
	for i := range u.nodeDeletes {
		nodeTouched[u.nodeDeletes[i].id]++
	}
	for i := range u.nodeCascades {
		nodeTouched[u.nodeCascades[i].id]++
	}
	endpointOfCreate := make(map[types.NodeID]bool, 2*len(u.rels))
	relTouched := make(map[types.RelID]int, len(u.rels)+len(u.relUpdates)+len(u.relCascades)+len(u.relDeletes))
	for i := range u.rels {
		relTouched[u.rels[i].rel.ID()]++
		endpointOfCreate[u.rels[i].startID] = true
		endpointOfCreate[u.rels[i].endID] = true
	}
	for i := range u.relUpdates {
		relTouched[u.relUpdates[i].id]++
	}
	for i := range u.relCascades {
		relTouched[u.relCascades[i].id]++
	}
	for i := range u.relDeletes {
		relTouched[u.relDeletes[i].id]++
	}
	cascaded := make(map[types.NodeID][]types.RelID, len(u.nodeDeletes))
	for i := range u.nodeDeletes {
		d := u.nodeDeletes[i]
		ids, err := c.nodeAdjacentRelIDs(d.id)
		if err != nil {
			if d.at != 0 {
				return BatchError{Op: d.callerTxOpName(), ID: types.EntityID(d.id), Err: c.nodeEndMissingErr(d, err)}
			}
			continue // a plain delete fails on its own at apply
		}
		cascaded[d.id] = ids
		for _, rid := range ids {
			relTouched[rid]++
		}
	}

	another := func(kind string, id int64, at types.Instant) error {
		return fmt.Errorf("%w: t %d: %s %d carries another operation in this unit, whose apply order is not the queue order; put successive writes on one entity in successive units", ErrTxOrder, at, kind, id)
	}

	for i := range u.nodeUpdates {
		pu := &u.nodeUpdates[i]
		at := pu.update.temporal.txAt
		if at == 0 {
			continue
		}
		refuse := func(err error) error {
			return BatchError{Op: "UpdateNodeWithTx", ID: types.EntityID(pu.id), Err: err}
		}
		if nodeTouched[pu.id] > 1 {
			return refuse(another("node", int64(pu.id), at))
		}
		if err := func() error {
			c.entityLocks.LockEntity(pu.id.SnowflakeID())
			defer c.entityLocks.UnlockEntity(pu.id.SnowflakeID())
			current, err := c.getCurrentNode(pu.id)
			if err != nil {
				return err
			}
			return c.checkNodeCallerUpdate(pu.id, current, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}(); err != nil {
			return refuse(err)
		}
	}
	for i := range u.nodeDeletes {
		d := u.nodeDeletes[i]
		if d.at == 0 {
			continue
		}
		refuse := func(err error) error {
			return BatchError{Op: d.callerTxOpName(), ID: types.EntityID(d.id), Err: c.nodeEndMissingErr(d, err)}
		}
		if nodeTouched[d.id] > 1 || endpointOfCreate[d.id] {
			return refuse(another("node", int64(d.id), d.at))
		}
		for _, rid := range cascaded[d.id] {
			if relTouched[rid] > 1 {
				return refuse(another("cascaded relationship", int64(rid), d.at))
			}
		}
		if err := c.precheckNodeCascade(d.id, cascaded[d.id], d.at); err != nil {
			return refuse(err)
		}
	}

	check := func(op string, id types.RelID, at types.Instant, retract bool, run func(current *types.Relationship) error) error {
		refuse := func(err error) error { return BatchError{Op: op, ID: types.EntityID(id), Err: err} }
		if relTouched[id] > 1 {
			return refuse(another("relationship", int64(id), at))
		}
		c.entityLocks.LockEntity(id.SnowflakeID())
		defer c.entityLocks.UnlockEntity(id.SnowflakeID())
		current, err := c.getCurrentRelationship(id)
		if err != nil {
			if retract {
				err = c.retractMissingRelErr(id, err)
			}
			return refuse(err)
		}
		if err := run(current); err != nil {
			return refuse(err)
		}
		return nil
	}
	for i := range u.relUpdates {
		pu := &u.relUpdates[i]
		if pu.update.temporal.txAt == 0 {
			continue
		}
		if err := check("UpdateRelationshipWithTx", pu.id, pu.update.temporal.txAt, false, func(current *types.Relationship) error {
			return c.checkRelCallerUpdate(pu.id, current, pu.update.provenance, pu.update.temporal, pu.update.properties)
		}); err != nil {
			return err
		}
	}
	for i := range u.relDeletes {
		d := u.relDeletes[i]
		if d.at == 0 {
			continue // a plain Delete/RetractRelationship fails on its own at apply
		}
		if err := check(d.opName(), d.id, d.at, d.retract, func(current *types.Relationship) error {
			return c.checkRelCallerDelete(d.id, current, d.at)
		}); err != nil {
			return err
		}
	}
	return nil
}

// nodeEndMissingErr is the pre-flight's refusal error for a queued node end
// whose node has no current row: a retraction classifies it exactly as the
// seam does (retractMissingNodeErr), a delete keeps the lookup's error.
func (c *Core) nodeEndMissingErr(d pendingNodeDelete, err error) error {
	if d.retract {
		return c.retractMissingNodeErr(d.id, err)
	}
	return err
}

// precheckNodeCascade runs the seam's cascade refusals for a caller-instant
// delete of node id at t, under the node's and every cascaded relationship's
// entity locks (as deleteNodeInternal's Phase B takes them).
func (c *Core) precheckNodeCascade(id types.NodeID, rels []types.RelID, at types.Instant) error {
	ids := make([]snowflake.ID, 0, 1+len(rels))
	ids = append(ids, id.SnowflakeID())
	for _, rid := range rels {
		ids = append(ids, rid.SnowflakeID())
	}
	c.entityLocks.LockMany(ids)
	defer c.entityLocks.UnlockMany(ids)
	current, err := c.getCurrentNode(id)
	if err != nil {
		return err
	}
	out, err := c.store.OutgoingRelationships(id, 0)
	if err != nil {
		return err
	}
	in, err := c.store.IncomingRelationships(id, 0)
	if err != nil {
		return err
	}
	all := make([]*types.Relationship, 0, len(out)+len(in))
	all = append(all, out...)
	all = append(all, in...)
	return c.checkNodeCascadeCallerTx(id, current, all, at)
}
