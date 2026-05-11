package core

import (
	"context"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// =============================================================================
// Tx — Add (Create / Import)
// =============================================================================

// AddNode creates a new node within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.Nodes.Add.
//
// Holds tx.mu for the whole call (R4-F2): the done check, the mutation,
// the event buffer append, and the rollback-log append are one critical
// section so concurrent goroutines using the same *GraphTx serialize
// instead of racing Commit/Rollback against an in-flight method.
func (tx *GraphTx) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	n, err := tx.g.addNodeInternal(context.Background(), labels, props)
	if n != nil {
		tx.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(n.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedNodeLocked(n.ID().SnowflakeID())
	}
	if err != nil {
		return n, err
	}

	return n, nil
}

// AddRelationship creates a new relationship within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.Rels.Add.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	r, err := tx.g.addRelationshipInternal(context.Background(), typeName, startNode, endNode, props)
	if r != nil {
		tx.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(r.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedRelLocked(r.ID().SnowflakeID())
	}
	if err != nil {
		return r, err
	}

	return r, nil
}

// AddRelationshipByID creates a relationship using endpoint snowflake IDs within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.addRelationshipByIDInternal.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	r, err := tx.g.addRelationshipByIDInternal(context.Background(), typeName, startID, endID, props)
	if r != nil {
		tx.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(r.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedRelLocked(r.ID().SnowflakeID())
	}
	if err != nil {
		return r, err
	}

	return r, nil
}

// AddRelationshipByIDIfAbsent atomically checks for an existing relationship of the same
// type between the same endpoints and creates one only if absent. Returns (rel, created, err)
// where created=true if a new relationship was created.
// The relationship ID is tracked for rollback only if created.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, false, storepkg.ErrTxDone
	}

	r, created, err := tx.g.addRelationshipByIDIfAbsentInternal(context.Background(), typeName, startID, endID, props)
	if created && r != nil {
		tx.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(r.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedRelLocked(r.ID().SnowflakeID())
	}
	if err != nil {
		return r, created, err
	}

	return r, created, nil
}

// ImportNodeWithID creates a node with a caller-specified snowflake ID within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.Nodes.Import.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) ImportNodeWithID(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	n, err := tx.g.importNodeWithIDInternal(ctx, id, labels, props)
	if n != nil {
		tx.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(n.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedNodeLocked(n.ID().SnowflakeID())
	}
	if err != nil {
		return n, err
	}

	return n, nil
}

// ImportRelationshipWithID creates a relationship with a caller-specified snowflake ID within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.Rels.Import.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) ImportRelationshipWithID(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	r, err := tx.g.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	if r != nil {
		tx.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(r.ID()), tx.g.now(), eventspkg.PriorityHigh)
		tx.trackCreatedRelLocked(r.ID().SnowflakeID())
	}
	if err != nil {
		return r, err
	}

	return r, nil
}

// =============================================================================
// Tx — Update
// =============================================================================

// UpdateNode applies property updates to a node within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.Nodes.Update.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
		// Empty-update no-op. Read directly from the store rather than via
		// the exported wrapper: the tx already holds c.mu.Lock(), and any
		// future addition of c.mu.RLock() to GetNodeWithContext would
		// deadlock the tx. The tx convention is: never call exported
		// methods, always reach the store via *Internal helpers or
		// tx.g.store directly.
		n, err := tx.g.store.GetNode(id)
		if err == nil {
			tx.g.opNodeReads.Add(1)
		}
		return n, err
	}

	if _, _, err := tx.g.prepareUpdateProperties(updates, "update node"); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		return nil, err
	}

	n, err := tx.g.updateNodeInternal(context.Background(), id, updates)
	if err == nil {
		tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	}
	return n, err
}

// UpdateRelationship applies property updates to a relationship within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.Rels.Update.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	if len(updates) == 0 {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
		// Empty-update no-op. See UpdateNode for why this avoids the
		// exported wrapper.
		r, err := tx.g.store.GetRelationship(id)
		if err == nil {
			tx.g.opRelReads.Add(1)
		}
		return r, err
	}

	if _, _, err := tx.g.prepareUpdateProperties(updates, "update relationship"); err != nil {
		return nil, err
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := tx.snapshotRelLocked(id.SnowflakeID()); err != nil {
		return nil, err
	}

	r, err := tx.g.updateRelationshipInternal(context.Background(), id, updates)
	if err == nil {
		tx.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	}
	return r, err
}

// SetNodeProperty sets a single property on a node within the transaction.
func (tx *GraphTx) SetNodeProperty(id types.NodeID, key string, value any) error {
	_, err := tx.UpdateNode(id, map[string]any{key: value})
	return err
}

// DeleteNodeProperty removes a single property from a node within the transaction.
func (tx *GraphTx) DeleteNodeProperty(id types.NodeID, key string) error {
	_, err := tx.UpdateNode(id, map[string]any{key: nil})
	return err
}

// SetRelationshipProperty sets a single property on a relationship within the transaction.
func (tx *GraphTx) SetRelationshipProperty(id types.RelID, key string, value any) error {
	_, err := tx.UpdateRelationship(id, map[string]any{key: value})
	return err
}

// DeleteRelationshipProperty removes a single property from a relationship within the transaction.
func (tx *GraphTx) DeleteRelationshipProperty(id types.RelID, key string) error {
	_, err := tx.UpdateRelationship(id, map[string]any{key: nil})
	return err
}

// =============================================================================
// Tx — Delete
// =============================================================================

// DeleteNode removes a node and all connected relationships within the transaction.
// Snapshots the node and all cascade-deleted relationships for rollback.
// Delegates the actual deletion to Graph.Nodes.Delete.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) DeleteNode(id types.NodeID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return storepkg.ErrTxDone
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	// Snapshot the node before deletion.
	node, err := tx.g.store.GetNode(id)
	if err != nil {
		return err
	}
	nodeCopy := node.DeepCopy()
	nodeHistory, err := copyNodeHistory(tx.g.store.GetNodeHistory(id))
	if err != nil {
		return err
	}

	// Snapshot all connected relationships before cascade deletion.
	outRels, err := tx.g.store.OutgoingRelationships(id, 0)
	if err != nil {
		return err
	}
	inRels, err := tx.g.store.IncomingRelationships(id, 0)
	if err != nil {
		return err
	}

	// Deduplicate self-loop rels and deep copy all.
	seen := make(map[snowflake.ID]bool)
	var relCopies []deletedRelSnapshot
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
	for _, r := range allRels {
		rid := r.ID().SnowflakeID()
		if seen[rid] {
			continue
		}
		seen[rid] = true
		history, err := copyRelHistory(tx.g.store.GetRelHistory(r.ID()))
		if err != nil {
			return err
		}
		relCopies = append(relCopies, deletedRelSnapshot{rel: r.DeepCopy(), history: history})
	}

	// Perform the actual deletion (internal — tx already holds c.mu.Lock).
	if err := tx.g.deleteNodeInternal(context.Background(), id); err != nil {
		return err
	}

	tx.g.publishEvent(eventspkg.EventNodeDelete, types.EntityID(id), tx.g.now(), eventspkg.PriorityCritical)
	tx.trackDeletedNodeLocked(id.SnowflakeID())
	for _, r := range relCopies {
		tx.trackDeletedRelLocked(r.rel.ID().SnowflakeID())
	}
	tx.deletedNodes = append(tx.deletedNodes, deletedNodeSnapshot{
		node:        nodeCopy,
		nodeHistory: nodeHistory,
		rels:        relCopies,
	})

	return nil
}

// DeleteRelationship removes a relationship within the transaction.
// Snapshots the relationship for rollback. Delegates to Graph.Rels.Delete.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) DeleteRelationship(id types.RelID) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return storepkg.ErrTxDone
	}
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}

	// Snapshot the relationship before deletion.
	rel, err := tx.g.store.GetRelationship(id)
	if err != nil {
		return err
	}
	relCopy := rel.DeepCopy()
	history, err := copyRelHistory(tx.g.store.GetRelHistory(id))
	if err != nil {
		return err
	}

	// Perform the actual deletion (internal — tx already holds c.mu.Lock).
	if err := tx.g.deleteRelationshipInternal(context.Background(), id); err != nil {
		return err
	}

	tx.g.publishEvent(eventspkg.EventRelDelete, types.EntityID(id), tx.g.now(), eventspkg.PriorityCritical)
	tx.trackDeletedRelLocked(id.SnowflakeID())
	tx.deletedRels = append(tx.deletedRels, deletedRelSnapshot{rel: relCopy, history: history})

	return nil
}

// =============================================================================
// Tx — Label mutations
// =============================================================================

// AddNodeLabel adds a label to a node within the transaction.
// Snapshots the pre-mutation state on first mutation so Rollback can restore
// both the node row and label indexes. Idempotent: if the node already has the
// label, returns nil with no snapshot recorded.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) AddNodeLabel(id types.NodeID, label string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return storepkg.ErrTxDone
	}
	if err := tx.g.validateName(label); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	// Look up the token (or pre-compute whether the label is already present)
	// to decide whether this call will actually mutate. We need the token for
	// the delta regardless, so resolve it up front under c.mu (already held by tx).
	tok, ok := tx.g.labels.Lookup(label)
	if ok {
		cur, err := tx.g.store.GetNode(id)
		if err == nil && cur.HasLabelTokenRaw(tok) {
			// Idempotent no-op: nothing to snapshot, no delta to record.
			return nil
		}
	}

	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		return err
	}

	mutated, err := tx.g.addNodeLabelInternal(id, label)
	if err != nil {
		return err
	}
	if !mutated {
		return nil
	}

	tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	return nil
}

// RemoveNodeLabel removes a label from a node within the transaction.
// Snapshots the pre-mutation state on first mutation so Rollback can restore
// both the node row and label indexes. Returns ErrLastLabel if the label is the
// only one on the node.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) RemoveNodeLabel(id types.NodeID, label string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return storepkg.ErrTxDone
	}
	if err := tx.g.validateName(label); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		return err
	}

	if err := tx.g.removeNodeLabelInternal(id, label); err != nil {
		return err
	}

	tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	return nil
}
