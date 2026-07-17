package core

import (
	"context"
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Tx — Add (Create / Import)
// =============================================================================

// AddNode creates a new node within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.Nodes.Add.
//
// Holds tx.mu for the whole call: the done check, the mutation,
// the event buffer append, and the rollback-log append are one critical
// section so concurrent goroutines using the same *GraphTx serialize
// instead of racing Commit/Rollback against an in-flight method.
func (tx *GraphTx) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	n, err := tx.g.addNodeInternal(context.Background(), labels, props)
	tx.noteNodeCreateResultLocked(n)
	if err != nil {
		return n, err
	}

	return n, nil
}

// AddRelationship creates a new relationship within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.Rels.Add.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	r, err := tx.g.addRelationshipInternal(context.Background(), typeName, startNode, endNode, props)
	tx.noteRelCreateResultLocked(r)
	if err != nil {
		return r, err
	}

	return r, nil
}

// AddRelationshipByID creates a relationship using endpoint snowflake IDs within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.addRelationshipByIDInternal.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	r, err := tx.g.addRelationshipByIDInternal(context.Background(), typeName, startID, endID, props)
	tx.noteRelCreateResultLocked(r)
	if err != nil {
		return r, err
	}

	return r, nil
}

// AddRelationshipByIDIfAbsent atomically checks for an existing relationship of the same
// type between the same endpoints and creates one only if absent. Returns (rel, created, err)
// where created=true if a new relationship was created.
// The relationship ID is tracked for rollback only if created.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, false, err
	}
	defer tx.unlockActiveCoreWrite()

	r, created, err := tx.g.addRelationshipByIDIfAbsentInternal(context.Background(), typeName, startID, endID, props)
	if created && r != nil {
		tx.noteRelCreateResultLocked(r)
	}
	if err != nil {
		return r, created, err
	}

	return r, created, nil
}

// ImportNodeWithID creates a node with a caller-specified snowflake ID within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.Nodes.Import.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) ImportNodeWithID(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	if err := tx.lockActiveCoreWriteContext(ctx); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	if _, deletedInTx := tx.deletedNodeSet[id.SnowflakeID()]; deletedInTx {
		if err := tx.materializeDeletedNodeHistoryLocked(id); err != nil {
			return nil, err
		}
	}

	n, err := tx.g.importNodeWithIDInternal(ctx, id, labels, props)
	tx.noteNodeCreateResultLocked(n)
	if err != nil {
		return n, err
	}

	return n, nil
}

// ImportRelationshipWithID creates a relationship with a caller-specified snowflake ID within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.Rels.Import.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) ImportRelationshipWithID(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWriteContext(ctx); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()
	if _, deletedInTx := tx.deletedRelSet[id.SnowflakeID()]; deletedInTx {
		if err := tx.materializeDeletedRelHistoryLocked(id); err != nil {
			return nil, err
		}
	}

	r, err := tx.g.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	tx.noteRelCreateResultLocked(r)
	if err != nil {
		return r, err
	}

	return r, nil
}

func (tx *GraphTx) noteNodeCreateResultLocked(n *types.Node) {
	if n == nil {
		return
	}
	tx.g.publishEvent(eventspkg.EventNodeCreate, types.EntityID(n.ID()), tx.g.now(), eventspkg.PriorityHigh)
	tx.trackCreatedNodeLocked(n.ID().SnowflakeID())
}

func (tx *GraphTx) noteRelCreateResultLocked(r *types.Relationship) {
	if r == nil {
		return
	}
	tx.g.publishEvent(eventspkg.EventRelCreate, types.EntityID(r.ID()), tx.g.now(), eventspkg.PriorityHigh)
	tx.trackCreatedRelLocked(r.ID().SnowflakeID())
}

// =============================================================================
// Tx — Update
// =============================================================================

// UpdateNode applies property updates to a node within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.Nodes.Update.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		// Empty-update no-op. Read directly from the store rather than via
		// the exported wrapper: the tx already holds c.mu.Lock(), and any
		// future addition of c.mu.RLock() to NodeOps.Get would
		// deadlock the tx. The tx convention is: never call exported
		// methods, always reach the store via *Internal helpers or
		// tx.g.store directly.
		n, err := tx.g.getCurrentNode(id)
		if err == nil {
			tx.g.opNodeReads.Add(1)
		}
		return n, err
	}

	prov, tmp, preparedUpdates, err := tx.g.prepareUpdateProperties(updates, "update node")
	if err != nil {
		return nil, err
	}
	if preparedUpdateCanBeReadOnlyNoOp(prov, tmp) {
		current, err := tx.g.getCurrentNode(id)
		if err != nil {
			return nil, err
		}
		if !nodePropertyUpdatesMutate(current, preparedUpdates) {
			tx.g.opNodeReads.Add(1)
			return current, nil
		}
		if err := tx.snapshotCurrentNodeLocked(current); err != nil {
			return nil, err
		}
	} else {
		if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
			return nil, err
		}
	}

	n, mutated, err := tx.g.updateNodePreparedInternal(context.Background(), id, prov, tmp, preparedUpdates)
	if err == nil && mutated {
		tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	}
	return n, err
}

// UpdateRelationship applies property updates to a relationship within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.Rels.Update.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if len(updates) == 0 {
		// Empty-update no-op. See UpdateNode for why this avoids the
		// exported wrapper.
		r, err := tx.g.getCurrentRelationship(id)
		if err == nil {
			tx.g.opRelReads.Add(1)
		}
		return r, err
	}

	prov, tmp, preparedUpdates, err := tx.g.prepareUpdateProperties(updates, "update relationship")
	if err != nil {
		return nil, err
	}
	if preparedUpdateCanBeReadOnlyNoOp(prov, tmp) {
		current, err := tx.g.getCurrentRelationship(id)
		if err != nil {
			return nil, err
		}
		if !relPropertyUpdatesMutate(current, preparedUpdates) {
			tx.g.opRelReads.Add(1)
			return current, nil
		}
		if err := tx.snapshotCurrentRelLocked(current); err != nil {
			return nil, err
		}
	} else {
		if err := tx.snapshotRelLocked(id.SnowflakeID()); err != nil {
			return nil, err
		}
	}

	r, mutated, err := tx.g.updateRelationshipPreparedInternal(context.Background(), id, prov, tmp, preparedUpdates)
	if err == nil && mutated {
		tx.g.publishEvent(eventspkg.EventRelUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	}
	return r, err
}

// SetNodeVersionInterval runs the full cascade timeline edit inside the
// transaction. The cascade reads + writes multiple history rows under the
// entity lock. Rollback support is partial: the tx snapshot captures the
// current row (restored on rollback), but per-version history-row edits
// made by the cascade are NOT undone. Callers that need full rollback for
// cascades should run the cascade OUTSIDE the tx.
func (tx *GraphTx) SetNodeVersionInterval(id types.NodeID, validFrom, validTo types.Instant, props map[string]any) (*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	if err := tx.snapshotNodeLocked(id.SnowflakeID()); err != nil {
		return nil, err
	}

	n, err := tx.g.cascadeNodeVersionInterval(context.Background(), id, validFrom, validTo, props)
	if err == nil && n != nil {
		tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	}
	return n, err
}

// SetRelVersionInterval is the relationship counterpart of SetNodeVersionInterval.
func (tx *GraphTx) SetRelVersionInterval(id types.RelID, validFrom, validTo types.Instant, props map[string]any) (*types.Relationship, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	if err := tx.snapshotRelLocked(id.SnowflakeID()); err != nil {
		return nil, err
	}

	r, err := tx.g.cascadeRelVersionInterval(context.Background(), id, validFrom, validTo, props)
	if err == nil && r != nil {
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
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) DeleteNode(id types.NodeID) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	// Snapshot the node before deletion.
	node, err := tx.g.getCurrentNode(id)
	if err != nil {
		return err
	}
	nodeCopy := node.DeepCopy()
	nodeHistory, nodeHistoryTrimFrom, useNodeHistoryTrim, err := tx.deletedNodeHistorySnapshot(id, node)
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
	if !tx.g.storeRowsTrust {
		if err := tx.g.validateOutgoingRelationshipRows(id, 0, outRels); err != nil {
			return err
		}
		if err := tx.g.validateIncomingRelationshipRows(id, 0, inRels); err != nil {
			return err
		}
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
		history, historyTrimFrom, useHistoryTrim, err := tx.deletedRelHistorySnapshot(r.ID(), r)
		if err != nil {
			return err
		}
		relCopies = append(relCopies, deletedRelSnapshot{
			rel:             r.DeepCopy(),
			history:         history,
			historyTrimFrom: historyTrimFrom,
			useHistoryTrim:  useHistoryTrim,
		})
	}

	// Perform the actual deletion (internal — tx already holds c.mu.Lock).
	cascadeRelIDs, err := tx.g.deleteNodeInternal(context.Background(), id)
	if err != nil {
		return err
	}

	tx.g.publishCascadeDeleteEvents(id, cascadeRelIDs, tx.g.now())
	tx.trackDeletedNodeLocked(id.SnowflakeID())
	for _, r := range relCopies {
		tx.trackDeletedRelLocked(r.rel.ID().SnowflakeID())
	}
	tx.deletedNodes = append(tx.deletedNodes, deletedNodeSnapshot{
		node:            nodeCopy,
		nodeHistory:     nodeHistory,
		historyTrimFrom: nodeHistoryTrimFrom,
		useHistoryTrim:  useNodeHistoryTrim,
		rels:            relCopies,
	})

	return nil
}

// DeleteRelationship removes a relationship within the transaction.
// Snapshots the relationship for rollback. Delegates to Graph.Rels.Delete.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) DeleteRelationship(id types.RelID) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	if err := storepkg.ValidateRelID(id); err != nil {
		return err
	}

	// Snapshot the relationship before deletion.
	rel, err := tx.g.getCurrentRelationship(id)
	if err != nil {
		return err
	}
	relCopy := rel.DeepCopy()
	history, historyTrimFrom, useHistoryTrim, err := tx.deletedRelHistorySnapshot(id, rel)
	if err != nil {
		return err
	}

	// Perform the actual deletion (internal — tx already holds c.mu.Lock).
	if err := tx.g.deleteRelationshipInternal(context.Background(), id); err != nil {
		return err
	}

	tx.g.publishEvent(eventspkg.EventRelDelete, types.EntityID(id), tx.g.now(), eventspkg.PriorityCritical)
	tx.trackDeletedRelLocked(id.SnowflakeID())
	tx.deletedRels = append(tx.deletedRels, deletedRelSnapshot{
		rel:             relCopy,
		history:         history,
		historyTrimFrom: historyTrimFrom,
		useHistoryTrim:  useHistoryTrim,
	})

	return nil
}

func (tx *GraphTx) deletedNodeHistorySnapshot(id types.NodeID, node *types.Node) ([]*types.Node, uint32, bool, error) {
	if tx.g.historyTrim == nil {
		history, err := tx.g.copyNodeHistory(id)
		return history, 0, false, err
	}
	if found, err := tx.nodeHistoryVersionExists(id, node.Version()); err != nil {
		return nil, 0, false, err
	} else if found {
		history, err := tx.g.copyNodeHistory(id)
		return history, 0, false, err
	}
	return nil, node.Version(), true, nil
}

func (tx *GraphTx) deletedRelHistorySnapshot(id types.RelID, rel *types.Relationship) ([]*types.Relationship, uint32, bool, error) {
	if tx.g.historyTrim == nil {
		history, err := tx.g.copyRelHistory(id)
		if errors.Is(err, storepkg.ErrSlotNotLocal) {
			// A Model-A foreign-incoming stub (ADR-0010): adjacency-only, foreign
			// rel-ID slot, no local history to snapshot for rollback.
			return nil, 0, false, nil
		}
		return history, 0, false, err
	}
	if found, err := tx.relHistoryVersionExists(id, rel.Version()); err != nil {
		return nil, 0, false, err
	} else if found {
		history, err := tx.g.copyRelHistory(id)
		return history, 0, false, err
	}
	return nil, rel.Version(), true, nil
}

func (tx *GraphTx) nodeHistoryVersionExists(id types.NodeID, version uint32) (bool, error) {
	if _, err := tx.g.getNodeVersion(id, version); err == nil {
		return true, nil
	} else if errors.Is(err, storepkg.ErrVersionNotFound) {
		return false, nil
	} else {
		return false, err
	}
}

func (tx *GraphTx) relHistoryVersionExists(id types.RelID, version uint32) (bool, error) {
	if _, err := tx.g.getRelVersion(id, version); err == nil {
		return true, nil
	} else if errors.Is(err, storepkg.ErrVersionNotFound) {
		return false, nil
	} else if errors.Is(err, storepkg.ErrSlotNotLocal) {
		// A Model-A foreign-incoming stub (ADR-0010): its rel-ID slot is foreign, so
		// a slot-routed history read fails closed. The stub is adjacency-only — its
		// version history's authority is the START machine — so it has no local
		// history version to snapshot for rollback.
		return false, nil
	} else {
		return false, err
	}
}

func (tx *GraphTx) materializeDeletedNodeHistoryLocked(id types.NodeID) error {
	for i := range tx.deletedNodes {
		snap := &tx.deletedNodes[i]
		if snap.node == nil || snap.node.ID() != id || !snap.useHistoryTrim {
			continue
		}
		history, err := tx.g.copyNodeHistory(id)
		if err != nil {
			return err
		}
		snap.nodeHistory = nodeHistoryBeforeVersion(history, snap.historyTrimFrom)
		snap.useHistoryTrim = false
		return nil
	}
	return nil
}

func (tx *GraphTx) materializeDeletedRelHistoryLocked(id types.RelID) error {
	for i := range tx.deletedRels {
		if err := tx.materializeDeletedRelSnapshotHistoryLocked(&tx.deletedRels[i], id); err != nil {
			return err
		}
	}
	for i := range tx.deletedNodes {
		for j := range tx.deletedNodes[i].rels {
			if err := tx.materializeDeletedRelSnapshotHistoryLocked(&tx.deletedNodes[i].rels[j], id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *GraphTx) materializeDeletedRelSnapshotHistoryLocked(snap *deletedRelSnapshot, id types.RelID) error {
	if snap.rel == nil || snap.rel.ID() != id || !snap.useHistoryTrim {
		return nil
	}
	history, err := tx.g.copyRelHistory(id)
	if err != nil {
		return err
	}
	snap.history = relHistoryBeforeVersion(history, snap.historyTrimFrom)
	snap.useHistoryTrim = false
	return nil
}

func nodeHistoryBeforeVersion(history []*types.Node, minVersion uint32) []*types.Node {
	out := history[:0]
	for _, n := range history {
		if n != nil && n.Version() < minVersion {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func relHistoryBeforeVersion(history []*types.Relationship, minVersion uint32) []*types.Relationship {
	out := history[:0]
	for _, r := range history {
		if r != nil && r.Version() < minVersion {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// =============================================================================
// Tx — Label mutations
// =============================================================================

// AddNodeLabel adds a label to a node within the transaction.
// Snapshots the pre-mutation state on first mutation so Rollback can restore
// both the node row and label indexes. Idempotent: if the node already has the
// label, returns nil with no snapshot recorded.
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) AddNodeLabel(id types.NodeID, label string) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	if err := tx.g.validateName(label); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	current, alreadyPresent, err := tx.preflightAddNodeLabelLocked(id, label)
	if err != nil {
		return err
	}
	if alreadyPresent {
		return nil
	}
	if err := tx.snapshotCurrentNodeLocked(current); err != nil {
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
// Holds tx.mu for the whole call — see AddNode.
func (tx *GraphTx) RemoveNodeLabel(id types.NodeID, label string) error {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return err
	}
	defer tx.unlockActiveCoreWrite()
	if err := tx.g.validateName(label); err != nil {
		return err
	}
	if err := storepkg.ValidateNodeID(id); err != nil {
		return err
	}

	current, err := tx.preflightRemoveNodeLabelLocked(id, label)
	if err != nil {
		return err
	}
	if err := tx.snapshotCurrentNodeLocked(current); err != nil {
		return err
	}

	if err := tx.g.removeNodeLabelInternal(id, label); err != nil {
		return err
	}

	tx.g.publishEvent(eventspkg.EventNodeUpdate, types.EntityID(id), tx.g.now(), eventspkg.PriorityNormal)
	return nil
}

func (tx *GraphTx) preflightAddNodeLabelLocked(id types.NodeID, label string) (*types.Node, bool, error) {
	current, err := tx.g.getCurrentNode(id)
	if err != nil {
		return nil, false, err
	}
	tok, known := tx.g.labels.Lookup(label)
	if known && current.HasLabelTokenRaw(tok) {
		return current, true, nil
	}
	if current.LabelTokenCount()+1 > tx.g.validation.MaxLabelsPerNode {
		return nil, false, fmt.Errorf("%w: %d > %d", ErrTooManyLabels, current.LabelTokenCount()+1, tx.g.validation.MaxLabelsPerNode)
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return nil, false, err
	}
	return current, false, nil
}

func (tx *GraphTx) preflightRemoveNodeLabelLocked(id types.NodeID, label string) (*types.Node, error) {
	tok, ok := tx.g.labels.Lookup(label)
	if !ok {
		return nil, ErrLabelNotFound
	}
	current, err := tx.g.getCurrentNode(id)
	if err != nil {
		return nil, err
	}
	if !current.HasLabelTokenRaw(tok) {
		return nil, ErrLabelNotFound
	}
	if current.LabelTokenCount() == 1 {
		return nil, ErrLastLabel
	}
	if err := rejectClosedNodeMutation(current); err != nil {
		return nil, err
	}
	return current, nil
}
