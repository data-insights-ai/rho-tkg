package graph

import (
	"context"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// nodeSnapshot captures pre-mutation state for rollback.
type nodeSnapshot struct {
	id   snowflake.ID
	prev *types.Node // DeepCopy before first mutation
}

// relSnapshot captures pre-mutation state for rollback.
type relSnapshot struct {
	id   snowflake.ID
	prev *types.Relationship // DeepCopy before first mutation
}

// deletedNodeSnapshot captures a deleted node and its cascade-deleted relationships.
type deletedNodeSnapshot struct {
	node *types.Node
	rels []*types.Relationship // cascade-deleted rels
}

// labelDelta records a single label index mutation so Rollback can reverse
// the change to the store-level label index after ReplaceNode has already
// restored the node's internal state. ReplaceNode leaves the label index
// untouched (labels are immutable on that path), so label-adding or
// label-removing transactions need this separate tracker.
type labelDelta struct {
	id    snowflake.ID
	tok   uint16
	added bool // true = label was added; false = label was removed
}

// GraphTx is a mutation transaction with snapshot-based rollback.
// It holds the graph write lock for the entire duration of the transaction,
// blocking concurrent standalone mutations, Batch, and Snapshot operations.
// All mutations (create, update, delete) are tracked so Rollback can restore
// pre-transaction state.
//
// Events are buffered during the transaction and published on Commit (after
// g.mu.Unlock). On Rollback, buffered events are discarded.
//
// All methods check the done flag and return ErrTxDone after Commit/Rollback.
type GraphTx struct {
	g             *Graph
	createdNodes  []snowflake.ID
	createdRels   []snowflake.ID
	updatedNodes  []nodeSnapshot
	updatedRels   []relSnapshot
	deletedNodes  []deletedNodeSnapshot
	deletedRels   []*types.Relationship
	labelDeltas   []labelDelta          // label index mutations — reversed on Rollback
	pendingEvents []Event               // buffered events — published on Commit, discarded on Rollback
	snapshotSet   map[snowflake.ID]bool // tracks already-snapshotted entities (first mutation only)
	mu            sync.Mutex            // protects done flag and snapshot tracking
	done          bool
}

// BeginTx starts a new mutation transaction.
// Acquires the graph write lock, blocking concurrent standalone mutations,
// Batch, and Snapshot operations. The lock is released on Commit() or Rollback().
// Events are buffered and published after Commit (or discarded on Rollback).
func (g *Graph) BeginTx() *GraphTx {
	g.mu.Lock()
	tx := &GraphTx{g: g, snapshotSet: make(map[snowflake.ID]bool)}
	g.txEventBuffer = &tx.pendingEvents
	return tx
}

// AddNode creates a new node within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.AddNode.
func (tx *GraphTx) AddNode(labels []string, props map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	n, err := tx.g.addNodeInternal(context.Background(), labels, props)
	if err != nil {
		return nil, err
	}

	tx.g.publishEvent(EventNodeCreate, types.EntityID(n.ID()), nowInstant(), PriorityHigh)

	tx.mu.Lock()
	tx.createdNodes = append(tx.createdNodes, n.ID().SnowflakeID())
	tx.mu.Unlock()

	return n, nil
}

// AddRelationship creates a new relationship within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.AddRelationship.
func (tx *GraphTx) AddRelationship(typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	r, err := tx.g.addRelationshipInternal(context.Background(), typeName, startNode, endNode, props)
	if err != nil {
		return nil, err
	}

	tx.g.publishEvent(EventRelCreate, types.EntityID(r.ID()), nowInstant(), PriorityHigh)

	tx.mu.Lock()
	tx.createdRels = append(tx.createdRels, r.ID().SnowflakeID())
	tx.mu.Unlock()

	return r, nil
}

// ImportNodeWithID creates a node with a caller-specified snowflake ID within the transaction.
// The node ID is tracked for rollback. Delegates to Graph.ImportNodeWithID.
func (tx *GraphTx) ImportNodeWithID(ctx context.Context, id types.NodeID, labels []string, props map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	n, err := tx.g.importNodeWithIDInternal(ctx, id, labels, props)
	if err != nil {
		return nil, err
	}

	tx.g.publishEvent(EventNodeCreate, types.EntityID(n.ID()), nowInstant(), PriorityHigh)

	tx.mu.Lock()
	tx.createdNodes = append(tx.createdNodes, n.ID().SnowflakeID())
	tx.mu.Unlock()

	return n, nil
}

// ImportRelationshipWithID creates a relationship with a caller-specified snowflake ID within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.ImportRelationshipWithID.
func (tx *GraphTx) ImportRelationshipWithID(ctx context.Context, id types.RelID, typeName string, startNode, endNode *types.Node, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	r, err := tx.g.importRelWithIDInternal(ctx, id, typeName, startNode, endNode, props)
	if err != nil {
		return nil, err
	}

	tx.g.publishEvent(EventRelCreate, types.EntityID(r.ID()), nowInstant(), PriorityHigh)

	tx.mu.Lock()
	tx.createdRels = append(tx.createdRels, r.ID().SnowflakeID())
	tx.mu.Unlock()

	return r, nil
}

// UpdateNode applies property updates to a node within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.UpdateNode.
func (tx *GraphTx) UpdateNode(id types.NodeID, updates map[string]any) (*types.Node, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	if len(updates) == 0 {
		// Empty-update no-op. Read directly from the store rather than via
		// the exported wrapper: the tx already holds g.mu.Lock(), and any
		// future addition of g.mu.RLock() to GetNodeWithContext would
		// deadlock the tx. The tx convention is: never call exported
		// methods, always reach the store via *Internal helpers or
		// tx.g.store directly.
		n, err := tx.g.store.GetNode(id)
		if err == nil {
			tx.g.opNodeReads.Add(1)
		}
		return n, err
	}

	if err := tx.snapshotNode(id.SnowflakeID()); err != nil {
		return nil, err
	}

	n, err := tx.g.updateNodeInternal(context.Background(), id, updates)
	if err == nil {
		tx.g.publishEvent(EventNodeUpdate, types.EntityID(id), nowInstant(), PriorityNormal)
	}
	return n, err
}

// UpdateRelationship applies property updates to a relationship within the transaction.
// Snapshots the pre-mutation state on first mutation (for rollback).
// Delegates the actual update to Graph.UpdateRelationship.
func (tx *GraphTx) UpdateRelationship(id types.RelID, updates map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	if len(updates) == 0 {
		// Empty-update no-op. See UpdateNode for why this avoids the
		// exported wrapper.
		r, err := tx.g.store.GetRelationship(id)
		if err == nil {
			tx.g.opRelReads.Add(1)
		}
		return r, err
	}

	if err := tx.snapshotRel(id.SnowflakeID()); err != nil {
		return nil, err
	}

	r, err := tx.g.updateRelationshipInternal(context.Background(), id, updates)
	if err == nil {
		tx.g.publishEvent(EventRelUpdate, types.EntityID(id), nowInstant(), PriorityNormal)
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

// DeleteNode removes a node and all connected relationships within the transaction.
// Snapshots the node and all cascade-deleted relationships for rollback.
// Delegates the actual deletion to Graph.DeleteNode.
func (tx *GraphTx) DeleteNode(id types.NodeID) error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return ErrTxDone
	}
	tx.mu.Unlock()

	// Snapshot the node before deletion.
	node, err := tx.g.store.GetNode(id)
	if err != nil {
		return err
	}
	nodeCopy := node.DeepCopy()

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
	var relCopies []*types.Relationship
	allRels := make([]*types.Relationship, 0, len(outRels)+len(inRels))
	allRels = append(allRels, outRels...)
	allRels = append(allRels, inRels...)
	for _, r := range allRels {
		rid := r.ID().SnowflakeID()
		if seen[rid] {
			continue
		}
		seen[rid] = true
		relCopies = append(relCopies, r.DeepCopy())
	}

	// Perform the actual deletion (internal — tx already holds g.mu.Lock).
	if err := tx.g.deleteNodeInternal(context.Background(), id); err != nil {
		return err
	}

	tx.g.publishEvent(EventNodeDelete, types.EntityID(id), nowInstant(), PriorityCritical)

	tx.mu.Lock()
	tx.deletedNodes = append(tx.deletedNodes, deletedNodeSnapshot{
		node: nodeCopy,
		rels: relCopies,
	})
	tx.mu.Unlock()

	return nil
}

// DeleteRelationship removes a relationship within the transaction.
// Snapshots the relationship for rollback. Delegates to Graph.DeleteRelationship.
func (tx *GraphTx) DeleteRelationship(id types.RelID) error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return ErrTxDone
	}
	tx.mu.Unlock()

	// Snapshot the relationship before deletion.
	rel, err := tx.g.store.GetRelationship(id)
	if err != nil {
		return err
	}
	relCopy := rel.DeepCopy()

	// Perform the actual deletion (internal — tx already holds g.mu.Lock).
	if err := tx.g.deleteRelationshipInternal(context.Background(), id); err != nil {
		return err
	}

	tx.g.publishEvent(EventRelDelete, types.EntityID(id), nowInstant(), PriorityCritical)

	tx.mu.Lock()
	tx.deletedRels = append(tx.deletedRels, relCopy)
	tx.mu.Unlock()

	return nil
}

// AddNodeLabel adds a label to a node within the transaction.
// Snapshots the pre-mutation state on first mutation and records a label
// delta so Rollback can reverse the label index change. Idempotent: if the
// node already has the label, returns nil with no snapshot or delta recorded.
func (tx *GraphTx) AddNodeLabel(id types.NodeID, label string) error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return ErrTxDone
	}
	tx.mu.Unlock()

	// Look up the token (or pre-compute whether the label is already present)
	// to decide whether this call will actually mutate. We need the token for
	// the delta regardless, so resolve it up front under g.mu (already held by tx).
	tok, ok := tx.g.labels.Lookup(label)
	if ok {
		cur, err := tx.g.store.GetNode(id)
		if err == nil && cur.HasLabelTokenRaw(tok) {
			// Idempotent no-op: nothing to snapshot, no delta to record.
			return nil
		}
	}

	if err := tx.snapshotNode(id.SnowflakeID()); err != nil {
		return err
	}

	mutated, err := tx.g.addNodeLabelInternal(id, label)
	if err != nil {
		return err
	}
	if !mutated {
		return nil
	}

	// Re-lookup the token after the call — GetOrCreate may have just registered it.
	if tok == 0 {
		tok, _ = tx.g.labels.Lookup(label)
	}

	tx.mu.Lock()
	tx.labelDeltas = append(tx.labelDeltas, labelDelta{id: id.SnowflakeID(), tok: tok, added: true})
	tx.mu.Unlock()

	tx.g.publishEvent(EventNodeUpdate, types.EntityID(id), nowInstant(), PriorityNormal)
	return nil
}

// RemoveNodeLabel removes a label from a node within the transaction.
// Snapshots the pre-mutation state on first mutation and records a label
// delta so Rollback can reverse the label index change. Returns ErrLastLabel
// if the label is the only one on the node.
func (tx *GraphTx) RemoveNodeLabel(id types.NodeID, label string) error {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return ErrTxDone
	}
	tx.mu.Unlock()

	// Resolve token before the mutation so we can record the delta.
	tok, _ := tx.g.labels.Lookup(label)

	if err := tx.snapshotNode(id.SnowflakeID()); err != nil {
		return err
	}

	if err := tx.g.removeNodeLabelInternal(id, label); err != nil {
		return err
	}

	tx.mu.Lock()
	tx.labelDeltas = append(tx.labelDeltas, labelDelta{id: id.SnowflakeID(), tok: tok, added: false})
	tx.mu.Unlock()

	tx.g.publishEvent(EventNodeUpdate, types.EntityID(id), nowInstant(), PriorityNormal)
	return nil
}

// snapshotNode captures the pre-mutation state of a node on first mutation only.
// If the node was already snapshotted in this transaction, this is a no-op.
func (tx *GraphTx) snapshotNode(id snowflake.ID) error {
	tx.mu.Lock()
	if tx.snapshotSet[id] {
		tx.mu.Unlock()
		return nil
	}
	tx.mu.Unlock()

	node, err := tx.g.store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	prev := node.DeepCopy()

	tx.mu.Lock()
	// Double-check after re-acquiring lock.
	if !tx.snapshotSet[id] {
		tx.snapshotSet[id] = true
		tx.updatedNodes = append(tx.updatedNodes, nodeSnapshot{id: id, prev: prev})
	}
	tx.mu.Unlock()
	return nil
}

// snapshotRel captures the pre-mutation state of a relationship on first mutation only.
func (tx *GraphTx) snapshotRel(id snowflake.ID) error {
	tx.mu.Lock()
	if tx.snapshotSet[id] {
		tx.mu.Unlock()
		return nil
	}
	tx.mu.Unlock()

	rel, err := tx.g.store.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}
	prev := rel.DeepCopy()

	tx.mu.Lock()
	if !tx.snapshotSet[id] {
		tx.snapshotSet[id] = true
		tx.updatedRels = append(tx.updatedRels, relSnapshot{id: id, prev: prev})
	}
	tx.mu.Unlock()
	return nil
}

// Commit finalizes the transaction, making all mutations permanent.
// Releases the graph write lock, then publishes buffered events outside the lock
// so that event handlers can safely call Graph read methods.
// After Commit, all tx methods return ErrTxDone.
func (tx *GraphTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}
	tx.done = true

	// Capture event publisher and buffer before unlocking.
	ep := tx.g.events
	events := tx.pendingEvents
	tx.g.txEventBuffer = nil
	tx.pendingEvents = nil

	// Release the write lock before publishing — handlers can call Graph methods.
	tx.g.mu.Unlock()

	// Publish buffered events outside all locks.
	if ep != nil {
		for _, e := range events {
			ep.publish(e)
		}
	}
	return nil
}

// Rollback undoes all mutations in reverse order, then releases the graph write lock.
// Buffered events are discarded — subscribers never see rolled-back mutations.
//
// Rollback order (reverse of application):
//  1. Restore deleted relationships (standalone deletes)
//  2. Restore deleted nodes + their cascade-deleted relationships
//  3. Restore updated relationships to pre-mutation state
//  4. Restore updated nodes to pre-mutation state
//  5. Delete created relationships (reverse creation order)
//  6. Delete created nodes (reverse creation order, cascade)
//
// Known limitation: rolled-back updates/deletes may leave phantom version history
// entries. Entity state is correct; history may have extra entries.
//
// Best-effort: continues on error, returns the first error encountered.
// After Rollback, all tx methods return ErrTxDone.
func (tx *GraphTx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	defer tx.g.mu.Unlock() // deferred so a store panic cannot permanently hold the write lock

	// Discard buffered events — rolled-back mutations should never reach subscribers.
	tx.g.txEventBuffer = nil
	tx.pendingEvents = nil

	var firstErr error
	capture := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// 1. Restore standalone-deleted relationships (reverse order).
	for i := len(tx.deletedRels) - 1; i >= 0; i-- {
		capture(tx.g.store.PutRelationship(tx.deletedRels[i]))
	}

	// 2. Restore deleted nodes + their cascade-deleted relationships (reverse order).
	for i := len(tx.deletedNodes) - 1; i >= 0; i-- {
		snap := tx.deletedNodes[i]
		capture(tx.g.store.PutNode(snap.node))
		for _, r := range snap.rels {
			capture(tx.g.store.PutRelationship(r))
		}
	}

	// 3. Restore updated relationships to pre-mutation snapshot (reverse order).
	for i := len(tx.updatedRels) - 1; i >= 0; i-- {
		capture(tx.g.store.ReplaceRelationship(tx.updatedRels[i].prev))
	}

	// 4. Restore updated nodes to pre-mutation snapshot (reverse order).
	for i := len(tx.updatedNodes) - 1; i >= 0; i-- {
		capture(tx.g.store.ReplaceNode(tx.updatedNodes[i].prev))
	}

	// 4a. Reverse label index deltas (reverse order).
	// ReplaceNode restores the node's own label set but does not touch the
	// store-level label index; label-adding/removing transactions must fix it.
	// After ReplaceNode has restored the node state, the restored node already
	// reflects the pre-mutation labels, so we just need to patch the index.
	for i := len(tx.labelDeltas) - 1; i >= 0; i-- {
		d := tx.labelDeltas[i]
		restored, err := tx.g.store.GetNode(types.NodeID(d.id))
		if err != nil {
			capture(err)
			continue
		}
		if d.added {
			// Undo an added label: remove it from the label index.
			capture(tx.g.store.RemoveNodeLabelToken(types.NodeID(d.id), d.tok, restored))
		} else {
			// Undo a removed label: add it back to the label index.
			capture(tx.g.store.AddNodeLabelToken(types.NodeID(d.id), d.tok, restored))
		}
	}

	// 5. Delete created relationships in reverse creation order.
	for i := len(tx.createdRels) - 1; i >= 0; i-- {
		capture(tx.g.store.DeleteRelationship(types.RelID(tx.createdRels[i])))
	}

	// 6. Delete created nodes in reverse creation order (cascade).
	for i := len(tx.createdNodes) - 1; i >= 0; i-- {
		capture(tx.g.store.DeleteNodeCascade(types.NodeID(tx.createdNodes[i])))
	}

	return firstErr
}

// GetNode reads a node by ID within the transaction.
// Safe because the tx holds the write lock — no concurrent modifications possible.
func (tx *GraphTx) GetNode(id types.NodeID) (*types.Node, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	return tx.g.store.GetNode(id)
}

// AddRelationshipByID creates a relationship using endpoint snowflake IDs within the transaction.
// The relationship ID is tracked for rollback. Delegates to Graph.addRelationshipByIDInternal.
func (tx *GraphTx) AddRelationshipByID(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, ErrTxDone
	}
	tx.mu.Unlock()

	r, err := tx.g.addRelationshipByIDInternal(context.Background(), typeName, startID, endID, props)
	if err != nil {
		return nil, err
	}

	tx.g.publishEvent(EventRelCreate, types.EntityID(r.ID()), nowInstant(), PriorityHigh)

	tx.mu.Lock()
	tx.createdRels = append(tx.createdRels, r.ID().SnowflakeID())
	tx.mu.Unlock()

	return r, nil
}

// AddRelationshipByIDIfAbsent atomically checks for an existing relationship of the same
// type between the same endpoints and creates one only if absent. Returns (rel, created, err)
// where created=true if a new relationship was created.
// The relationship ID is tracked for rollback only if created.
func (tx *GraphTx) AddRelationshipByIDIfAbsent(typeName string, startID, endID types.NodeID, props map[string]any) (*types.Relationship, bool, error) {
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return nil, false, ErrTxDone
	}
	tx.mu.Unlock()

	r, created, err := tx.g.addRelationshipByIDIfAbsentInternal(context.Background(), typeName, startID, endID, props)
	if err != nil {
		return nil, false, err
	}

	if created {
		tx.g.publishEvent(EventRelCreate, types.EntityID(r.ID()), nowInstant(), PriorityHigh)

		tx.mu.Lock()
		tx.createdRels = append(tx.createdRels, r.ID().SnowflakeID())
		tx.mu.Unlock()
	}

	return r, created, nil
}

// CreatedNodeIDs returns the typed IDs of all nodes created in this transaction.
// Useful for inspecting transaction state in tests.
func (tx *GraphTx) CreatedNodeIDs() []types.NodeID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	cp := make([]types.NodeID, len(tx.createdNodes))
	for i, id := range tx.createdNodes {
		cp[i] = types.NodeID(id)
	}
	return cp
}

// CreatedRelIDs returns the typed IDs of all relationships created in this transaction.
func (tx *GraphTx) CreatedRelIDs() []types.RelID {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	cp := make([]types.RelID, len(tx.createdRels))
	for i, id := range tx.createdRels {
		cp[i] = types.RelID(id)
	}
	return cp
}
