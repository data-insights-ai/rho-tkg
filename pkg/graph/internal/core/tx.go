package core

import (
	"sync"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

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
// c.mu.Unlock). On Rollback, buffered events are discarded.
//
// All methods check the done flag and return storepkg.ErrTxDone after Commit/Rollback.
type GraphTx struct {
	g             *Core
	createdNodes  []snowflake.ID
	createdRels   []snowflake.ID
	updatedNodes  []nodeSnapshot
	updatedRels   []relSnapshot
	deletedNodes  []deletedNodeSnapshot
	deletedRels   []*types.Relationship
	labelDeltas   []labelDelta          // label index mutations — reversed on Rollback
	pendingEvents []eventspkg.Event     // buffered events — published on Commit, discarded on Rollback
	snapshotSet   map[snowflake.ID]bool // tracks already-snapshotted entities (first mutation only)
	mu            sync.Mutex            // protects done flag and snapshot tracking
	done          bool
}

// BeginTx starts a new mutation transaction. Returns ErrGraphClosed if
// the graph has already been closed.
//
// On success, BeginTx acquires the graph write lock, blocking concurrent
// standalone mutations, Batch, and Snapshot operations. The lock is
// released on Commit() or Rollback(). Events are buffered and published
// after Commit (or discarded on Rollback).
func (c *Core) BeginTx() (*GraphTx, error) {
	c.mu.Lock()
	if c.closed.Load() {
		c.mu.Unlock()
		return nil, ErrGraphClosed
	}
	tx := &GraphTx{g: c, snapshotSet: make(map[snowflake.ID]bool)}
	c.txEventBuffer = &tx.pendingEvents
	return tx, nil
}

// =============================================================================
// Snapshot helpers (caller holds tx.mu)
// =============================================================================

// snapshotNodeLocked captures the pre-mutation state of a node on first
// mutation only. If the node was already snapshotted in this transaction,
// this is a no-op.
//
// Caller must hold tx.mu (R4-F2: every public tx method holds tx.mu for
// its entire body so snapshot accesses do not race with Commit/Rollback).
func (tx *GraphTx) snapshotNodeLocked(id snowflake.ID) error {
	if tx.snapshotSet[id] {
		return nil
	}

	node, err := tx.g.store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	prev := node.DeepCopy()

	tx.snapshotSet[id] = true
	tx.updatedNodes = append(tx.updatedNodes, nodeSnapshot{id: id, prev: prev})
	return nil
}

// snapshotRelLocked captures the pre-mutation state of a relationship on
// first mutation only. Caller must hold tx.mu — see snapshotNodeLocked.
func (tx *GraphTx) snapshotRelLocked(id snowflake.ID) error {
	if tx.snapshotSet[id] {
		return nil
	}

	rel, err := tx.g.store.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}
	prev := rel.DeepCopy()

	tx.snapshotSet[id] = true
	tx.updatedRels = append(tx.updatedRels, relSnapshot{id: id, prev: prev})
	return nil
}

// =============================================================================
// Commit / Rollback
// =============================================================================

// Commit finalizes the transaction, making all mutations permanent.
// Releases the graph write lock, then publishes buffered events outside the lock
// so that event handlers can safely call Graph read methods.
// After Commit, all tx methods return storepkg.ErrTxDone.
func (tx *GraphTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return storepkg.ErrTxDone
	}
	tx.done = true

	// Capture event publisher and buffer before unlocking.
	ep := tx.g.events
	events := tx.pendingEvents
	tx.g.txEventBuffer = nil
	tx.pendingEvents = nil

	// Release the write lock before publishing — handlers can call Graph methods.
	tx.g.mu.Unlock()

	// Publish buffered events outside all locks. PublishBatch is
	// atomic on AsyncEventBus, so all tx events land in priority
	// order even if other goroutines are publishing concurrently.
	if ep != nil && len(events) > 0 {
		ep.PublishBatch(events...)
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
// After Rollback, all tx methods return storepkg.ErrTxDone.
func (tx *GraphTx) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.done {
		return storepkg.ErrTxDone
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

// =============================================================================
// Read-only / inspection
// =============================================================================

// GetNode reads a node by ID within the transaction.
// Safe because the tx holds the write lock — no concurrent modifications possible.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) GetNode(id types.NodeID) (*types.Node, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if tx.done {
		return nil, storepkg.ErrTxDone
	}

	return tx.g.store.GetNode(id)
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
