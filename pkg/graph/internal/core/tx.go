package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// nodeSnapshot captures pre-mutation state for rollback.
type nodeSnapshot struct {
	id              snowflake.ID
	prev            *types.Node   // DeepCopy before first mutation
	history         []*types.Node // DeepCopy fallback for stores without history trimming
	historyTrimFrom uint32        // First history version written by this tx for the entity
	useHistoryTrim  bool
}

// relSnapshot captures pre-mutation state for rollback.
type relSnapshot struct {
	id              snowflake.ID
	prev            *types.Relationship   // DeepCopy before first mutation
	history         []*types.Relationship // DeepCopy fallback for stores without history trimming
	historyTrimFrom uint32                // First history version written by this tx for the entity
	useHistoryTrim  bool
}

// deletedNodeSnapshot captures a deleted node and its cascade-deleted relationships.
type deletedNodeSnapshot struct {
	node            *types.Node
	nodeHistory     []*types.Node
	historyTrimFrom uint32
	useHistoryTrim  bool
	rels            []deletedRelSnapshot // cascade-deleted rels
}

type deletedRelSnapshot struct {
	rel             *types.Relationship
	history         []*types.Relationship
	historyTrimFrom uint32
	useHistoryTrim  bool
}

type opCounterSnapshot struct {
	nodeAdds, nodeReads, nodeUpdates, nodeDeletes int64
	relAdds, relReads, relUpdates, relDeletes     int64
}

type txSnapshotKind uint8

const (
	txSnapshotNode txSnapshotKind = iota
	txSnapshotRel
)

type txSnapshotKey struct {
	kind txSnapshotKind
	id   snowflake.ID
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
	g               *Core
	createdNodes    []snowflake.ID
	createdRels     []snowflake.ID
	updatedNodes    []nodeSnapshot
	updatedRels     []relSnapshot
	deletedNodes    []deletedNodeSnapshot
	deletedRels     []deletedRelSnapshot
	labelSnapshot   []string               // registry names at BeginTx, restored on Rollback
	relTypeSnapshot []string               // registry names at BeginTx, restored on Rollback
	opSnapshot      opCounterSnapshot      // graph operation counters at BeginTx, restored on successful Rollback
	pendingEvents   []eventspkg.Event      // buffered events — published on Commit, discarded on Rollback
	snapshotSet     map[txSnapshotKey]bool // tracks already-snapshotted entities (first mutation only)
	createdNodeSet  map[snowflake.ID]struct{}
	createdRelSet   map[snowflake.ID]struct{}
	deletedNodeSet  map[snowflake.ID]struct{}
	deletedRelSet   map[snowflake.ID]struct{}
	mu              sync.Mutex // protects done flag and snapshot tracking
	done            bool
}

// BeginTx starts a new mutation transaction. Returns ErrGraphClosed if
// the graph has already been closed.
//
// On success, BeginTx acquires the tx-serialization mutex (c.txMu), NOT
// c.mu.Lock. Other transactions and batches are serialized against this
// one, but standalone mutations and reads from other goroutines proceed
// concurrently — they only block on the per-entity lock manager when
// they happen to touch an entity this tx has already locked.
//
// Inside the tx, each mutation/read method takes a brief c.mu.RLock
// around its body so the *Internal/*Locked helpers see the same lock
// context the standalone path provides. This closes the v3.4/v4.0.x
// deadlock class: g.Nodes.ByLabel, g.Temporal.NodesAt, etc., called
// inside an open tx no longer hang waiting for c.mu.RLock against a
// writer that doesn't exist anymore.
//
// Events are buffered into c.txEventBuffer (still a global field —
// c.txMu serialization keeps it single-writer) and published on Commit
// (or discarded on Rollback). Standalone mutations use dispatchEvent
// with a per-call publisher and never touch c.txEventBuffer.
//
// Isolation semantics: serializable per touched entity. Entities the tx
// has mutated stay consistent for the tx's view (entity locks taken by
// the *Internal mutation paths). Reads inside the tx may observe
// changes to entities NOT yet touched by this tx that were committed
// by a concurrent standalone op — this is a relaxation of the v3.4
// "tx blocks all concurrent mutations" guarantee, and is the
// minor-bump price documented in CHANGELOG [4.1.0]. Code that relied
// on the old guarantee must take its own external lock.
func (c *Core) BeginTx() (*GraphTx, error) {
	c.txMu.Lock()
	if c.closed.Load() {
		c.txMu.Unlock()
		return nil, ErrGraphClosed
	}
	if c.readOnlyReplica {
		c.txMu.Unlock()
		return nil, ErrReadOnlyReplica
	}
	tx := &GraphTx{
		g:               c,
		labelSnapshot:   c.labels.ExportNames(),
		relTypeSnapshot: c.relTypes.ExportNames(),
		opSnapshot:      c.snapshotOpCounters(),
		snapshotSet:     make(map[txSnapshotKey]bool),
		createdNodeSet:  make(map[snowflake.ID]struct{}),
		createdRelSet:   make(map[snowflake.ID]struct{}),
		deletedNodeSet:  make(map[snowflake.ID]struct{}),
		deletedRelSet:   make(map[snowflake.ID]struct{}),
	}
	c.txEventBuffer = &tx.pendingEvents
	if c.txLogScope != nil {
		// Open the per-tx change-log buffer: every mutation's record is diverted
		// (under lockActiveCoreWrite's exclusive lock) into it and emitted on Commit
		// / dropped on Rollback, so a rolled-back tx ships nothing to the feed.
		if err := c.txLogScope.BeginLogScope(); err != nil {
			c.txEventBuffer = nil
			c.txMu.Unlock()
			return nil, err
		}
	}
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
	node, err := tx.g.getCurrentNode(types.NodeID(id))
	if err != nil {
		return err
	}
	return tx.snapshotCurrentNodeLocked(node)
}

func (tx *GraphTx) snapshotCurrentNodeLocked(node *types.Node) error {
	id := node.ID().SnowflakeID()
	key := txSnapshotKey{kind: txSnapshotNode, id: id}
	if tx.snapshotSet[key] {
		return nil
	}
	if _, createdInTx := tx.createdNodeSet[id]; createdInTx {
		// Created rows are removed by the created-node rollback log. An
		// update snapshot would only restore an intermediate in-transaction
		// shape before the row is deleted.
		return nil
	}
	if _, deletedInTx := tx.deletedNodeSet[id]; deletedInTx {
		// A delete followed by an import of the same ID is restored from the
		// delete snapshot. Do not let later replacement updates overwrite the
		// original row during rollback.
		return nil
	}

	prev := node.DeepCopy()
	if tx.g.historyTrim != nil {
		if found, err := tx.nodeHistoryVersionExists(types.NodeID(id), node.Version()); err != nil {
			return err
		} else if !found {
			tx.snapshotSet[key] = true
			tx.updatedNodes = append(tx.updatedNodes, nodeSnapshot{
				id:              id,
				prev:            prev,
				historyTrimFrom: node.Version(),
				useHistoryTrim:  true,
			})
			return nil
		}
	}

	history, err := tx.g.copyNodeHistory(types.NodeID(id))
	if err != nil {
		return err
	}

	tx.snapshotSet[key] = true
	tx.updatedNodes = append(tx.updatedNodes, nodeSnapshot{id: id, prev: prev, history: history})
	return nil
}

// snapshotRelLocked captures the pre-mutation state of a relationship on
// first mutation only. Caller must hold tx.mu — see snapshotNodeLocked.
func (tx *GraphTx) snapshotRelLocked(id snowflake.ID) error {
	rel, err := tx.g.getCurrentRelationship(types.RelID(id))
	if err != nil {
		return err
	}
	return tx.snapshotCurrentRelLocked(rel)
}

func (tx *GraphTx) snapshotCurrentRelLocked(rel *types.Relationship) error {
	id := rel.ID().SnowflakeID()
	key := txSnapshotKey{kind: txSnapshotRel, id: id}
	if tx.snapshotSet[key] {
		return nil
	}
	if _, createdInTx := tx.createdRelSet[id]; createdInTx {
		return nil
	}
	if _, deletedInTx := tx.deletedRelSet[id]; deletedInTx {
		return nil
	}

	prev := rel.DeepCopy()
	if tx.g.historyTrim != nil {
		if found, err := tx.relHistoryVersionExists(types.RelID(id), rel.Version()); err != nil {
			return err
		} else if !found {
			tx.snapshotSet[key] = true
			tx.updatedRels = append(tx.updatedRels, relSnapshot{
				id:              id,
				prev:            prev,
				historyTrimFrom: rel.Version(),
				useHistoryTrim:  true,
			})
			return nil
		}
	}

	history, err := tx.g.copyRelHistory(types.RelID(id))
	if err != nil {
		return err
	}

	tx.snapshotSet[key] = true
	tx.updatedRels = append(tx.updatedRels, relSnapshot{id: id, prev: prev, history: history})
	return nil
}

func (c *Core) copyNodeHistory(id types.NodeID) ([]*types.Node, error) {
	history, err := c.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	return copyNodeHistoryRows(history), nil
}

func copyNodeHistoryRows(history []*types.Node) []*types.Node {
	out := make([]*types.Node, len(history))
	for i, n := range history {
		out[i] = n.DeepCopy()
	}
	return out
}

func (c *Core) copyRelHistory(id types.RelID) ([]*types.Relationship, error) {
	history, err := c.getRelHistory(id)
	if err != nil {
		return nil, err
	}
	return copyRelHistoryRows(history), nil
}

func copyRelHistoryRows(history []*types.Relationship) []*types.Relationship {
	out := make([]*types.Relationship, len(history))
	for i, r := range history {
		out[i] = r.DeepCopy()
	}
	return out
}

// trackCreated* records only entities that did not exist at transaction start.
// Caller must hold tx.mu. Imported caller-specified IDs can reuse a row deleted
// earlier in the same transaction; those replacements must not be deleted again
// after rollback restores the original row.
func (tx *GraphTx) trackCreatedNodeLocked(id snowflake.ID) {
	if _, ok := tx.createdNodeSet[id]; ok {
		return
	}
	if _, ok := tx.deletedNodeSet[id]; ok {
		return
	}
	tx.createdNodeSet[id] = struct{}{}
	tx.createdNodes = append(tx.createdNodes, id)
}

func (tx *GraphTx) trackCreatedRelLocked(id snowflake.ID) {
	if _, ok := tx.createdRelSet[id]; ok {
		return
	}
	if _, ok := tx.deletedRelSet[id]; ok {
		return
	}
	tx.createdRelSet[id] = struct{}{}
	tx.createdRels = append(tx.createdRels, id)
}

func (tx *GraphTx) trackDeletedNodeLocked(id snowflake.ID) {
	tx.deletedNodeSet[id] = struct{}{}
}

func (tx *GraphTx) trackDeletedRelLocked(id snowflake.ID) {
	tx.deletedRelSet[id] = struct{}{}
}

func (tx *GraphTx) lockActive() error {
	if tx == nil || tx.g == nil {
		return ErrNilGraph
	}
	tx.mu.Lock()
	if tx.done {
		tx.mu.Unlock()
		return storepkg.ErrTxDone
	}
	return nil
}

// lockActiveCore takes tx.mu and c.mu.RLock, checks tx is active and graph is
// open, and returns. The caller must defer unlockActiveCore. Returns:
//   - ErrNilGraph if tx is nil or detached
//   - ErrTxDone if Commit/Rollback already ran
//   - ErrGraphClosed if the graph has been closed since BeginTx
//
// This is the v4.1.0 path-B pattern for every tx mutation and read mirror:
// hold c.mu.RLock briefly so the *Internal/*Locked helpers run with the
// same lock context the standalone callers provide via runUnderRLock /
// readUnderRLock, but DON'T hold c.mu.Lock for the tx lifetime (that's
// what deadlocked g.Nodes.ByLabel inside a tx in v4.0.x).
func (tx *GraphTx) lockActiveCore() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	tx.g.mu.RLock()
	if tx.g.closed.Load() {
		tx.g.mu.RUnlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	return nil
}

// unlockActiveCore releases c.mu.RLock and tx.mu in the reverse order of
// lockActiveCore. Always call via defer.
func (tx *GraphTx) unlockActiveCore() {
	tx.g.mu.RUnlock()
	tx.mu.Unlock()
}

// lockActiveCoreWrite is the MUTATION variant of lockActiveCore. When a per-tx
// change-log scope is active it takes c.mu.Lock (EXCLUSIVE) instead of RLock and
// turns on record diversion (SetLogDivert) — so for the duration of this single
// mutation no concurrent standalone mutation (which holds only c.mu.RLock) can run
// and have its own change-log record misrouted into this tx's buffer. The lock is
// per-mutation (acquired+released around one mutation method), NOT held for the tx
// lifetime, so in-tx reads (which take their own brief lock) never deadlock
// against it (lesson 31). When there is no scope it is exactly lockActiveCore.
func (tx *GraphTx) lockActiveCoreWrite() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	if tx.g.txLogScope == nil {
		tx.g.mu.RLock()
		if tx.g.closed.Load() {
			tx.g.mu.RUnlock()
			tx.mu.Unlock()
			return ErrGraphClosed
		}
		return nil
	}
	tx.g.mu.Lock()
	if tx.g.closed.Load() {
		tx.g.mu.Unlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	tx.g.txLogScope.SetLogDivert(true)
	return nil
}

// lockActiveCoreWriteContext is lockActiveCoreWrite with context cancellation.
func (tx *GraphTx) lockActiveCoreWriteContext(ctx context.Context) error {
	if err := tx.lockActiveContext(ctx); err != nil {
		return err
	}
	if tx.g.txLogScope == nil {
		tx.g.mu.RLock()
		if tx.g.closed.Load() {
			tx.g.mu.RUnlock()
			tx.mu.Unlock()
			return ErrGraphClosed
		}
		return nil
	}
	tx.g.mu.Lock()
	if tx.g.closed.Load() {
		tx.g.mu.Unlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	tx.g.txLogScope.SetLogDivert(true)
	return nil
}

// unlockActiveCoreWrite reverses lockActiveCoreWrite (turns off diversion and
// releases the exclusive lock when a scope is active; otherwise releases RLock).
func (tx *GraphTx) unlockActiveCoreWrite() {
	if tx.g.txLogScope == nil {
		tx.g.mu.RUnlock()
		tx.mu.Unlock()
		return
	}
	tx.g.txLogScope.SetLogDivert(false)
	tx.g.mu.Unlock()
	tx.mu.Unlock()
}

// lockActiveCoreContext is the ctx-honouring variant of lockActiveCore.
// Same release semantics — defer unlockActiveCore.
func (tx *GraphTx) lockActiveCoreContext(ctx context.Context) error {
	if err := tx.lockActiveContext(ctx); err != nil {
		return err
	}
	tx.g.mu.RLock()
	if tx.g.closed.Load() {
		tx.g.mu.RUnlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	return nil
}

// lockActiveContext is lockActive with a pre-canceled context fast path.
// If the transaction mutex is immediately available, ErrTxDone still wins so
// the GraphTx lifecycle contract remains stable after Commit/Rollback. If
// another goroutine is actively using the transaction, a canceled context does
// not wait behind tx.mu.
func (tx *GraphTx) lockActiveContext(ctx context.Context) error {
	if tx == nil || tx.g == nil {
		return ErrNilGraph
	}
	if err := checkCtx(ctx); err == nil {
		return tx.lockActive()
	} else if tx.mu.TryLock() {
		defer tx.mu.Unlock()
		if tx.done {
			return storepkg.ErrTxDone
		}
		return err
	} else {
		return err
	}
}

// =============================================================================
// Commit / Rollback
// =============================================================================

// Commit finalizes the transaction, making all mutations permanent.
// Persists registries (briefly under c.mu.Lock for safe pointer mutation),
// then releases c.txMu and publishes buffered events outside all locks.
// After Commit, all tx methods return storepkg.ErrTxDone.
func (tx *GraphTx) Commit() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	// A transaction can contain create/import calls that wrote rows but
	// returned a trailing registry checkpoint error. Commit is the final
	// durability boundary, so retry the registry checkpoint before making
	// the transaction irreversible. Take c.mu.Lock briefly because
	// persistRegistries may serialize via the store and we want to fence
	// against concurrent registry mutations from standalone ops.
	tx.g.mu.Lock()
	if err := tx.g.persistRegistries(); err != nil {
		tx.g.mu.Unlock()
		return err
	}
	// Emit the tx's buffered change-log records: mint their LSNs at commit time
	// (so this committing tx orders after everything committed during its body and
	// a rolled-back tx would have burned none) and co-commit them with the tx's
	// pending data. AFTER persistRegistries so any label/rel-type token a record
	// references is already durable. On error the tx is NOT marked done (its data
	// is in pending but unlogged — not yet durable-as-committed); the caller can
	// retry. The scope's own records remain buffered for the retry.
	if tx.g.txLogScope != nil {
		if err := tx.g.txLogScope.CommitLogScope(); err != nil {
			tx.g.mu.Unlock()
			return err
		}
	}
	tx.done = true

	// Capture event publisher and buffer before unlocking.
	ep := tx.g.events
	events := tx.pendingEvents
	tx.g.txEventBuffer = nil
	tx.pendingEvents = nil

	tx.g.mu.Unlock()
	tx.g.txMu.Unlock()

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
// Rollback order:
//  1. Restore deleted node rows and their pre-delete history
//  2. Restore relationships cascade-deleted by node deletes and their history
//  3. Restore standalone-deleted relationships and their pre-delete history
//  4. Restore updated relationships to pre-mutation state and history
//  5. Restore updated nodes to pre-mutation state and history
//  6. Delete created relationships and truncate their history (reverse creation order)
//  7. Delete created nodes and truncate their history (reverse creation order, cascade)
//
// Best-effort: continues on error, returns the first error encountered.
// After Rollback, all tx methods return storepkg.ErrTxDone.
func (tx *GraphTx) Rollback() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	defer tx.mu.Unlock()
	tx.done = true
	// Rollback applies store mutations and replaces the registry pointers
	// (via restoreRegistries). Take c.mu.Lock for the duration so concurrent
	// standalone readers don't observe torn registry pointers and so
	// concurrent writers can't race the restore. Deferred so a store panic
	// during a restore step releases both c.mu and c.txMu cleanly.
	tx.g.mu.Lock()
	defer tx.g.txMu.Unlock()
	defer tx.g.mu.Unlock()

	// Discard buffered events — rolled-back mutations should never reach subscribers.
	tx.g.txEventBuffer = nil
	tx.pendingEvents = nil

	// Keep change-log diversion ON across the reverse mutations below so their
	// records (DeleteNodeCascade, ReplaceNode, PutNodeVersion, Truncate*) ALSO land
	// in the scope buffer, then drop the whole buffer (forward + reverse) with
	// DiscardLogScope before unlocking — a rolled-back tx emits NOTHING. Rollback
	// holds c.mu.Lock, so no concurrent standalone mutation can run while diverted.
	if tx.g.txLogScope != nil {
		tx.g.txLogScope.SetLogDivert(true)
	}

	var firstErr error
	capture := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// 1. Restore deleted node rows first. Standalone-deleted relationships can
	// reference an endpoint that was deleted later in the same transaction, so
	// relationship restores must wait until all deleted endpoints are live again.
	for i := len(tx.deletedNodes) - 1; i >= 0; i-- {
		snap := tx.deletedNodes[i]
		capture(tx.restoreDeletedNodeRow(snap.node))
		capture(tx.restoreDeletedNodeHistory(snap))
	}

	// 2. Restore relationships that were cascade-deleted by node deletes.
	for i := len(tx.deletedNodes) - 1; i >= 0; i-- {
		snap := tx.deletedNodes[i]
		for _, r := range snap.rels {
			capture(tx.restoreDeletedRelRow(r.rel))
			capture(tx.restoreDeletedRelHistory(r))
		}
	}

	// 3. Restore standalone-deleted relationships (reverse order).
	for i := len(tx.deletedRels) - 1; i >= 0; i-- {
		snap := tx.deletedRels[i]
		capture(tx.restoreDeletedRelRow(snap.rel))
		capture(tx.restoreDeletedRelHistory(snap))
	}

	// 4. Restore updated relationships to pre-mutation snapshot (reverse order).
	for i := len(tx.updatedRels) - 1; i >= 0; i-- {
		snap := tx.updatedRels[i]
		capture(tx.g.store.ReplaceRelationship(snap.prev))
		capture(tx.restoreRelSnapshotHistory(types.RelID(snap.id), snap))
	}

	// 5. Restore updated nodes to pre-mutation snapshot (reverse order).
	for i := len(tx.updatedNodes) - 1; i >= 0; i-- {
		snap := tx.updatedNodes[i]
		capture(tx.restoreUpdatedNode(snap))
		capture(tx.restoreNodeSnapshotHistory(types.NodeID(snap.id), snap))
	}

	// 6. Delete created relationships in reverse creation order.
	for i := len(tx.createdRels) - 1; i >= 0; i-- {
		rid := types.RelID(tx.createdRels[i])
		capture(tx.g.store.DeleteRelationship(rid))
		capture(tx.g.store.TruncateRelHistory(rid, 0))
	}

	// 7. Delete created nodes in reverse creation order (cascade).
	for i := len(tx.createdNodes) - 1; i >= 0; i-- {
		nid := types.NodeID(tx.createdNodes[i])
		capture(tx.g.store.DeleteNodeCascade(nid))
		capture(tx.g.store.TruncateNodeHistory(nid, 0))
	}

	capture(tx.restoreRegistries())
	if firstErr == nil {
		tx.g.restoreOpCounters(tx.opSnapshot)
	}

	// Drop the buffered forward + reverse records (also clears diversion). No LSN
	// was burned, so the feed has no gap.
	if tx.g.txLogScope != nil {
		capture(tx.g.txLogScope.DiscardLogScope())
	}

	return firstErr
}

func (tx *GraphTx) restoreDeletedNodeRow(n *types.Node) error {
	current, err := tx.g.getCurrentNode(n.ID())
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		return tx.g.store.PutNode(n)
	} else if err != nil {
		return err
	}
	if !sameNodeLabelTokens(current, n) {
		if err := tx.restoreNodeLabels(n.ID(), current, n); err != nil {
			return err
		}
	}
	return tx.g.store.ReplaceNode(n)
}

func (tx *GraphTx) restoreDeletedRelRow(r *types.Relationship) error {
	current, err := tx.g.getCurrentRelationship(r.ID())
	if errors.Is(err, storepkg.ErrRelNotFound) {
		return tx.g.store.PutRelationship(r)
	} else if err != nil {
		return err
	}
	if !sameRelationshipIndexFields(current, r) {
		if err := tx.g.store.DeleteRelationship(r.ID()); err != nil {
			return err
		}
		return tx.g.store.PutRelationship(r)
	}
	return tx.g.store.ReplaceRelationship(r)
}

func sameRelationshipIndexFields(a, b *types.Relationship) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.TypeToken().Value() == b.TypeToken().Value() &&
		a.StartNodeID() == b.StartNodeID() &&
		a.EndNodeID() == b.EndNodeID()
}

func (c *Core) snapshotOpCounters() opCounterSnapshot {
	return opCounterSnapshot{
		nodeAdds:    c.opNodeAdds.Load(),
		nodeReads:   c.opNodeReads.Load(),
		nodeUpdates: c.opNodeUpdates.Load(),
		nodeDeletes: c.opNodeDeletes.Load(),
		relAdds:     c.opRelAdds.Load(),
		relReads:    c.opRelReads.Load(),
		relUpdates:  c.opRelUpdates.Load(),
		relDeletes:  c.opRelDeletes.Load(),
	}
}

func (c *Core) restoreOpCounters(s opCounterSnapshot) {
	c.opNodeAdds.Store(s.nodeAdds)
	c.opNodeReads.Store(s.nodeReads)
	c.opNodeUpdates.Store(s.nodeUpdates)
	c.opNodeDeletes.Store(s.nodeDeletes)
	c.opRelAdds.Store(s.relAdds)
	c.opRelReads.Store(s.relReads)
	c.opRelUpdates.Store(s.relUpdates)
	c.opRelDeletes.Store(s.relDeletes)
}

func (tx *GraphTx) restoreUpdatedNode(snap nodeSnapshot) error {
	id := types.NodeID(snap.id)
	current, err := tx.g.getCurrentNode(id)
	if err != nil {
		return err
	}

	if !sameNodeLabelTokens(current, snap.prev) {
		if err := tx.restoreNodeLabels(id, current, snap.prev); err != nil {
			return err
		}
	}
	return tx.g.store.ReplaceNode(snap.prev)
}

func (tx *GraphTx) restoreNodeLabels(id types.NodeID, current, target *types.Node) error {
	working := current.DeepCopy()
	targetLabels := nodeLabelTokenValues(target)
	if len(targetLabels) == 0 {
		return fmt.Errorf("%w: transaction rollback target has no labels for node %d", storepkg.ErrInvalidStoreMutation, id)
	}

	for !sameNodeLabelTokens(working, target) {
		labels := nodeLabelTokenValues(working)
		prefix := commonLabelPrefix(labels, targetLabels)

		if prefix == 0 && labels[0] != targetLabels[0] {
			targetPrimary := targetLabels[0]
			if !working.HasLabelTokenRaw(targetPrimary) {
				next := working.DeepCopy()
				if !next.AddLabelTokenRaw(targetPrimary) {
					return fmt.Errorf("%w: transaction rollback could not add label token %d for node %d", storepkg.ErrInvalidStoreMutation, targetPrimary, id)
				}
				if err := tx.g.store.AddNodeLabelToken(id, targetPrimary, next); err != nil {
					return err
				}
				working = next
				continue
			}

			targetPrimaryIndex := indexLabelToken(labels, targetPrimary)
			if targetPrimaryIndex < 0 {
				return fmt.Errorf("%w: transaction rollback target primary label token %d is unreachable for node %d", storepkg.ErrInvalidStoreMutation, targetPrimary, id)
			}
			removeIndex := 0
			if targetPrimaryIndex > 1 {
				removeIndex = 1
			}
			tok := labels[removeIndex]
			next := working.DeepCopy()
			if !next.RemoveLabelTokenRaw(tok) {
				return fmt.Errorf("%w: transaction rollback could not remove label token %d for node %d", storepkg.ErrInvalidStoreMutation, tok, id)
			}
			if err := tx.g.store.RemoveNodeLabelToken(id, tok, next); err != nil {
				return err
			}
			working = next
			continue
		}

		if prefix < len(targetLabels) && !working.HasLabelTokenRaw(targetLabels[prefix]) {
			next := working.DeepCopy()
			if !next.AddLabelTokenRaw(targetLabels[prefix]) {
				return fmt.Errorf("%w: transaction rollback could not add label token %d for node %d", storepkg.ErrInvalidStoreMutation, targetLabels[prefix], id)
			}
			if err := tx.g.store.AddNodeLabelToken(id, targetLabels[prefix], next); err != nil {
				return err
			}
			working = next
			continue
		}

		if prefix >= len(labels) {
			return fmt.Errorf("%w: transaction rollback label restore made no progress for node %d", storepkg.ErrInvalidStoreMutation, id)
		}
		if len(labels) == 1 {
			return fmt.Errorf("%w: transaction rollback would remove the last label for node %d", storepkg.ErrInvalidStoreMutation, id)
		}

		tok := labels[prefix]
		next := working.DeepCopy()
		if !next.RemoveLabelTokenRaw(tok) {
			return fmt.Errorf("%w: transaction rollback could not remove label token %d for node %d", storepkg.ErrInvalidStoreMutation, tok, id)
		}
		if err := tx.g.store.RemoveNodeLabelToken(id, tok, next); err != nil {
			return err
		}
		working = next
	}
	return nil
}

func sameNodeLabelTokens(a, b *types.Node) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.LabelTokenCount() != b.LabelTokenCount() {
		return false
	}
	for i := 0; i < a.LabelTokenCount(); i++ {
		if a.LabelTokenRawAt(i) != b.LabelTokenRawAt(i) {
			return false
		}
	}
	return true
}

func nodeLabelTokenValues(n *types.Node) []uint16 {
	out := make([]uint16, n.LabelTokenCount())
	for i := range out {
		out[i] = n.LabelTokenRawAt(i)
	}
	return out
}

func indexLabelToken(labels []uint16, tok uint16) int {
	for i, label := range labels {
		if label == tok {
			return i
		}
	}
	return -1
}

func commonLabelPrefix(a, b []uint16) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func (tx *GraphTx) restoreNodeHistory(id types.NodeID, history []*types.Node) error {
	if err := tx.g.store.TruncateNodeHistory(id, 0); err != nil {
		return err
	}
	for _, n := range history {
		if err := tx.g.store.PutNodeVersion(id, n.Version(), n); err != nil {
			return err
		}
	}
	return nil
}

func (tx *GraphTx) restoreRelHistory(id types.RelID, history []*types.Relationship) error {
	if err := tx.g.store.TruncateRelHistory(id, 0); err != nil {
		return err
	}
	for _, r := range history {
		if err := tx.g.store.PutRelVersion(id, r.Version(), r); err != nil {
			return err
		}
	}
	return nil
}

func (tx *GraphTx) restoreDeletedNodeHistory(snap deletedNodeSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.historyTrim.TrimNodeHistoryFrom(snap.node.ID(), snap.historyTrimFrom)
	}
	return tx.restoreNodeHistory(snap.node.ID(), snap.nodeHistory)
}

func (tx *GraphTx) restoreDeletedRelHistory(snap deletedRelSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.historyTrim.TrimRelHistoryFrom(snap.rel.ID(), snap.historyTrimFrom)
	}
	return tx.restoreRelHistory(snap.rel.ID(), snap.history)
}

func (tx *GraphTx) restoreNodeSnapshotHistory(id types.NodeID, snap nodeSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.historyTrim.TrimNodeHistoryFrom(id, snap.historyTrimFrom)
	}
	return tx.restoreNodeHistory(id, snap.history)
}

func (tx *GraphTx) restoreRelSnapshotHistory(id types.RelID, snap relSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.historyTrim.TrimRelHistoryFrom(id, snap.historyTrimFrom)
	}
	return tx.restoreRelHistory(id, snap.history)
}

func (tx *GraphTx) restoreRegistries() error {
	// Restore the exact pre-tx registry, de-allocating any label/rel-type token
	// this tx allocated. This is safe even with the change-log enabled BECAUSE the
	// per-tx change-log scope (store.TxChangeLogScope) discarded the tx's buffered
	// records on rollback — the rolled-back tx emitted NOTHING, so no durable feed
	// record references the de-allocated token. (Before the per-tx buffer existed,
	// the tx body's puts emitted records in-backend immediately, so de-allocating
	// here poisoned the feed and stalled replicas — lesson 55; the append-only
	// STOPGAP that guarded this is now superseded by the buffer for the tx path.)
	labels := registrypkg.NewLabelRegistry()
	if err := labels.ImportNames(tx.labelSnapshot); err != nil {
		return err
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := relTypes.ImportNames(tx.relTypeSnapshot); err != nil {
		return err
	}
	// The pointer swap + persist must hold registryMu IN ADDITION to the
	// c.mu.Lock Rollback already holds: readers under c.mu.RLock are excluded
	// by c.mu, but Close's final persistRegistries and the ingest sessions'
	// declare-on-prepare path read the registry pointers under registryMu
	// WITHOUT c.mu — the swap must synchronize with both classes (the
	// Close-vs-Rollback race was caught by the lifecycle storm under -race).
	tx.g.registryMu.Lock()
	defer tx.g.registryMu.Unlock()
	tx.g.labels = labels
	tx.g.relTypes = relTypes
	tx.g.clearRelTypeCache()
	setTieredLabelRegistryIfSupported(tx.g.store, tx.g.labels)
	return tx.g.persistRegistries()
}

// =============================================================================
// Read-only / inspection
// =============================================================================

// GetNode reads a node by ID within the transaction.
// Safe because the tx holds the write lock — no concurrent modifications possible.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) GetNode(id types.NodeID) (*types.Node, error) {
	if err := tx.lockActive(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()

	if err := storepkg.ValidateNodeID(id); err != nil {
		return nil, err
	}
	n, err := tx.g.getCurrentNode(id)
	if err == nil {
		tx.g.opNodeReads.Add(1)
	}
	return n, err
}

// GetRelationship reads a relationship by ID within the transaction.
// Safe because the tx holds the write lock — no concurrent modifications possible.
// Holds tx.mu for the whole call — see AddNode (R4-F2).
func (tx *GraphTx) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if err := tx.lockActive(); err != nil {
		return nil, err
	}
	defer tx.mu.Unlock()

	if err := storepkg.ValidateRelID(id); err != nil {
		return nil, err
	}
	r, err := tx.g.getCurrentRelationship(id)
	if err == nil {
		tx.g.opRelReads.Add(1)
	}
	return r, err
}

// CreatedNodeIDs returns the typed IDs of all nodes created in this transaction.
// Useful for inspecting transaction state in tests.
func (tx *GraphTx) CreatedNodeIDs() []types.NodeID {
	if tx == nil || tx.g == nil {
		return nil
	}
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
	if tx == nil || tx.g == nil {
		return nil
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()
	cp := make([]types.RelID, len(tx.createdRels))
	for i, id := range tx.createdRels {
		cp[i] = types.RelID(id)
	}
	return cp
}
