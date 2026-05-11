package core

import (
	"errors"
	"fmt"
	"sync"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	tiered "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	node        *types.Node
	nodeHistory []*types.Node
	rels        []deletedRelSnapshot // cascade-deleted rels
}

type deletedRelSnapshot struct {
	rel     *types.Relationship
	history []*types.Relationship
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
	key := txSnapshotKey{kind: txSnapshotNode, id: id}
	if tx.snapshotSet[key] {
		return nil
	}

	node, err := tx.g.store.GetNode(types.NodeID(id))
	if err != nil {
		return err
	}
	prev := node.DeepCopy()
	if tx.g.historyTrim != nil {
		tx.snapshotSet[key] = true
		tx.updatedNodes = append(tx.updatedNodes, nodeSnapshot{
			id:              id,
			prev:            prev,
			historyTrimFrom: node.Version(),
			useHistoryTrim:  true,
		})
		return nil
	}

	history, err := copyNodeHistory(tx.g.store.GetNodeHistory(types.NodeID(id)))
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
	key := txSnapshotKey{kind: txSnapshotRel, id: id}
	if tx.snapshotSet[key] {
		return nil
	}

	rel, err := tx.g.store.GetRelationship(types.RelID(id))
	if err != nil {
		return err
	}
	prev := rel.DeepCopy()
	if tx.g.historyTrim != nil {
		tx.snapshotSet[key] = true
		tx.updatedRels = append(tx.updatedRels, relSnapshot{
			id:              id,
			prev:            prev,
			historyTrimFrom: rel.Version(),
			useHistoryTrim:  true,
		})
		return nil
	}

	history, err := copyRelHistory(tx.g.store.GetRelHistory(types.RelID(id)))
	if err != nil {
		return err
	}

	tx.snapshotSet[key] = true
	tx.updatedRels = append(tx.updatedRels, relSnapshot{id: id, prev: prev, history: history})
	return nil
}

func copyNodeHistory(history []*types.Node, err error) ([]*types.Node, error) {
	if err != nil {
		return nil, err
	}
	out := make([]*types.Node, len(history))
	for i, n := range history {
		out[i] = n.DeepCopy()
	}
	return out, nil
}

func copyRelHistory(history []*types.Relationship, err error) ([]*types.Relationship, error) {
	if err != nil {
		return nil, err
	}
	out := make([]*types.Relationship, len(history))
	for i, r := range history {
		out[i] = r.DeepCopy()
	}
	return out, nil
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

	// A transaction can contain create/import calls that wrote rows but
	// returned a trailing registry checkpoint error. Commit is the final
	// durability boundary, so retry the registry checkpoint before making
	// the transaction irreversible. On failure the tx stays open; callers can
	// retry Commit or Rollback.
	if err := tx.g.persistRegistries(); err != nil {
		return err
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

	// 1. Restore deleted node rows first. Standalone-deleted relationships can
	// reference an endpoint that was deleted later in the same transaction, so
	// relationship restores must wait until all deleted endpoints are live again.
	for i := len(tx.deletedNodes) - 1; i >= 0; i-- {
		snap := tx.deletedNodes[i]
		capture(tx.restoreDeletedNodeRow(snap.node))
		capture(tx.restoreNodeHistory(snap.node.ID(), snap.nodeHistory))
	}

	// 2. Restore relationships that were cascade-deleted by node deletes.
	for i := len(tx.deletedNodes) - 1; i >= 0; i-- {
		snap := tx.deletedNodes[i]
		for _, r := range snap.rels {
			capture(tx.restoreDeletedRelRow(r.rel))
			capture(tx.restoreRelHistory(r.rel.ID(), r.history))
		}
	}

	// 3. Restore standalone-deleted relationships (reverse order).
	for i := len(tx.deletedRels) - 1; i >= 0; i-- {
		snap := tx.deletedRels[i]
		capture(tx.restoreDeletedRelRow(snap.rel))
		capture(tx.restoreRelHistory(snap.rel.ID(), snap.history))
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

	return firstErr
}

func (tx *GraphTx) restoreDeletedNodeRow(n *types.Node) error {
	current, err := tx.g.store.GetNode(n.ID())
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
	current, err := tx.g.store.GetRelationship(r.ID())
	if errors.Is(err, storepkg.ErrRelNotFound) {
		return tx.g.store.PutRelationship(r)
	} else if err != nil {
		return err
	}
	if current.TypeToken().Value() != r.TypeToken().Value() {
		if err := tx.g.store.DeleteRelationship(r.ID()); err != nil {
			return err
		}
		return tx.g.store.PutRelationship(r)
	}
	return tx.g.store.ReplaceRelationship(r)
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
	current, err := tx.g.store.GetNode(id)
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
	aLabels := a.AllLabelTokens()
	bLabels := b.AllLabelTokens()
	if len(aLabels) != len(bLabels) {
		return false
	}
	for i := range aLabels {
		if aLabels[i] != bLabels[i] {
			return false
		}
	}
	return true
}

func nodeLabelTokenValues(n *types.Node) []uint16 {
	labels := n.AllLabelTokens()
	out := make([]uint16, len(labels))
	for i, label := range labels {
		out[i] = label.Value()
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
	labels := registrypkg.NewLabelRegistry()
	if err := labels.ImportNames(tx.labelSnapshot); err != nil {
		return err
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := relTypes.ImportNames(tx.relTypeSnapshot); err != nil {
		return err
	}
	tx.g.labels = labels
	tx.g.relTypes = relTypes
	tx.g.clearRelTypeCache()
	if ts, ok := tx.g.store.(*tiered.Store); ok {
		ts.SetLabelRegistry(tx.g.labels)
	}
	return tx.g.persistRegistries()
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
