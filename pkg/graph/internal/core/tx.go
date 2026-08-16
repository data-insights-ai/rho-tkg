package core

import (
	"context"
	"errors"
	"fmt"
	"sync"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"

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
// Path B (v4.1.0+): it holds c.txMu for its entire lifetime — serializing
// tx-vs-tx and tx-vs-batch — but NOT c.mu (the graph write lock) for its
// whole duration. Each method takes c.mu briefly around its own body
// (lockActiveCore()/lockActiveCoreWrite()), so concurrent standalone
// mutations and reads from OTHER goroutines proceed in parallel with an open
// tx; only entity-level conflicts block, via the existing entity-lock
// manager. All mutations (create, update, delete) are tracked so Rollback
// can restore pre-transaction state.
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
	// writePathUsed is set by the shared transaction mutation-lock seam after
	// it has acquired the graph lock and verified the graph is open. Commit
	// uses it to preserve the final registry checkpoint for every mutation-
	// capable transaction while allowing a clean read-only transaction to
	// finalize without touching durable registry metadata.
	writePathUsed bool
	// registrySizesAtBegin is the size of each registry (labels, rel types, property keys)
	// when the transaction opened. Commit compares against it to decide whether there is any
	// registry state to make durable.
	//
	// WHY A SIZE AND NOT A FLAG ON EVERY ALLOCATION SITE: a token can be interned through many
	// doors -- node add, relationship add, import, and label resolution reached from a query --
	// and a flag on each is a list that drifts. The registries themselves are the authority on
	// whether they changed, and asking them costs three integer reads.
	//
	// SHARED REGISTRIES MAKE THIS CONSERVATIVE IN THE SAFE DIRECTION. Another transaction
	// interning a token concurrently makes this one see growth it did not cause, so it
	// checkpoints when it need not have. The opposite error -- missing a token this
	// transaction interned -- cannot happen, and only that direction can lose data.
	registrySizesAtBegin [3]int
	committedLSN         uint64 // max change-log LSN this tx's commit assigned (0 = none / log off)
	// scopeToken (BACKLOG 11f) — set by BeginTx via c.scopedChangeLog.BeginScopedLog()
	// when the store supports the full token-routed mechanism (see
	// storepkg.ScopedTxCapability). 0 when the mechanism isn't in use (store
	// doesn't support it) OR the change-log is disabled (BeginScopedLog itself
	// returns 0 in that case) — either way every *ScopedAware door call and
	// doorCtx() naturally fall through to the plain unscoped path. Read via
	// doorCtx() (forward doors, ctx-threaded) or directly (Rollback's
	// *ScopedAwareToken helpers, which have no ctx to thread it through).
	scopeToken uint64
}

// doorCtx returns the context a GraphTx mutation method should pass to its
// underlying *Internal helper: withScopeToken(ctx, tx.scopeToken) when the
// tx is using the BACKLOG 11f token-routed mechanism (tx.g.scopedChangeLog
// != nil), else a plain context.Background() — identical to every call site
// before this mechanism existed. Every GraphTx mutation method that
// previously passed a bare context.Background() to its *Internal call now
// passes tx.doorCtx() instead; this is the ONE place that decision is made,
// so no call site can drift out of sync with BeginTx's capability check.
func (tx *GraphTx) doorCtx() context.Context {
	if tx.g.scopedChangeLog != nil {
		return withScopeToken(context.Background(), tx.scopeToken)
	}
	return context.Background()
}

// doorCtxFrom is doorCtx's sibling for GraphTx methods that already receive
// a real ctx from their caller (e.g. ImportNodeWithID's cancellation
// context) rather than constructing their own — it carries the scope token
// on TOP of the caller's ctx (preserving its cancellation/deadline/values)
// instead of starting fresh from context.Background().
func (tx *GraphTx) doorCtxFrom(ctx context.Context) context.Context {
	if tx.g.scopedChangeLog != nil {
		return withScopeToken(ctx, tx.scopeToken)
	}
	return ctx
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
	if c.scopedChangeLog != nil {
		// BACKLOG 11f: open this tx's OWN independently-addressed scope. Every
		// mutation's record is routed into THIS token's buffer (via
		// doorCtx()/scopeTokenFrom and the *ScopedAware store wrappers), never
		// a single shared divert flag — so lockActiveCoreWrite can use a
		// shared RLock instead of the legacy exclusive lock. token == 0 when
		// the change-log is disabled (BeginScopedLog's own contract); every
		// *ScopedAware wrapper then falls through to the plain unscoped door,
		// identical to today's "log disabled" behavior.
		token, err := c.scopedChangeLog.BeginScopedLog()
		if err != nil {
			c.txEventBuffer = nil
			c.txMu.Unlock()
			return nil, err
		}
		tx.scopeToken = token
	} else if c.txLogScope != nil {
		// Legacy mechanism: the store doesn't implement the full BACKLOG 11f
		// contract, so fall back to the single shared implicit buffer + the
		// exclusive-lock divert dance in lockActiveCoreWrite.
		if err := c.txLogScope.BeginLogScope(); err != nil {
			c.txEventBuffer = nil
			c.txMu.Unlock()
			return nil, err
		}
	}
	// The registry sizes this transaction inherits. Commit compares against these to tell a
	// transaction that interned a token from one that only touched existing ones.
	tx.registrySizesAtBegin = tx.registrySizes()
	return tx, nil
}

// =============================================================================
// Snapshot helpers (caller holds tx.mu)
// =============================================================================

// snapshotNodeLocked captures the pre-mutation state of a node on first
// mutation only. If the node was already snapshotted in this transaction,
// this is a no-op.
//
// Caller must hold tx.mu: every public tx method holds tx.mu for
// its entire body so snapshot accesses do not race with Commit/Rollback.
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

// usesSharedLock reports whether this tx's mutation methods can use the
// shared c.mu.RLock instead of the legacy exclusive c.mu.Lock — true when
// EITHER the BACKLOG 11f token-routed mechanism is in use (tx.g.scopedChangeLog
// != nil: routing is decided per-call by an explicit token argument, so
// there is no shared divert flag a concurrent standalone write could ever
// race) OR there is no legacy scope at all (tx.g.txLogScope == nil: nothing
// to divert, change-log off or unsupported). Only false when the store is on
// the legacy single-implicit-buffer SetLogDivert mechanism, which genuinely
// needs the exclusive lock to stay correct — see lockActiveCoreWrite.
func (tx *GraphTx) usesSharedLock() bool {
	return tx.g.scopedChangeLog != nil || tx.g.txLogScope == nil
}

// lockActiveCoreWrite is the MUTATION variant of lockActiveCore.
//
// Two mechanisms decide the lock (see usesSharedLock):
//   - BACKLOG 11f token-routed (tx.g.scopedChangeLog != nil): this tx opened
//     its OWN scope at BeginTx (tx.scopeToken) and every mutation method
//     threads that token through doorCtx() to the *ScopedAware store
//     wrappers, which route each record into the token's OWN buffer. Because
//     routing is decided per-call by an explicit argument — never a shared
//     flag — a concurrent standalone mutation (also c.mu.RLock) can never
//     have its record misrouted into this tx's buffer, so a shared RLock is
//     sufficient. This is the actual point of BACKLOG 11f: a change-log-
//     enabled tx no longer blocks concurrent standalone writers or
//     concurrent-mode (Lanes:N) ingest for the duration of each mutation.
//   - Legacy (tx.g.txLogScope != nil, scopedChangeLog nil — store doesn't
//     implement the full BACKLOG 11f contract): takes c.mu.Lock (EXCLUSIVE)
//     and turns on record diversion (SetLogDivert) — so for the duration of
//     this single mutation no concurrent standalone mutation (which holds
//     only c.mu.RLock) can run and have its own change-log record misrouted
//     into this tx's single shared buffer.
//
// The lock is per-mutation (acquired+released around one mutation method),
// NOT held for the tx lifetime, so in-tx reads (which take their own brief
// lock) never deadlock against it (lesson 31). When there is no scope at all
// it is exactly lockActiveCore either way.
func (tx *GraphTx) lockActiveCoreWrite() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	if tx.usesSharedLock() {
		tx.g.mu.RLock()
		if tx.g.closed.Load() {
			tx.g.mu.RUnlock()
			tx.mu.Unlock()
			return ErrGraphClosed
		}
		tx.writePathUsed = true
		return nil
	}
	tx.g.mu.Lock()
	if tx.g.closed.Load() {
		tx.g.mu.Unlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	tx.g.txLogScope.SetLogDivert(true)
	tx.writePathUsed = true
	return nil
}

// lockActiveCoreWriteContext is lockActiveCoreWrite with context cancellation.
func (tx *GraphTx) lockActiveCoreWriteContext(ctx context.Context) error {
	if err := tx.lockActiveContext(ctx); err != nil {
		return err
	}
	if tx.usesSharedLock() {
		tx.g.mu.RLock()
		if tx.g.closed.Load() {
			tx.g.mu.RUnlock()
			tx.mu.Unlock()
			return ErrGraphClosed
		}
		tx.writePathUsed = true
		return nil
	}
	tx.g.mu.Lock()
	if tx.g.closed.Load() {
		tx.g.mu.Unlock()
		tx.mu.Unlock()
		return ErrGraphClosed
	}
	tx.g.txLogScope.SetLogDivert(true)
	tx.writePathUsed = true
	return nil
}

// unlockActiveCoreWrite reverses lockActiveCoreWrite (turns off diversion and
// releases the exclusive lock on the legacy path; otherwise just releases
// the shared RLock — see usesSharedLock).
func (tx *GraphTx) unlockActiveCoreWrite() {
	if tx.usesSharedLock() {
		tx.g.mu.RUnlock()
		tx.mu.Unlock()
		return
	}
	tx.g.txLogScope.SetLogDivert(false)
	tx.g.mu.Unlock()
	tx.mu.Unlock()
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
// Mutation-capable transactions take a final registry checkpoint; a clean
// read-only transaction skips it. A previously failed registry checkpoint is
// retried even when this transaction itself was read-only. Commit then releases
// c.txMu and publishes buffered events outside all locks.
// After Commit, all tx methods return storepkg.ErrTxDone.
func (tx *GraphTx) Commit() error {
	if err := tx.lockActive(); err != nil {
		return err
	}
	defer tx.mu.Unlock()

	// A transaction can contain create/import calls that wrote rows but
	// returned a trailing registry checkpoint error. Commit is the final
	// durability boundary for every mutation-capable transaction, so preserve
	// that checkpoint and retry any globally dirty registry state before making
	// the transaction irreversible. A clean read-only transaction has no
	// registry durability work and must not turn a read into an fsync.
	tx.g.mu.Lock()
	if err := tx.checkpointRegistriesOnCommit(); err != nil {
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
	if tx.g.scopedChangeLog != nil {
		lsn, err := tx.g.scopedChangeLog.CommitScopedLog(tx.scopeToken)
		if err != nil {
			tx.g.mu.Unlock()
			return err
		}
		tx.committedLSN = lsn
	} else if tx.g.txLogScope != nil {
		lsn, err := tx.g.txLogScope.CommitLogScope()
		if err != nil {
			tx.g.mu.Unlock()
			return err
		}
		tx.committedLSN = lsn
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

// checkpointRegistriesOnCommit runs with c.mu held. registryMu closes the
// second registry-access lock class used by ingest and Close (lesson 66); the
// deferred unlock prevents that second lock from leaking if a backend
// persister panics.
func (tx *GraphTx) checkpointRegistriesOnCommit() error {
	tx.g.registryMu.Lock()
	defer tx.g.registryMu.Unlock()
	// PERSIST WHEN THERE IS SOMETHING TO PERSIST, which is not the same as "a write happened".
	//
	// The previous condition checkpointed on every mutation-capable commit. Since
	// SavePropertyKeyRegistry fsyncs, that was one full disk sync per write transaction
	// whether or not any registry had changed -- and a workload whose transactions intern no
	// new tokens (the steady state of any ingest, once its labels and property keys exist)
	// paid a sync per commit forever. Measured on a signal-ingestion workload: 1.000 fsync per
	// signal at 12.5ms, against ~30 tokens interned in total across the whole run.
	//
	// The two things that genuinely need the checkpoint are kept:
	//   - this transaction interned a token, so a row it wrote may reference one that is not
	//     yet durable; and
	//   - a previous checkpoint FAILED, which is what registryDirty records and what the
	//     "retry before becoming irreversible" contract is about.
	if !tx.registriesChangedSinceBegin() && !tx.g.registryDirty.Load() {
		return nil
	}
	return tx.g.persistRegistries()
}

// registriesChangedSinceBegin reports whether any registry has a different size than when the
// transaction opened.
//
// Inequality rather than growth: a rollback restores the snapshot exactly, so equal sizes mean
// equal contents, and any difference in either direction is a reason to write the registry
// back rather than assume which way it moved.
func (tx *GraphTx) registriesChangedSinceBegin() bool {
	return tx.registrySizes() != tx.registrySizesAtBegin
}

// registrySizes reads the three registry sizes as one comparable value.
func (tx *GraphTx) registrySizes() [3]int {
	return [3]int{tx.g.labels.Len(), tx.g.relTypes.Len(), tx.g.propKeys.Len()}
}

// CommittedLSN returns the MAX change-log LSN this transaction's commit assigned
// — the exact commit-LSN for a read-your-writes write-bookmark, unaffected by
// concurrent writers (unlike the global LastCommittedLSN head). Valid only AFTER
// a successful Commit; 0 when the tx emitted no change-log records (no mutations,
// or the change-log is disabled). Safe to read after Commit returns nil.
func (tx *GraphTx) CommittedLSN() uint64 {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.committedLSN
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

	// A read-only transaction has no entity/history/registry state to undo.
	// It still restores read counters to the BeginTx snapshot and closes the
	// change-log scope so the transaction releases every lifecycle resource.
	// In particular, do not call restoreRegistries here: that performs durable
	// registry writes and may reclaim unrelated concurrent allocations.
	if !tx.writePathUsed {
		tx.g.restoreOpCounters(tx.opSnapshot)
		if tx.g.scopedChangeLog != nil {
			return tx.g.scopedChangeLog.DiscardScopedLog(tx.scopeToken)
		}
		if tx.g.txLogScope != nil {
			return tx.g.txLogScope.DiscardLogScope()
		}
		return nil
	}

	// Mutation rollback rewrites history via direct store calls (ReplaceNode,
	// TruncateNodeHistory, DeleteNodeCascade, PutNodeVersion, ...), bypassing
	// the higher-level mutation doors that normally bump this cache as they
	// run. A clean read-only rollback returned above because it cannot have
	// exposed an in-flight history rewrite and must not invalidate an AS-OF
	// column merely for reading.
	tx.g.asOfColumns.bump()

	// Legacy mechanism only: keep change-log diversion ON across the reverse
	// mutations below so their records ALSO land in the single shared scope
	// buffer, then drop the whole buffer (forward + reverse) with
	// DiscardLogScope before unlocking — a rolled-back tx emits NOTHING.
	// Rollback holds c.mu.Lock, so no concurrent standalone mutation can run
	// while diverted. BACKLOG 11f token-routed mechanism needs no divert
	// flag at all: every reverse-mutation call below goes through the
	// *ScopedAwareToken/*ScopedAware helpers (rollback_scoped.go), which
	// route explicitly by tx.scopeToken — the SAME token the tx's forward
	// mutations already used — so the reverse records land in the identical
	// discardable buffer without needing exclusivity.
	if tx.g.scopedChangeLog == nil && tx.g.txLogScope != nil {
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
		capture(tx.g.replaceRelationshipScopedAwareToken(tx.scopeToken, snap.prev))
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
		capture(tx.g.deleteRelationshipScopedAware(tx.scopeToken, rid))
		capture(tx.g.truncateRelHistoryScopedAware(tx.scopeToken, rid, 0))
	}

	// 7. Delete created nodes in reverse creation order (cascade).
	for i := len(tx.createdNodes) - 1; i >= 0; i-- {
		nid := types.NodeID(tx.createdNodes[i])
		capture(tx.g.deleteNodeCascadeScopedAware(tx.scopeToken, nid))
		capture(tx.g.truncateNodeHistoryScopedAware(tx.scopeToken, nid, 0))
	}

	capture(tx.restoreRegistries())
	if firstErr == nil {
		tx.g.restoreOpCounters(tx.opSnapshot)
	}

	// Drop the buffered forward + reverse records. No LSN was burned, so the
	// feed has no gap either way.
	if tx.g.scopedChangeLog != nil {
		capture(tx.g.scopedChangeLog.DiscardScopedLog(tx.scopeToken))
	} else if tx.g.txLogScope != nil {
		capture(tx.g.txLogScope.DiscardLogScope())
	}

	return firstErr
}

func (tx *GraphTx) restoreDeletedNodeRow(n *types.Node) error {
	current, err := tx.g.getCurrentNode(n.ID())
	if errors.Is(err, storepkg.ErrNodeNotFound) {
		return tx.g.putNodeScopedAwareToken(tx.scopeToken, n)
	} else if err != nil {
		return err
	}
	if !sameNodeLabelTokens(current, n) {
		if err := tx.restoreNodeLabels(n.ID(), current, n); err != nil {
			return err
		}
	}
	return tx.g.replaceNodeScopedAwareToken(tx.scopeToken, n)
}

func (tx *GraphTx) restoreDeletedRelRow(r *types.Relationship) error {
	current, err := tx.g.getCurrentRelationship(r.ID())
	if errors.Is(err, storepkg.ErrRelNotFound) {
		return tx.g.putRelationshipScopedAwareToken(tx.scopeToken, r)
	} else if errors.Is(err, storepkg.ErrSlotNotLocal) {
		// A Model-A foreign-incoming stub (ADR-0010): its rel-ID slot is foreign, so
		// a slot-routed point read fails closed and PutRelationship/ReplaceRelationship
		// would too. It can only be restored via the foreign-incoming capability,
		// which routes by the END-node slot. The end node was restored earlier in
		// the rollback (deleted nodes first), so it is live. Idempotent.
		if tx.g.foreignIncomingRel == nil {
			return err // non-partitioned store cannot hold a stub — surface the error
		}
		rerr := tx.g.foreignIncomingRel.RecordForeignIncoming(r, generatedcreate.FreshGraphID())
		if errors.Is(rerr, storepkg.ErrRelExists) {
			return nil // already present — idempotent restore
		}
		return rerr
	} else if err != nil {
		return err
	}
	if !sameRelationshipIndexFields(current, r) {
		if err := tx.g.deleteRelationshipScopedAware(tx.scopeToken, r.ID()); err != nil {
			return err
		}
		return tx.g.putRelationshipScopedAwareToken(tx.scopeToken, r)
	}
	return tx.g.replaceRelationshipScopedAwareToken(tx.scopeToken, r)
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
	return tx.g.replaceNodeScopedAwareToken(tx.scopeToken, snap.prev)
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
				if err := tx.g.addNodeLabelTokenScopedAware(tx.scopeToken, id, targetPrimary, next); err != nil {
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
			if err := tx.g.removeNodeLabelTokenScopedAware(tx.scopeToken, id, tok, next); err != nil {
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
			if err := tx.g.addNodeLabelTokenScopedAware(tx.scopeToken, id, targetLabels[prefix], next); err != nil {
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
		if err := tx.g.removeNodeLabelTokenScopedAware(tx.scopeToken, id, tok, next); err != nil {
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
	if err := tx.g.truncateNodeHistoryScopedAware(tx.scopeToken, id, 0); err != nil {
		return err
	}
	for _, n := range history {
		if err := tx.g.putNodeVersionScopedAwareToken(tx.scopeToken, id, n.Version(), n); err != nil {
			return err
		}
	}
	return nil
}

func (tx *GraphTx) restoreRelHistory(id types.RelID, history []*types.Relationship) error {
	if err := tx.g.truncateRelHistoryScopedAware(tx.scopeToken, id, 0); err != nil {
		return err
	}
	for _, r := range history {
		if err := tx.g.putRelVersionScopedAwareToken(tx.scopeToken, id, r.Version(), r); err != nil {
			return err
		}
	}
	return nil
}

func (tx *GraphTx) restoreDeletedNodeHistory(snap deletedNodeSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.trimNodeHistoryFromScopedAware(tx.scopeToken, snap.node.ID(), snap.historyTrimFrom)
	}
	return tx.restoreNodeHistory(snap.node.ID(), snap.nodeHistory)
}

func (tx *GraphTx) restoreDeletedRelHistory(snap deletedRelSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.trimRelHistoryFromScopedAware(tx.scopeToken, snap.rel.ID(), snap.historyTrimFrom)
	}
	return tx.restoreRelHistory(snap.rel.ID(), snap.history)
}

func (tx *GraphTx) restoreNodeSnapshotHistory(id types.NodeID, snap nodeSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.trimNodeHistoryFromScopedAware(tx.scopeToken, id, snap.historyTrimFrom)
	}
	return tx.restoreNodeHistory(id, snap.history)
}

func (tx *GraphTx) restoreRelSnapshotHistory(id types.RelID, snap relSnapshot) error {
	if snap.useHistoryTrim {
		return tx.g.trimRelHistoryFromScopedAware(tx.scopeToken, id, snap.historyTrimFrom)
	}
	return tx.restoreRelHistory(id, snap.history)
}

// restoreRegistries de-allocates any label/rel-type token this tx allocated —
// UNLESS a concurrently-persisted entity has already adopted it (BACKLOG 11b).
//
// A tx's newly-allocated label/rel-type token is registered (and persisted)
// immediately, before Commit — Rollback does NOT hold c.mu for the tx's whole
// lifetime (only per-mutation, see lockActiveCoreWrite), so a concurrent
// standalone Add can Lookup and persist an entity referencing the token before
// this tx rolls back. Blindly de-allocating the token in that case would leave
// the already-persisted entity's label/type dangling: the next distinct
// name allocated anywhere reuses the freed token number, silently reassigning
// that entity's label/type. So each newly-allocated name is reclaimed only when
// NO current entity references its token (checked via the O(1) label/rel-type
// counters, under the exclusive c.mu.Lock Rollback already holds — no concurrent
// writer can be adding a NEW reference while this check runs). A referenced
// token is left registered rather than risking corruption; the (rare) leaked
// registry slot is a strictly better failure mode than a silently mis-labeled
// entity.
//
// This is safe even with the change-log enabled BECAUSE the per-tx change-log
// scope (store.TxChangeLogScope) discarded the tx's buffered records on
// rollback — the rolled-back tx emitted NOTHING, so no durable feed record
// references a token this function does end up de-allocating. (Before the
// per-tx buffer existed, the tx body's puts emitted records in-backend
// immediately, so de-allocating here poisoned the feed and stalled replicas —
// lesson 55; the append-only STOPGAP that guarded this is now superseded by
// the buffer for the tx path.)
func (tx *GraphTx) restoreRegistries() error {
	// registryMu additionally guards Close's final persistRegistries and the
	// ingest sessions' declare-on-prepare path, which read the registry
	// pointers WITHOUT c.mu (the Close-vs-Rollback race was caught by the
	// lifecycle storm under -race) — held here even though this function no
	// longer swaps the registry pointers (RollbackNames mutates the existing
	// object in place, under its own internal lock), so persistRegistries
	// below observes a state consistent with those other readers.
	tx.g.registryMu.Lock()
	defer tx.g.registryMu.Unlock()

	labelAllocated := newlyAllocatedNames(tx.labelSnapshot, tx.g.labels.ExportNames())
	relTypeAllocated := newlyAllocatedNames(tx.relTypeSnapshot, tx.g.relTypes.ExportNames())

	var firstErr error
	// No `firstErr == nil` guard on the FIRST assignment: it is provably nil here,
	// and govet's nilness check flags the tautology.
	if _, err := tx.g.rollbackLabelsIfUnreferenced(tx.labelSnapshot, labelAllocated); err != nil {
		firstErr = err
	}
	if _, err := tx.g.rollbackRelTypesIfUnreferenced(tx.relTypeSnapshot, relTypeAllocated); err != nil && firstErr == nil {
		firstErr = err
	}
	tx.g.clearRelTypeCache()
	setTieredLabelRegistryIfSupported(tx.g.store, tx.g.labels)
	if err := tx.g.persistRegistries(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// =============================================================================
// Read-only / inspection
// =============================================================================

// GetNode reads a node by ID within the transaction.
// Safe under v4.1.0 Path B: the tx does NOT hold c.mu for its whole lifetime
// (only per-call, see lockActiveCore) — this method takes c.mu.RLock briefly,
// like every other tx read mirror, so it observes ErrGraphClosed instead of
// racing a concurrent Close (BACKLOG 11a).
func (tx *GraphTx) GetNode(id types.NodeID) (*types.Node, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()

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
// Safe under v4.1.0 Path B: the tx does NOT hold c.mu for its whole lifetime
// (only per-call, see lockActiveCore) — this method takes c.mu.RLock briefly,
// like every other tx read mirror, so it observes ErrGraphClosed instead of
// racing a concurrent Close (BACKLOG 11a).
func (tx *GraphTx) GetRelationship(id types.RelID) (*types.Relationship, error) {
	if err := tx.lockActiveCore(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCore()

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
