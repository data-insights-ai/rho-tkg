package store

import "github.com/data-insights-ai/rho-tkg/v4/pkg/types"

// ChangeTag identifies the kind of mutation a change-log record describes. It
// is the first byte of a persisted change-log value (followed by the
// tag-specific msgpack body) and the discriminator a replica-apply path
// switches on. Tags are append-only: never renumber an existing tag, because
// the value is durable on disk.
type ChangeTag byte

// Change-log record tags. The body layout for each tag is defined alongside the
// wire format in internal/storeutil (the bodies reference the NodeWire/RelWire
// types); current-state Put records carry a bare NodeWire/RelWire payload.
const (
	// ChangeNodePut / ChangeRelPut carry the new CURRENT state of an entity
	// (a bare NodeWire / RelWire). Emitted by create, replace, replace-with-
	// history, and label-token mutation doors. Apply = upsert the current row.
	ChangeNodePut ChangeTag = 1
	ChangeRelPut  ChangeTag = 2

	// ChangeNodeDelete / ChangeRelDelete describe a delete. The body carries a
	// sub-kind (hard-cascade vs with-history): a hard cascade removes the
	// current rows with no tombstone/history; a with-history delete appends
	// tombstone version rows. ChangeNodeDelete additionally carries the
	// relationship tombstones for a cascade-with-history node delete.
	ChangeNodeDelete ChangeTag = 3
	ChangeRelDelete  ChangeTag = 4

	// ChangeNodeHistoryVersion / ChangeRelHistoryVersion append a history row at
	// an EXPLICIT version (not the current row). Emitted by PutNodeVersion /
	// PutRelVersion — the import, bitemporal-cascade-correction, migration, and
	// transaction-rollback-restore paths.
	ChangeNodeHistoryVersion ChangeTag = 5
	ChangeRelHistoryVersion  ChangeTag = 6

	// ChangeNodeHistoryTruncate / ChangeRelHistoryTruncate mirror a history
	// truncation (keep the N most recent versions) or a trim-from (drop versions
	// at or above a minimum). The body carries the entity ID and the bound plus
	// a flag distinguishing truncate (keep-count) from trim (min-version).
	ChangeNodeHistoryTruncate ChangeTag = 7
	ChangeRelHistoryTruncate  ChangeTag = 8

	// ChangeMeta is RESERVED for a MetaSet (schema / migration markers) record;
	// no door emits it yet (deferred to Phase 1 — a synchronous meta write cannot
	// share the async buffer's LSN/watermark without a non-monotonic-watermark
	// hazard). Body = key+value (MetaBody).
	ChangeMeta ChangeTag = 9

	// ChangeClear marks a full store clear (DropAll). A replica applying it must
	// reproduce the exact state a primary's Admin.Reset() ends up in, not just
	// wipe this Store implementation's own data: the graph layer above the Store
	// interface also holds Core-level in-memory state a bare store.Clear() cannot
	// reach (the as-of DocValues cache, unique-constraint ownership registries,
	// compaction/retention watermarks, operation counters) — see
	// core.reapCoreStateForClear, invoked by the replica apply path (BACKLOG 12a)
	// immediately after this Store's own Clear(). Carries no body.
	ChangeClear ChangeTag = 10

	// ChangeForeignIncoming is the cross-machine incoming half-edge stub (ADR-0010
	// Model A): a relationship whose ID belongs to a FOREIGN slot but is stored
	// on the END node's shard so IncomingRelationships(END) is locally complete.
	// Body is the same as ChangeRelPut (the stub's RelWire), but a replica MUST
	// route apply by the END-node's slot (not the rel's slot, which is foreign),
	// idempotently — a plain ChangeRelPut apply would route by rel-slot and fail
	// ErrSlotNotLocal. Emitted only by the sharded store's RecordForeignIncoming.
	ChangeForeignIncoming ChangeTag = 11

	// ChangeForeignIncomingDelete removes a Model-A incoming half-edge stub
	// (ADR-0010 §3.3 cascade). It is the delete counterpart of ChangeForeignIncoming
	// and, like it, MUST route apply by the END-node's slot — the rel's own slot is
	// foreign, so a plain ChangeRelDelete apply would fail ErrSlotNotLocal. Emitted
	// when the END node is deleted (the stub's row + adjacency are removed from the
	// END shard) BEFORE the node's own with-history delete, so the node-delete
	// tombstone validation sees a stub-free adjacency. Body is ForeignIncoming
	// DeleteBody (rel ID + END-node ID for routing). Idempotent on apply.
	ChangeForeignIncomingDelete ChangeTag = 12

	// ChangeRangePurge is a retention range purge (ADR-0008 R3): a single logical
	// record naming a PREDICATE ("label L older than T"), NOT N per-entity deletes.
	// A replica RE-EXECUTES the predicate against its own state — because replicas
	// apply LSN-ordered, their pre-purge state for label L below the boundary is
	// byte-identical to the primary's, so the same range predicate removes exactly
	// the same entities (even onto a different shard count). Body is RangePurgeBody
	// (label token + boundary + mode). Idempotent: re-executing below an
	// already-advanced watermark is a no-op.
	ChangeRangePurge ChangeTag = 13
)

// String renders the tag for diagnostics and tests.
func (t ChangeTag) String() string {
	switch t {
	case ChangeNodePut:
		return "NodePut"
	case ChangeRelPut:
		return "RelPut"
	case ChangeNodeDelete:
		return "NodeDelete"
	case ChangeRelDelete:
		return "RelDelete"
	case ChangeNodeHistoryVersion:
		return "NodeHistoryVersion"
	case ChangeRelHistoryVersion:
		return "RelHistoryVersion"
	case ChangeNodeHistoryTruncate:
		return "NodeHistoryTruncate"
	case ChangeRelHistoryTruncate:
		return "RelHistoryTruncate"
	case ChangeMeta:
		return "Meta"
	case ChangeClear:
		return "Clear"
	case ChangeForeignIncoming:
		return "ForeignIncoming"
	case ChangeForeignIncomingDelete:
		return "ForeignIncomingDelete"
	case ChangeRangePurge:
		return "RangePurge"
	default:
		return "ChangeTag(unknown)"
	}
}

// Valid reports whether t is a known change tag.
func (t ChangeTag) Valid() bool {
	return t >= ChangeNodePut && t <= ChangeRangePurge
}

// ChangeRecord is one entry of the durable ordered change-log (op-log). LSN is
// the monotonic cluster commit sequence (gap-free, ascending == commit order);
// Tag is the mutation kind; Payload is the tag-specific msgpack body (decoded
// by the consumer / replica-apply path). The Payload is owned by the caller —
// implementations return a copy, never an alias into store-internal buffers.
type ChangeRecord struct {
	LSN     uint64
	Tag     ChangeTag
	Payload []byte
}

// ChangeFeedCapability is OPTIONAL. Backends that maintain a durable, ordered
// change-log (op-log) expose it so a primary can stream committed mutations and
// a replica resume from a checkpoint. It is the topology-agnostic foundation
// for replication; on its own it is also usable as change-data-capture, an
// audit trail, and point-in-time recovery.
//
// Semantics:
//   - Records are returned in ascending LSN order; LSNs are strictly monotonic
//     across the lifetime of a store (preserved across restart) and gap-free on
//     the happy path. A failed flush can leave a permanent gap (the LSN was
//     consumed but its record never committed) — consumers must tolerate gaps
//     and rely only on monotonic ascending order.
//   - Only DURABLY-COMMITTED records are visible: a record buffered but not yet
//     flushed is not surfaced. A consumer resumes from LastCommittedLSN.
//   - The feed is MUTATION-LEVEL: a rolled-back transaction appears as its
//     forward operations followed by its compensating operations. Replaying the
//     full ordered feed still converges a replica to the primary's state, but a
//     CDC consumer observes the intermediate (later-undone) operations.
//   - The change-log ALONE does not converge a replica from empty: a replica
//     bootstraps from a full snapshot (export, including the token registry)
//     and then tails the feed from the snapshot's LSN. Tokens referenced by a
//     record are resolvable from the snapshot plus the record's own wire bytes.
//
// Callbacks passed to ForEachChange are invoked OUTSIDE backend locks (the same
// contract as IterationCapability.ForEachNodeID), so callback code may re-enter
// Store methods. A nil callback returns ErrInvalidStoreMutation.
type ChangeFeedCapability interface {
	// ChangeFeed returns up to limit committed records with LSN > afterLSN, in
	// ascending LSN order. limit <= 0 returns all available records (callers
	// concerned with memory should prefer ForEachChange or pass a positive
	// limit). Payloads are owned copies.
	ChangeFeed(afterLSN uint64, limit int) ([]ChangeRecord, error)

	// ForEachChange streams committed records with LSN > afterLSN in ascending
	// LSN order, invoking fn for each. Returning false from fn stops iteration
	// early. The ChangeRecord (including Payload) passed to fn is valid only for
	// the duration of the call; copy Payload to retain it.
	ForEachChange(afterLSN uint64, fn func(ChangeRecord) bool) error

	// LastCommittedLSN returns the highest durably-committed LSN, or 0 when the
	// change-log is empty.
	LastCommittedLSN() (uint64, error)
}

// ChangeLogStatusCapability is OPTIONAL. A backend that exposes
// ChangeFeedCapability whether or not its change-log is actually recording (the
// in-tree memory and badger stores always expose the feed methods, returning an
// empty feed when the log is disabled) implements this so a consumer can tell
// "recording" from "present but off". ExportSince / Watermark use it to fail
// closed when the log is disabled — a delta that silently recorded nothing would
// be a data-loss footgun in a backup. A feed backend that does not implement it
// is assumed active (it opted into exposing the feed). Core also uses it to gate
// the per-tx change-log buffer and the append-only-registry behavior.
type ChangeLogStatusCapability interface {
	// ChangeLogEnabled reports whether this store is recording committed
	// mutations to its change-log.
	ChangeLogEnabled() bool
}

// TxChangeLogScope is an optional capability that lets a transaction (or batch)
// buffer the change-log records its mutations produce, so a ROLLED-BACK tx emits
// NOTHING to the feed (mirroring the in-memory txEventBuffer for events). It is
// the proper fix for the rolled-back-token-poisoning / transient-replica-phantom /
// CDC-asymmetry that arise because the change-log is otherwise a physical redo log
// that records a rolled-back tx's forward AND reverse mutations.
//
// Lifecycle (the core, holding c.txMu, drives it):
//   - BeginLogScope opens a per-tx record buffer.
//   - SetLogDivert(true)/(false) brackets EACH tx mutation; the core calls these
//     only while it holds the EXCLUSIVE write lock that guards the mutation, so a
//     concurrent standalone mutation (which holds only a shared read lock) can
//     never observe diversion active and have its own record misrouted into the
//     tx's buffer. Records produced while diverted carry NO LSN.
//   - CommitLogScope mints contiguous LSNs for the buffered records at COMMIT time
//     (so a rolled-back tx burns no LSN and leaves no feed gap) and co-commits
//     them with the tx's pending data in one atomic batch.
//   - DiscardLogScope drops the buffer; the rolled-back tx emits nothing.
//
// A store that does not implement this (e.g. tiered) makes the core emit records
// eagerly as before. All methods are no-ops when the change-log is disabled.
//
// CONCURRENCY POSITION (deliberate design, not a gap): the scope is
// EXCLUSIVE-WRITE-LOCK-ONLY machinery. There is exactly one implicit scope, and
// BeginLogScope while a scope is open is an error — handle-based scopes for N
// concurrent writers are deliberately NOT provided. Concurrent writers (the
// standalone mutation doors, and the ingest pipeline's concurrent mode) do not
// need them: they use the EAGER in-door path, where each store mutation door
// appends its record(s), mints their LSN(s), and stages its data under ONE
// store-write-mutex window — so per-door record atomicity, LSN gaplessness
// (records are minted only after validation, on the same path that stages the
// data, so a failed door burns nothing), and crash-consistent co-commit all hold
// under any number of concurrent writers. The feed is then a linearization of
// committed doors in LSN order, which is exactly what a tailing replica applies.
// The scope exists only to give a multi-door EXCLUSIVE unit (an interactive tx,
// or the batch/strong-mode ingest applier) all-or-nothing feed semantics with
// commit-time LSNs; anything running under the shared read lock must never
// touch it.
type TxChangeLogScope interface {
	BeginLogScope() error
	SetLogDivert(on bool)
	// CommitLogScope mints contiguous LSNs for the scope's buffered records and
	// co-commits them, returning the MAX (last) LSN this commit assigned — the
	// exact commit-LSN a read-your-writes write-bookmark needs, unaffected by
	// concurrent writers or async-flush timing (unlike the global LastCommittedLSN
	// head). Returns 0 when the scope emitted no records (or the log is disabled).
	CommitLogScope() (uint64, error)
	DiscardLogScope() error
}

// ScopedTxChangeLog is an OPTIONAL capability (BACKLOG 11f Batch A —
// FOUNDATION ONLY: implemented and independently testable, but nothing in the
// core/tx layer constructs a nonzero token yet, so this has zero effect on any
// existing behavior). It lets MULTIPLE independent transactions each buffer
// their own change-log records CONCURRENTLY, each addressed by an explicit
// token, instead of sharing the single implicit buffer TxChangeLogScope above
// provides.
//
// Why this exists: TxChangeLogScope's single scopeActive flag is store-global,
// so the core can only prove "no concurrent standalone mutation will misroute
// its record into my tx's buffer" by holding the store's FULL exclusive write
// lock for the duration of every scoped mutation (see lockActiveCoreWrite in
// internal/core/tx.go) — a tx that only ever touches its own entities still
// pays serialization against every unrelated concurrent writer. A token-keyed
// scope removes the shared flag entirely: routing is decided by an explicit
// argument on the call (see the *Scoped store doors, e.g. PutNodeScoped),
// never by hidden ambient state, so two open scopes (or a scope and a
// standalone eager write) cannot cross-contaminate regardless of what lock
// discipline the caller uses. This is what will eventually let a tx mutation
// take only the same shared read-lock a standalone mutation takes — that
// lock-behavior flip is deliberately NOT part of this capability and lands in
// a later BACKLOG 11f batch, once every mutation door has a Scoped sibling.
//
// token == 0 is reserved and always means "no scope" — it is the value
// BeginScopedLog returns when the change-log is disabled, and every *Scoped
// door treats token == 0 as behaviorally identical to its unscoped
// counterpart. A store implementing ScopedTxChangeLog also implements
// whichever of ScopedPutCapability / the generatedcreate scoped endpoint-hash
// capability it supports (a store that opens scopes but exposes no Scoped
// door would be useless in practice, but the interfaces are kept separate so
// a store can decline door-level scoping while still supporting others, or
// vice versa).
type ScopedTxChangeLog interface {
	// BeginScopedLog opens a new, independently-addressed scope and returns
	// its token. Returns (0, nil) when the change-log is disabled (mirrors
	// TxChangeLogScope.BeginLogScope's no-op).
	BeginScopedLog() (token uint64, err error)
	// CommitScopedLog mints contiguous LSNs for the scope's buffered records
	// and co-commits them, returning the max (last) LSN assigned — 0 when the
	// scope emitted no records or the log is disabled. The token is retired:
	// reusing it after Commit/Discard fails closed with ErrInvalidStoreMutation.
	CommitScopedLog(token uint64) (maxLSN uint64, err error)
	// DiscardScopedLog drops the scope's buffered records without minting any
	// LSN — a rolled-back tx emits nothing to the feed and burns no sequence
	// number. The token is retired (see CommitScopedLog).
	DiscardScopedLog(token uint64) error
}

// ScopedPutCapability is the BACKLOG 11f Batch A scoped counterpart of the two
// plain create doors PutNode / PutRelationship (store/capabilities.go): each
// method behaves exactly like its unscoped sibling except that the change-log
// record it produces is routed into the ScopedTxChangeLog buffer named by
// token (opened via BeginScopedLog) instead of the eager pending log or the
// legacy single TxChangeLogScope buffer. token == 0 is exactly PutNode /
// PutRelationship. A store implementing ScopedPutCapability MUST also
// implement ScopedTxChangeLog (the token comes from nowhere else).
//
// See internal/generatedcreate.RelationshipEndpointHashScopedCapability for
// the scoped counterpart of the third Batch-A door,
// PutRelationshipGeneratedIDWithEndpointHashes — it lives in that internal
// package (not here) because its unscoped sibling does too.
type ScopedPutCapability interface {
	PutNodeScoped(n *types.Node, token uint64) error
	PutRelationshipScoped(r *types.Relationship, token uint64) error
}

// ScopedReplaceCapability is the BACKLOG 11f Batch B scoped counterpart of the
// two atomic replace-plus-history update doors ReplaceNodeWithHistory /
// ReplaceRelWithHistory (store/capabilities.go): each method behaves exactly
// like its unscoped sibling — persisting the new current row AND a version-
// history row for prevState/prevVersion in one atomic write — except the
// change-log record it produces is routed into the ScopedTxChangeLog buffer
// named by token (opened via BeginScopedLog) instead of the eager pending log
// or the legacy single TxChangeLogScope buffer. token == 0 is exactly
// ReplaceNodeWithHistory / ReplaceRelWithHistory. A store implementing
// ScopedReplaceCapability MUST also implement ScopedTxChangeLog (the token
// comes from nowhere else).
//
// FOUNDATION ONLY, same status as ScopedPutCapability: nothing in the core/tx
// layer constructs a token-carrying context yet, so this has zero effect on
// any existing behavior. See store.ScopedTxChangeLog's doc comment for the
// full design rationale.
type ScopedReplaceCapability interface {
	ReplaceNodeWithHistoryScoped(n *types.Node, prevVersion uint32, prevState *types.Node, token uint64) error
	ReplaceRelWithHistoryScoped(r *types.Relationship, prevVersion uint32, prevState *types.Relationship, token uint64) error
}

// ScopedDeleteCapability is the BACKLOG 11f Batch C scoped counterpart of the
// two with-history delete doors DeleteNodeWithHistory / DeleteRelWithHistory
// (store/capabilities.go): each method behaves EXACTLY like its unscoped
// sibling — including DeleteNodeWithHistory's invariant that relTombstones
// must cover every live connected relationship exactly once — except the
// change-log record it produces is routed into the ScopedTxChangeLog buffer
// named by token (opened via BeginScopedLog) instead of the eager pending log
// or the legacy single TxChangeLogScope buffer. token == 0 is exactly
// DeleteNodeWithHistory / DeleteRelWithHistory. A store implementing
// ScopedDeleteCapability MUST also implement ScopedTxChangeLog (the token
// comes from nowhere else).
//
// FOUNDATION ONLY, same status as ScopedPutCapability / ScopedReplaceCapability:
// nothing in the core/tx layer constructs a token-carrying context yet, so this
// has zero effect on any existing behavior. See store.ScopedTxChangeLog's doc
// comment for the full design rationale.
type ScopedDeleteCapability interface {
	DeleteNodeWithHistoryScoped(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone, token uint64) error
	DeleteRelWithHistoryScoped(id types.RelID, prevVersion uint32, tombstone *types.Relationship, token uint64) error
}

// ScopedLabelCapability is the BACKLOG 11f Batch D scoped counterpart of the
// two atomic label-mutation-plus-history doors AddNodeLabelTokenWithHistory /
// RemoveNodeLabelTokenWithHistory (HistoryCapability, store/capabilities.go):
// each method behaves EXACTLY like its unscoped sibling — persisting the
// updated current row, the label index entry, AND a version-history row for
// prevState/prevVersion atomically — except the change-log record it produces
// is routed into the ScopedTxChangeLog buffer named by token (opened via
// BeginScopedLog) instead of the eager pending log or the legacy single
// TxChangeLogScope buffer. token == 0 is exactly AddNodeLabelTokenWithHistory
// / RemoveNodeLabelTokenWithHistory. A store implementing
// ScopedLabelCapability MUST also implement ScopedTxChangeLog (the token
// comes from nowhere else).
//
// FOUNDATION ONLY, same status as ScopedPutCapability / ScopedReplaceCapability
// / ScopedDeleteCapability: nothing in the core/tx layer constructs a
// token-carrying context yet, so this has zero effect on any existing
// behavior. See store.ScopedTxChangeLog's doc comment for the full design
// rationale.
type ScopedLabelCapability interface {
	AddNodeLabelTokenWithHistoryScoped(id types.NodeID, tok uint16, updatedNode *types.Node, prevVersion uint32, prevState *types.Node, token uint64) error
	RemoveNodeLabelTokenWithHistoryScoped(id types.NodeID, tok uint16, updatedNode *types.Node, prevVersion uint32, prevState *types.Node, token uint64) error
}

// ScopedCascadeCapability is the BACKLOG 11f Batch E scoped counterpart of
// the four store doors the bitemporal cascade doors (GraphTx.
// SetNodeVersionInterval / SetRelVersionInterval, via
// cascadeNodeVersionInterval / cascadeRelVersionInterval in
// internal/core/temporal_cascade.go) call: PutNodeVersion / ReplaceNode
// (MandatoryStore/HistoryCapability, store/capabilities.go) and their
// relationship mirrors PutRelVersion / ReplaceRelationship. Each Scoped
// method behaves EXACTLY like its unscoped sibling — the cascade's append-
// only history-row inserts and the new-current-row replace are UNCHANGED —
// except the change-log record it produces is routed into the
// ScopedTxChangeLog buffer named by token instead of the eager pending log
// or the legacy single TxChangeLogScope buffer. token == 0 is exactly the
// unscoped door. A store implementing ScopedCascadeCapability MUST also
// implement ScopedTxChangeLog (the token comes from nowhere else).
//
// FOUNDATION ONLY, same status as every other BACKLOG 11f Scoped capability:
// nothing in the core/tx layer constructs a token-carrying context yet, so
// this has zero effect on any existing behavior. See
// store.ScopedTxChangeLog's doc comment for the full design rationale.
type ScopedCascadeCapability interface {
	PutNodeVersionScoped(id types.NodeID, version uint32, n *types.Node, token uint64) error
	ReplaceNodeScoped(n *types.Node, token uint64) error
	PutRelVersionScoped(id types.RelID, version uint32, r *types.Relationship, token uint64) error
	ReplaceRelationshipScoped(r *types.Relationship, token uint64) error
}
