package store

import (
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Capability interfaces — narrowed views of the full Store contract.
//
// The full Store interface is a 65-method aggregate. Splitting it into
// behavioural clusters (this file) lets new backends implement only the
// capabilities they actually support and lets call sites depend on the
// narrowest surface that gets the job done.
//
// All capability interfaces are EMBEDDED into Store (see store.go), so every
// existing implementation continues to satisfy Store as a whole — this is a
// purely additive refactor. Future migrations can convert specific call
// sites to depend on the narrowest capability without touching the
// implementations.
//
// Optional capabilities (DepthHistoryIterationCapability,
// HistoryVersionPageCapability, PropertyIndexCapability,
// TemporalIndexCapability, VectorIndexCapability, HighFrequencyIndexCapability)
// can be removed from a backend's Store implementation in a future major
// version; consumers that rely on them type-assert before calling.

// Lifecycle is the always-mandatory housekeeping subset every backend must
// implement. Close releases resources; Clear truncates without removing
// registries (a graph-layer concern). Nil concrete store receivers return
// ErrNilStore from lifecycle methods rather than panicking.
type Lifecycle interface {
	// Clear removes all entities, indexes, history, and counters.
	// Registries (label/reltype tokens) are a graph-layer concern and
	// untouched.
	Clear() error
	// Close releases any resources held by the store. Idempotent.
	Close() error
}

// NodeCRUDCapability is the node-mutation surface. Every backend must
// implement it — node CRUD plus the label-mutation helpers required by the
// graph's history-aware label flows. Label-mutation helpers validate that the
// supplied current row adds or removes exactly the requested token.
type NodeCRUDCapability interface {
	PutNode(n *types.Node) error
	GetNode(id types.NodeID) (*types.Node, error)
	ReplaceNode(n *types.Node) error
	// DeleteNode removes only an unconnected node row. Backends return
	// ErrInvalidStoreMutation instead of orphaning connected relationships.
	DeleteNode(id types.NodeID) error

	// DeleteNodeCascade removes the node and every relationship it
	// participates in. Used when the graph layer needs a single-call
	// delete that the store can implement atomically.
	DeleteNodeCascade(id types.NodeID) error

	RemoveNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
	AddNodeLabelToken(id types.NodeID, tok uint16, updatedNode *types.Node) error
}

// NodeIntegrityHashCapability is OPTIONAL. Backends that can expose a live
// node's integrity hash without returning the full node can implement this to
// let relationship creation capture endpoint hashes without forcing a defensive
// node deep copy. The method must still validate that the node currently exists.
type NodeIntegrityHashCapability interface {
	NodeIntegrityHash(id types.NodeID) (string, error)
}

// EndpointIntegrityHashCapability is OPTIONAL. It is the batched form of
// NodeIntegrityHashCapability for relationship creation paths that need both
// endpoint hashes under one backend read window.
type EndpointIntegrityHashCapability interface {
	EndpointIntegrityHashes(startID, endID types.NodeID) (string, string, error)
}

// RelationshipCRUDCapability is the relationship-mutation surface.
type RelationshipCRUDCapability interface {
	PutRelationship(r *types.Relationship) error
	GetRelationship(id types.RelID) (*types.Relationship, error)
	ReplaceRelationship(r *types.Relationship) error
	DeleteRelationship(id types.RelID) error
}

// AdjacencyCapability is the outgoing/incoming relationship lookup surface
// plus the batch ForNodes variants. Mandatory because it is on the hot path
// of every traversal-shaped query. Explicit node IDs are all-or-error:
// existing nodes with zero matching relationships return an empty result, but
// any missing requested node returns ErrNodeNotFound.
type AdjacencyCapability interface {
	OutgoingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)
	IncomingRelationships(nodeID types.NodeID, typeToken uint16) ([]*types.Relationship, error)

	OutgoingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
	IncomingRelationshipsForNodes(nodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error)
}

// DegreeCapability is an OPTIONAL fast-path for counting a node's relationships
// without materializing them. It returns the number of adjacency-index entries
// for the node (type-filtered), which equals the count of the corresponding
// AdjacencyCapability result in normal operation. The two can differ only by
// transient, crash-induced orphan index entries (an in/out index key persisted
// while its entity write was lost mid cross-shard write); those are reconciled
// by repair/restart — the same consistency model adjacency reads rely on.
//
// Backends that do not implement this interface are served by the graph layer
// via len(Outgoing/IncomingRelationships), so the capability is purely additive.
type DegreeCapability interface {
	OutgoingDegree(nodeID types.NodeID, typeToken uint16) (int, error)
	IncomingDegree(nodeID types.NodeID, typeToken uint16) (int, error)
}

// BulkReadCapability is the unfiltered/label-filtered listing surface. The
// graph layer uses it for export, snapshot, and the `*ByLabel` /
// `*ByType` query shapes.
type BulkReadCapability interface {
	NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error)
	RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error)

	AllNodes(opts QueryOpts) ([]*types.Node, error)
	AllRelationships(opts QueryOpts) ([]*types.Relationship, error)
	GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error)
	GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error)
}

// BatchCapability is the batched-mutation surface used by BatchBuilder and
// the import path. All-or-nothing: empty/nil input returns nil with zero
// mutations. Delete batches treat duplicate IDs as one requested deletion
// before validation and counter/index updates. DeleteNodesBatch removes only
// unconnected node rows; connected batch deletes must use graph-level cascade
// or history-aware delete flows.
type BatchCapability interface {
	PutNodesBatch(nodes []*types.Node) error
	PutRelationshipsBatch(rels []*types.Relationship) error
	DeleteNodesBatch(ids []types.NodeID) error
	DeleteRelationshipsBatch(ids []types.RelID) error
}

// PreEncodedPutCapability is OPTIONAL — the ingest apply-side fast path
// (ADR-0006 §4.5, Scenario B). PutNodesBatchPreEncoded persists nodes whose v2
// ENTITY-ROW wire was pre-encoded on the producer thread (with a zero
// transaction-time tail) and had that tail patched with the applier-stamped
// TxFrom by the single applier — so the store SKIPS the second msgpack pass for
// those rows. It is otherwise identical to PutNodesBatch (same validation,
// duplicate check, indexing, change-log co-commit).
//
// wireBodies[i] is the patched pre-encoded buffer for nodes[i], or nil to signal
// "re-encode node i" — the applier's CONSERVATIVE fallback whenever the buffer
// could not be proven byte-identical to what the store would emit (e.g. a probe
// label token was re-stamped to a different real token at apply). A backend MUST
// treat a nil element exactly as PutNodesBatch would (encode at flush), so the
// persisted bytes are IDENTICAL whether a row arrived pre-encoded or not.
//
// Provenance is by the typed in-process buffer the applier hands down, NEVER by
// sniffing stored bytes: a backend must not decide a row is patchable from the
// wire alone. Only the exact native memory/badger stores implement this; tiered
// and wrapper stores decline (the graph layer type-asserts at wiring and falls
// back to PutNodesBatch). The change-log put body is unaffected — it stays the
// UNTOKENIZED encode-at-flush form so cross-backend feed parity is byte-identical.
type PreEncodedPutCapability interface {
	PutNodesBatchPreEncoded(nodes []*types.Node, wireBodies [][]byte) error
}

// PreEncodedPutLogCapability is OPTIONAL — the change-log extension of
// PreEncodedPutCapability (kept separate so existing implementers stay
// source-compatible). logBodies[i], when non-nil, is the producer-thread
// pre-encoded ChangeNodePut PAYLOAD for nodes[i] (untokenized wire, v2
// temporal tail patched by the applier — the tail is TERMINAL in a create's
// payload, see the storeutil pre-encode) and MUST be used verbatim as the
// record body, skipping the second msgpack pass. A nil element means "build
// the payload at the door" (the conservative fallback), which by the crown
// equivalence produces byte-identical record bytes — cross-backend feed
// parity is unaffected either way. Only meaningful when the store's
// change-log is recording; with the log off, implementations behave exactly
// like PutNodesBatchPreEncoded.
type PreEncodedPutLogCapability interface {
	PutNodesBatchPreEncodedLog(nodes []*types.Node, wireBodies, logBodies [][]byte) error
}

// OwnedPreEncodedPutCapability is OPTIONAL — the OWNERSHIP-TRANSFER variant of
// PreEncodedPutLogCapability for the ingest bulk apply path. It behaves exactly
// like PutNodesBatchPreEncodedLog EXCEPT the caller transfers ownership of the
// nodes: it guarantees it will never read or mutate them again, so the store MAY
// freeze each node IN PLACE and cache it directly instead of deep-copying it
// into the cache. That deep copy (freezeNodeCopy) is the single largest per-node
// allocation on the apply path; skipping it is the point of this capability.
//
// CONTRACT: the store's cached (frozen) entry IS the passed object. The caller
// MUST NOT touch a node after this call — behaviour is undefined otherwise. Use
// ONLY for write-only creates that are never returned to the caller (the ingest
// bulk door, keepResults==false); the graph layer enforces the gate. A store
// that does not implement this is simply used via PutNodesBatchPreEncoded(Log),
// which deep-copies as usual — the capability is a pure allocation optimization,
// never a correctness requirement.
type OwnedPreEncodedPutCapability interface {
	PutNodesBatchOwnedPreEncoded(nodes []*types.Node, wireBodies, logBodies [][]byte) error
}

// HistoryCapability is the version-history surface. Every backend must
// implement it because the graph layer applies pre-mutation snapshots on
// every Update*. The atomic ReplaceWith*History and Delete*WithHistory
// methods exist so backends can avoid the "history written, then crash, then
// data write fails" orphan window. DeleteNodeWithHistory relTombstones must
// cover every live connected relationship exactly once.
type HistoryCapability interface {
	ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error
	ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error

	PutNodeVersion(id types.NodeID, version uint32, n *types.Node) error
	GetNodeVersion(id types.NodeID, version uint32) (*types.Node, error)
	GetNodeHistory(id types.NodeID) ([]*types.Node, error)
	TruncateNodeHistory(id types.NodeID, keepVersions int) error

	PutRelVersion(id types.RelID, version uint32, r *types.Relationship) error
	GetRelVersion(id types.RelID, version uint32) (*types.Relationship, error)
	GetRelHistory(id types.RelID) ([]*types.Relationship, error)
	TruncateRelHistory(id types.RelID, keepVersions int) error

	RemoveNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error
	AddNodeLabelTokenWithHistory(id types.NodeID, tok uint16, updatedNode *types.Node,
		prevVersion uint32, prevState *types.Node) error

	DeleteNodeWithHistory(id types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error
	DeleteRelWithHistory(id types.RelID, prevVersion uint32, tombstone *types.Relationship) error
}

// TransactionTimeQueryCapability is OPTIONAL. Backends that can inspect
// current rows and version history under one backend read window can implement
// this to avoid materializing every history slice for transaction-time reads.
// Single-entity misses return ErrVersionNotFound; bulk queries omit misses and
// return nil, nil when no entity existed at the requested transaction time.
type TransactionTimeQueryCapability interface {
	NodeAsOf(id types.NodeID, txTime types.Instant) (*types.Node, error)
	RelAsOf(id types.RelID, txTime types.Instant) (*types.Relationship, error)
	NodesAsOf(txTime types.Instant) ([]*types.Node, error)
	RelsAsOf(txTime types.Instant) ([]*types.Relationship, error)
}

// LabelTxMembershipCapability is OPTIONAL. A backend that maintains a
// transaction-time label-membership sidecar can enumerate, for a label token,
// the node IDs that EVER carried it — in the current row OR any historical
// version — each tagged with a lower bound on the transaction time of its
// earliest acquisition of the label. This lets the graph layer make a pinned
// label scan OUTPUT-SENSITIVE: the candidate set for a history-aware
// ByLabel scan is scoped to the label's ever-members (O(matches)) instead of
// the whole node-history fold (O(everything that ever carried ANY label)), and
// a member whose earliest acquisition post-dates a transaction-time pin is
// rejected WITHOUT loading its version chain.
//
// Membership is APPEND-ONLY: removing a label from a node, or hard-deleting the
// node, does NOT drop the member — a pin BEFORE the removal/delete must still
// admit it as a candidate. The enumerated set is therefore a sound SUPERSET of
// {nodes whose belief-state version at the pin carried token}; the core chain
// resolver is the correctness authority and rejects the over-included
// candidates, so a spurious member only costs a resolver probe, never a wrong
// result. firstTxFrom is a LOWER bound (0 = unknown → never prune): pruning is
// sound only as `pin < firstTxFrom → skip`.
type LabelTxMembershipCapability interface {
	// ForEachLabelTxMember streams (id, firstTxFrom) for every node that ever
	// carried token. fn returning false stops the scan. Order is unspecified.
	// The sidecar is built lazily on first use, so the first call after open (or
	// after Clear) pays a one-time build; subsequent calls are cheap.
	ForEachLabelTxMember(token uint16, fn func(id types.NodeID, firstTxFrom types.Instant) bool) error
}

// RelTypeTxMembershipCapability is the relationship mirror of
// LabelTxMembershipCapability. A relationship's type is structurally immutable,
// so a rel of type T carried T in EVERY version — its ever-membership is exactly
// {rels ever created with type T}, recorded once at creation. Same append-only
// superset + lower-bound-prune contract.
type RelTypeTxMembershipCapability interface {
	ForEachRelTypeTxMember(token uint16, fn func(id types.RelID, firstTxFrom types.Instant) bool) error
}

// HistoryRollbackTrimCapability is OPTIONAL. It supports transaction rollback
// without eager deep copies of entire history chains. Graph mutation paths
// append superseded versions at the entity's previous Version(); rollback can
// therefore restore the pre-transaction current row and trim history entries
// written at or after that version.
type HistoryRollbackTrimCapability interface {
	TrimNodeHistoryFrom(id types.NodeID, minVersion uint32) error
	TrimRelHistoryFrom(id types.RelID, minVersion uint32) error
}

// MetaKVCapability is OPTIONAL. Backends that can persist arbitrary
// key/value metadata implement it so the graph layer can stamp schema
// versions, migration markers, and other one-shot bookkeeping that has to
// survive restarts. The key namespace is shared between graph-layer markers
// (e.g. "schema_version") and any backend-specific entries — callers MUST
// use distinct keys. MetaGet on a missing key returns nil, nil.
type MetaKVCapability interface {
	MetaGet(key string) ([]byte, error)
	MetaSet(key string, value []byte) error
}

// MetaWrite is one key/value entry for HistoryCompactionCapability. The Key is
// the graph-layer meta key (the same namespace as MetaKVCapability); the backend
// stamps it under its own MetaKey encoding. A nil Value deletes the key.
type MetaWrite struct {
	Key   string
	Value []byte
}

// HistoryCompactionCapability is OPTIONAL. It trims an entity's oldest history
// versions AND persists the accompanying per-entity compaction stub (the
// metaWrites). keepVersions has the same meaning as TruncateNodeHistory: retain
// the newest keepVersions history versions and delete the rest.
//
// Single-shard backends (memory, badger) commit the trim and the metaWrites in
// ONE atomic write, so a crash can never leave trimmed history without its stub
// (a silently-incomplete chain) or a stub without the trim. The tiered backend
// also implements it: because a node's whole chain is single-shard (B33) but the
// stub must live on the reference shard (where the store-level MetaGet reads it),
// the trim and the stub land on different shards and cannot share one batch;
// tiered writes the stub BEFORE the trim so a crash between them fails closed
// (over-rejection, repaired by an idempotent re-run) rather than silently
// incomplete — see pkg/graph/store/tiered/tieredstore_compaction.go. The graph
// watermark is NOT part of metaWrites; the graph layer routes it once via the
// store-level MetaSet (see core/compaction.go advanceCompactionWatermark).
type HistoryCompactionCapability interface {
	CompactNodeHistory(id types.NodeID, keepVersions int, metaWrites []MetaWrite) error
	CompactRelHistory(id types.RelID, keepVersions int, metaWrites []MetaWrite) error
}

// RetentionPurgeResult reports what one PurgeNodesByLabelBefore chunk removed.
// More is true when the label may still hold purgeable nodes below the boundary
// (the caller loops until it is false) — it never under-reports completion.
//
// PurgedNodeIDs lists the node IDs this chunk removed. A single-machine backend
// leaves it nil (its cascade already removed every co-located edge); a PARTITIONED
// backend (sharded) uses it to sweep edges MINTED IN ANOTHER node's slot that
// point at a purged node and therefore live on a different shard — the one edge
// class a per-shard label purge cannot see (an event-as-END cross-shard edge).
//
// PurgedRels lists the relationships this chunk TOUCHED (removed at least one row
// or adjacency leg of). The tiered backend — whose SPLIT-WRITE layout stores a
// rel's entity+out-leg on the start node's shard and its in-leg on the end node's
// shard — uses it to sweep each rel's residue on its OTHER endpoint shard (an
// orphan in-leg, or a fully-local survivor→purged rel), which a per-shard label
// purge leaves behind. Left nil by memory (single store) and unused by sharded
// (co-located legs, node-ID sweep).
type RetentionPurgeResult struct {
	NodesPurged   int
	RelsPurged    int
	More          bool
	PurgedNodeIDs []types.NodeID
	PurgedRels    []PurgedRel
}

// PurgedRel is the routing descriptor of a relationship a purge touched — enough
// for a partitioned backend to locate and clean the rel's residue on either
// endpoint's shard without re-reading the (already-removed) row.
type PurgedRel struct {
	ID        types.RelID
	TypeToken uint16
	StartID   types.NodeID
	EndID     types.NodeID
}

// RetentionPurgeCapability is OPTIONAL (ADR-0008 R2). It HARD-removes whole
// aged-out nodes of a label WITHOUT tombstones — the range-scale removal door for
// event-retention workloads, where a per-entity tombstoning delete would double
// write volume just to delete.
//
// PurgeNodesByLabelBefore removes up to `chunk` nodes carrying labelToken whose
// IMMUTABLE snowflake-ID mint-time is < before, and for each removed node ALSO
// removes: every connected relationship (both adjacency legs — so a surviving
// endpoint's incoming index is cleaned, no phantom edge), ALL index entries
// (label/property/temporal/vector), and the ENTIRE version history of the node
// and each removed relationship. Each call is ONE atomic batch; the caller loops
// on More. It emits NO per-entity change-log record — the graph layer emits ONE
// logical ChangeRangePurge for the whole range and advances the retention
// watermark only after a range is fully clean (crash mid-range re-runs to the
// same end state; the R1 read-guard turns a below-watermark read into
// ErrRetentionExpired). Purge is idempotent: a node already gone is skipped.
//
// The predicate is on snowflake mint-time (not ValidFrom / not a backfilled
// TxFrom) because snowflake IDs are time-ordered, so "label L older than T" is a
// clustered key range, and a backfilled fact below the boundary is rejected at
// WRITE, never silently purged here.
type RetentionPurgeCapability interface {
	PurgeNodesByLabelBefore(labelToken uint16, before types.Instant, chunk int) (RetentionPurgeResult, error)
}

// RetentionPurgeByValidToCapability is OPTIONAL (ADR-0008 R5). It is the ByValidTo
// sibling of RetentionPurgeCapability: it HARD-removes nodes of a label whose
// world-time validity ENDED before the boundary — current-version ValidTo != 0 &&
// ValidTo < before — with the identical cascade/history/atomic-batch/More contract.
// It is a SEPARATE optional interface (not folded into RetentionPurgeCapability) so
// backends can offer age-purge without validity-purge and the addition stays a
// purely additive, non-breaking capability.
//
// The predicate reads the node's CURRENT-version ValidTo. It is immutable-once-true:
// a node that qualifies is CLOSED (ValidTo != 0), and the graph layer freezes a
// closed entity against every interactive mutation door, so a selected victim's
// ValidTo cannot change under a chunked selection — no separate under-lock re-confirm
// is required. A node with an open interval (ValidTo == 0) is never purged by
// validity. Implemented by the native memory + badger backends and fanned out by the
// sharded/tiered stores.
type RetentionPurgeByValidToCapability interface {
	PurgeNodesByLabelValidToBefore(labelToken uint16, before types.Instant, chunk int) (RetentionPurgeResult, error)
}

// RangePurgeLogCapability is OPTIONAL (ADR-0008 R3). It appends ONE
// ChangeRangePurge record — the PREDICATE a replica re-executes — to the store's
// change-log, co-committed like any other record. No-op (nil) when the store has
// no change-log enabled. The graph layer calls it once per purge so a replica of
// the primary converges by re-running the same range predicate against its own
// LSN-consistent state (never per-entity delete records). Implemented by the
// native memory + badger backends alongside RetentionPurgeCapability.
type RangePurgeLogCapability interface {
	LogRangePurge(labelToken uint16, before types.Instant, mode uint8) error
}

// HistoryVersionPageCapability is OPTIONAL. Backends that can page through an
// individual entity's history chain implement it so export and other streaming
// readers do not have to materialize every version for one heavily updated
// entity. startVersion is inclusive, and limit 0 returns all remaining versions.
// Results must be sorted by ascending Version().
type HistoryVersionPageCapability interface {
	NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error)
	RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error)
}

// StatsCapability is the counter surface used by `g.Stats().*` and by the
// graph layer's internal accounting.
type StatsCapability interface {
	NodeCount() (int, error)
	RelationshipCount() (int, error)
	NodeCountByLabel(token uint16) (int, error)
	RelCountByType(token uint16) (int, error)
}

// NodePropertyKeyStatsCapability is an OPTIONAL statistics surface for stores
// that maintain per-label property-key presence counts. The count is over
// current nodes carrying labelToken with an indexable scalar propertyKey value.
// It is intentionally key-presence only, not value-selectivity, so planners can
// cheaply skip labels that cannot satisfy scalar equality lookups.
type NodePropertyKeyStatsCapability interface {
	NodeCountByLabelAndPropertyKey(labelToken uint16, propertyKey string) (int, error)
}

// IterationCapability is the iteration surface (ForEach + paginated AllIDs).
// Distinct from BulkReadCapability because iteration returns IDs only — no
// entity deserialisation, no deep copy. The graph layer uses iteration in
// snapshot/diff and on the export path. Nil ForEach callbacks return
// ErrInvalidStoreMutation. Non-nil callbacks are invoked outside backend locks,
// Badger transactions, and Tiered shard checkouts so callback code may call
// back into Store methods. History iterators must not let callback-created
// higher IDs extend the active iteration, and async-buffered stores must make
// pending history deletes visible to ID scans before flush.
type IterationCapability interface {
	AllNodeIDs(opts QueryOpts) ([]types.NodeID, error)
	AllRelIDs(opts QueryOpts) ([]types.RelID, error)

	AllNodeHistoryIDs() ([]types.NodeID, error)
	AllRelHistoryIDs() ([]types.RelID, error)

	AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error)
	AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error)

	ForEachNodeID(fn func(types.NodeID) bool) error
	ForEachRelID(fn func(types.RelID) bool) error
	ForEachNodeHistoryID(fn func(types.NodeID) bool) error
	ForEachRelHistoryID(fn func(types.RelID) bool) error
}

// DepthHistoryIterationCapability is OPTIONAL. Tiered backends implement it
// so graph-layer history-aware queries can combine temporal filters with
// shard-depth filtering without scanning history from tiers the caller
// excluded. Single-shard backends may omit it because every valid depth
// selector maps to the same shard. Nil callbacks return ErrInvalidStoreMutation.
type DepthHistoryIterationCapability interface {
	ForEachNodeHistoryIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachRelHistoryIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// DeletedIterationCapability is OPTIONAL. Backends implement it so the graph
// layer can fold ONLY deleted IDs (entities with history rows but no current
// row) onto a narrow indexed candidate list when answering temporal adjacency
// or label/property queries at a point/interval in the past. Without it the
// graph layer falls back to iterating the full history set — correct but
// O(total history) per query regardless of how narrow the candidate list is.
//
// Implementations must visit every ID that has at least one history version
// AND no current row, and must not visit any ID that currently exists. Nil
// callbacks return ErrInvalidStoreMutation. Callbacks are invoked outside
// backend locks (same contract as ForEachNodeHistoryID).
type DeletedIterationCapability interface {
	ForEachDeletedNodeID(fn func(types.NodeID) bool) error
	ForEachDeletedRelID(fn func(types.RelID) bool) error
}

// DepthDeletedIterationCapability is the depth-aware counterpart of
// DeletedIterationCapability. Tiered backends implement it so history-aware
// queries can combine deleted-only iteration with shard-depth filtering.
// Single-shard backends do not need it because every valid depth maps to the
// same shard. Nil callbacks return ErrInvalidStoreMutation.
type DepthDeletedIterationCapability interface {
	ForEachDeletedNodeIDByDepth(depth ShardDepth, fn func(types.NodeID) bool) error
	ForEachDeletedRelIDByDepth(depth ShardDepth, fn func(types.RelID) bool) error
}

// PropertyIndexCapability is OPTIONAL. Backends that do not implement
// property indexes should omit it; consumers that need property-indexed
// lookup should type-assert and surface ErrCapabilityNotSupported (or a
// purpose-specific sentinel) when the assertion fails.
//
// Today every Store implementation in this repository implements it; the
// optional-ness is forward-looking for future read-only or analytic
// backends that have no use for write-side index management.
type PropertyIndexCapability interface {
	CreatePropertyIndex(labelToken uint16, propertyKey string) error
	DropPropertyIndex(labelToken uint16, propertyKey string) error
	NodesByLabelAndProperty(labelToken uint16, key string, value any, opts QueryOpts) ([]*types.Node, error)
}

// RelPropertyIndexCapability is the relationship mirror of
// PropertyIndexCapability (Node/Rel parity), keyed by rel-type token instead
// of label token. OPTIONAL — a backend that has no use for accelerated
// rel-value equality lookup may omit it, and the graph layer answers
// RelsByTypeAndProperty by a type-scan + property filter over the mandatory
// RelationshipsByType surface.
//
// The tiered store IMPLEMENTS this capability but its Create/Drop doors return
// ErrRelPropertyIndexUnsupported: a shard-local rel-value index cannot answer a
// query whose matches are scattered across timestamp-routed event shards. Its
// RelationshipsByTypeAndProperty still answers correctly (unaccelerated
// cross-shard scan+filter), so query semantics are uniform across backends.
type RelPropertyIndexCapability interface {
	CreateRelPropertyIndex(relTypeToken uint16, propertyKey string) error
	DropRelPropertyIndex(relTypeToken uint16, propertyKey string) error
	RelationshipsByTypeAndProperty(relTypeToken uint16, key string, value any, opts QueryOpts) ([]*types.Relationship, error)
}

// CompositePropertyIndexCapability is OPTIONAL. A backend implements it to
// accelerate an EQUALITY lookup across an ordered tuple of 2..4 property keys
// under one label with O(matches) cost, instead of the label-scan +
// post-filter every backend must still support for correctness (see
// NodesByLabelAndProperties's doc comment). v1 is equality-only — no
// partial-prefix or range semantics (a query must supply a value for every
// declared key); see docs/query-planners.md "Composite property indexes" for
// planner guidance on when this beats a single-key index + post-filter.
//
// Unlike PropertyIndexCapability, this is NOT embedded in Store: a backend
// that never implements it (e.g. a sharded backend that only supports
// reference-label single-key indexes today) still satisfies Store/
// MandatoryStore unchanged, and the graph layer answers
// NodesByLabelAndProperties correctly (unaccelerated) via its own
// label-scan + post-filter fallback.
type CompositePropertyIndexCapability interface {
	// CreateCompositePropertyIndex creates a composite index over the
	// declared, ORDER-PRESERVING keys (2..4) under labelToken. Returns
	// ErrIndexExists if an index for the exact same (labelToken, ordered
	// keys) already exists — a different key ORDER for the same key SET is
	// a distinct definition (no implicit dedup across orderings).
	CreateCompositePropertyIndex(labelToken uint16, keys []string) error
	// DropCompositePropertyIndex removes the composite index declared over
	// the exact ordered keys. Returns ErrIndexNotFound if no such
	// definition exists.
	DropCompositePropertyIndex(labelToken uint16, keys []string) error
	// NodesByLabelAndProperties returns nodes carrying labelToken whose
	// current row matches EVERY (key, value) pair in values (AND-conjunction,
	// equality only). All of a matching composite index's declared keys must
	// be present in values for the index to accelerate the call; otherwise
	// (or when no matching composite index exists) implementations fall back
	// to a label-scan + post-filter, mirroring PropertyIndexCapability's
	// internal fallback contract.
	NodesByLabelAndProperties(labelToken uint16, values map[string]any, opts QueryOpts) ([]*types.Node, error)
}

// CompositeIndexIntrospectionCapability is OPTIONAL — the query-planner
// existence door for composite definitions. A planner must KNOW whether a
// matching composite definition exists before routing a multi-property
// equality match through NodesByLabelAndProperties: without one, that door
// falls back to a label scan + post-filter, which can be strictly worse than
// the single-key property-index plan the planner would otherwise choose.
// Kept separate from CompositePropertyIndexCapability so existing out-of-tree
// implementations of that interface stay source-compatible.
type CompositeIndexIntrospectionCapability interface {
	// ListCompositePropertyIndexes returns the DECLARED, ORDER-PRESERVING
	// key tuple of every composite index registered under labelToken (one
	// entry per definition; distinct orderings of the same key set are
	// distinct definitions). Unregistered labels return an empty slice, not
	// an error. The returned slices are caller-owned copies. O(definitions
	// on the label) — cheap enough to call per query; there is NO index-DDL
	// epoch/invalidation signal, so cache-averse callers should simply call
	// it each time.
	ListCompositePropertyIndexes(labelToken uint16) ([][]string, error)
}

// PropertyIndexIntrospectionCapability is OPTIONAL — the query-planner
// existence door for single-key property index definitions (BACKLOG 21b),
// mirroring CompositeIndexIntrospectionCapability's role for composites: a
// planner must know whether an accelerated path exists before routing a
// single-property predicate, rather than inferring it from query latency.
type PropertyIndexIntrospectionCapability interface {
	// HasPropertyIndex reports whether a property index exists on
	// (labelToken, propertyKey). Unregistered labels return false, not an
	// error. O(1) — no index-DDL epoch/invalidation signal, call per plan.
	HasPropertyIndex(labelToken uint16, propertyKey string) (bool, error)
}

// TemporalIndexIntrospectionCapability is OPTIONAL — the query-planner
// existence door for temporal interval index definitions (BACKLOG 21b). Only
// one temporal index KIND (interval or high-frequency) can exist per label
// (see HighFrequencyIndexCapability), so this answers "interval kind
// specifically", not "any temporal acceleration exists".
type TemporalIndexIntrospectionCapability interface {
	// HasTemporalIndex reports whether a temporal interval index exists on
	// labelToken. Unregistered labels return false, not an error.
	HasTemporalIndex(labelToken uint16) (bool, error)
}

// VectorIndexInfo is the declared configuration of a vector index — the read
// side of VectorIndexOptions plus the two fields fixed at creation
// (Dims/Metric) that CreateVectorWithOptions takes as separate parameters
// rather than folding into VectorIndexOptions itself.
type VectorIndexInfo struct {
	Dims    int
	Metric  DistanceMetric
	Options VectorIndexOptions
}

// VectorIndexIntrospectionCapability is OPTIONAL — the query-planner
// existence+config door for vector index definitions (BACKLOG 21b). A
// planner needs BOTH existence and dims/metric/engine BEFORE issuing
// SearchNearest, so a dimension mismatch or an unexpectedly-brute-force
// index is a routing decision, not a runtime error discovered mid-query.
type VectorIndexIntrospectionCapability interface {
	// VectorIndexInfo returns the declared configuration of the vector index
	// on (labelToken, propertyKey), or (zero value, false) if none exists.
	// Reflects the DEFINITION only (dims/metric/engine/tuning), never a
	// runtime rebuild-status signal — see CLAUDE.md "Vector Indexes: Not
	// persisted" for what does and does not survive restart.
	VectorIndexInfo(labelToken uint16, propertyKey string) (VectorIndexInfo, bool, error)
}

// RelPropertyIndexIntrospectionCapability is OPTIONAL — the query-planner
// existence door for relationship-type-scoped property index definitions
// (BACKLOG 21b), mirroring PropertyIndexIntrospectionCapability for the rel
// side.
type RelPropertyIndexIntrospectionCapability interface {
	// HasRelPropertyIndex reports whether a relationship property index
	// exists on (relTypeToken, propertyKey). Unregistered rel types return
	// false, not an error.
	HasRelPropertyIndex(relTypeToken uint16, propertyKey string) (bool, error)
}

// PropertyTypeClassCounts is the EXACT per-(label, property key) partition of
// a label's current nodes by the type class of the key's value
// (types.PropertyTypeClass — see its doc for the classification rule).
// Maintained counters, O(1) to read, exact by construction: they are adjusted
// on the SAME node-mutation doors (same call, same loop) as the
// NodeCountByLabelAndPropertyKey presence counter, so the two can never
// drift. Every field counts NODES (a node contributes to exactly one class
// per key it carries).
//
// Missing (nodes carrying the label WITHOUT the key) is computed by the
// graph layer as NodeCountByLabel − (Numeric+NaN+String+Bool+Other); store
// implementations always return it as 0.
type PropertyTypeClassCounts struct {
	Numeric int64 // finite ints/uints/floats and ±Inf (orderable numbers)
	NaN     int64 // float NaN values (numeric kind, unorderable)
	String  int64
	Bool    int64
	Other   int64 // slices (incl. []float32/[]byte), maps, registered structs
	Missing int64 // graph-layer computed; always 0 at the store boundary
}

// Present returns the number of nodes carrying the key at all (any class).
func (c PropertyTypeClassCounts) Present() int64 {
	return c.Numeric + c.NaN + c.String + c.Bool + c.Other
}

// NodePropertyTypeClassCountsCapability is OPTIONAL — the exact O(1)
// type-class cardinality door for query planners (ordering-soundness gates
// like "the gap between label count and numeric count is nulls only" need
// EXACT counts; the HLL-based NDV statistics are not usable there). See
// PropertyTypeClassCounts for semantics.
type NodePropertyTypeClassCountsCapability interface {
	NodePropertyTypeClassCounts(labelToken uint16, propertyKey string) (PropertyTypeClassCounts, error)
}

// RelPropertyTypeClassCountsCapability is OPTIONAL — the relationship mirror of
// NodePropertyTypeClassCountsCapability (rule 2): the exact per-(relType, property
// key) partition of a type's current relationships by value class, the correctness
// gate for the rel ORDER BY r.prop LIMIT k push-down. Implemented by the native
// memory + badger stores; tiered/sharded decline (rel property indexes — the whole
// rel-ordering path — are RAM-only per-shard, so the primitive is memory/badger
// only). See PropertyTypeClassCounts for semantics.
type RelPropertyTypeClassCountsCapability interface {
	RelPropertyTypeClassCounts(relTypeToken uint16, propertyKey string) (PropertyTypeClassCounts, error)
}

// TemporalIndexCapability is OPTIONAL — see the note on
// PropertyIndexCapability.
type TemporalIndexCapability interface {
	CreateTemporalIndex(labelToken uint16) error
	DropTemporalIndex(labelToken uint16) error
}

// TemporalCandidateCapability is OPTIONAL (B4). It lets the graph layer prune a
// temporal query's candidate node-ID set against the per-label valid-time ENVELOPE
// index — the valid-time analogue of the K1 label-membership sidecar. It is SOUND
// for any store to implement: an id the index does not cover is always kept, so an
// incomplete or wrapper-inherited index costs pruning recall, never correctness
// (the chain resolver stays authoritative). A store without a temporal index for
// the label returns ok=false, and the caller keeps every candidate.
type TemporalCandidateCapability interface {
	// PruneTemporalCandidates returns the subset of ids that may still satisfy the
	// valid-time filter in opts (ValidAt point, or ValidStart/ValidEnd interval): an
	// id whose envelope is present AND provably cannot overlap the query is dropped;
	// every other id is kept. ok is false when no temporal index covers labelToken
	// or opts carries no valid-time filter — the caller then keeps ids unchanged.
	PruneTemporalCandidates(labelToken uint16, ids []types.NodeID, opts QueryOpts) (kept []types.NodeID, ok bool)
}

// RelTypeTemporalIndexCapability is OPTIONAL — the relationship-type mirror of
// TemporalIndexCapability (BACKLOG 21c), keyed by rel-type token instead of
// label token, in its own independent index namespace.
type RelTypeTemporalIndexCapability interface {
	CreateRelTemporalIndex(relType uint16) error
	DropRelTemporalIndex(relType uint16) error
}

// RelTypeTemporalCandidateCapability is OPTIONAL (B4 rel-side mirror,
// BACKLOG 21c). It lets the graph layer prune a temporal query's candidate
// relationship-ID set against the per-rel-type valid-time ENVELOPE index, the
// same sound-superset contract as TemporalCandidateCapability: an id the
// index does not cover is always kept, so an incomplete index costs pruning
// recall, never correctness.
type RelTypeTemporalCandidateCapability interface {
	// PruneRelTypeTemporalCandidates returns the subset of ids that may still
	// satisfy the valid-time filter in opts. ok is false when no temporal index
	// covers relType or opts carries no valid-time filter — the caller then
	// keeps ids unchanged.
	PruneRelTypeTemporalCandidates(relType uint16, ids []types.RelID, opts QueryOpts) (kept []types.RelID, ok bool)
}

// VectorIndexCapability is OPTIONAL. The reference implementation is a
// brute-force in-memory k-NN. Backends backed by a remote vector store
// should plug in here without having to implement the rest of Store.
// Implementations must reject indexed vectors and search queries containing
// NaN or infinity with ErrInvalidVectorValue.
type VectorIndexCapability interface {
	CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error
	DropVectorIndex(labelToken uint16, propertyKey string) error
	SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error)
}

// VectorIndexOptions configures the search engine and tuning chosen at
// CreateVectorIndex time. Zero values select the documented defaults: the
// approximate HNSW engine (see pkg/graph/internal/index/hnsw.go) with
// M=16, EfConstruction=200, EfSearch=64 — see CLAUDE.md "Vector Indexes".
// Additive: a backend need not implement VectorIndexOptionsCapability to
// remain a conformant Store — it simply keeps handling CreateVectorIndex
// through the mandatory VectorIndexCapability door with its own default
// engine, and callers reaching for the options door on such a backend
// silently fall back to plain CreateVectorIndex (opts unavailable).
type VectorIndexOptions struct {
	// UseBruteForce selects the exact linear-scan k-NN engine instead of
	// the default approximate HNSW engine — the escape hatch for
	// exact-recall requirements (compliance workloads, small indexes,
	// correctness oracles).
	UseBruteForce bool

	// HNSW tuning. Ignored when UseBruteForce is true. Zero = default.
	M              int // Max bidirectional links per node above layer 0. Default 16.
	EfConstruction int // Candidate list size while building. Default 200.
	EfSearch       int // Candidate list size while searching. Default 64.
}

// VectorIndexOptionsCapability is OPTIONAL. A backend implementing it
// accepts the engine/tuning choice in VectorIndexOptions at creation time
// in addition to the mandatory VectorIndexCapability.CreateVectorIndex
// door (which keeps working — it now defaults to the same HNSW engine with
// VectorIndexOptions{} i.e. all-default tuning). Every in-tree backend
// (memory/badger/tiered) implements this capability.
type VectorIndexOptionsCapability interface {
	CreateVectorIndexWithOptions(labelToken uint16, propertyKey string, dims int, metric DistanceMetric, opts VectorIndexOptions) error
}

// FilteredVectorSearchCapability is OPTIONAL but strongly recommended for
// any backend that exposes vector search under temporal filters or any
// other graph-layer eligibility predicate. With it, the graph pushes the
// eligibility filter into the search BEFORE the k-cut so the top-k is
// taken from the eligible-only set.
//
// Backends that cannot natively pre-filter still expose useful behaviour
// through `SearchNearestNodes`; the graph-layer fallback performs
// iterative over-fetch (k → 2k → 4k …, clamped to the package-internal
// overfetchCeiling = 65536) until k eligible results are accumulated or
// the backend exhausts. For k > overfetchCeiling, the fallback returns
// at most overfetchCeiling eligible matches; backends that need to serve
// larger k correctly should implement this capability so the graph can
// skip the loop and rely on the backend's pre-filtered top-k.
//
// snowflake.ID is the raw entity-ID type used by the predicate so the
// hook is independent of the typed NodeID/RelID wrappers.
type FilteredVectorSearchCapability interface {
	SearchNearestFiltered(labelToken uint16, propertyKey string, query []float32, k int, filter func(rawID snowflake.ID) bool) ([]snowflake.ID, error)
}

// HighFrequencyIndexCapability is OPTIONAL — see the note on
// PropertyIndexCapability. Implementations that have no need for the
// time-bucketed amortised insertion path may omit it. Implementations must
// reject bucket sizes that are not positive whole milliseconds with
// ErrInvalidTemporalIndexConfig.
type HighFrequencyIndexCapability interface {
	CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error
	DropHighFrequencyIndex(labelToken uint16) error
}

// MandatoryStore is the embedded composition of capabilities every
// implementation MUST satisfy. The graph layer's Core type narrows its
// store field to MandatoryStore and type-asserts for optional capabilities
// at the call sites that need them, returning ErrCapabilityNotSupported
// (wrapped with the missing-capability name) on assertion miss.
//
// Out-of-tree backends that need to implement only a subset of
// functionality can satisfy MandatoryStore alone; they will work for the
// graph's core CRUD/history/iteration/stats surface and will surface
// ErrCapabilityNotSupported for index operations they do not support.
type MandatoryStore interface {
	Lifecycle
	NodeCRUDCapability
	RelationshipCRUDCapability
	AdjacencyCapability
	BulkReadCapability
	BatchCapability
	HistoryCapability
	StatsCapability
	IterationCapability
}
