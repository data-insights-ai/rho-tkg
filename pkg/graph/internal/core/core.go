// Package core holds the implementation behind *graph.Graph. It is internal
// because customers only ever see the thin facade in pkg/graph; everything
// load-bearing lives here.
//
// All fields, all methods, all package-level helpers (validation, snowflake,
// resolution helpers, sentinel errors needed by methods) live here. The
// pkg/graph package re-exports the few symbols customers must reach.
package core

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/dgraph-io/badger/v4/options"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/locks"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	snowflakepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/snowflake"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Core struct (was *graph.Graph)
// =============================================================================

// Core is the central graph implementation. Customers see *graph.Graph, which
// is a thin facade holding *Core plus sub-API accessors.
type Core struct {
	labels    *registrypkg.LabelRegistry
	relTypes  *registrypkg.RelTypeRegistry
	propKeys  *registrypkg.PropertyKeyRegistry
	nodeIDGen *snowflake.Node
	relIDGen  *snowflake.Node
	// laneGenerators holds the per-lane UNIFIED ID generators built when
	// Config.IngestLanes > 0 (ADR-0007 S4). Index 0 is lane 1, index k-1 is
	// lane k; each generator mints BOTH nodes and rels from its own distinct
	// node-field (slot). Nil / empty when IngestLanes == 0 (legacy dual model).
	laneGenerators []*snowflake.Node
	// laneSlots[i] is the node-field (slot) carried by laneGenerators[i] — kept
	// for diagnostics / docs (the sharded store routes an ID to a shard by this
	// slot). Same length as laneGenerators.
	laneSlots             []uint8
	store                 storepkg.MandatoryStore
	generatedCreate       generatedcreate.Capability
	endpointHash          storepkg.EndpointIntegrityHashCapability
	endpointHashWrite     generatedcreate.RelationshipEndpointHashCapability
	foreignEndpointRel    generatedcreate.ForeignEndpointRelCapability
	foreignIncomingRel    generatedcreate.ForeignIncomingRelCapability
	nodeHash              storepkg.NodeIntegrityHashCapability
	txTimeQuery           storepkg.TransactionTimeQueryCapability
	txTimeQueryCopy       bool
	historyTrim           storepkg.HistoryRollbackTrimCapability
	propertyQuery         storepkg.PropertyIndexCapability
	propertyQueryTrust    bool
	relPropertyQuery      storepkg.RelPropertyIndexCapability
	relPropertyQueryTrust bool
	compositeQuery        storepkg.CompositePropertyIndexCapability
	compositeQueryTrust   bool
	filteredVector        storepkg.FilteredVectorSearchCapability
	vectorIndexOptions    storepkg.VectorIndexOptionsCapability
	depthHistory          storepkg.DepthHistoryIterationCapability
	deletedIter           storepkg.DeletedIterationCapability
	deletedDepthIter      storepkg.DepthDeletedIterationCapability
	// transaction-time membership sidecars: scope a pinned label/type scan's
	// candidate set to the label/type's ever-members (O(matches)) instead of the
	// whole node/rel-history fold (O(everything ever)). nil = store declines
	// (tiered), so the query falls back to the full-history candidate fold.
	labelTxMembers   storepkg.LabelTxMembershipCapability
	relTypeTxMembers storepkg.RelTypeTxMembershipCapability
	// belief watermarks (BACKLOG 10c): a store that maintains, per entity, the
	// maximum TxFrom ever recorded across its whole version chain lets
	// nodeAtLockedTx/relAtLockedTx take a SAFE current-row-only fast path for
	// point-in-time queries — see store.NodeBeliefWatermarkCapability's doc
	// comment. nil = store declines (tiered, sharded today), so every query
	// resolves through the full chain scan (correct, unaccelerated).
	nodeBeliefWatermark storepkg.NodeBeliefWatermarkCapability
	relBeliefWatermark  storepkg.RelBeliefWatermarkCapability
	// temporalCandidates — valid-time candidate prune: narrow a temporal
	// ByLabel/ByType query's candidate set by the per-label valid-time envelope
	// index before resolving each chain. nil = store declines (no temporal-index
	// support), so the query resolves every candidate (correct, unaccelerated).
	// Sound for any store: an id the index does not cover is never pruned.
	temporalCandidates storepkg.TemporalCandidateCapability
	// relTypeTemporalCandidates — BACKLOG 21c, the rel-type-keyed mirror of
	// temporalCandidates. nil = store declines (tiered, sharded today), so the
	// rel-by-type temporal query resolves every candidate (correct,
	// unaccelerated). Same sound-superset contract.
	relTypeTemporalCandidates storepkg.RelTypeTemporalCandidateCapability
	// preEncodedPut — ADR-0006 §4.5 Scenario B: the ingest applier hands the
	// store a v2 entity-row wire pre-encoded on the producer thread (tail patched
	// with the stamped TxFrom) instead of a second msgpack pass. nil = store
	// declines (tiered, wrappers), so the applier uses the encode-at-flush
	// PutNodesBatch path. Native memory/badger only.
	preEncodedPut storepkg.PreEncodedPutCapability
	// ownedPut — lever #2 ownership-transfer put. Set for any store implementing
	// OwnedPreEncodedPutCapability (badger, sharded), independent of the
	// badger-only preEncodedPut gate. nil = store copies as usual (memory,
	// tiered, wrappers). Used only by the concurrent ingest bulk apply.
	ownedPut         storepkg.OwnedPreEncodedPutCapability
	changeFeed       storepkg.ChangeFeedCapability
	changeLogEnabled bool                      // store's change-log actually on (records emitted)
	txLogScope       storepkg.TxChangeLogScope // per-tx record buffer (nil when off / unsupported)
	// scopedChangeLog (BACKLOG 11f) — the token-routed multi-scope mechanism.
	// Set ONLY when the store implements the FULL storepkg.ScopedTxCapability
	// (every door's Scoped sibling AND ScopedTxChangeLog together) — partial
	// support is deliberately never granted the fast path, see
	// ScopedTxCapability's doc comment. When non-nil, GraphTx uses this
	// INSTEAD of txLogScope: BeginTx opens a per-tx token via BeginScopedLog,
	// every tx mutation method threads that token through ctx (scopeTokenFrom)
	// so the *ScopedAware store wrappers route each record into the token's
	// own buffer, and lockActiveCoreWrite takes only a shared RLock — the
	// token-keyed routing (not a single shared divert flag) is what makes the
	// exclusive lock unnecessary. txLogScope and its SetLogDivert mechanism
	// stay completely unused for a GraphTx once this is set (BatchBuilder.
	// Execute is unaffected either way — it still uses txLogScope always,
	// out of scope for this flip).
	scopedChangeLog storepkg.ScopedTxCapability
	// Metadata facet (ADR-0003) — durable arbitrary-KV + atomic history
	// compaction. Both are bare optional capabilities (no wrapper-visibility
	// dance): the store is fixed after New, so a nil handle is exactly the
	// pre-consolidation "_, ok := c.store.(X) → !ok" decline, byte-for-byte.
	// Resolved once here instead of at ~23 scattered call sites (the largest
	// ad-hoc-assert sprawl STAGE 0 identified).
	metaKV                storepkg.MetaKVCapability
	historyCompaction     storepkg.HistoryCompactionCapability
	retentionPurge        storepkg.RetentionPurgeCapability
	retentionPurgeValidTo storepkg.RetentionPurgeByValidToCapability
	rangePurgeLog         storepkg.RangePurgeLogCapability
	readOnlyReplica       bool
	// allowRetentionPurge gates the ADR-0008 R2 hard-purge admin door
	// (g.Admin().PurgeExpiredNodes). Off by default — a destructive, no-tombstone
	// range removal must be explicitly enabled. Wired from Config.AllowRetentionPurge.
	allowRetentionPurge bool
	// allowReset gates the whole-graph destructive wipe door (g.Admin().Reset()).
	// Off by default. Wired from Config.AllowReset.
	allowReset bool
	// allowTxBackfill enables the privileged transaction-time backfill door:
	// when true, create doors honor a caller-supplied tkg_tx_from (or
	// AddWithTx) instead of stamping c.now(), so a re-ingest can faithfully
	// reproduce a historical knowledge time (Erkenntniszeit) addressable via
	// AS OF SYSTEM TIME. Off by default (production rejects backfill with
	// ErrTxBackfillDisabled). Wired from Config.AllowTxBackfill; set once in New.
	allowTxBackfill bool
	replSource      storepkg.ReplicationSource
	replSourceMu    sync.RWMutex
	vectorRowsTrust bool
	storeRowsTrust  bool
	nativeAdjacency bool
	// bitemporalMigrated is true after the one-shot inherited-ValidFrom
	// migration has run successfully on this store. When false, the resolver
	// keeps the legacy inheritance heuristic active for back-compat.
	bitemporalMigrated bool
	entityLocks        *locks.Manager
	validation         ValidationLimits
	constraints        ConstraintSet
	events             eventspkg.Publisher
	txEventBuffer      *[]eventspkg.Event
	// mu is a striped RWMutex (ADR-0007 lever #1): exact sync.RWMutex semantics
	// (a writer via Lock excludes ALL readers), but the reader fast path fans
	// out across stripes so concurrent-ingest lanes taking c.mu.RLock do not
	// serialize on one reader-count cache line. Hot ingest paths call
	// RLockShard(lane)/runUnderRLockShard; every other reader uses the drop-in
	// RLock()/RUnlock() (stripe 0) and every writer uses Lock()/Unlock().
	mu shardedRWMutex
	// txMu serializes transaction/batch lifecycles (BeginTx → Commit/Rollback
	// and BatchExecute). Held for the entire tx/batch lifetime instead of
	// c.mu.Lock (which was held in v3.4 / v4.0.x). Standalone mutations and
	// reads acquire c.mu.RLock and proceed concurrently with an open tx —
	// only entity-level conflicts now block, not the whole graph. This
	// closes the "read accessor inside a tx deadlocks against c.mu" bug
	// class introduced in v3.4 (see lesson 31). The tx code path takes a
	// brief c.mu.RLock around each mutation/read so the *Internal/*Locked
	// helpers run with a non-zero lock context they expect.
	txMu       sync.Mutex
	registryMu sync.Mutex
	// asofMu serializes the read-modify-write of the durable named as-of-tag
	// map (asof_tags MetaKV entry) so concurrent TagAsOf/RemoveAsOfTag calls
	// cannot lose updates. Taken only by the WRITERS; readers (ResolveAsOf /
	// AsOfTags) are lock-free because MetaGet returns an atomic snapshot.
	asofMu sync.Mutex
	// valueLocks stripes unique-property-constraint enforcement by
	// (labelToken, keyToken, canonical value). Lock order: entity -> value ->
	// idxMu (see internal/locks.ValueManager and CLAUDE.md).
	valueLocks *locks.ValueManager
	// uniqueMu guards the in-memory unique-constraint registry (uniqueConstraints).
	// Read on every constrained write (fast-path gated by hasUniqueConstraints);
	// written by CreateUnique / DropUnique / reset.
	uniqueMu sync.RWMutex
	// uniqueConstraints maps labelToken -> propertyKey -> constraint state.
	// UniqueCurrent and UniqueForever scopes are implemented (UniqueValidOverlap
	// is reserved). Durable copy lives in MetaKV under uniqueConstraintsMeta;
	// this in-memory map is loaded at open and kept in sync by the write doors.
	uniqueConstraints map[uint16]map[string]*uniqueConstraintState
	// hasUniqueConstraints is the lock-free fast path: when false, the write
	// doors skip the uniqueMu.RLock + enforcement entirely.
	hasUniqueConstraints atomic.Bool
	// uniqueOwners is the durable UniqueForever value-ownership registry
	// (ADR-0002 Stage F): ownerKey(labelTok, propKey, valueKey) -> owning NodeID.
	// A value once claimed is barred from every OTHER entity forever, across
	// delete and reopen. Guarded by uniqueMu; durable copy lives in MetaKV under
	// uniqueForeverOwnersMeta with a self-hash (loaded at open, reaped by Reset).
	uniqueOwners map[string]types.NodeID
	// compactedThroughTx is the graph-level history-compaction watermark
	// (CompactedThroughTx = max over per-entity stubs of LastTrimmedTxTo),
	// loaded from MetaKV at open and advanced by CompactHistory*. It is the
	// lock-free fast gate for every temporal read: when it is 0 no compaction
	// has ever run, so point/scan doors skip the per-entity stub probe entirely.
	// A pin (TxAt / TxPin / txTime) at or above the watermark cannot require any
	// trimmed version. See compaction.go.
	compactedThroughTx atomic.Int64
	// retentionMaxWatermark is the graph-level maximum over all per-label
	// retention watermarks (ADR-0008), loaded from MetaKV at open and advanced by
	// advanceRetentionWatermark. Like compactedThroughTx it is the lock-free fast
	// gate for every temporal read: 0 means no retention watermark has ever been
	// set, so point/scan doors skip the retention probe entirely, and a pin at or
	// above it cannot precede any label's watermark. See retention.go.
	retentionMaxWatermark atomic.Int64
	// asOfColumns caches the columnar as-of snapshot of a label's members at a fixed
	// past txAt, invalidated only by history rewrites (compaction / retention purge /
	// truncate / past-dated backfill or replica apply) — a past belief is immutable
	// under forward ingest, so the cache survives write-active ingest. See
	// docvalues_asof_cache.go and buildAsOfColumns.
	asOfColumns    *asOfColumnCache
	registryDirty  atomic.Bool
	relTypeCache   map[string]uint16
	relTypeCacheMu sync.RWMutex
	closeOnce      sync.Once
	closed         atomic.Bool

	// clock is the time source used by every mutation path that stamps
	// TxFrom / UpdatedAt / DeletedAt / event.Timestamp. Defaults to
	// time.Now in New(); c.now() makes the observed instant monotonic
	// per Core so fast same-millisecond mutations still get ordered
	// transaction intervals. Test helpers swap it for a deterministic
	// counter without relying on wall-clock sleeps. Only ever
	// read from goroutines that hold the appropriate Core lock — the
	// value is set once in New and (in tests) replaced under exclusive
	// access.
	clock func() time.Time
	// lastInstant stores the last millisecond instant handed out by c.now().
	// It is atomic because event publishing and read-side helpers can call
	// c.now() from different goroutines even though mutation state is guarded.
	lastInstant atomic.Int64

	// floorSeedUnreadable records that seedInstantFloor could not READ the
	// durable commit-clock watermark at open (as opposed to reading a malformed
	// one). Close then leaves the watermark alone rather than overwriting a
	// high-water mark this session never saw. Written once during New before the
	// graph is published; read once in Close.
	floorSeedUnreadable bool

	indexProviders map[string]*indexProviderEntry

	opNodeAdds    atomic.Int64
	opNodeReads   atomic.Int64
	opNodeUpdates atomic.Int64
	opNodeDeletes atomic.Int64
	opRelAdds     atomic.Int64
	opRelReads    atomic.Int64
	opRelUpdates  atomic.Int64
	opRelDeletes  atomic.Int64

	// Sub-Core groupings — narrow operation surfaces that mirror the public
	// sub-API field grouping on *graph.Graph. Wired in New().
	Nodes       *NodeOps
	Rels        *RelOps
	Temporal    *TempOps
	Index       *IndexOps
	Events      *EventOps
	Admin       *AdminOps
	Constraints *ConstraintOps
	Hash        *HashOps
	IO          *IOOps
	Resolve     *ResolveOps
	Stats       *StatOps
	Repl        *ReplOps
	Ingest      *IngestOps

	// Ingest pipeline (ADR-0006 stage 1). ingest is the lazily-started single
	// applier goroutine; ingestMu guards its lifecycle (start on first session,
	// stop at Close). ingestClosing is set under ingestMu by stopIngestApplier so
	// a NewSession racing Close can NEVER lazily start a fresh applier AFTER the
	// stop swept c.ingest — such an applier would be orphaned (never stopped) and
	// would apply groups against the closing graph (C1 lifecycle race).
	ingestMu      sync.Mutex
	ingest        *ingestApplier
	ingestClosing bool
	// ingestLaneCtr mints nonzero lane identifiers for CONCURRENT ingest
	// sessions (§14 concurrent mode); lane 0 is the strong-mode applier.
	ingestLaneCtr atomic.Uint32
}

// =============================================================================
// Errors
// =============================================================================

var (
	ErrNoLabels        = errors.New("graph: node requires at least one label")
	ErrNilNode         = types.ErrNilNode
	ErrNilRelationship = types.ErrNilRelationship
	ErrZeroID          = errors.New("graph: zero ID is not valid for import")
	ErrInvalidID       = errors.New("graph: invalid ID is not valid for import")
	ErrVersionOverflow = errors.New("graph: entity version overflow")
	ErrNotTieredStore  = errors.New("graph: operation requires tiered.Store")
	ErrAlreadyClosed   = errors.New("graph: entity already closed")
	ErrGraphClosed     = errors.New("graph: graph is closed")
	ErrReadOnlyReplica = errors.New("graph: read-only replica (writes go to the primary)")
	ErrNilGraph        = grapherr.ErrNilGraph
	ErrNilStore        = storepkg.ErrNilStore
	ErrNilContext      = errors.New("graph: context must not be nil")
	ErrNilTxCallback   = errors.New("graph: transaction callback must not be nil")
	ErrLabelNotFound   = errors.New("graph: node does not have the specified label")
	ErrLastLabel       = errors.New("graph: cannot remove the last label from a node")
	ErrBatchFailed     = errors.New("graph: batch execution had failed operations")
	ErrBatchDone       = errors.New("graph: batch already executed")
)

var (
	ErrTooManyLabels     = errors.New("graph: too many labels")
	ErrTooManyProperties = errors.New("graph: too many properties")
	ErrKeyTooLong        = errors.New("graph: property key too long")
	ErrValueTooLarge     = errors.New("graph: property value too large")
	ErrNameTooLong       = errors.New("graph: name too long")
	ErrSelfLoop          = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")

	// ErrValidFromBeforePrevious is returned by Update when the caller-supplied
	// tkg_valid_from is <= the previous version's effective ValidFrom. This
	// would create a backwards interval on the previous version's tile and is
	// rejected. Bitemporally correct backdating must use Phase 3's cascade
	// edit (SetVersionInterval) instead.
	ErrValidFromBeforePrevious = errors.New("graph: tkg_valid_from must be greater than previous version's effective ValidFrom")

	// ErrConflictingTemporalOpts is returned by a generic query door
	// (ByLabel / ByType / All and their property/vector siblings) when
	// QueryOpts.TxPin is set together with any other temporal filter. TxPin is
	// a pure knowledge-time belief-state pin (identical to NodesAsOf) and does
	// NOT valid-time filter; combining it with ValidAt / ValidStart / ValidEnd
	// (valid-time) or with TxAt (the combined bitemporal door) is contradictory,
	// so the query fails loudly rather than silently mis-resolving.
	ErrConflictingTemporalOpts = errors.New("graph: QueryOpts.TxPin is mutually exclusive with ValidAt / ValidStart / ValidEnd / TxAt")

	// ErrTxBackfillDisabled is returned by a create door when a caller supplies
	// a transaction-time override (tkg_tx_from property or AddWithTx) but the
	// graph was not opened with Config.AllowTxBackfill. TxFrom is normally
	// system-stamped; backdating it is a privileged, deliberately-gated ingest
	// operation (§4.1 — reproduce a historical Erkenntniszeit).
	ErrTxBackfillDisabled = errors.New("graph: transaction-time backfill is disabled (set Config.AllowTxBackfill to permit tkg_tx_from / AddWithTx)")

	// ErrInvalidTxFrom is returned when a backfilled transaction time is not a
	// positive Unix-millisecond instant not in the future (0 means "unset — use
	// the system clock"; a negative value is malformed; a value greater than the
	// current clock is incoherent — a knowledge/transaction time cannot exceed
	// wall-clock at write, and the feature is backfill).
	ErrInvalidTxFrom = errors.New("graph: backfilled tkg_tx_from must be a positive instant not in the future")

	// ErrInvalidClockAdvance is returned by TempOps.AdvanceClock when the
	// caller-supplied floor target lands implausibly far ahead of wall-clock
	// (see maxClockAdvanceSkewMillis) — the same bug class lesson 59 closed for
	// AllowTxBackfill. AdvanceClock has no upper-bound protection otherwise: a
	// single bad call (e.g. a unit mixup, lesson 59's exact trigger) would
	// permanently poison the transaction clock for the process's life.
	ErrInvalidClockAdvance = errors.New("graph: AdvanceClock target is implausibly far ahead of wall-clock")

	// ErrInvalidAsOfTag is returned by TagAsOf for a blank tag name or a
	// non-positive instant.
	ErrInvalidAsOfTag = errors.New("graph: as-of tag requires a non-blank name and a positive instant")

	// ErrTooManyAsOfTags is returned by TagAsOf when the durable tag registry
	// is at capacity (maxAsOfTags distinct names).
	ErrTooManyAsOfTags = errors.New("graph: too many as-of tags")

	// ErrUniqueViolation is returned by a create/update/label-add door when a
	// write would make two current nodes carry the same value for a constrained
	// (label, property). Wrapped with the label, key, and winning entity ID.
	ErrUniqueViolation = errors.New("graph: unique constraint violation")

	// ErrUniqueViolationExisting is returned by CreateUnique when existing data
	// already contains duplicate values for the constrained (label, property);
	// the constraint is NOT installed. Wrapped with up to five offender IDs.
	ErrUniqueViolationExisting = errors.New("graph: unique constraint violated by existing data")

	// ErrUniqueConstraintExists is returned by CreateUnique when a unique
	// constraint already exists for the given (label, property).
	ErrUniqueConstraintExists = errors.New("graph: unique constraint already exists")

	// ErrUniqueConstraintNotFound is returned by DropUnique when no unique
	// constraint exists for the given (label, property).
	ErrUniqueConstraintNotFound = errors.New("graph: unique constraint not found")

	// ErrUniqueUnsupportedType is returned when a unique constraint's key or an
	// existing/incoming value is a floating-point type. Bit-pattern float
	// equality makes value uniqueness user-hostile; int64/string/bool/temporal
	// values are supported. Also returned for an unimplemented UniqueScope.
	ErrUniqueUnsupportedType = errors.New("graph: unique constraint does not support this value/scope")

	// ErrUniqueEventLabelUnsupported is returned by CreateUnique /
	// CreateUniqueForever on a TIERED store when the constrained label is
	// event-class. On tiered, reference-class entities all live on the reference
	// shard, so a reference-label unique constraint enforces globally via the
	// ref-shard property index; but an event label's values are spread across
	// unbounded time shards with no global value index, so uniqueness cannot be
	// enforced without a store-wide index that contradicts the shard-local
	// design (ADR-0005 §3.5). This is a permanent correctness boundary, not
	// deferred work. Distinct from the tiered store's ErrEventPropertyIndex so
	// the caller sees a unique-specific message.
	ErrUniqueEventLabelUnsupported = errors.New("graph: unique constraints on event-class labels are not supported on the tiered store")
)

// History-retention / compaction sentinels (ADR-0001).
var (
	// ErrHistoryCompacted is returned by a temporal read whose transaction-time
	// pin (TxAt / TxPin / txTime for point doors; graph watermark for scans)
	// falls before compacted knowledge, so the answer would require a trimmed
	// version. The store never silently returns an incomplete result — the loss
	// of answerability is explicit (ADR Decision 3).
	ErrHistoryCompacted = errors.New("graph: history compacted below the requested transaction time")

	// ErrCompactionProtectedTag is returned by CompactHistory* when a registered
	// named as-of tag (§4.2) pins a knowledge time that the requested policy
	// would trim. A registered tag is a promise that a knowledge state stays
	// addressable; removing the tag first is the operator's explicit act
	// (ADR Decision 4). No history is trimmed when this is returned.
	ErrCompactionProtectedTag = errors.New("graph: compaction would strand a registered as-of tag; remove the tag first")

	// ErrInvalidRetentionPolicy is returned when a RetentionPolicy has no bound
	// set (both KeepVersions and KeepSince zero) or a negative bound. An empty
	// policy is almost always a mistake, so compaction refuses it rather than
	// trimming every entity down to its mandatory-minimum chain.
	ErrInvalidRetentionPolicy = errors.New("graph: retention policy requires a positive KeepVersions or KeepSince bound")

	// ErrCompactionChangeLogEnabled is returned when CompactHistory* runs on a
	// graph with the change-log enabled. Compaction records / replica apply /
	// delta interplay are a later stage (ADR Decision 5); refusing here keeps a
	// replica from silently diverging from a compacted primary.
	ErrCompactionChangeLogEnabled = errors.New("graph: history compaction is unavailable while the change-log is enabled (compaction + replication lands later)")

	// ErrRetentionExpired is returned by a temporal read whose pin falls before a
	// relevant label's retention watermark (ADR-0008 §2). Retention PURGE
	// hard-removes whole entities below a policy boundary WITHOUT tombstones, so a
	// read pinned below the boundary cannot be answered completely — the loss of
	// answerability is EXPLICIT and fail-closed, never a silently-incomplete
	// result (mirroring ErrHistoryCompacted for trimmed history). Point doors
	// check the queried entity's label watermark(s); scan doors fail the whole
	// scan when the pin precedes the graph's maximum retention watermark. R1
	// installs this guard BEFORE any purge exists (ADR staged plan), so a
	// half-built purge can never read as complete.
	ErrRetentionExpired = errors.New("graph: entities purged per retention policy below the requested time")

	// ErrRetentionPurgeDisabled is returned by the purge admin door when the graph
	// was not opened with Config.AllowRetentionPurge. A no-tombstone hard removal
	// must be explicitly enabled.
	ErrRetentionPurgeDisabled = errors.New("graph: retention purge is disabled (set Config.AllowRetentionPurge to enable g.Admin().PurgeExpiredNodes)")

	// ErrResetDisabled is returned by g.Admin().Reset() when the graph was not
	// opened with Config.AllowReset. A whole-graph destructive wipe must be
	// explicitly enabled, mirroring ErrRetentionPurgeDisabled.
	ErrResetDisabled = errors.New("graph: reset is disabled (set Config.AllowReset to enable g.Admin().Reset)")

	// ErrRetentionPurgeChangeLogEnabled is returned by the purge admin door while a
	// change-log is enabled: the single ChangeRangePurge record + a replica's
	// re-execution of the predicate (ADR-0008 R3) are not yet built, so a purge on
	// a replicated store would silently diverge the replica. Mirrors
	// ErrCompactionChangeLogEnabled — the restriction lifts when R3 lands.
	ErrRetentionPurgeChangeLogEnabled = errors.New("graph: retention purge is unavailable while the change-log is enabled (purge + replication lands in R3)")

	// ErrInvalidPurgePolicy is returned when a PurgePolicy is missing its Label,
	// carries a non-positive Before, or names an unsupported Mode.
	ErrInvalidPurgePolicy = errors.New("graph: retention purge policy requires a non-empty Label, a positive Before, and a supported Mode")
)

// Re-exports of registry errors and index errors used by methods on *Core.
var (
	ErrEmptyName                  = registrypkg.ErrEmptyName
	ErrRegistryNotEmpty           = registrypkg.ErrRegistryNotEmpty
	ErrVectorIndexExists          = storepkg.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storepkg.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storepkg.ErrDimensionMismatch
	ErrInvalidTemporalIndexConfig = storepkg.ErrInvalidTemporalIndexConfig
	ErrInvalidVectorIndexConfig   = storepkg.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storepkg.ErrInvalidVectorValue
	ErrInvalidTimeRange           = storepkg.ErrInvalidTimeRange
	ErrInvalidQueryLimit          = storepkg.ErrInvalidQueryLimit
	ErrInvalidQueryCursor         = storepkg.ErrInvalidQueryCursor
)

// =============================================================================
// Config & ValidationLimits
// =============================================================================

const (
	defaultMaxLabelsPerNode           = 50
	defaultMaxPropertiesPerEntity     = 1000
	defaultMaxPropertyKeyLength       = 256
	defaultMaxPropertyValueSize       = 65536
	defaultMaxPropertyContainerLength = 100000
	defaultMaxNameLength              = 256
)

// ValidationLimits configures limits on entity structure.
type ValidationLimits struct {
	MaxLabelsPerNode       int
	MaxPropertiesPerEntity int
	MaxPropertyKeyLength   int
	MaxPropertyValueSize   int
	// MaxPropertyContainerLength bounds the aggregate element/entry count of a
	// slice- or map-typed property value (or the byte length of a []byte) —
	// distinct from MaxPropertyValueSize, which bounds the length of a STRING
	// value (top-level, a []string element, or a map[string]string entry).
	// The two are deliberately separate: MaxPropertyValueSize's natural scale
	// is "how long can one string be", while a container's natural scale is
	// "how many elements" (e.g. a vector-index embedding — []float32 — can
	// legitimately have thousands of dimensions, far more than a reasonable
	// string-length cap, yet still needs SOME bound against a pathological
	// 100M-element payload). Zero = default (100000).
	MaxPropertyContainerLength int
	MaxNameLength              int
	AllowSelfLoops             bool
}

// Config holds configuration for the Core.
type Config struct {
	SnowflakeNodeID int64
	// Store is the persistence backend. The accepted type is the
	// MandatoryStore composite (the union of always-required capabilities
	// from pkg/graph/store/capabilities.go); optional capabilities
	// (PropertyIndex / TemporalIndex / VectorIndex / HighFrequencyIndex)
	// are type-asserted at the call sites that need them and produce
	// ErrCapabilityNotSupported when missing. Every in-tree backend
	// (memory.Store, badger.Store, tiered.Store) satisfies the full
	// composition.
	Store                storepkg.MandatoryStore
	BadgerDir            string
	BadgerInMemory       bool
	Validation           ValidationLimits
	SyncWrites           bool
	Compression          options.CompressionType
	ZSTDCompressionLevel int
	// CacheCapacity is the per-entity-cache (nodes, rels) soft limit for the
	// badger-backed store constructed from BadgerDir/BadgerInMemory.
	// 0 means the store default (10,000). Size it to the scan working set:
	// label scans over more nodes than the cache holds decode every miss
	// from badger (~3µs/row) instead of hitting the cache (~0.1µs/row).
	// Ignored when Store is provided explicitly.
	CacheCapacity int
	// CacheBudgetBytes bounds each entity cache (nodes, rels) of the
	// badger-backed store by estimated resident BYTES instead of entry
	// count alone — entries vary 100B-64KB, so CacheCapacity cannot bound
	// memory under mixed payloads. Soft limit (dirty entries are never
	// evicted). 0 disables byte accounting. When set and CacheCapacity is
	// 0, the byte budget alone governs. Ignored when Store is provided
	// explicitly.
	CacheBudgetBytes int64
	// ResidentCache keeps every decoded node/rel resident in the badger-backed
	// store's entity caches: clean entries are never evicted and fetches skip
	// LRU promotion. For an in-memory store (BadgerInMemory) the backing data
	// already lives in RAM, so re-decoding on cache miss is pure waste that
	// makes graph-larger-than-cache traversal scale super-linearly; resident
	// mode restores linear (Memgraph-like) big-O at the cost of holding the
	// decoded working set resident. Overrides CacheCapacity/CacheBudgetBytes
	// eviction. Ignored when Store is provided explicitly.
	ResidentCache bool
	// LabelIndexOnDisk keeps the badger-backed store's label→nodes index
	// out of RAM: label scans iterate the persisted label keyspace instead
	// of an in-memory map (~50-100B per label entry — the memory ceiling
	// at hundreds of millions of nodes). Existing data directories need no
	// migration. Trade-off: label snapshots cost a disk prefix iteration.
	// Ignored when Store is provided explicitly.
	LabelIndexOnDisk bool
	// AdjacencyIndexOnDisk is the adjacency sibling of LabelIndexOnDisk:
	// outgoing/incoming snapshots come from the persisted adjacency
	// keyspaces instead of the in-memory maps (~2 map entries per
	// relationship — the largest index maps). No migration needed.
	// Ignored when Store is provided explicitly.
	AdjacencyIndexOnDisk bool
	// PropertyIndexOnDisk is the property-index sibling of LabelIndexOnDisk:
	// entries created by g.Index().CreatePropertyIndex live in the
	// badger-backed store's persisted 0x0A keyspace instead of its
	// in-memory PropertyIndex.Entries/numBuckets maps. Unlike
	// LabelIndexOnDisk/AdjacencyIndexOnDisk, the property-index keyspace is
	// NEW (not written since inception), so an existing directory with
	// prior property-index definitions is backfilled from current node
	// state exactly once, the first time this flag is turned on. Ignored
	// when Store is provided explicitly.
	PropertyIndexOnDisk bool
	// TemporalIndexOnDisk is a rebuild-at-open accelerator for the
	// maxTo-augmented temporal interval index (g.Index().CreateTemporalIndex)
	// on the badger-backed store — NOT a RAM-vs-disk trade-off like
	// LabelIndexOnDisk/AdjacencyIndexOnDisk/PropertyIndexOnDisk above: the
	// index always stays fully resident in RAM at runtime. Off (default),
	// reopening a store with an existing temporal index definition rebuilds
	// it via a full node fetch+decode per entity. On, a compact per-entity
	// row is maintained in the persisted 0x0B keyspace alongside the node row
	// (see badgerstore_temporal_disk.go), and open streams straight from a
	// prefix iteration over it instead. An existing directory with temporal
	// index definitions but no prior 0x0B rows is backfilled from current
	// node state exactly once, the first time this flag is turned on. Ignored
	// when Store is provided explicitly.
	TemporalIndexOnDisk bool
	// DisablePlannerStats turns OFF maintenance of the query-planner statistics
	// (per-(label, property key) presence counts, NDV + min/max range-cardinality,
	// and exact type-class counts). These are maintained on every node write (a
	// full per-property sweep) and rebuilt at store open, but consumed ONLY by
	// query-planning APIs (g.Stats().NodeCountByLabelAndPropertyKey /
	// PropertyTypeClassCounts and the planner's range-cardinality gate). A
	// pure-ingest or non-planning deployment pays that cost for data it never
	// reads. When set, maintenance is skipped and those stat APIs fail closed with
	// ErrCapabilityNotSupported — the same signal a backend without the capability
	// returns, so planners fall back gracefully. NO correctness path depends on
	// these counters (unique constraints use the property index). Default false =
	// stats maintained (unchanged). Honored by the memory and badger backends
	// (and tiered/sharded via their per-shard badger config); ignored when Store
	// is supplied explicitly.
	DisablePlannerStats bool
	// HistoryDeltaEncoding turns ON anchor+delta storage for version-history rows
	// on the badger backend (and tiered/sharded shards) — see
	// badger.Config.HistoryDeltaEncoding. A full ANCHOR every 16 versions, deltas
	// in between carrying only changed properties vs the interval anchor (~39% less
	// history storage post-Snappy on wide history-heavy entities; ADR-0009 / B6).
	// Reads accept both full and delta rows, so the flag is toggleable on an
	// existing store with no migration. Default false (opt-in while it soaks);
	// ignored when Store is supplied explicitly, and a no-op on the memory backend
	// (which keeps full snapshots as the differential oracle).
	HistoryDeltaEncoding bool
	// HistoryAnchorInterval overrides the anchor spacing for HistoryDeltaEncoding
	// (0 = default 16). Baked into the on-disk delta layout and pinned by a persisted
	// marker — a mismatched reopen fails closed (ErrHistoryAnchorIntervalMismatch).
	// Validated at New (0 or [2, 4096]); moot when HistoryDeltaEncoding is off. See
	// badger.Config.HistoryAnchorInterval.
	HistoryAnchorInterval int
	// ValueLogFileSize / MemTableSize / BlockCacheSize / IndexCacheSize /
	// NumCompactors tune Badger's per-instance footprint for the store
	// constructed from BadgerDir/BadgerInMemory. Zero keeps Badger's stock
	// defaults (1GB vlog / 64MB memtable / 256MB block cache / 0 index cache,
	// i.e. indices kept resident uncached / 4 compactors). The defaults
	// multiply per open instance, so these matter most when many stores share
	// a process. Validated at construction: ValueLogFileSize [1MB, 2GB),
	// MemTableSize [8MB, 1GB], BlockCacheSize >= 0, IndexCacheSize >= 0,
	// NumCompactors 0 or >= 2. Ignored when Store is provided explicitly.
	ValueLogFileSize int64
	MemTableSize     int64
	BlockCacheSize   int64
	IndexCacheSize   int64
	NumCompactors    int
	// EncryptionKey / EncryptionKeyRotation enable AES encryption-at-rest on
	// the badger-backed store constructed from BadgerDir/BadgerInMemory (see
	// badger.Config for the length validation and the wrong-key/plaintext-dir
	// failure modes). Encryption REQUIRES both BlockCacheSize > 0 and
	// IndexCacheSize > 0 above (Badger panics at Open without the former, and
	// on the first encrypted SSTable flush without the latter); New() fails
	// closed with badger.ErrEncryptionRequiresBlockCache /
	// badger.ErrEncryptionRequiresIndexCache instead of letting either panic
	// escape. Ignored when Store is supplied explicitly (enable it on the
	// injected store directly, e.g. badger.Config.EncryptionKey).
	EncryptionKey         []byte
	EncryptionKeyRotation time.Duration
	// ChangeLog enables the durable, ordered change-log (op-log) on the
	// badger-backed store constructed from BadgerDir/BadgerInMemory: every
	// committed mutation appends a framed record tagged with a monotonic cluster
	// LSN, surfaced via g.Replication() (store.ChangeFeedCapability) for
	// change-data-capture / audit / point-in-time recovery. Off by default (zero
	// overhead). Ignored when Store is supplied explicitly (enable it on the
	// injected store directly, e.g. badger.Config.ChangeLog / memory.WithChangeLog).
	ChangeLog bool
	// ReadOnlyReplica marks this graph as a log-shipped read replica: every USER
	// mutation door fails closed with ErrReadOnlyReplica, while the replica apply
	// path (g.Replication().ApplyChange) and the bootstrap importer (g.IO().Import)
	// stay open so the replica converges to its primary. Reads are unaffected.
	// Distinct from the badger ReadOnly store mode (which disables the change-log
	// and the write buffer entirely); a replica is a normal writable store that
	// the core gates above the store layer.
	ReadOnlyReplica bool
	// ReplicationSource is the replica's handle to its primary's token registry.
	// When set, the apply path refetches and append-only-extends its registry on
	// a token the primary allocated after the replica's bootstrap snapshot,
	// instead of failing closed. nil = fail closed (the behaviour before this
	// capability existed). Also settable post-New via g.SetReplicationSource.
	// In-process, a primary's g.Replication() satisfies store.ReplicationSource.
	ReplicationSource storepkg.ReplicationSource
	// AllowTxBackfill opens the privileged transaction-time backfill door on
	// this graph: create doors (Add/Import, rel create, batch create, and the
	// explicit AddWithTx) honor a caller-supplied tkg_tx_from and stamp it as
	// the entity's TxFrom instead of the system clock. It is the "audit flag,
	// import scope" gate from §4.1 — enable it only in a controlled re-ingest
	// so a documented historical Erkenntniszeit (e.g. 2026-01-15 12:00) is
	// reproducible via AS OF SYSTEM TIME; leave off in production, where any
	// tkg_tx_from is rejected with ErrTxBackfillDisabled. Backfill applies to
	// CREATES only — updates/deletes keep the monotonic system TxFrom (a
	// correction recorded now is stamped now). TxFrom is not part of the
	// integrity hash, so a backfilled row still verifies and replicates verbatim.
	AllowTxBackfill bool

	// AllowRetentionPurge enables the ADR-0008 R2 retention-purge admin door
	// (g.Admin().PurgeExpiredNodes), which HARD-removes whole aged-out nodes of a
	// label — no tombstones — for range-scale event retention. Off by default: a
	// destructive removal that cannot be undone must be opted into. When off the
	// door fails closed with ErrRetentionPurgeDisabled.
	AllowRetentionPurge bool

	// AllowReset enables the g.Admin().Reset() door, which wipes EVERY entity,
	// index, history row, named as-of tag, and unique-constraint definition from
	// the graph in one call (registries are preserved). Off by default, mirroring
	// AllowRetentionPurge: a whole-graph destructive wipe that cannot be undone
	// must be opted into explicitly, not reachable by any caller that merely
	// holds a *Graph handle. When off the door fails closed with
	// ErrResetDisabled.
	AllowReset bool

	// IngestLanes is the number of extra per-lane UNIFIED ID generators built for
	// concurrent-ingest write parallelism (ADR-0007 S4). Zero (default) keeps the
	// legacy dual generator model unchanged — every write mints from the even
	// node-field (nodes) / odd node-field (rels) pair and there is ZERO behavior
	// change. When >0, New additionally builds IngestLanes unified generators,
	// each pinned to its OWN distinct snowflake node-field (slot) drawn from
	// 0..31 excluding the interactive pair {SnowflakeNodeID*2, *2+1}; a concurrent
	// ingest session pins lane->slot and mints BOTH its nodes and its rels from
	// that one generator (the sharded catalog's disciplineUnified contract), so a
	// whole commit group lands in one slot -> one shard as one batched door call.
	// Value-level ID uniqueness is preserved: a unified generator never mints the
	// same (time, seq) twice, so a node and a rel in one slot never collide, and
	// distinct node-fields separate the slots. Requires 2+IngestLanes <= 32 (the
	// 5-bit node field); New fails closed otherwise. Interactive writes
	// (standalone / tx / plain batch) always mint from the interactive pair.
	IngestLanes uint8
}

// ValidationDefaults returns the resolved validation limits (for testing).
func (c *Core) ValidationDefaults() ValidationLimits {
	return c.validation
}

func isExactNativeStore(store storepkg.MandatoryStore) bool {
	switch store.(type) {
	case *memory.Store, *badger.Store, *tiered.Store:
		return true
	default:
		return false
	}
}

var nativeStoreTypes = [...]reflect.Type{
	reflect.TypeOf((*memory.Store)(nil)),
	reflect.TypeOf((*badger.Store)(nil)),
	reflect.TypeOf((*tiered.Store)(nil)),
}

func embedsNativeCapability(store storepkg.MandatoryStore, capability reflect.Type, directMethods ...string) bool {
	if typeDeclaresMethods(reflect.TypeOf(store), directMethods...) {
		return false
	}
	return valueEmbedsNativeCapability(reflect.ValueOf(store), capability, make(map[reflect.Type]bool))
}

func typeDeclaresMethods(t reflect.Type, names ...string) bool {
	if len(names) == 0 {
		return false
	}
	for _, name := range names {
		if !typeDeclaresMethod(t, name) {
			return false
		}
	}
	return true
}

func typeDeclaresMethod(t reflect.Type, name string) bool {
	if t == nil {
		return false
	}
	candidates := []reflect.Type{t}
	if t.Kind() != reflect.Pointer {
		candidates = append(candidates, reflect.PointerTo(t))
	}
	for _, candidate := range candidates {
		method, ok := candidate.MethodByName(name)
		if !ok {
			continue
		}
		fn := runtime.FuncForPC(method.Func.Pointer())
		if fn == nil {
			continue
		}
		file, _ := fn.FileLine(method.Func.Pointer())
		// Promoted embedded-field methods are compiler wrappers reported from
		// <autogenerated>; source-backed methods are declared by the wrapper.
		if !strings.Contains(file, "<autogenerated>") {
			return true
		}
	}
	return false
}

func typeCanPromoteCapability(t, capability reflect.Type) bool {
	if t == nil {
		return false
	}
	if t.Implements(capability) {
		return true
	}
	if t.Kind() == reflect.Pointer {
		return false
	}
	return reflect.PointerTo(t).Implements(capability)
}

func nativeTypeCanPromoteCapability(t, capability reflect.Type) bool {
	for _, native := range nativeStoreTypes {
		if t == native {
			return native.Implements(capability)
		}
		if t.Kind() != reflect.Pointer && reflect.PointerTo(t) == native {
			return native.Implements(capability)
		}
	}
	return false
}

func typeEmbedsNativeCapability(t, capability reflect.Type, seen map[reflect.Type]bool) bool {
	if t == nil {
		return false
	}
	if nativeTypeCanPromoteCapability(t, capability) {
		return true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	if seen[t] {
		return false
	}
	seen[t] = true

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.Anonymous {
			continue
		}
		ft := field.Type
		if !typeCanPromoteCapability(ft, capability) {
			continue
		}
		if nativeTypeCanPromoteCapability(ft, capability) {
			return true
		}
		if typeEmbedsNativeCapability(ft, capability, seen) {
			return true
		}
	}
	return false
}

func valueEmbedsNativeCapability(v reflect.Value, capability reflect.Type, seen map[reflect.Type]bool) bool {
	if !v.IsValid() {
		return false
	}
	t := v.Type()
	for t.Kind() == reflect.Interface {
		if v.IsNil() {
			return false
		}
		v = v.Elem()
		t = v.Type()
	}
	if nativeTypeCanPromoteCapability(t, capability) {
		return true
	}
	for t.Kind() == reflect.Pointer {
		if v.IsNil() {
			return typeEmbedsNativeCapability(t, capability, seen)
		}
		v = v.Elem()
		t = v.Type()
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	if seen[t] {
		return false
	}
	seen[t] = true

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.Anonymous {
			continue
		}
		ft := field.Type
		if !typeCanPromoteCapability(ft, capability) {
			continue
		}
		if nativeTypeCanPromoteCapability(ft, capability) {
			return true
		}
		fv := v.Field(i)
		if ft.Kind() == reflect.Interface {
			if !fv.IsNil() && fv.CanInterface() {
				if valueEmbedsNativeCapability(reflect.ValueOf(fv.Interface()), capability, seen) {
					return true
				}
			}
			continue
		}
		if valueEmbedsNativeCapability(fv, capability, seen) {
			return true
		}
	}
	return false
}

func nativeRelationshipEndpointHashWrite(store storepkg.MandatoryStore) generatedcreate.RelationshipEndpointHashCapability {
	cap, ok := store.(generatedcreate.RelationshipEndpointHashCapability)
	if !ok {
		return nil
	}
	// Wrapper stores often embed an in-tree backend but override PutRelationship
	// for instrumentation or fault injection. Do not route around that override
	// through an embedded generated-create method.
	switch store.(type) {
	case *memory.Store, *tiered.Store:
		return cap
	default:
		return nil
	}
}

func nativeEndpointIntegrityHash(store storepkg.MandatoryStore) storepkg.EndpointIntegrityHashCapability {
	cap, ok := store.(storepkg.EndpointIntegrityHashCapability)
	if !ok {
		return nil
	}
	// Endpoint hash reads replace GetNode calls on relationship create/update.
	// Keep that shortcut on exact in-tree stores so wrappers that override
	// GetNode for fault injection, stale reads, or policy checks stay visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.EndpointIntegrityHashCapability)(nil)).Elem(), "EndpointIntegrityHashes") {
		return nil
	}
	return cap
}

func filteredVectorSearchCapability(store storepkg.MandatoryStore) storepkg.FilteredVectorSearchCapability {
	cap, ok := store.(storepkg.FilteredVectorSearchCapability)
	if !ok {
		return nil
	}
	// This is both a correctness and performance hook. External backends may
	// implement it directly, but concrete wrappers that merely inherit an
	// in-tree method should still exercise their SearchNearestNodes override
	// through the graph-layer over-fetch fallback.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.FilteredVectorSearchCapability)(nil)).Elem(), "SearchNearestFiltered") {
		return nil
	}
	return cap
}

func vectorIndexOptionsCapability(store storepkg.MandatoryStore) storepkg.VectorIndexOptionsCapability {
	cap, ok := store.(storepkg.VectorIndexOptionsCapability)
	if !ok {
		return nil
	}
	// Same rationale as filteredVectorSearchCapability: a concrete wrapper
	// that overrides CreateVectorIndex for fault injection, policy checks,
	// or a different engine choice — but only inherits
	// CreateVectorIndexWithOptions from an embedded native store — must not
	// have that override silently bypassed by a direct call to the
	// inherited method. Only trust the capability on an exact in-tree store
	// or a wrapper that declares CreateVectorIndexWithOptions itself.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.VectorIndexOptionsCapability)(nil)).Elem(), "CreateVectorIndexWithOptions") {
		return nil
	}
	return cap
}

func propertyQueryCapability(store storepkg.MandatoryStore) storepkg.PropertyIndexCapability {
	cap, ok := store.(storepkg.PropertyIndexCapability)
	if !ok {
		return nil
	}
	// The graph can answer property equality by scanning NodesByLabel, so the
	// store capability is an acceleration/extension path. Let exact in-tree and
	// direct external implementations use it, but keep concrete wrappers on the
	// graph fallback so their NodesByLabel overrides stay visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.PropertyIndexCapability)(nil)).Elem(), "NodesByLabelAndProperty") {
		return nil
	}
	return cap
}

func relPropertyQueryCapability(store storepkg.MandatoryStore) storepkg.RelPropertyIndexCapability {
	cap, ok := store.(storepkg.RelPropertyIndexCapability)
	if !ok {
		return nil
	}
	// Mirror propertyQueryCapability: the graph can answer rel-property equality
	// by scanning RelationshipsByType, so the store capability is an
	// acceleration/extension path. Trust exact in-tree and direct external
	// implementations; keep concrete wrappers on the graph fallback so their
	// RelationshipsByType overrides stay visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelPropertyIndexCapability)(nil)).Elem(), "RelationshipsByTypeAndProperty") {
		return nil
	}
	return cap
}

func compositeQueryCapability(store storepkg.MandatoryStore) storepkg.CompositePropertyIndexCapability {
	cap, ok := store.(storepkg.CompositePropertyIndexCapability)
	if !ok {
		return nil
	}
	// Same rationale as propertyQueryCapability: the graph can always answer
	// a composite equality query by scanning NodesByLabel + post-filtering
	// every declared pair, so the store capability is an acceleration path
	// only, not the sole source of correctness.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.CompositePropertyIndexCapability)(nil)).Elem(), "NodesByLabelAndProperties") {
		return nil
	}
	return cap
}

func nativeNodeIntegrityHash(store storepkg.MandatoryStore) storepkg.NodeIntegrityHashCapability {
	cap, ok := store.(storepkg.NodeIntegrityHashCapability)
	if !ok {
		return nil
	}
	// Node hash reads are the one-by-one fallback for endpoint hash reads and
	// must obey the same wrapper boundary as nativeEndpointIntegrityHash.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.NodeIntegrityHashCapability)(nil)).Elem(), "NodeIntegrityHash") {
		return nil
	}
	return cap
}

// nativePreEncodedPut resolves the ADR-0006 §4.5 pre-encoded-put fast path.
// Routed for the exact native *badger.Store and *sharded.Store: both
// genuinely serialize each entity row to msgpack (sharded IS a collection of
// per-slot badger.Store shards), so handing either a pre-encoded buffer
// skips a second encode pass, and both hold the applier's shared
// property-key registry so the buffer's tokens match their own encode
// byte-for-byte. sharded.putNodesBatchInternal partitions the pre-encoded
// wireBodies/logBodies arrays per shard WITH INDEX ALIGNMENT PRESERVED
// specifically for this door (tested for byte-identity per shard against
// unsharded badger — BACKLOG 20e; this was a routing gap, not a declined
// capability: sharded's OwnedPreEncodedPutCapability, gated only by a plain
// type-assertion below with no badger-only restriction, was already routed).
// The memory store also IMPLEMENTS the capability (contract + direct test),
// but it stores live *types.Node objects and never serializes a row, so
// pre-encoding there is pure wasted prepare work — it is deliberately NOT
// routed (a nil handle disables the prepare-side pre-encode for memory
// sessions). Tiered declines (no per-shard-partitioned implementation exists
// — no single WriteBatch across its ref/archive/event shards) and wrapper
// stores decline (an overridden PutNodesBatch must not be bypassed, and a
// plain type assertion against the exact concrete type already excludes any
// wrapper that merely embeds badger.Store/sharded.Store as a field); all
// fall back to encode-at-flush via putGeneratedNodesBatch.
func nativePreEncodedPut(store storepkg.MandatoryStore) storepkg.PreEncodedPutCapability {
	cap, ok := store.(storepkg.PreEncodedPutCapability)
	if !ok {
		return nil
	}
	if _, isBadger := store.(*badger.Store); isBadger {
		return cap
	}
	if _, isSharded := store.(*sharded.Store); isSharded {
		return cap
	}
	return nil
}

func nativeGeneratedCreate(store storepkg.MandatoryStore) generatedcreate.Capability {
	cap, ok := store.(generatedcreate.Capability)
	if !ok {
		return nil
	}
	// Keep generated-ID duplicate-probe shortcuts on exact in-tree stores only.
	// Embedded wrappers may override PutNode/PutRelationship/PutNodesBatch for
	// instrumentation or fault injection, and the fast path must not bypass them.
	switch store.(type) {
	case *tiered.Store:
		return cap
	default:
		return nil
	}
}

func nativeTransactionTimeQuery(store storepkg.MandatoryStore) storepkg.TransactionTimeQueryCapability {
	cap, ok := store.(storepkg.TransactionTimeQueryCapability)
	if !ok {
		return nil
	}
	// Transaction-time queries replace the mandatory Get*/history/iteration
	// path. Keep them native-only so wrapper stores can still inject read and
	// history faults through the mandatory Store methods.
	switch store.(type) {
	case *memory.Store:
		return cap
	default:
		if embedsNativeCapability(store, reflect.TypeOf((*storepkg.TransactionTimeQueryCapability)(nil)).Elem(),
			"NodeAsOf", "RelAsOf", "NodesAsOf", "RelsAsOf") {
			return nil
		}
		return cap
	}
}

func nativeHistoryRollbackTrim(store storepkg.MandatoryStore) storepkg.HistoryRollbackTrimCapability {
	cap, ok := store.(storepkg.HistoryRollbackTrimCapability)
	if !ok {
		return nil
	}
	// Rollback trimming replaces the generic truncate+restore history path.
	// Only exact native stores get that shortcut; wrappers must observe their
	// Truncate*/Put*Version hooks during rollback.
	switch store.(type) {
	case *memory.Store, *badger.Store:
		return cap
	default:
		if embedsNativeCapability(store, reflect.TypeOf((*storepkg.HistoryRollbackTrimCapability)(nil)).Elem(),
			"TrimNodeHistoryFrom", "TrimRelHistoryFrom") {
			return nil
		}
		return cap
	}
}

func historyVersionPageCapability(store storepkg.MandatoryStore) storepkg.HistoryVersionPageCapability {
	cap, ok := store.(storepkg.HistoryVersionPageCapability)
	if !ok {
		return nil
	}
	// Export can fall back to mandatory Get*History reads. Keep inherited
	// in-tree pagers off concrete wrappers so tests, fault injectors, and
	// policy wrappers that override Get*History stay observable.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.HistoryVersionPageCapability)(nil)).Elem(),
		"NodeHistoryVersionsFrom", "RelHistoryVersionsFrom") {
		return nil
	}
	return cap
}

func depthHistoryIterationCapability(store storepkg.MandatoryStore) storepkg.DepthHistoryIterationCapability {
	cap, ok := store.(storepkg.DepthHistoryIterationCapability)
	if !ok {
		return nil
	}
	// Depth-scoped history iteration is a tiered-store optimization. Concrete
	// wrappers that only inherit tiered's methods must fall back through their
	// mandatory ForEach*HistoryID hooks so policy/fault wrappers remain visible.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DepthHistoryIterationCapability)(nil)).Elem(),
		"ForEachNodeHistoryIDByDepth", "ForEachRelHistoryIDByDepth") {
		return nil
	}
	return cap
}

func deletedIterationCapability(store storepkg.MandatoryStore) storepkg.DeletedIterationCapability {
	cap, ok := store.(storepkg.DeletedIterationCapability)
	if !ok {
		return nil
	}
	// Deleted iteration is an optional acceleration. Wrappers that only inherit
	// the native methods must fall through so wrapper-injected ForEach*HistoryID
	// behaviour remains observable on the candidate-fold path.
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DeletedIterationCapability)(nil)).Elem(),
		"ForEachDeletedNodeID", "ForEachDeletedRelID") {
		return nil
	}
	return cap
}

func depthDeletedIterationCapability(store storepkg.MandatoryStore) storepkg.DepthDeletedIterationCapability {
	cap, ok := store.(storepkg.DepthDeletedIterationCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.DepthDeletedIterationCapability)(nil)).Elem(),
		"ForEachDeletedNodeIDByDepth", "ForEachDeletedRelIDByDepth") {
		return nil
	}
	return cap
}

// labelTxMembershipCapability resolves the K1 transaction-time label-membership
// sidecar. Only the exact native single-shard backends (memory, badger) own a
// coherent whole-store membership index; a wrapper that merely EMBEDS a native
// store, or the multi-shard tiered store, is forced to nil so the query falls
// back to the correct (if unaccelerated) full-history candidate fold.
func labelTxMembershipCapability(store storepkg.MandatoryStore) storepkg.LabelTxMembershipCapability {
	cap, ok := store.(storepkg.LabelTxMembershipCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.LabelTxMembershipCapability)(nil)).Elem(),
		"ForEachLabelTxMember") {
		return nil
	}
	return cap
}

// nodeBeliefWatermarkCapability resolves the BACKLOG 10c belief-watermark
// sidecar (see the Core field's doc comment). Same "exact native store only"
// discipline as labelTxMembershipCapability — a wrapper that merely embeds a
// native store, or the multi-shard tiered/sharded stores, is forced to nil.
func nodeBeliefWatermarkCapability(store storepkg.MandatoryStore) storepkg.NodeBeliefWatermarkCapability {
	cap, ok := store.(storepkg.NodeBeliefWatermarkCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.NodeBeliefWatermarkCapability)(nil)).Elem(),
		"NodeBeliefWatermark") {
		return nil
	}
	return cap
}

// relBeliefWatermarkCapability mirrors nodeBeliefWatermarkCapability for relationships.
func relBeliefWatermarkCapability(store storepkg.MandatoryStore) storepkg.RelBeliefWatermarkCapability {
	cap, ok := store.(storepkg.RelBeliefWatermarkCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelBeliefWatermarkCapability)(nil)).Elem(),
		"RelBeliefWatermark") {
		return nil
	}
	return cap
}

// relTypeTxMembershipCapability is the rel-type mirror of labelTxMembershipCapability.
func relTypeTxMembershipCapability(store storepkg.MandatoryStore) storepkg.RelTypeTxMembershipCapability {
	cap, ok := store.(storepkg.RelTypeTxMembershipCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelTypeTxMembershipCapability)(nil)).Elem(),
		"ForEachRelTypeTxMember") {
		return nil
	}
	return cap
}

// changeFeedCapability resolves the optional change-log capability. Only the
// exact native single-shard backends (memory, badger) own a coherent global
// LSN sequence. A wrapper that merely EMBEDS a native store (e.g. a future
// sharded backend, or tiered if it ever embedded a shard) would promote the
// badger ChangeFeed methods and expose ONE shard's per-shard log as if it were
// the cluster feed — force nil there so such a backend must implement a real
// global feed itself. tiered.Store now provides a coherent store-global LSN
// sequence via its OWN allocator (ADR-0005 §2 — not shard promotion): the
// store-level changeLogAllocator hands every shard's record a store-global LSN,
// and ForEachChange/ChangeFeed/LastCommittedLSN k-way merge the per-shard logs by
// LSN behind a flush-before-read barrier. So tiered is admitted here alongside
// the single-shard backends; the embedsNativeCapability reflection guard stays
// for UNKNOWN future backends that merely embed a native shard.
func changeFeedCapability(store storepkg.MandatoryStore) storepkg.ChangeFeedCapability {
	cap, ok := store.(storepkg.ChangeFeedCapability)
	if !ok {
		return nil
	}
	switch store.(type) {
	case *memory.Store, *badger.Store, *tiered.Store, *sharded.Store:
		return cap
	default:
		if embedsNativeCapability(store, reflect.TypeOf((*storepkg.ChangeFeedCapability)(nil)).Elem(),
			"ChangeFeed", "ForEachChange", "LastCommittedLSN") {
			return nil
		}
		return cap
	}
}

// changeLogStatusEnabled reports whether the store's change-log is actually on
// (records are emitted). A store always implements ChangeFeedCapability's methods
// but only EMITS when its log is enabled; this optional probe is the only reliable
// signal. A store that does not implement store.ChangeLogStatusCapability is
// log-disabled.
func changeLogStatusEnabled(store storepkg.MandatoryStore) bool {
	s, ok := store.(storepkg.ChangeLogStatusCapability)
	return ok && s.ChangeLogEnabled()
}

// txChangeLogScope resolves the per-tx change-log buffer capability. Returns nil
// when the change-log is off or the backend does not implement it (tiered) — the
// core then emits records eagerly as before. Resolving it only when enabled keeps
// the tx/batch paths zero-cost on a non-change-log graph.
func txChangeLogScope(store storepkg.MandatoryStore, enabled bool) storepkg.TxChangeLogScope {
	if !enabled {
		return nil
	}
	s, _ := store.(storepkg.TxChangeLogScope)
	return s
}

// scopedTxCapability resolves the BACKLOG 11f token-routed multi-scope
// capability. Unlike txChangeLogScope, this does NOT gate on the change-log
// being enabled: BeginScopedLog itself returns (0, nil) when the log is off,
// so scopeToken stays 0 for every tx and every *ScopedAware wrapper falls
// through to its plain unscoped door — exactly the existing "log disabled"
// behavior — while lockActiveCoreWrite still safely uses the shared RLock
// (there is nothing to divert either way). Gating on the FULL
// storepkg.ScopedTxCapability interface (not just ScopedTxChangeLog) is
// deliberate: a store implementing the scope mechanism but missing even one
// door's Scoped sibling must never be granted the fast path, or that one
// door's *ScopedAware fallback could leak a record into the eager feed
// before a possible rollback. See ScopedTxCapability's doc comment.
func scopedTxCapability(store storepkg.MandatoryStore) storepkg.ScopedTxCapability {
	s, _ := store.(storepkg.ScopedTxCapability)
	return s
}

func nativeAdjacencyReadsValidateNodeExistence(store storepkg.MandatoryStore) bool {
	switch store.(type) {
	case *memory.Store, *badger.Store, *tiered.Store:
		return true
	default:
		return false
	}
}

// =============================================================================
// Snowflake helpers
// =============================================================================

// IDComponents holds the decomposed fields of a snowflake ID. Re-exported
// here so methods on *Core can use the type without importing pkg/graph.
type IDComponents = snowflakepkg.IDComponents

// DecomposeID extracts creation time, node ID, sequence number.
func DecomposeID(id snowflake.ID) IDComponents {
	return snowflakepkg.DecomposeID(id)
}

var snowflakeEpoch = snowflakepkg.Epoch

// =============================================================================
// Lifecycle
// =============================================================================

type registriesPersister interface {
	SaveRegistries(*registrypkg.LabelRegistry, *registrypkg.RelTypeRegistry) error
}

func (c *Core) persistRegistries() error {
	if rp, ok := c.store.(registriesPersister); ok {
		if err := rp.SaveRegistries(c.labels, c.relTypes); err != nil {
			c.registryDirty.Store(true)
			return fmt.Errorf("graph: save registries: %w", err)
		}
	}
	if pk, ok := c.store.(propertyKeyPersister); ok {
		if err := pk.SavePropertyKeyRegistry(c.propKeys); err != nil {
			c.registryDirty.Store(true)
			return fmt.Errorf("graph: save property-key registry: %w", err)
		}
	}
	c.registryDirty.Store(false)
	return nil
}

// propertyKeyPersister matches the badger.Store / tiered.Store property-key
// persistence shape. OPTIONAL — backends without persisted property-key
// registries are skipped.
type propertyKeyPersister interface {
	SavePropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) error
	LoadPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry) (bool, error)
}

// propertyKeyRegistrySetter matches the badger.Store / tiered.Store shape
// for installing the property-key registry on the store. The store uses
// the registry to tokenize property keys on the wire. OPTIONAL.
type propertyKeyRegistrySetter interface {
	SetPropertyKeyRegistry(*registrypkg.PropertyKeyRegistry)
}

func (c *Core) persistRegistriesIfDirtyLocked() error {
	if !c.registryDirty.Load() {
		return nil
	}
	return c.persistRegistries()
}

func (c *Core) persistRegistriesIfDirtyLockedPanicSafe() error {
	if !c.registryDirty.Load() {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			c.registryMu.Unlock()
			panic(r)
		}
	}()
	return c.persistRegistries()
}

func (c *Core) checkpointDirtyRegistriesBeforeMutation(op string) error {
	if !c.registryDirty.Load() {
		return nil
	}
	c.registryMu.Lock()
	defer c.registryMu.Unlock()
	err := c.persistRegistriesIfDirtyLocked()
	if err != nil {
		return fmt.Errorf("graph: %s: %w", op, err)
	}
	return nil
}

// New creates a new Core with the given configuration. See pkg/graph.New for docs.
func New(config Config) (*Core, error) {
	if config.SnowflakeNodeID < 0 || config.SnowflakeNodeID > 15 {
		return nil, fmt.Errorf("graph: SnowflakeNodeID must be 0-15, got %d", config.SnowflakeNodeID)
	}
	if config.Store != nil && isNilInterfaceValue(config.Store) {
		return nil, ErrNilStore
	}

	nodeGen, err := snowflake.NewNode(config.SnowflakeNodeID*2,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: node ID generator: %w", err)
	}
	relGen, err := snowflake.NewNode(config.SnowflakeNodeID*2+1,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, fmt.Errorf("graph: rel ID generator: %w", err)
	}

	laneGens, laneSlots, err := buildLaneGenerators(config.SnowflakeNodeID, config.IngestLanes)
	if err != nil {
		return nil, err
	}
	// A read-only replica never mints its own IDs — every write it ever makes
	// reproduces a primary's exact ID verbatim via ApplyChange, so its own
	// SnowflakeNodeID/IngestLanes generator-slot coverage is irrelevant and
	// must not be validated (a replica's SnowflakeNodeID is commonly a
	// deliberately-different value from the primary's, precisely to prove
	// its own identity doesn't matter).
	if !config.ReadOnlyReplica {
		if err := validateShardedSlotCoverage(config.Store, config.SnowflakeNodeID, laneSlots); err != nil {
			return nil, err
		}
	}

	v := config.Validation
	if v.MaxLabelsPerNode == 0 {
		v.MaxLabelsPerNode = defaultMaxLabelsPerNode
	}
	if v.MaxPropertiesPerEntity == 0 {
		v.MaxPropertiesPerEntity = defaultMaxPropertiesPerEntity
	}
	if v.MaxPropertyKeyLength == 0 {
		v.MaxPropertyKeyLength = defaultMaxPropertyKeyLength
	}
	if v.MaxPropertyValueSize == 0 {
		v.MaxPropertyValueSize = defaultMaxPropertyValueSize
	}
	if v.MaxPropertyContainerLength == 0 {
		v.MaxPropertyContainerLength = defaultMaxPropertyContainerLength
	}
	if v.MaxNameLength == 0 {
		v.MaxNameLength = defaultMaxNameLength
	}

	if v.MaxLabelsPerNode < 0 || v.MaxPropertiesPerEntity < 0 ||
		v.MaxPropertyKeyLength < 0 || v.MaxPropertyValueSize < 0 ||
		v.MaxPropertyContainerLength < 0 || v.MaxNameLength < 0 {
		return nil, fmt.Errorf("graph: validation limits must not be negative")
	}

	c := &Core{
		labels:              registrypkg.NewLabelRegistry(),
		relTypes:            registrypkg.NewRelTypeRegistry(),
		propKeys:            registrypkg.NewPropertyKeyRegistry(),
		nodeIDGen:           nodeGen,
		relIDGen:            relGen,
		laneGenerators:      laneGens,
		laneSlots:           laneSlots,
		entityLocks:         locks.NewManager(),
		valueLocks:          locks.NewValueManager(),
		validation:          v,
		indexProviders:      make(map[string]*indexProviderEntry),
		relTypeCache:        make(map[string]uint16),
		asOfColumns:         newAsOfColumnCache(),
		clock:               time.Now,
		readOnlyReplica:     config.ReadOnlyReplica,
		allowTxBackfill:     config.AllowTxBackfill,
		allowRetentionPurge: config.AllowRetentionPurge,
		allowReset:          config.AllowReset,
		replSource:          config.ReplicationSource,
	}
	c.Nodes = &NodeOps{c: c}
	c.Rels = &RelOps{c: c}
	c.Temporal = &TempOps{c: c}
	c.Index = &IndexOps{c: c}
	c.Events = &EventOps{c: c}
	c.Admin = &AdminOps{c: c}
	c.Constraints = &ConstraintOps{c: c}
	c.Hash = &HashOps{c: c}
	c.Repl = &ReplOps{c: c}
	c.Ingest = &IngestOps{c: c}
	c.IO = &IOOps{c: c}
	c.Resolve = &ResolveOps{c: c}
	c.Stats = &StatOps{c: c}

	if config.Store == nil && config.BadgerDir != "" {
		if strings.TrimSpace(config.BadgerDir) == "" {
			return nil, fmt.Errorf("graph: BadgerDir is whitespace-only; use a valid path or omit for MemoryStore")
		}
	}

	store := config.Store
	if store == nil {
		if config.BadgerDir != "" || config.BadgerInMemory {
			bs, err := badger.New(badger.Config{
				Dir:                   config.BadgerDir,
				InMemory:              config.BadgerInMemory,
				SyncWrites:            config.SyncWrites,
				Compression:           config.Compression,
				ZSTDCompressionLevel:  config.ZSTDCompressionLevel,
				CacheCapacity:         config.CacheCapacity,
				CacheBudgetBytes:      config.CacheBudgetBytes,
				ResidentCache:         config.ResidentCache,
				LabelIndexOnDisk:      config.LabelIndexOnDisk,
				AdjacencyIndexOnDisk:  config.AdjacencyIndexOnDisk,
				PropertyIndexOnDisk:   config.PropertyIndexOnDisk,
				TemporalIndexOnDisk:   config.TemporalIndexOnDisk,
				DisablePlannerStats:   config.DisablePlannerStats,
				HistoryDeltaEncoding:  config.HistoryDeltaEncoding,
				HistoryAnchorInterval: config.HistoryAnchorInterval,
				ValueLogFileSize:      config.ValueLogFileSize,
				MemTableSize:          config.MemTableSize,
				BlockCacheSize:        config.BlockCacheSize,
				IndexCacheSize:        config.IndexCacheSize,
				NumCompactors:         config.NumCompactors,
				EncryptionKey:         config.EncryptionKey,
				EncryptionKeyRotation: config.EncryptionKeyRotation,
				ChangeLog:             config.ChangeLog,
			})
			if err != nil {
				return nil, fmt.Errorf("graph: badger store: %w", err)
			}
			loadedLabels := registrypkg.NewLabelRegistry()
			if found, err := bs.LoadLabelRegistry(loadedLabels); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load label registry: %w", err)
			} else if found {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					_ = bs.Close()
					return nil, fmt.Errorf("graph: load label registry: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if found, err := bs.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				_ = bs.Close()
				return nil, fmt.Errorf("graph: load reltype registry: %w", err)
			} else if found {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					_ = bs.Close()
					return nil, fmt.Errorf("graph: load reltype registry: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
			store = bs
		} else {
			var memOpts []memory.Option
			if config.DisablePlannerStats {
				memOpts = append(memOpts, memory.WithoutPlannerStats())
			}
			store = memory.New(memOpts...)
		}
	}

	c.store = store
	c.preEncodedPut = nativePreEncodedPut(store)
	// Ownership-transfer put (lever #2) is independent of the §4.5 pre-encode
	// gate (which is badger-only): any store implementing the capability honors
	// the freeze-in-place contract, so recognize sharded here too. Used only by
	// the concurrent ingest bulk apply for all-write-only groups.
	if oc, ok := store.(storepkg.OwnedPreEncodedPutCapability); ok {
		c.ownedPut = oc
	}
	c.generatedCreate = nativeGeneratedCreate(store)
	c.endpointHash = nativeEndpointIntegrityHash(store)
	c.endpointHashWrite = nativeRelationshipEndpointHashWrite(store)
	c.foreignEndpointRel, _ = store.(generatedcreate.ForeignEndpointRelCapability)
	c.foreignIncomingRel, _ = store.(generatedcreate.ForeignIncomingRelCapability)
	c.nodeHash = nativeNodeIntegrityHash(store)
	c.txTimeQuery = nativeTransactionTimeQuery(store)
	_, txTimeQueryIsNativeMemory := store.(*memory.Store)
	c.txTimeQueryCopy = c.txTimeQuery != nil && !txTimeQueryIsNativeMemory
	c.historyTrim = nativeHistoryRollbackTrim(store)
	c.propertyQuery = propertyQueryCapability(store)
	c.propertyQueryTrust = isExactNativeStore(store)
	c.relPropertyQuery = relPropertyQueryCapability(store)
	c.relPropertyQueryTrust = isExactNativeStore(store)
	c.compositeQuery = compositeQueryCapability(store)
	c.compositeQueryTrust = isExactNativeStore(store)
	c.filteredVector = filteredVectorSearchCapability(store)
	c.vectorIndexOptions = vectorIndexOptionsCapability(store)
	c.depthHistory = depthHistoryIterationCapability(store)
	c.deletedIter = deletedIterationCapability(store)
	c.changeFeed = changeFeedCapability(store)
	c.changeLogEnabled = changeLogStatusEnabled(store)
	c.txLogScope = txChangeLogScope(store, c.changeLogEnabled)
	c.scopedChangeLog = scopedTxCapability(store)
	c.deletedDepthIter = depthDeletedIterationCapability(store)
	c.labelTxMembers = labelTxMembershipCapability(store)
	c.relTypeTxMembers = relTypeTxMembershipCapability(store)
	c.nodeBeliefWatermark = nodeBeliefWatermarkCapability(store)
	c.relBeliefWatermark = relBeliefWatermarkCapability(store)
	// valid-time candidate prune is sound for ANY store that offers it (an
	// unknown id is never pruned), so unlike the membership sidecar it needs no
	// exact-native-store guard — a plain probe admits memory/badger now and
	// tiered/sharded once they implement it.
	c.temporalCandidates, _ = store.(storepkg.TemporalCandidateCapability)
	c.relTypeTemporalCandidates, _ = store.(storepkg.RelTypeTemporalCandidateCapability)
	// Metadata facet: bare optional-capability probes, resolved once (byte-for-byte
	// equal to the former per-site `_, ok := c.store.(X)` since store is immutable).
	c.metaKV, _ = store.(storepkg.MetaKVCapability)
	c.historyCompaction, _ = store.(storepkg.HistoryCompactionCapability)
	c.retentionPurge, _ = store.(storepkg.RetentionPurgeCapability)
	c.retentionPurgeValidTo, _ = store.(storepkg.RetentionPurgeByValidToCapability)
	c.rangePurgeLog, _ = store.(storepkg.RangePurgeLogCapability)
	c.vectorRowsTrust = isExactNativeStore(store)
	c.storeRowsTrust = isExactNativeStore(store)
	c.nativeAdjacency = nativeAdjacencyReadsValidateNodeExistence(store)

	// Registry rehydration for caller-injected stores. The Core-
	// constructed badger.Store path above already loads registries; the
	// inject path also has to. Without this, opening an
	// existing badger.Store separately and passing it via
	// `Config{Store: bs}` would start the graph with empty in-memory
	// registries even though the persisted entities use tokenised
	// labels and reltypes — Close would then save the empty registry
	// state back over the persisted mappings.
	//
	// Two interface shapes exist in-tree because badger and tiered
	// landed at different times — badger.Store returns (bool, error)
	// from its loaders while tiered.Store returns (int, error). Both
	// are matched here so the rehydration is dispatched by capability
	// rather than concrete type.
	if config.Store != nil {
		if lr, ok := store.(badgerRegistryLoader); ok {
			loadedLabels := registrypkg.NewLabelRegistry()
			if found, err := lr.LoadLabelRegistry(loadedLabels); err != nil {
				return nil, fmt.Errorf("graph: load label registry from injected store: %w", err)
			} else if found {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load label registry from injected store: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if found, err := lr.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				return nil, fmt.Errorf("graph: load reltype registry from injected store: %w", err)
			} else if found {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load reltype registry from injected store: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
		}
		if ts, ok := store.(tieredRegistryLoader); ok {
			loadedLabels := registrypkg.NewLabelRegistry()
			if n, err := ts.LoadLabelRegistry(loadedLabels); err != nil {
				return nil, fmt.Errorf("graph: load label registry from injected tiered store: %w", err)
			} else if n > 0 {
				if err := c.validateRegistryNames("label", loadedLabels.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load label registry from injected tiered store: %w", err)
				}
				c.labels = loadedLabels
			}
			loadedRelTypes := registrypkg.NewRelTypeRegistry()
			if n, err := ts.LoadRelTypeRegistry(loadedRelTypes); err != nil {
				return nil, fmt.Errorf("graph: load reltype registry from injected tiered store: %w", err)
			} else if n > 0 {
				if err := c.validateRegistryNames("reltype", loadedRelTypes.ExportNames()); err != nil {
					return nil, fmt.Errorf("graph: load reltype registry from injected tiered store: %w", err)
				}
				c.relTypes = loadedRelTypes
			}
			setTieredLabelRegistryIfSupported(store, c.labels)
		}
	}

	if pk, ok := store.(propertyKeyPersister); ok {
		loaded := registrypkg.NewPropertyKeyRegistry()
		if found, err := pk.LoadPropertyKeyRegistry(loaded); err != nil {
			return nil, fmt.Errorf("graph: load property-key registry: %w", err)
		} else if found {
			c.propKeys = loaded
		}
	}
	// Install the registry on the store so the wire encoder / decoder
	// dictionary-encode property keys. Optional capability — backends
	// without it keep the pre-tokenization on-disk format.
	if setter, ok := store.(propertyKeyRegistrySetter); ok {
		setter.SetPropertyKeyRegistry(c.propKeys)
	}

	c.runBitemporalMigrationBestEffort()

	// Rehydrate the durable unique-constraint registry (fail closed on a corrupt
	// MetaKV blob — a constraint silently dropped at open would let a duplicate
	// slip in). Backends without MetaKV simply start with no constraints.
	if err := c.loadUniqueConstraints(); err != nil {
		return nil, err
	}
	if err := c.loadUniqueForeverOwners(); err != nil {
		return nil, err
	}

	// Rehydrate the history-compaction watermark (ADR-0001). Fail closed on a
	// corrupt blob — a silently-dropped watermark would let a scan below
	// compacted knowledge return incomplete data as if complete.
	if err := c.loadCompactionWatermark(); err != nil {
		return nil, err
	}

	// Rehydrate the graph retention watermark (ADR-0008) — same fail-closed
	// contract as the compaction watermark above.
	if err := c.loadRetentionWatermark(); err != nil {
		return nil, err
	}

	// Reseed the commit-clock floor from the previous session's durable watermark
	// so NowTx()/c.now() stay above every persisted TxFrom across a reopen (a burst
	// whose monotonic floor outran the wall leaves stamps above the reopened wall).
	// Deliberately NOT fail-closed, unlike the watermarks above: an absent or
	// unreadable floor is the expected state after an unclean shutdown, and it
	// self-heals as the wall clock advances past the drift (lesson 71).
	c.seedInstantFloor()

	return c, nil
}

// badgerRegistryLoader matches the badger.Store rehydration shape.
// Backends that persist registries with the same `(found bool, err
// error)` signature as badger satisfy this interface and get
// automatic rehydration on graph construction. Tiered stores have a
// different signature and are dispatched by tieredRegistryLoader above.
type badgerRegistryLoader interface {
	LoadLabelRegistry(*registrypkg.LabelRegistry) (bool, error)
	LoadRelTypeRegistry(*registrypkg.RelTypeRegistry) (bool, error)
}

// Close saves registries (if Badger) and closes the underlying store.
//
// Close is serialized against in-flight standalone mutations and reads. The closed-state flag is set BEFORE acquiring c.mu.Lock so
// that any RLock acquired after Close releases its Lock observes
// closed=true and short-circuits with ErrGraphClosed (see
// runUnderRLock). Provider drain happens under c.mu.Lock so it cannot
// race with concurrent RegisterProvider; provider Close is then run
// outside the lock so a slow Close cannot block the lifecycle lock.
func (c *Core) Close() error {
	var closeErr error
	c.closeOnce.Do(func() {
		// Drain and stop the ingest applier BEFORE marking the graph closed, so
		// every accepted-but-unapplied intent is applied while the graph is
		// still writable (§4.8: entity writes are never dropped). The applier
		// takes c.txMu + c.mu.Lock per group, so it must finish before Close
		// acquires c.mu.Lock below.
		c.stopIngestApplier()

		c.closed.Store(true)

		// Drain in-flight RLock holders. New mutations that win the
		// race to RLock between Store and Lock observe closed=true via
		// the defer-protected check inside runUnderRLock and return
		// ErrGraphClosed without touching the store.
		c.mu.Lock()
		entries := make([]*indexProviderEntry, 0, len(c.indexProviders))
		for _, e := range c.indexProviders {
			entries = append(entries, e)
		}
		c.indexProviders = make(map[string]*indexProviderEntry)
		asyncBus, _ := c.events.(*eventspkg.AsyncEventBus)
		c.mu.Unlock()

		// Provider Close runs outside the lifecycle lock — providers
		// may flush/close their own backends and we do not want to
		// hold the graph lock for that latency. Initializable
		// providers are registered before Init runs, so wait for any
		// in-flight Init callback before invoking Close.
		for _, e := range entries {
			e.unsubscribe()
			e.waitInit()
			closeErr = errors.Join(closeErr, closeIndexProvider(e))
		}

		// An installed AsyncEventBus starts its own dispatcher goroutine
		// (NewAsyncEventBus) that only exits via its own Close() — without
		// this, every open/close cycle with an async bus configured leaked
		// that goroutine permanently. Runs outside c.mu (Close blocks
		// draining the queue). Nil-safe / idempotent, and closed==true by
		// now means no concurrent SetAsync/publishEvent can install or use a
		// different bus underneath this call.
		if asyncBus != nil {
			asyncBus.Close()
		}

		// persistRegistries reads the registry POINTERS, which a concurrent
		// tx Rollback / import rollback may still be swapping (they hold
		// c.mu.Lock, which Close released above) — take registryMu, the
		// second guard every swap site now holds.
		c.registryMu.Lock()
		closeErr = errors.Join(closeErr, c.persistRegistries())
		c.registryMu.Unlock()
		// Persist the commit-clock floor BEFORE the store closes+flushes it, so the
		// next open can reseed NowTx()'s reopen-safety watermark (seedInstantFloor).
		// Outside registryMu: that guard covers the registry pointers only, and this
		// writes an independent MetaKV key.
		closeErr = errors.Join(closeErr, c.persistInstantFloor())
		closeErr = errors.Join(closeErr, c.store.Close())
	})
	return closeErr
}

func closeIndexProvider(e *indexProviderEntry) (err error) {
	providerName := "<unknown>"
	defer func() {
		if r := recover(); r != nil {
			err = errors.Join(err, fmt.Errorf("index provider %q close panic: %v", providerName, r))
		}
	}()

	e.stopEvents()
	providerName = e.provider.Name()
	if closeErr := e.provider.Close(); closeErr != nil {
		err = errors.Join(err, fmt.Errorf("index provider %q close: %w", providerName, closeErr))
	}
	return err
}
