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
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
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
	relDegrees            relDegreeCache   // StatOps.RelTypeDegreeStats, keyed on the relationship epochs
	memberTime            memberTimeCache  // StatOps.LabelTimeStats / RelTypeTimeStats explicit-valid counts, keyed on the mutation epochs
	relDegreeTrack        relDegreeTracker // StatOps.TrackRelTypeDegrees: maintained counts of opted-in types
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
	// propertyKeysQuery / relPropertyKeysQuery: the one-walk equality
	// lookup over a value's forms (round 11), resolved by nativeCapability;
	// nil = one store lookup per form.
	propertyKeysQuery    storepkg.PropertyKeysLookupCapability
	relPropertyKeysQuery storepkg.RelPropertyKeysLookupCapability
	compositeQuery       storepkg.CompositePropertyIndexCapability
	compositeQueryTrust  bool
	filteredVector       storepkg.FilteredVectorSearchCapability
	vectorIndexOptions   storepkg.VectorIndexOptionsCapability
	depthHistory         storepkg.DepthHistoryIterationCapability
	relCreateUndo        storepkg.RelCreateUndoCapability       // undoRelationshipCreate
	scopedRelCreateUndo  storepkg.ScopedRelCreateUndoCapability // undoRelationshipCreate
	// exAdj is the store's ex-adjacency (backlog 50) once it covers every
	// relationship (backfillExAdjacencyBestEffort); nil = the adjacency doors
	// fold every relationship with history instead (forEachRelExAdjacentID).
	exAdj storepkg.ExAdjacencyCapability
	// transaction-time membership sidecars: scope a pinned label/type scan's
	// candidate set to the label/type's ever-members (O(matches)) instead of the
	// whole node/rel-history fold (O(everything ever)). nil = store declines
	// (sharded, wrappers), so the query falls back to the full-history
	// candidate fold; tiered answers DepthAll reads only (membershipSidecarUsable).
	labelTxMembers   storepkg.LabelTxMembershipCapability
	relTypeTxMembers storepkg.RelTypeTxMembershipCapability
	// labelTxLookup / relTypeTxLookup are the point forms of the K1
	// sidecars (round 5 request 8), resolved with the same discipline.
	labelTxLookup   storepkg.LabelTxMemberLookupCapability
	relTypeTxLookup storepkg.RelTypeTxMemberLookupCapability
	// property membership sidecars (backlog 8): scope a temporal property
	// lookup (ByTypeAndProperty / ByLabelAndProperty(ies) and the named
	// *PropertyAt / *PropertyDuring doors) to the value's ever-members for a
	// declared index instead of the whole history fold. nil = store declines
	// (wrappers): the fold, same answers. Tiered's node form declines per call
	// (store.ErrIndexNotFound) when the label has members off its reference
	// shard; its rel form always declines (no rel property index).
	relPropTxMembers  storepkg.RelPropertyTxMembershipCapability
	nodePropTxMembers storepkg.NodePropertyTxMembershipCapability
	// relPropTxPage / nodePropTxPage page those sidecars in ID order above a
	// cursor (backlog 69, the paged effective doors), resolved with the same
	// discipline; nil = every type takes its ever-member gather.
	relPropTxPage  storepkg.RelPropertyTxMemberPageCapability
	nodePropTxPage storepkg.NodePropertyTxMemberPageCapability
	// relPropTxWindow / nodePropTxWindow narrow those sidecars to the members
	// whose rows may meet a valid-time window (ledger row 62), resolved with
	// the same discipline; nil = the whole value's members.
	relPropTxWindow  storepkg.RelPropertyTxMemberWindowCapability
	nodePropTxWindow storepkg.NodePropertyTxMemberWindowCapability
	// relPropTxKeys / nodePropTxKeys enumerate those sidecars (plain and
	// windowed) over a value's equal keys in one call (round 11: one fan-out
	// on tiered and sharded); nil = one call per key, same candidates.
	relPropTxKeys  storepkg.RelPropertyTxMemberKeysCapability
	nodePropTxKeys storepkg.NodePropertyTxMemberKeysCapability
	// chainLoadTestHook, when set (tests only), is called once per candidate a
	// generic temporal door hands to the per-entity resolver
	// (findNodeVersionForOpts / findRelVersionForOpts), and once per entity a
	// valid-end bounded scan reads (streamValidFrom): one call = one version
	// chain loaded. It lets a test assert the STRUCTURAL cost of a pinned lookup
	// (chain loads), which wall time cannot pin. nil in production.
	chainLoadTestHook func()
	// scanReadTestHook, when set (tests only), is called before each entity an
	// effective scan reads, under the graph read lock (streamEntityTimelines).
	scanReadTestHook func()
	// belief watermarks (BACKLOG 10c): a store that maintains, per entity, the
	// maximum TxFrom ever recorded across its whole version chain lets
	// nodeAtLockedTx/relAtLockedTx take a SAFE current-row-only fast path for
	// point-in-time queries — see store.NodeBeliefWatermarkCapability's doc
	// comment. nil = store declines (tiered, sharded today), so every query
	// resolves through the full chain scan (correct, unaccelerated).
	nodeBeliefWatermark storepkg.NodeBeliefWatermarkCapability
	relBeliefWatermark  storepkg.RelBeliefWatermarkCapability
	// temporalMetaHistory — historical-pin resolution on temporal skeletons: a
	// store that enumerates a chain's per-version temporal metadata WITHOUT
	// materializing full rows lets nodeAtLockedTx/relAtLockedTx select the
	// winning version on skeletons and decode ONLY the winner (O(1) full
	// decodes per resolution instead of O(chain) — the valid-time
	// depth ask). nil = store declines (memory, tiered, sharded today), so the
	// query materializes the full chain (correct, unaccelerated). The chain
	// resolver stays the single selection authority: the SAME resolveNodeChain
	// runs on skeletons and again after winner hydration.
	temporalMetaHistory storepkg.TemporalMetaHistoryCapability
	// historyEpochs — per-entity history tokens (backlog 68): the head-pin
	// effective timeline is kept per entity while its token holds
	// (effective_head.go). nil = store declines: every head read folds.
	historyEpochs storepkg.HistoryEpochCapability
	heads         headCache
	headCacheOff  bool           // tests: fold every read (the differential oracle)
	headHits      atomic.Uint64  // head reads answered from an entry
	headOverlays  atomic.Uint64  // corrections that extended an entry
	headWriteOff  bool           // tests: corrections and tx snapshots read the whole history (backlog 68b twin)
	headWrites    atomic.Uint64  // corrections planned from an entry (backlog 68b)
	appendWrites  atomic.Uint64  // open appends planned from the current row alone (backlog 67)
	historyRead   func(rows int) // tests: called with the rows each history read returns
	// readSnapshots — effective scans read their timelines through one store
	// read snapshot (backlog 55, effective_scan_reads.go). nil = per-entity reads.
	readSnapshots storepkg.ReadSnapshotCapability
	// currentStamps — the current-row half of LatestStamps (backlog 42): a
	// row's stamps without the row (badger: the temporal block of a cache
	// miss). nil = store declines or a wrapper only inherits it, so
	// LatestStamps reads the current row (currentStampsCapability).
	currentStamps storepkg.CurrentStampsCapability
	// historyStamps — the history's newest stamps without its rows (backlog
	// 30), for an append planned from the current row (backlog 67). nil =
	// store declines or a wrapper only inherits it (its own history writes
	// would not be noted): the append is planned from the history.
	historyStamps storepkg.HistoryStampsCapability
	// memberIDs — a label's / type's current members from the store's index
	// without their rows (backlog 33): the candidate gather of the effective
	// scans. nil = store declines or a wrapper only inherits it, so the gather
	// reads the rows (memberIDsCapability).
	memberIDs storepkg.MemberIDsCapability
	// valid-end indexes (backlog 51, effective_validfrom.go): the candidates
	// of the valid-end bounded scans. nil = store declines (sharded,
	// wrappers): the unbounded door's candidates, same answers.
	relValidEnds  storepkg.RelValidEndCapability
	nodeValidEnds storepkg.NodeValidEndCapability
	// Ordered index walks and stated bounds (nativeCapability): nil = the
	// store declines or a wrapper only inherits them. A store whose rows the
	// graph does not trust (sharded) is served with every row validated and
	// copied (trustedCurrentNode / trustedCurrentRel).
	nodeOrderedRange storepkg.NodeOrderedRangeScanCapability
	nodePrefixScan   storepkg.NodePrefixScanCapability
	relOrderedRange  storepkg.RelOrderedRangeScanCapability
	relPrefixScan    storepkg.RelPrefixScanCapability
	nodeBounds       storepkg.NodePropertyBoundsCapability
	relBounds        storepkg.RelPropertyBoundsCapability
	// The DocValues snapshot doors of round 10 (docvalues.go), nativeCapability.
	docMultiSnap   nodeDocValuesMultiSnapshotter
	docOrdinalSnap nodeDocValuesOrdinalSnapshotter
	// temporalCandidates — valid-time candidate prune: narrow a temporal
	// ByLabel/ByType query's candidate set by the per-label valid-time envelope
	// index before resolving each chain. nil = store declines (no temporal-index
	// support), so the query resolves every candidate (correct, unaccelerated).
	// Sound for any store: an id the index does not cover is never pruned.
	temporalCandidates storepkg.TemporalCandidateCapability
	// relTypeTemporalCandidates — BACKLOG 21c, the rel-type-keyed mirror of
	// temporalCandidates. nil = store declines (no rel-type temporal index), so the
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
	// compaction. Both are bare optional capabilities: the store is fixed after
	// New, so a nil handle means the store declines the capability.
	metaKV storepkg.MetaKVCapability
	// hashesOff is Config.DisableIntegrityHashes (backlog 61): no content
	// hash is computed or stored (integrity_switch.go).
	hashesOff bool
	// integrityModeStamp: New persists the setting once every check passed
	// (resolveIntegrityMode / persistIntegrityMode).
	integrityModeStamp bool
	// untimedUnbounded is the untimed valid-start mode the graph writes
	// creates under (Config.UntimedValidStart == UntimedUnbounded, or after
	// Admin().SetUntimedValidStart); untimedModeStamp / untimedResume are
	// resolveUntimedMode's decisions for New (backlog 73, untimed_mode.go).
	// untimedIncomplete mirrors the persisted "converting" key: a conversion
	// started and did not finish, so exports and untimed creates fail closed
	// (checkUntimedComplete).
	untimedUnbounded atomic.Bool
	untimedWholeAxis atomic.Bool // UntimedWholeAxis (round 11); untimedUnbounded is set too
	// validAxis is the valid axis the graph reads and writes rows on (round
	// 11, types.ValidAxis): the literal axis under UntimedWholeAxis, else the
	// legacy one; set with the mode (setUntimedMode), never per row.
	validAxis             storeutil.ValidAxisCell
	untimedIncomplete     atomic.Bool
	untimedModeStamp      bool
	untimedResume         bool
	historyCompaction     storepkg.HistoryCompactionCapability
	retentionPurge        storepkg.RetentionPurgeCapability
	retentionPurgeValidTo storepkg.RetentionPurgeByValidToCapability
	exactErasure          storepkg.ExactErasureCapability
	rangePurgeLog         storepkg.RangePurgeLogCapability
	relRetentionPurge     storepkg.RelRetentionPurgeCapability
	relRangePurgeLog      storepkg.RelRangePurgeLogCapability
	readOnlyReplica       bool
	// allowRetentionPurge gates the ADR-0008 R2 hard-purge admin door
	// (g.Admin().PurgeExpiredNodes). Off by default — a destructive, no-tombstone
	// range removal must be explicitly enabled. Wired from Config.AllowRetentionPurge.
	allowRetentionPurge bool
	// allowReset gates the whole-graph destructive wipe door (g.Admin().Reset()).
	// Off by default. Wired from Config.AllowReset.
	allowReset bool
	// allowExactErasure gates the bounded legal-erasure admin door. Off by
	// default; unlike Reset, scope is explicit and fail-closed.
	allowExactErasure bool
	// durableFlush is set when Config.DurableCommit is on: every commit group
	// (GraphTx.Commit, Batch.Execute) calls it once before returning success
	// (flushDurableCommit). Nil = off, no flush on commit.
	durableFlush storepkg.DurableFlushCapability
	// flushHold is set when DurableCommit is on and the store flushes every
	// mutation (Badger SyncWrites): a group's mutations run under c.mu.Lock
	// with the store's per-mutation flush held, and the group's one
	// DurableFlush makes them durable (backlog 52). Nil = no hold.
	flushHold storepkg.MutationFlushHoldCapability
	// allowTxBackfill enables the privileged transaction-time backfill doors:
	// when true, create doors honor a caller-supplied tkg_tx_from (or
	// AddWithTx) instead of stamping c.now(), and DeleteWithTx / UpdateWithTx
	// (and their GraphTx, batch and ingest twins) end or supersede belief at a
	// caller instant, so a re-ingest can faithfully reproduce a historical
	// knowledge time (Erkenntniszeit) addressable via AS OF SYSTEM TIME. Off by
	// default (production rejects backfill with ErrTxBackfillDisabled). Wired
	// from Config.AllowTxBackfill; set once in New.
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
	// c.mu.Lock. Standalone mutations and reads acquire c.mu.RLock and proceed
	// concurrently with an open tx, so only entity-level conflicts block. This
	// avoids the "read accessor inside a tx deadlocks against c.mu" bug class
	// (lesson 31). The tx code path takes a brief c.mu.RLock around each
	// mutation/read so the *Internal/*Locked helpers run with the lock context
	// they expect.
	txMu sync.Mutex
	// openTxStart is the start instant (GraphTx.StartInstant) of the open
	// transaction, 0 when none is open: stored by BeginTx before it returns
	// (before the transaction's first write) and cleared by Commit and
	// Rollback before c.txMu is released. TempOps.CommittedTx reads it.
	openTxStart atomic.Int64
	registryMu  sync.Mutex
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
	// indexEpoch is the index-inventory epoch (round 4 R1): advanced after every
	// index create/drop that reached the store and after every store Clear
	// (index_inventory_epoch.go).
	indexEpoch atomic.Uint64
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
	// retentionRelMaxWatermark is the max over the per-type relationship
	// retention watermarks (backlog 21), relRetentionWatermarks the per-type
	// values (copy-on-write, nil until a relationship purge ran), and
	// retentionAnyMax the max of both families: the one-load fast gate of every
	// scan and point door. retentionMu serializes the watermark advances (the
	// purge doors do not hold c.mu). See retention_rel.go.
	retentionRelMaxWatermark atomic.Int64
	relRetentionWatermarks   atomic.Pointer[map[uint16]int64]
	retentionAnyMax          atomic.Int64
	retentionMu              sync.Mutex
	// asOfColumns caches the columnar as-of snapshot of a label's members at a fixed
	// past txAt, invalidated only by history rewrites (compaction / retention purge /
	// truncate / past-dated backfill or replica apply) — a past belief is immutable
	// under forward ingest, so the cache survives write-active ingest. See
	// docvalues_asof_cache.go and buildAsOfColumns.
	asOfColumns *asOfColumnCache
	// backfillGateHook, when non-nil, runs each time resolveBackfillTxFrom
	// honors a caller transaction instant — before the door's store write. A test
	// seam (like compactionChunkHook) letting a test run an as-of column build
	// between the gate and the write. Set before the graph is shared; never set
	// in production code.
	backfillGateHook func()
	registryDirty    atomic.Bool
	relTypeCache     map[string]uint16
	relTypeCacheMu   sync.RWMutex
	closeOnce        sync.Once
	closed           atomic.Bool
	// purgeChunkHook, when non-nil, runs inside a retention purge after the
	// watermark and the change-log record, before the first chunk (test seam,
	// like backfillGateHook).
	purgeChunkHook func()

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

	// clockExhausted is set once now() saturates at MaxInt64. See
	// ErrCommitClockExhausted.
	clockExhausted atomic.Bool

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

	// chainReadHook, when non-nil, runs between a per-entity chain
	// assembly's current-row read and its history read (afterCurrentRead).
	// Production leaves it nil; tests land a concurrent move there.
	chainReadHook func(id int64)
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
	// ErrCommitClockExhausted rejects a write once transaction time has reached
	// MaxInt64. The clock saturates rather than wrapping, but a saturated clock
	// hands out the SAME instant repeatedly, so two versions would be stamped
	// identically and the superseded row's [TxFrom, TxTo) would collapse to zero
	// width — invisible, TxFrom/TxTo being outside the integrity hash. Refusing
	// the write is the only outcome that is neither silent nor wrong. Unreachable
	// in practice: every untrusted door bounds its stamps, so this is defence in
	// depth.
	ErrCommitClockExhausted = errors.New("graph: commit clock exhausted (transaction time reached its maximum)")
	// ErrForeignStampImplausible rejects a cross-machine transaction stamp that
	// is implausibly far past this host's clock. RecordForeignIncoming stores
	// edge.TxFrom verbatim and advances the commit-clock floor to it, so the
	// stamp is untrusted input on a public door — bounded like an import stream.
	ErrForeignStampImplausible = errors.New("graph: foreign transaction stamp implausibly far past this host's clock")
	ErrSelfLoop                = errors.New("graph: self-loop relationship not allowed; set AllowSelfLoops in ValidationLimits to permit")

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

	// ErrVectorSearchTxPinUnsupported is returned by SearchNearest (standalone
	// and tx mirror) when QueryOpts.TxPin is set. The vector index holds only
	// the LATEST vector per node and drops deleted nodes entirely, so a
	// belief-state (AS OF SYSTEM TIME) nearest-neighbor ranking is ill-defined:
	// a node hard-deleted after the pin — visible to every other TxPin door —
	// would be silently missing from the candidate set, and distances would
	// rank by post-pin vector values. The door refuses loudly instead of
	// returning a silently wrong belief state.
	ErrVectorSearchTxPinUnsupported = errors.New("graph: SearchNearest does not support QueryOpts.TxPin (vector index holds only latest vectors; belief-state ranking is ill-defined)")

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

	// ErrTxOrder is returned by a door that ends or supersedes belief at a
	// caller-supplied transaction instant t (Rels().DeleteWithTx,
	// Rels().UpdateWithTx) when t cannot be placed on the entity's chain: t is
	// not after every TxFrom/TxTo recorded for the entity, not after the
	// current version's start (UpdatedAt or effective ValidFrom), a recorded
	// close (ValidTo) lies at or after a delete's t, or an update at t records
	// no change. The wrapped error names the conflicting stamp. It wraps
	// ErrInvalidTxFrom, so errors.Is matches both.
	ErrTxOrder = fmt.Errorf("graph: caller transaction instant does not follow the entity's recorded history (%w)", ErrInvalidTxFrom)

	// ErrZeroInstantLegacyAxis (round 11) refuses a valid-time claim of the
	// instant 0 (types.ExplicitInstant(0) as tkg_valid_from or tkg_valid_to)
	// on a graph whose store is on the legacy valid axis
	// (Config.UntimedValidStart UntimedFromTxFrom or UntimedUnbounded): its
	// rows read a stored valid-from of 0 as "no claim" and a stored valid-to
	// of 0 as the open end, so they cannot record the instant 0. An
	// UntimedWholeAxis store records it (every stored instant is literal).
	// Nothing is written. It wraps ErrInvalidTimeRange, so errors.Is matches
	// both.
	ErrZeroInstantLegacyAxis = fmt.Errorf("graph: the instant 0 cannot be claimed on a store with the legacy valid axis (%w)", ErrInvalidTimeRange)

	// ErrEntityDeleted is returned by Temporal().SetNodeVersionInterval /
	// SetRelVersionInterval and their GraphTx, Batch and ingest Session twins
	// when the entity was hard-deleted: its chain ends in a tombstone and a
	// correction appended after it would be a belief nothing reads
	// consistently. The returned error also wraps ErrNodeNotFound or
	// ErrRelNotFound, so errors.Is matches both this sentinel and the kind's
	// not-found sentinel. An ID that never existed returns only the not-found
	// sentinel.
	ErrEntityDeleted = errors.New("graph: entity is deleted")

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

	// ErrIntegrityDisabled is returned by the integrity doors (Verify*Chain)
	// of a graph opened with Config.DisableIntegrityHashes: it holds no hashes,
	// so there is nothing to verify, and the answer is not "valid". New also
	// returns it for Config.RelSegments with hashes off (a segment's stored
	// witness is built over the rows' hashes).
	ErrIntegrityDisabled = errors.New("graph: integrity hashes are disabled (Config.DisableIntegrityHashes)")

	// ErrIntegrityModeMismatch is returned by New when the store was written
	// under the other Config.DisableIntegrityHashes setting. The setting is
	// persisted with the store; a store holding rows from before the setting
	// existed was written with hashes on. A store is never mixed.
	ErrIntegrityModeMismatch = errors.New("graph: store was written under the other integrity-hash setting (Config.DisableIntegrityHashes)")

	// ErrUntimedModeMismatch is returned by New when the store's rows were
	// written under the other Config.UntimedValidStart mode, configured
	// explicitly (UntimedDefault takes the store's mode; a store with rows
	// and no persisted mode is an UntimedFromTxFrom store) or its conversion
	// to UntimedUnbounded did not finish, and by Admin().SetUntimedValidStart
	// for a conversion back to UntimedFromTxFrom (backlog 73).
	ErrUntimedModeMismatch = errors.New("graph: store was written under the other untimed valid-start mode (Config.UntimedValidStart)")

	// ErrInvalidUntimedValidStart is returned by New and by
	// Admin().SetUntimedValidStart for an UntimedValidStart value that names
	// no mode, and by Admin().SetUntimedValidStart for UntimedDefault (a
	// conversion names its target) (backlog 73).
	ErrInvalidUntimedValidStart = errors.New("graph: invalid untimed valid-start mode (Config.UntimedValidStart)")

	// ErrCommitNotDurable is returned by GraphTx.Commit, Batch.Execute and the
	// strong ingest applier under Config.DurableCommit when the group committed
	// (it is visible, cannot be rolled back, and its change-log records are
	// minted) but the store could not make it durable. The group stays in the
	// store's pending write buffer; the next successful flush (the next durable
	// commit, an empty Tx().Run, the background flush, or Close) persists it.
	// Until then a crash can lose it. The store's error is wrapped alongside.
	ErrCommitNotDurable = errors.New("graph: commit applied but not durable")

	// ErrExactErasureDisabled is returned unless the destructive exact-erasure
	// admin door was explicitly enabled.
	ErrExactErasureDisabled = errors.New("graph: exact erasure is disabled (set Config.AllowExactErasure to enable g.Admin().ExactErase)")

	// ErrInvalidExactErasureRequest is returned for an empty request or a
	// non-positive node/relationship ID.
	ErrInvalidExactErasureRequest = errors.New("graph: exact erasure requires at least one positive node or relationship ID")

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
	Store          storepkg.MandatoryStore
	BadgerDir      string
	BadgerInMemory bool
	Validation     ValidationLimits
	// SyncWrites makes the badger-backed store flush every mutation before it
	// returns and Badger fsync each write (strict power-loss durability). Under
	// DurableCommit the mutations of a commit group are synced together at the
	// group's durable commit instead (see DurableCommit). Power-loss durability
	// of a commit group needs both flags, on the badger and sharded stores
	// (badger.Config / sharded.Config SyncWrites). The tiered store has no
	// SyncWrites: its DurableCommit groups are process-crash safe only. Ignored
	// when Store is provided explicitly (set it on the store's own config).
	SyncWrites           bool
	Compression          options.CompressionType
	ZSTDCompressionLevel int
	// CacheCapacity is the per-entity-cache (nodes, rels) soft limit for the
	// badger-backed store constructed from BadgerDir/BadgerInMemory.
	// 0 means the store default (10,000). Size it to the scan working set:
	// label scans over more nodes than the cache holds decode every miss
	// from badger instead of hitting the cache. Ignored when Store is provided
	// explicitly.
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
	// of an in-memory map. Existing data directories need no migration.
	// Trade-off: label snapshots cost a disk prefix iteration.
	// Ignored when Store is provided explicitly.
	LabelIndexOnDisk bool
	// AdjacencyIndexOnDisk is the adjacency sibling of LabelIndexOnDisk:
	// outgoing/incoming snapshots come from the persisted adjacency
	// keyspaces instead of the in-memory maps. No migration needed.
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
	// in between carrying only changed properties vs the interval anchor
	// (ADR-0009 / B6). Reads accept both full and delta rows, so the flag is toggleable on an
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
	// tkg_tx_from is rejected with ErrTxBackfillDisabled. The plain update and
	// delete doors keep the monotonic system clock (a correction recorded now
	// is stamped now); only the explicit DeleteWithTx / UpdateWithTx doors —
	// Nodes() and Rels(), and their GraphTx, BatchBuilder and ingest Session
	// twins — end or supersede belief at a caller instant, under the same gate
	// and an order check against the recorded chain (ErrTxOrder). TxFrom and
	// TxTo are not part of the integrity hash, so such a row still verifies and
	// replicates verbatim.
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

	// AllowExactErasure enables g.Admin().ExactErase, a bounded hard-removal
	// door for caller-declared node/relationship sets. It removes current rows,
	// full history and index residue without tombstones, refuses scope escape,
	// and is unavailable while any change-log material is retained.
	AllowExactErasure bool

	// DurableCommit makes every commit group durable before its door returns
	// success: GraphTx.Commit (and so Tx().Run / RunContext / RunWithLSN),
	// Batch.Execute and the strong ingest applier call the store's
	// store.DurableFlushCapability once, after the group is applied, which
	// writes the pending write buffer and fsyncs the write-ahead log of every
	// open shard that took writes. A crash after the door returned keeps the
	// whole group; a crash before it returned can keep any subset (not
	// promised either way). Rollback never flushes (except under SyncWrites,
	// below). A failed flush returns
	// ErrCommitNotDurable: the group is committed in memory and stays pending
	// for the next flush. Not covered: standalone mutations and
	// concurrent-mode ingest Submit, which keep the async flush. A buffer
	// above Badger's transaction size limit is split by Badger, so a crash
	// during that flush can persist a subset. Power loss: Badger's Sync covers
	// only the active memtable WAL and the current value-log file, so rows of
	// a group whose flush filled the memtable (or a value-log file) can stay
	// unsynced after the return — safe against a process crash, not a power
	// loss. Power-loss durability needs DurableCommit + SyncWrites on a badger
	// or sharded store (backlog 15). The tiered store has no SyncWrites, so its
	// groups keep that power-loss limit. With SyncWrites on a badger store a
	// group costs one synced WriteBatch, not one sync per mutation: its
	// mutations (each GraphTx mutation, the Batch.Execute window, a Rollback)
	// run under the exclusive graph lock with the per-mutation
	// flush held (store.MutationFlushHoldCapability), the held buffer flushes
	// in steps at MaxPendingWrites, and a Rollback ends with one durable flush
	// (part of the group can already be on disk). Side effect: with both
	// flags, each GraphTx mutation takes the exclusive graph lock instead of
	// the shared one, so standalone writers, readers and concurrent ingest
	// lanes wait for each tx mutation (the sync is at the commit, outside the
	// lock). New fails with ErrCapabilityNotSupported when the store has no
	// stable storage (memory store, BadgerInMemory, an in-memory tiered or
	// sharded store). Default false: no flush on commit.
	DurableCommit bool

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

	// RelSegments declares bulk relationship types (ADR-0011): the store seals
	// each declared type's unsealed rows into immutable column segments once
	// the unsealed rows of all declared types exceed SegmentMemoryBudget, and
	// every read door answers from the union of the unsealed rows and the
	// segments — identical answers, per-row heap removed. Empty (default) =
	// no segments, byte-for-byte today's behavior. S2: the memory store keeps
	// segments in RAM; any other backend fails New with
	// ErrCapabilityNotSupported. A declared type's token is created at New when
	// the type is new.
	RelSegments []storepkg.RelSegmentSpec
	// SegmentMemoryBudget is the memtable budget in bytes (the unsealed rows'
	// types.Relationship.ApproxHeapBytes, all declared types together); 0 =
	// store.DefaultSegmentMemtableBudget (256 MiB); negative fails New.
	SegmentMemoryBudget int64
	// SegmentDir is the directory (the NVMe) that sealed segments of the
	// declared types are written to as self-contained files, listed in its
	// manifest and read memory-mapped (ADR-0011 S3). Empty (default) = the
	// segments stay in RAM (S2). New takes the directory's lock, and a
	// directory that already holds segments is recovered and its rows become
	// sealed rows of the graph (the manifest is the truth). Requires
	// RelSegments and a backend with store.RelSegmentDirCapability (memory);
	// otherwise New fails closed. Whitespace-only is rejected.
	SegmentDir string

	// DisableIntegrityHashes turns the integrity hashes off (backlog 61):
	// no content hash is computed and none is stored, resident or on disk —
	// Hash, PrevHash, FromNodeHash and ToNodeHash stay empty on every row
	// (provenance fields are unaffected). For development and quick tests;
	// production keeps the default false (hashes on, tamper evidence through
	// g.Hash().VerifyNodeChain / VerifyRelChain). With it set, the Verify*Chain
	// doors return ErrIntegrityDisabled, Config.RelSegments is refused with
	// ErrIntegrityDisabled, an import or replica apply drops the hashes its
	// records carry (verifying any it gets), and an on-graph refuses an
	// export written by an off-graph (ErrCorruptExport, no hash). The setting
	// is persisted with the store at the first open; reopening under the other
	// setting fails with ErrIntegrityModeMismatch (a store holding rows from
	// before this field existed is an on-store).
	DisableIntegrityHashes bool

	// UntimedValidStart is where an entity's first version written without a
	// valid-from starts in valid time (backlog 73). UntimedFromTxFrom starts
	// it at its TxFrom (backlog 62); UntimedUnbounded at -inf: the create
	// stores ValidFrom = types.UnboundedValidFrom, so the entity is valid at
	// every instant before its first change. A later version without a
	// valid-from starts at its TxFrom in both modes. The mode is persisted
	// with the store at the first open; reopening under the other explicit
	// mode fails with ErrUntimedModeMismatch (a store holding rows from
	// before this field existed is an UntimedFromTxFrom store). An existing
	// store is converted once with Admin().SetUntimedValidStart(UntimedUnbounded).
	// UntimedWholeAxis (round 11, fixed at store creation): a write
	// without a valid time makes no valid-time claim; an untimed update
	// applies to every valid-time segment (SQL:2011 UPDATE without FOR
	// PORTION OF) and an untimed delete is a retraction. See docs/api.md.
	//
	// The zero value, UntimedDefault, opens an existing store under the mode
	// it was written under and makes a new store an UntimedUnbounded store
	// (v4.57.0; up to v4.56.x the zero value was UntimedFromTxFrom, so a
	// store created under v4.56.x defaults stays an UntimedFromTxFrom store).
	// Set UntimedFromTxFrom explicitly to keep the TxFrom start for new
	// stores.
	//
	// Deprecated default (announced in round 11): from the next minor
	// release the zero value makes a new store an UntimedWholeAxis store
	// (when its store implements store.ValidAxisCapability; memory, badger,
	// tiered and sharded do). Existing stores keep their persisted mode. Set
	// UntimedUnbounded explicitly to keep today's default for new stores.
	UntimedValidStart UntimedValidStart
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
	// sharded is not an exact native store (its rows are validated and
	// copied), but a wrapper embedding it must not have its overrides routed
	// around through a capability it only inherits.
	reflect.TypeOf((*sharded.Store)(nil)),
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
// unsharded badger — BACKLOG 20e). The memory store also IMPLEMENTS the capability (contract + direct test),
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

// labelTxMembershipCapability resolves the K1 transaction-time label-membership
// sidecar. Only the exact native single-shard backends (memory, badger) own a
// coherent whole-store membership index (tiered: the union of its shards'); a
// wrapper that merely EMBEDS a native store is forced to nil so the query
// falls back to the correct (if unaccelerated) full-history candidate fold.
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
// temporalMetaHistoryCapability admits the selection-skeleton history door for
// exact native stores only — a wrapper embedding a native store must not
// promote it (same visibility guard as every cached capability handle).
func temporalMetaHistoryCapability(store storepkg.MandatoryStore) storepkg.TemporalMetaHistoryCapability {
	cap, ok := store.(storepkg.TemporalMetaHistoryCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.TemporalMetaHistoryCapability)(nil)).Elem(),
		"NodeHistoryTemporalMeta") {
		return nil
	}
	return cap
}

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

// labelTxMemberLookupCapability resolves the point form of the K1 label
// sidecar with labelTxMembershipCapability's discipline: an exact native
// store, or a store that declares the method itself; a wrapper that merely
// embeds a native store is forced to nil (its writes may bypass the
// embedded store's sidecar maintenance).
func labelTxMemberLookupCapability(store storepkg.MandatoryStore) storepkg.LabelTxMemberLookupCapability {
	cap, ok := store.(storepkg.LabelTxMemberLookupCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.LabelTxMemberLookupCapability)(nil)).Elem(),
		"LabelTxMembersOf") {
		return nil
	}
	return cap
}

// relTypeTxMemberLookupCapability is the rel-type mirror of
// labelTxMemberLookupCapability.
func relTypeTxMemberLookupCapability(store storepkg.MandatoryStore) storepkg.RelTypeTxMemberLookupCapability {
	cap, ok := store.(storepkg.RelTypeTxMemberLookupCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelTypeTxMemberLookupCapability)(nil)).Elem(),
		"RelTypeTxMembersOf") {
		return nil
	}
	return cap
}

// relPropertyTxMembershipCapability resolves the backlog-8 rel property
// membership sidecar with the K1 discipline: exact native stores and direct
// implementations (sharded) are trusted; a wrapper that merely embeds a native
// store is forced to nil, because its read overrides would not be visible to
// the sidecar, so its lookups take the full-history fold.
func relPropertyTxMembershipCapability(store storepkg.MandatoryStore) storepkg.RelPropertyTxMembershipCapability {
	cap, ok := store.(storepkg.RelPropertyTxMembershipCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelPropertyTxMembershipCapability)(nil)).Elem(),
		"ForEachRelPropertyTxMember") {
		return nil
	}
	return cap
}

// nodePropertyTxMembershipCapability is the node twin of
// relPropertyTxMembershipCapability.
func nodePropertyTxMembershipCapability(store storepkg.MandatoryStore) storepkg.NodePropertyTxMembershipCapability {
	cap, ok := store.(storepkg.NodePropertyTxMembershipCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.NodePropertyTxMembershipCapability)(nil)).Elem(),
		"ForEachNodePropertyTxMember") {
		return nil
	}
	return cap
}

// relPropertyTxMemberPageCapability resolves the backlog-69 paged form of the
// rel property membership sidecar with relPropertyTxMembershipCapability's
// discipline (a wrapper that merely embeds a native store is forced to nil).
func relPropertyTxMemberPageCapability(store storepkg.MandatoryStore) storepkg.RelPropertyTxMemberPageCapability {
	cap, ok := store.(storepkg.RelPropertyTxMemberPageCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.RelPropertyTxMemberPageCapability)(nil)).Elem(),
		"ForEachRelPropertyTxMemberAfter") {
		return nil
	}
	return cap
}

// nodePropertyTxMemberPageCapability is the node twin of
// relPropertyTxMemberPageCapability.
func nodePropertyTxMemberPageCapability(store storepkg.MandatoryStore) storepkg.NodePropertyTxMemberPageCapability {
	cap, ok := store.(storepkg.NodePropertyTxMemberPageCapability)
	if !ok {
		return nil
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*storepkg.NodePropertyTxMemberPageCapability)(nil)).Elem(),
		"ForEachNodePropertyTxMemberAfter") {
		return nil
	}
	return cap
}

// nativeCapability resolves an optional capability C with the discipline of
// relPropertyTxMemberPageCapability: an exact native store's, or a wrapper's
// own methods (every one named), never one a wrapper only promotes from an
// embedded native store (that one answers for the inner store, not the
// wrapper's rows). The zero C where the store declines.
func nativeCapability[C any](store storepkg.MandatoryStore, methods ...string) C {
	var zero C
	cap, ok := store.(C)
	if !ok {
		return zero
	}
	if isExactNativeStore(store) {
		return cap
	}
	if embedsNativeCapability(store, reflect.TypeOf((*C)(nil)).Elem(), methods...) {
		return zero
	}
	return cap
}

// changeFeedCapability resolves the optional change-log capability. Only the
// exact native single-shard backends (memory, badger) own a coherent global
// LSN sequence. A wrapper that merely EMBEDS a native store (e.g. a future
// sharded backend, or tiered if it ever embedded a shard) would promote the
// badger ChangeFeed methods and expose ONE shard's per-shard log as if it were
// the cluster feed — force nil there so such a backend must implement a real
// global feed itself. tiered.Store provides a coherent store-global LSN
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

// persistRegistriesLockedPanicSafe persists the registries unconditionally.
// Caller holds registryMu; a backend panic releases it before propagating,
// like the dirty-only sibling below.
func (c *Core) persistRegistriesLockedPanicSafe() error {
	defer func() {
		if r := recover(); r != nil {
			c.registryMu.Unlock()
			panic(r)
		}
	}()
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
	if config.UntimedValidStart > UntimedWholeAxis {
		return nil, fmt.Errorf("graph: Config.UntimedValidStart %d: %w", config.UntimedValidStart, ErrInvalidUntimedValidStart)
	}
	if config.DisableIntegrityHashes && len(config.RelSegments) > 0 {
		// A segment stores no per-row hash: its witness is the block roots
		// over the rows' hashes, recomputed on every decode (ADR-0011 §4.3).
		return nil, fmt.Errorf("graph: RelSegments needs integrity hashes: %w", ErrIntegrityDisabled)
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
		allowExactErasure:   config.AllowExactErasure,
		hashesOff:           config.DisableIntegrityHashes,
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
	if config.DurableCommit {
		// The flag promises durability; a store without stable storage
		// declines instead of turning it into a silent no-op.
		df, ok := store.(storepkg.DurableFlushCapability)
		if !ok || !df.DurableFlushSupported() {
			if config.Store == nil {
				_ = store.Close()
			}
			return nil, fmt.Errorf("graph: Config.DurableCommit needs a store with stable storage (a disk badger, tiered or sharded store): %w", storepkg.ErrCapabilityNotSupported)
		}
		c.durableFlush = df
		if h, ok := store.(storepkg.MutationFlushHoldCapability); ok && h.FlushesEveryMutation() {
			c.flushHold = h
		}
	}
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
	c.propertyKeysQuery = nativeCapability[storepkg.PropertyKeysLookupCapability](store, "NodesByLabelAndPropertyKeys")
	c.relPropertyKeysQuery = nativeCapability[storepkg.RelPropertyKeysLookupCapability](store, "RelationshipsByTypeAndPropertyKeys")
	c.compositeQuery = compositeQueryCapability(store)
	c.compositeQueryTrust = isExactNativeStore(store)
	c.filteredVector = filteredVectorSearchCapability(store)
	c.vectorIndexOptions = vectorIndexOptionsCapability(store)
	c.depthHistory = depthHistoryIterationCapability(store)
	c.relCreateUndo, c.scopedRelCreateUndo = relCreateUndoCapabilities(store)
	c.changeFeed = changeFeedCapability(store)
	c.changeLogEnabled = changeLogStatusEnabled(store)
	c.txLogScope = txChangeLogScope(store, c.changeLogEnabled)
	c.scopedChangeLog = scopedTxCapability(store)
	c.labelTxMembers = labelTxMembershipCapability(store)
	c.relTypeTxMembers = relTypeTxMembershipCapability(store)
	c.labelTxLookup = labelTxMemberLookupCapability(store)
	c.relTypeTxLookup = relTypeTxMemberLookupCapability(store)
	c.relPropTxMembers = relPropertyTxMembershipCapability(store)
	c.nodePropTxMembers = nodePropertyTxMembershipCapability(store)
	c.relPropTxPage = relPropertyTxMemberPageCapability(store)
	c.nodePropTxPage = nodePropertyTxMemberPageCapability(store)
	c.relPropTxWindow = nativeCapability[storepkg.RelPropertyTxMemberWindowCapability](store, "ForEachRelPropertyTxMemberDuring")
	c.nodePropTxWindow = nativeCapability[storepkg.NodePropertyTxMemberWindowCapability](store, "ForEachNodePropertyTxMemberDuring")
	c.relPropTxKeys = nativeCapability[storepkg.RelPropertyTxMemberKeysCapability](store, "ForEachRelPropertyTxMemberKeys", "ForEachRelPropertyTxMemberKeysDuring")
	c.nodePropTxKeys = nativeCapability[storepkg.NodePropertyTxMemberKeysCapability](store, "ForEachNodePropertyTxMemberKeys", "ForEachNodePropertyTxMemberKeysDuring")
	c.nodeBeliefWatermark = nodeBeliefWatermarkCapability(store)
	c.relBeliefWatermark = relBeliefWatermarkCapability(store)
	c.temporalMetaHistory = temporalMetaHistoryCapability(store)
	c.historyEpochs = historyEpochCapability(store)
	c.readSnapshots = readSnapshotCapability(store)
	c.currentStamps = currentStampsCapability(store)
	c.historyStamps = nativeCapability[storepkg.HistoryStampsCapability](store, "NodeHistoryStamps", "RelHistoryStamps")
	c.memberIDs = memberIDsCapability(store)
	c.relValidEnds, c.nodeValidEnds = validEndCapabilities(store)
	c.nodeOrderedRange = nativeCapability[storepkg.NodeOrderedRangeScanCapability](store, "ForEachNodeByLabelPropertyRangeOrdered")
	c.nodePrefixScan = nativeCapability[storepkg.NodePrefixScanCapability](store, "ForEachNodeByLabelPropertyPrefix")
	c.relOrderedRange = nativeCapability[storepkg.RelOrderedRangeScanCapability](store, "ForEachRelByTypePropertyRangeOrdered")
	c.relPrefixScan = nativeCapability[storepkg.RelPrefixScanCapability](store, "ForEachRelByTypePropertyPrefix")
	c.nodeBounds = nativeCapability[storepkg.NodePropertyBoundsCapability](store, "NodePropertyBounds")
	c.relBounds = nativeCapability[storepkg.RelPropertyBoundsCapability](store, "RelPropertyBounds")
	c.docMultiSnap = nativeCapability[nodeDocValuesMultiSnapshotter](store, "DocValuesSnapshotMulti")
	c.docOrdinalSnap = nativeCapability[nodeDocValuesOrdinalSnapshotter](store, "DocValuesSnapshotOrdinal", "DocValuesSnapshotMultiOrdinal")
	// valid-time candidate prune is sound for ANY store that offers it (an
	// unknown id is never pruned), so unlike the membership sidecar it needs no
	// exact-native-store guard — a plain probe admits any store that implements it.
	c.temporalCandidates, _ = store.(storepkg.TemporalCandidateCapability)
	c.relTypeTemporalCandidates, _ = store.(storepkg.RelTypeTemporalCandidateCapability)
	// Metadata facet: bare optional-capability probes, resolved once because the
	// store is immutable after New.
	c.metaKV, _ = store.(storepkg.MetaKVCapability)
	c.historyCompaction, _ = store.(storepkg.HistoryCompactionCapability)
	c.retentionPurge, _ = store.(storepkg.RetentionPurgeCapability)
	c.retentionPurgeValidTo, _ = store.(storepkg.RetentionPurgeByValidToCapability)
	c.exactErasure, _ = store.(storepkg.ExactErasureCapability)
	c.rangePurgeLog, _ = store.(storepkg.RangePurgeLogCapability)
	c.relRetentionPurge, _ = store.(storepkg.RelRetentionPurgeCapability)
	c.relRangePurgeLog, _ = store.(storepkg.RelRangePurgeLogCapability)
	c.vectorRowsTrust = isExactNativeStore(store)
	c.storeRowsTrust = isExactNativeStore(store)
	c.nativeAdjacency = nativeAdjacencyReadsValidateNodeExistence(store)

	// Backlog 61: the store's integrity-hash setting must equal the configured
	// one, before anything writes a row (integrity_switch.go).
	if err := c.resolveIntegrityMode(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}
	// Backlog 73: likewise the untimed valid-start mode (untimed_mode.go).
	c.setUntimedMode(config.UntimedValidStart)
	if err := c.resolveUntimedMode(config.UntimedValidStart == UntimedDefault); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}
	// Round 11: the store reads its rows on the graph's valid axis.
	if err := c.syncStoreAxis(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}

	// Registry rehydration for caller-injected stores. The Core-
	// constructed badger.Store path above already loads registries; the
	// inject path also has to. Without this, opening an
	// existing badger.Store separately and passing it via
	// `Config{Store: bs}` would start the graph with empty in-memory
	// registries even though the persisted entities use tokenised
	// labels and reltypes — Close would then save the empty registry
	// state back over the persisted mappings.
	//
	// Two interface shapes exist in-tree: badger.Store returns (bool, error)
	// from its loaders while tiered.Store returns (int, error). Both are
	// matched here so the rehydration is dispatched by capability rather than
	// concrete type.
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
	c.backfillExAdjacencyBestEffort(store)
	if err := c.resumeUntimedConversion(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}

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
	if err := c.loadRelRetentionWatermarks(); err != nil {
		return nil, err
	}

	// Reseed the commit-clock floor from the previous session's durable watermark
	// so NowTx()/c.now() stay above every persisted TxFrom across a reopen (a burst
	// whose monotonic floor outran the wall leaves stamps above the reopened wall).
	// Deliberately NOT fail-closed, unlike the watermarks above: an absent or
	// unreadable floor is the expected state after an unclean shutdown, where
	// the store's commit floor (seedCommitFloor below) covers the rows (lesson 71).
	c.seedInstantFloor()

	// ADR-0011: declare bulk relationship types last, once registries are
	// loaded, so a declared name resolves to its persisted token.
	if err := c.declareRelSegments(config.RelSegments, config.SegmentMemoryBudget, config.SegmentDir); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}

	// The watermark above is written only by Close; after a crash the store's
	// commit floor (persisted with the rows, backlog 44) is what covers them.
	// After the segment directory: its sealed rows are part of the floor.
	if err := c.seedCommitFloor(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}

	// Last: a New that failed above has persisted no integrity-hash setting
	// and no untimed valid-start mode.
	if err := c.persistIntegrityMode(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}
	if err := c.persistUntimedMode(); err != nil {
		if config.Store == nil {
			_ = store.Close()
		}
		return nil, err
	}

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
		// (NewAsyncEventBus) that only exits via its own Close(), so it must be
		// closed here or the goroutine leaks. Runs outside c.mu (Close blocks
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
