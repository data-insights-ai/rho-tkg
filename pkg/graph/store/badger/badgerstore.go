// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/valuedict"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// Store-contract sentinel error aliases for readability inside this package.
// The canonical sentinel-error declarations live in pkg/graph/store.
var (
	ErrNodeExists                 = storecontract.ErrNodeExists
	ErrNodeNotFound               = storecontract.ErrNodeNotFound
	ErrRelExists                  = storecontract.ErrRelExists
	ErrRelNotFound                = storecontract.ErrRelNotFound
	ErrVersionNotFound            = storecontract.ErrVersionNotFound
	ErrIndexExists                = storecontract.ErrIndexExists
	ErrIndexNotFound              = storecontract.ErrIndexNotFound
	ErrTemporalIndexExists        = storecontract.ErrTemporalIndexExists
	ErrTemporalIndexNotFound      = storecontract.ErrTemporalIndexNotFound
	ErrInvalidTemporalIndexConfig = storecontract.ErrInvalidTemporalIndexConfig
	ErrVectorIndexExists          = storecontract.ErrVectorIndexExists
	ErrVectorIndexNotFound        = storecontract.ErrVectorIndexNotFound
	ErrDimensionMismatch          = storecontract.ErrDimensionMismatch
	ErrInvalidVectorIndexConfig   = storecontract.ErrInvalidVectorIndexConfig
	ErrInvalidVectorValue         = storecontract.ErrInvalidVectorValue
	ErrInvalidStoreMutation       = storecontract.ErrInvalidStoreMutation
	ErrNilStore                   = storecontract.ErrNilStore
	ErrStoreClosed                = storecontract.ErrStoreClosed
)

func errNilIterationCallback() error {
	return fmt.Errorf("%w: nil iteration callback", ErrInvalidStoreMutation)
}

func errNilLabelRegistry() error {
	return fmt.Errorf("%w: nil label registry", ErrInvalidStoreMutation)
}

func errNilRelTypeRegistry() error {
	return fmt.Errorf("%w: nil relationship type registry", ErrInvalidStoreMutation)
}

// QueryOpts is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type QueryOpts = storecontract.QueryOpts

// DistanceMetric is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type DistanceMetric = storecontract.DistanceMetric

// RelTombstone is a Store-contract alias; canonical declaration lives in
// pkg/graph/store.
type RelTombstone = storecontract.RelTombstone

// Store-contract distance-metric alias constants.
const (
	DistanceCosine    = storecontract.DistanceCosine
	DistanceEuclidean = storecontract.DistanceEuclidean
)

// Default configuration values for Store.
const (
	DefaultCacheCapacity  = 10_000
	DefaultFlushInterval  = 100 * time.Millisecond
	DefaultGCInterval     = 5 * time.Minute
	DefaultGCDiscardRatio = 0.5
	// DefaultMaxPendingWrites bounds the async write buffer: at this many pending ops
	// the writer flushes synchronously (backpressure).
	DefaultMaxPendingWrites = 100_000
)

// Config configures a Store instance.
type Config struct {
	// Dir is the Badger data directory. Required unless InMemory is true.
	Dir string
	// ValidAxis is the valid axis (round 11) of a store that holds no
	// persisted untimed mode of its own: a tiered or sharded shard, whose
	// composite keeps the mode on its reference or anchor shard. A store
	// whose MetaKV holds the mode reads its axis from it. Zero: legacy.
	ValidAxis types.ValidAxis
	// InMemory enables memory-only mode (no disk I/O). Useful for testing.
	InMemory bool
	// Logger is the Badger logger, also used for rho's reports about badger.
	// Nil sends badger's errors and warnings to slog (its info and debug at
	// slog's Debug level).
	Logger badgerv4.Logger
	// CacheCapacity is the per-cache (nodes, rels) soft limit. Default: 10,000.
	CacheCapacity int
	// CacheBudgetBytes bounds each entity cache (nodes, rels) by estimated resident
	// bytes. Soft limit: dirty entries are never evicted. 0 disables byte accounting;
	// with CacheCapacity 0 the byte budget alone governs.
	CacheBudgetBytes int64
	// ResidentCache keeps every decoded node/rel resident (no clean eviction, no LRU
	// promotion on fetch); for in-memory stores this avoids re-decoding on cache miss.
	// Ignores CacheCapacity/CacheBudgetBytes.
	ResidentCache bool
	// LabelIndexOnDisk keeps the label->nodes index out of RAM: label snapshots come
	// from the persisted label keyspace (prefix iterators, unflushed writes overlaid).
	// No migration needed. See badgerstore_label_disk.go.
	LabelIndexOnDisk bool
	// AdjacencyIndexOnDisk is the adjacency sibling of LabelIndexOnDisk: outgoing and
	// incoming snapshots come from the persisted OutKey/InKey keyspaces; typed queries
	// use an 11-byte prefix. See badgerstore_adjacency_disk.go. The open builds no
	// adjacency map, not even transiently, and seeds no rel revisions (a rel's
	// first prefetched write after open re-reads its row under the lock).
	AdjacencyIndexOnDisk bool
	// PropertyIndexOnDisk keeps property-index entries in the persisted 0x0A keyspace
	// (badgerstore_property_disk.go), written with the node row, instead of RAM maps.
	// A directory with property-index definitions but no 0x0A rows is backfilled once
	// on the first open with this flag (storeutil.PropertyIndexOnDiskBuiltKey).
	// Requires a property-key registry (always present via pkg/graph); without one
	// CreatePropertyIndex fails with ErrInvalidStoreMutation. Ignored when Store is
	// provided explicitly.
	PropertyIndexOnDisk bool
	// TemporalIndexOnDisk is a rebuild-at-open accelerator, not a RAM-vs-disk trade:
	// the temporal index stays resident at runtime. On, a compact row per (labelToken,
	// entity) is kept beside the node row (badgerstore_temporal_disk.go) and
	// loadIndexesScan streams it instead of decoding every node. A directory without
	// 0x0B rows is backfilled once on first open with this flag
	// (storeutil.TemporalIndexOnDiskBuiltKey). Ignored when Store is provided
	// explicitly.
	TemporalIndexOnDisk bool
	// DropRelTemporalIndexesAtOpen deletes the persisted rel-type temporal index
	// definitions at open instead of rebuilding them (a read-only open only skips the
	// rebuild). The tiered store sets it for cold shards. Ignored when Store is
	// provided explicitly.
	DropRelTemporalIndexesAtOpen bool
	// HistoryPresenceProbeOnly answers HasNodeHistory / HasRelHistory with a per-ID
	// key probe instead of building the RAM ID set on first call. The tiered store sets
	// it for cold shards. Ignored when Store is provided explicitly.
	HistoryPresenceProbeOnly bool
	// ColumnsOnDisk persists built columnar snapshots so a cold store can decode a
	// column instead of re-reading every entity. Observable only as latency. A persisted
	// column is a rebuild accelerator, never a read authority (as TemporalIndexOnDisk):
	// a stale stamp, unreadable blob or missing key means rebuild, so no wire-format
	// bump. Ignored when Store is provided explicitly.
	ColumnsOnDisk bool
	// DisablePlannerStats turns off maintenance of the query-planner statistics
	// (per-(label, property key) counts, NDV + min/max, type-class counts), which only
	// the planning APIs read (NodeCountByLabelAndPropertyKey, NodeRangeCardinality,
	// NodePropertyTypeClassCounts and rel mirrors). Those methods then fail with
	// ErrCapabilityNotSupported (range-cardinality returns exact=false). No correctness
	// path reads these counters. Ignored when Store is provided explicitly.
	DisablePlannerStats bool
	// HistoryDeltaEncoding stores version-history rows (0x07/0x08) as anchor + delta:
	// version V with V%HistoryAnchorInterval == 0 is a full anchor, the rest carry only
	// properties changed vs the anchor. Reads accept both forms, so the flag can be
	// toggled on an existing store without migration. The current row is always full.
	// Opt-in. See ADR-0009.
	HistoryDeltaEncoding bool
	// HistoryAnchorInterval overrides the anchor spacing for HistoryDeltaEncoding
	// (0 = default 16; validated 0 or [2, 4096]). The interval is baked into the
	// on-disk delta layout: a persisted marker is checked at open and a mismatch fails
	// with ErrHistoryAnchorIntervalMismatch, since a delta read against the wrong
	// anchor is a silent misread. Moot when HistoryDeltaEncoding is off.
	HistoryAnchorInterval int
	// FlushInterval is the time between async write batches. Default: 100ms.
	// Zero disables periodic flushing (manual flush only — for testing).
	FlushInterval time.Duration
	// GCInterval is the time between value log GC runs. Default: 5min.
	// Zero disables GC. Ignored in InMemory mode.
	GCInterval time.Duration
	// GCDiscardRatio is the discard ratio for RunValueLogGC. Default: 0.5.
	GCDiscardRatio float64
	// ReadOnly opens Badger in read-only mode. No flushLoop, no gcLoop,
	// no write operations. Used for warm/cold shards in TieredStore.
	ReadOnly bool
	// SyncWrites flushes every mutation to Badger immediately (no async buffer), at
	// higher write latency. Inside a store.MutationFlushHoldCapability hold (graph
	// DurableCommit group) the flush waits for the group's DurableFlush. Ignored in
	// ReadOnly mode.
	SyncWrites bool
	// MaxPendingWrites bounds the async write buffer: at this many pending ops, or
	// at half of one badger transaction (MaxBatchSize bytes or MaxBatchCount
	// entries) if that comes first, the mutating call flushes synchronously, so
	// such a flush commits as one transaction, not staged. Default
	// DefaultMaxPendingWrites; negative disables the bound, both parts. Under SyncWrites it bounds only writes held by a durable
	// group (HoldMutationFlushes). See docs/architecture.md (Write-Pressure Bound).
	MaxPendingWrites int
	// Compression sets the SSTable compression algorithm.
	// options.Snappy or options.ZSTD. options.None is the zero value and so
	// keeps the Badger default (Snappy): no compression cannot be selected.
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15).
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
	// ValueLogFileSize / MemTableSize / BlockCacheSize / NumCompactors tune Badger's
	// per-instance footprint. Zero keeps Badger's stock defaults (1GB vlog / 64MB
	// memtable / 256MB block cache / 4 compactors); the owner opts in explicitly. One
	// Badger instance opens per shard, so stock sizes multiply by shard count.
	// ValueLogFileSize is validated to [1MB, 2GB); the vlog file appears at 2x while
	// open (sparse).
	ValueLogFileSize int64 // bytes; valid [1MB, 2GB)
	// MemTableSize is the RAM knob: a heap arena of ~size is allocated per open
	// instance, the WAL appears at 2x. Validated to [8MB, 1GB] (Badger needs
	// ValueThreshold 1MB <= 15% of it). Shrinking on an existing dir triggers a
	// one-time WAL migration (see New).
	MemTableSize int64 // bytes; valid [8MB, 1GB]
	// BlockCacheSize bounds the Ristretto block cache (filled lazily, not
	// pre-allocated). Must be >= 0; 0 keeps the stock default.
	BlockCacheSize int64 // bytes; >= 0
	// IndexCacheSize bounds the Ristretto table-index/bloom cache; must be >= 0 (0 =
	// Badger default, no cache). Required (> 0) when EncryptionKey is set, because
	// Badger panics creating an encrypted SSTable without it. See
	// ErrEncryptionRequiresIndexCache.
	IndexCacheSize int64 // bytes; >= 0
	// NumCompactors sets the compactor goroutine count. 0 keeps the stock
	// default (4); any non-zero value must be >= 2 (Badger's minimum).
	NumCompactors int
	// ChangeLog enables the durable ordered change-log: every committed mutation
	// appends a framed record under KeyChangeLog in the same WriteBatch as the data,
	// tagged with a monotonic LSN. Off by default (write path unchanged). Surfaces
	// store.ChangeFeedCapability. Ignored in ReadOnly mode. See
	// badgerstore_changelog.go.
	ChangeLog bool
	// ChangeLogSeqSource, when non-nil, replaces the shard's self-owned LSN counter
	// with an injected store-global allocator, so all shards of a sharded owner (the
	// tiered store) draw LSNs from one sequence. Ignored when ChangeLog is off or in
	// ReadOnly mode. See badgerstore_changelog.go.
	ChangeLogSeqSource ChangeLogSeqSource
	// SharedMutationEpochs, when non-nil, is advanced together with this store's node
	// and relationship mutation epochs, so an owner of several stores keeps store-wide
	// epochs that move on every shard write, open or closed.
	SharedMutationEpochs *SharedMutationEpochs
	// SharedFlushHold, when non-nil, is this store's mutation flush-hold flag
	// (store.MutationFlushHoldCapability), shared with the other stores of an
	// owner: the tiered store holds every shard with one Store(true), and a
	// shard opened during the hold starts held. Matters only under SyncWrites.
	SharedFlushHold *atomic.Bool
	// DisableOrdinals turns the dense ordinals (store.OrdinalCapability) off:
	// every row carries 0 and Max*Ordinal return 0. The tiered store sets it
	// for its shards: a cold shard that closes and reopens would renumber its
	// entities while the tiered store stays open.
	DisableOrdinals bool
	// SharedOrdinals, when non-nil, is the allocator this store draws
	// ordinals from instead of its own, so several stores never hand out
	// the same ordinal (the sharded store's slots share one).
	SharedOrdinals *OrdinalAllocator
	// ForeignNodeOrdinal, when non-nil, gives the dense ordinal of a node this store
	// holds no current row for, so a relationship whose endpoint lives in another store
	// carries that ordinal (Relationship.StartOrdinal / EndOrdinal). Must not call back
	// into this store's ordinal state; nil states 0.
	ForeignNodeOrdinal func(types.NodeID) uint32
	// PropertyKeyRegistry, when non-nil, is the owner's canonical property-key token
	// registry (one for all shards). It is installed before loadIndexes so rows decode,
	// and the store does not load or persist its own copy. When nil the store loads its
	// own from meta.
	PropertyKeyRegistry *registrypkg.PropertyKeyRegistry
	// ValueDictionaries lists the value dictionaries new rows are coded with
	// (DICT, store.ValueDictionary): a member's string value is stored once in
	// this store and a token in each row. The members are named; the graph
	// installs its label and type registries at open (SetNameRegistries).
	// Coded rows decode with or without this list. Validated at New.
	ValueDictionaries []storecontract.ValueDictionary
	// ValueDictionaryPlan, when non-nil, replaces ValueDictionaries with a plan
	// shared by several stores (a tiered store's shards: one set of counters).
	ValueDictionaryPlan *valuedict.Plan
	// OnPropertyKeyGrow, when non-nil, is called from flush() before the row
	// WriteBatch whenever the registry grew (write-ahead registry persistence; the
	// tiered store commits it to the reference shard). When nil the store persists to
	// its own meta. A non-nil error aborts the flush, so rows never become durable
	// before the registry entries they depend on.
	OnPropertyKeyGrow func() error
	// OnChangeLogFlush, when non-nil, is called from flush() after a WriteBatch with
	// change-log records commits (the tiered store persists the allocator high-water,
	// ADR-0005 §2.1-reseed). An error surfaces to the writer without rollback (the
	// records are already durable). nil = no watermark persistence.
	OnChangeLogFlush func() error
	// OnCommitFloor, when non-nil, receives the commit floor (newest transaction stamp
	// of every row written, store.CommitFloorCapability) from flush() before the row
	// WriteBatch whenever it grew, and must persist it where the owner's open reads it.
	// An error aborts the flush. When nil the floor rides the row WriteBatch.
	// durable is true inside DurableFlush: the hook must make the floor durable before
	// returning, so a power loss cannot keep a row whose floor it lost. Called before the
	// batch, and with durable=true again before DurableFlush's WAL fsync for rows a
	// background flush wrote; a background flush passes false (no fsync).
	OnCommitFloor func(floor types.Instant, durable bool) error
	// BeforeFlushRows, when non-nil, runs after a flush takes its snapshot of the
	// pending writes and before it writes them, and again before DurableFlush's WAL
	// fsync of rows an earlier flush wrote; durable is true when those rows are
	// synced (SyncWrites, or inside DurableFlush). The tiered store syncs its owner
	// index there (UNIQIDX), so a claim is durable before the row it backs. An
	// error aborts the flush.
	BeforeFlushRows func(durable bool) error
	// EncryptionKey enables AES encryption-at-rest (SSTables, value log, WAL, key
	// registry). Length must be 0 (disabled), 16, 24 or 32, else New fails with
	// ErrInvalidEncryptionKeyLength. Badger panics on Open without a BlockCacheSize, so
	// New fails with ErrEncryptionRequiresBlockCache instead. Opening with the wrong
	// key, or a plaintext dir with a key, fails with an error wrapping
	// badgerv4.ErrEncryptionKeyMismatch.
	EncryptionKey []byte
	// EncryptionKeyRotation is Badger's EncryptionKeyRotationDuration — how
	// often a new internal data key is generated for the encrypted value log.
	// Zero keeps Badger's stock default (10 days). Ignored when EncryptionKey
	// is empty.
	EncryptionKeyRotation time.Duration
}

// writeOpType indicates the type of deferred write operation.
type writeOpType byte

const (
	writeOpSet    writeOpType = 1
	writeOpDelete writeOpType = 2
)

// writeOp is a single deferred write to Badger.
type writeOp struct {
	opType writeOpType
	key    []byte
	value  []byte // nil for deletes and index entries
}

// pendingLogRecord is a change-log record buffered for the next flush: value is
// the framed on-disk value, lsn its cluster sequence. pendingLog is append-only
// (never coalesced) and guarded by wbMu with pending, so flush() snapshots a record
// and its entity ops together.
type pendingLogRecord struct {
	lsn   uint64
	value []byte
}

// Store implements the Store interface on Badger. In-memory state (LRU caches
// with dirty tracking, label/type/adjacency indexes) is the source of truth while
// running; writes update it immediately and queue writeOps for async batch
// persistence by a background flush loop (every FlushInterval). A GC loop runs
// RunValueLogGC; shutdown does a final flush. Counters are atomic and persisted in
// the flush WriteBatch. See docs/architecture.md (badger.Store).
type Store struct {
	db *badgerv4.DB

	// validAxis is the axis the store reads its rows on (round 11,
	// store.ValidAxisCapability), fixed at open (openValidAxis).
	validAxis storepkg.ValidAxisCell

	// In-memory indexes (source of truth while running).
	// Protected by idxMu for concurrent read/write access.
	idxMu                     sync.RWMutex
	nodeIDs                   indexpkg.IDSet                                // live node IDs (EXISTMAP: sorted chunks, ~8 B per ID)
	nodeHashes                map[types.NodeID]string                       // lazy cache of current node integrity hashes (NodeIntegrityHash fills, every node-row write drops; row 66), at most nodeHashLimit() entries (EXISTMAP)
	nodeHashCap               int                                           // test switch for nodeHashLimit; 0 = nodeHashCacheCap
	imageSectionFilterForTest func(kind byte) bool                          // test switch: an open image section it refuses is left out
	nodeRevs                  revTable                                      // node row revisions for safe prefetch handoff (striped, EXISTMAP)
	relIDs                    indexpkg.IDSet                                // live rel IDs (as nodeIDs)
	relRevs                   revTable                                      // rel row revisions for safe prefetch handoff (BACKLOG 18b; mirrors nodeRevs)
	labelIdx                  map[uint16]map[types.NodeID]struct{}          // labelToken → set(nodeID); EMPTY in labelOnDisk mode
	labelOnDisk               bool                                          // answer label snapshots from the persisted keyspace
	adjOnDisk                 bool                                          // answer adjacency snapshots from the persisted keyspaces
	propIdxOnDisk             bool                                          // maintain/answer property-index entries via the persisted 0x0A keyspace
	formatMarker              uint16                                        // store-level format marker (raiseFormatMarkerPendingLocked; guarded by wbMu)
	temporalIdxOnDisk         bool                                          // maintain the persisted 0x0B raw-entry log so loadIndexesScan can rebuild without a full node fetch per entity
	disablePlannerStats       bool                                          // skip planner-stat maintenance (presence/NDV/min-max/type-class) on writes + open; the stat capabilities fail closed with ErrCapabilityNotSupported
	openImageSuppressed       atomic.Bool                                   // the open's index-key repair failed: Close writes no image (badgerstore_index_repair.go)
	statsDeferred             atomic.Bool                                   // a clean open left planner stats unbuilt: writes skip them, the first stat read builds them from the rows (ensurePlannerStats)
	historyDelta              bool                                          // store version-history rows as anchor+delta (ADR-0009); reads accept both forms regardless
	historyAnchorInterval     uint64                                        // anchor spacing for historyDelta (>=1, default 16); baked into the on-disk layout — verified against a persisted marker at open
	typeIdx                   map[uint16]*indexpkg.IDSet                    // relTypeToken → set(relID), ~9 B per rel (OPENWALK)
	labelOrder                storepkg.MemberOrder[types.NodeID]            // scan order of labelIdx (addLabelIdxLocked; RAM label mode)
	typeOrder                 storepkg.MemberOrder[types.RelID]             // scan order of typeIdx (addTypeIdxLocked)
	outIdx                    map[types.NodeID]map[types.RelID]types.NodeID // startNodeID → relID → endNodeID
	inIdx                     map[types.NodeID]map[types.RelID]inEdge       // endNodeID → relID → {startNodeID, typeToken}
	relValidIdx               map[types.RelID]relValidStamp                 // relID → effective {validFrom, validTo} for inline-stamp temporal traversal; nil until lazily built on the first temporal traversal
	relValidIdxBuilt          atomic.Bool                                   // fast-path "already built" check outside idxMu

	// Transaction-time membership sidecars (store.LabelTxMembershipCapability /
	// RelTypeTxMembershipCapability): label (rel-type) token -> node IDs that ever
	// carried it, with a lower bound on the earliest acquisition tx time. Append-only;
	// nil until lazily built on the first pinned scan. Guarded by idxMu.
	labelTxMembers      map[uint16]map[types.NodeID]types.Instant
	labelTxMembersBuilt atomic.Bool
	relTypeTxMembers    map[uint16]map[types.RelID]types.Instant
	relTypeMembersBuilt atomic.Bool

	// Property membership sidecars (backlog 8, badgerstore_propertytxmembers.go), one
	// per declared property index, built by one pass per kind (backlog 74).
	// relPropTx / nodePropTx / propTxGen and each sidecar's pass claim are guarded
	// by idxMu; no lock is held while a lookup waits for a running pass.
	relPropTx        map[indexpkg.RelPropertyIndexKey]*propTxSidecar[types.RelID]
	nodePropTx       map[indexpkg.PropertyIndexKey]*propTxSidecar[types.NodeID]
	propTxGen        uint64
	propTxBuilds     atomic.Int64 // installed build passes (one scan each)
	propTxBuildNanos atomic.Int64

	// Belief-watermark sidecars (store.NodeBeliefWatermarkCapability /
	// RelBeliefWatermarkCapability, backlog 10c): ID -> max TxFrom over its whole
	// version chain. Lazily built, then maintained at every door that persists a
	// current or history row. Guarded by idxMu.
	nodeBeliefWatermark      map[types.NodeID]types.Instant
	nodeBeliefWatermarkBuilt atomic.Bool
	relBeliefWatermark       map[types.RelID]types.Instant
	relBeliefWatermarkBuilt  atomic.Bool

	// History counts (store.HistoryCountCapability, badgerstore_history_count.go):
	// histNodeEpoch / histRelEpoch advance whenever a node (relationship) history
	// key enters the write buffer (appendOps, appendOpsLoggedRouted) and on Clear;
	// the exact counts are cached against them and recounted after a change.
	histNodeEpoch atomic.Uint64
	histRelEpoch  atomic.Uint64
	histNodeCount historyCount
	histRelCount  historyCount
	// History epochs (store.HistoryEpochCapability, backlog 68,
	// badgerstore_history_epoch.go): per-ID watch tables noted by
	// noteHistoryKey under wbMu, dropped by Clear.
	histNodeEpochs storepkg.HistoryEpochs[snowflake.ID]
	histRelEpochs  storepkg.HistoryEpochs[snowflake.ID]

	// History presence (store.HistoryPresenceCapability,
	// badgerstore_history_presence.go): per kind, a RAM set of IDs with history rows,
	// built once by a key scan and maintained by noteHistoryKey under wbMu. Never built
	// when historyPresenceProbeOnly.
	histNodePresence         historyPresence
	histRelPresence          historyPresence
	historyPresenceProbeOnly bool
	// historyPresenceBuildHook, when non-nil, runs after a presence build's key
	// scan and before it installs the scanned IDs (the window lessons 63 / 74
	// guard). Set only from the owning test.
	historyPresenceBuildHook func()
	// historyPresenceProbeHook, when non-nil, runs after a per-ID probe and
	// before it installs its answer. Set only from the owning test.
	historyPresenceProbeHook func()
	// historyNoteTestHook, when non-nil, runs in noteHistoryKey under wbMu
	// with the op in the write buffer, before the presence note. Set only
	// from the owning test.
	historyNoteTestHook func()
	// History stamps (store.HistoryStampsCapability, badgerstore_history_stamps.go):
	// per kind, a RAM sidecar of the newest TxFrom / TxTo-or-DeletedAt over each ID's
	// history rows, built once on the first NodeHistoryStamps / RelHistoryStamps call
	// and maintained by noteHistoryKey under wbMu. Unbuilt when
	// historyPresenceProbeOnly. The hooks are set only by the owning test.
	histNodeStamps         historyStamps
	histRelStamps          historyStamps
	historyStampsBuildHook func()
	historyStampsProbeHook func()
	// Valid-end indexes (backlog 51, badgerstore_valid_end.go): RAM, per
	// queried type / label, maintained by noteValidEndOp under wbMu and the K1
	// membership doors. validEndTrackHook runs between a token's enlisting and
	// its members' re-read (tests only).
	relValidEnds      indexpkg.ValidEndIndex
	nodeValidEnds     indexpkg.ValidEndIndex
	validEndTrackHook func()
	// clearDropTestHook, when non-nil, runs in Clear right before the keyspace
	// drop; a non-nil error is returned as the drop's failure. Test only.
	clearDropTestHook func() error

	// DocValues: cached per-label columnar snapshots plus a global node-mutation epoch
	// bumped on every node write including deletes (nextNodeRev misses deletes). docMu
	// guards docColumns only; builds run lock-free keyed on nodeEpoch (see
	// ForEachDocValues).
	nodeEpoch atomic.Uint64
	// Per-label node-mutation epochs (backlog 4b): a node write bumps only its labels'
	// epochs, so cached columns of unrelated labels survive. labelEpoch(token) =
	// nodeLabelEpochs[token%256] + nodeEpochSalt. A stripe collision over-invalidates
	// (safe, never stale). nodeEpochSalt is bumped by label-less invalidations (Clear,
	// retention purge). Counters are monotonic, so stamp-and-recheck is a total-order
	// freshness test. Bumped in add/removeNodePropertyKeyCounts, which every
	// node-content write funnels through.
	nodeLabelEpochs [nodeLabelEpochStripes]atomic.Uint64
	nodeEpochSalt   atomic.Uint64

	// Append-delta bookkeeping (R3): per-label record of pure inserts since that
	// label's columnar snapshot was built, letting a read EXTEND the snapshot
	// instead of rebuilding it. See badgerstore_append_delta.go.
	appendMu     sync.Mutex
	appendDeltas map[uint16]*appendDelta
	// patchable holds the labels a single-label or intersection column snapshot was
	// cached for: only for those does the removal seam record the node (an update's
	// remove half) instead of poisoning. Guarded by appendMu; dropped with
	// appendDeltas.
	patchable map[uint16]struct{}
	// Refresh counters: how many columnar snapshots were EXTENDED, PATCHED
	// (in-place updates, round 5 request 4) or rebuilt.
	columnExtends  atomic.Uint64
	columnPatches  atomic.Uint64
	columnRebuilds atomic.Uint64
	// multiFromMember counts the label-intersection snapshots taken from a
	// member label's columns instead of the records (round 7 request 6).
	multiFromMember atomic.Uint64
	// relEpoch is a distinct generation counter bumped on every relationship write:
	// the expand-aggregation column path reads adjacency, so its re-check must see edge
	// mutations. Separate from nodeEpoch so node-only column caches survive edge writes.
	relEpoch atomic.Uint64
	// sharedEpochs is Config.SharedMutationEpochs (nil standalone).
	sharedEpochs *SharedMutationEpochs
	// ords holds the dense ordinals (badgerstore_ordinal.go); nil on a zero
	// value store, where every ordinal reads 0.
	ords *ordinals
	// relTypeEpochs stripes rel-type column invalidation so a write to one type does
	// not discard another's columns; relEpochCoarse is the term every UNCONVERTED
	// mutation site bumps, which is what makes over-invalidation the default. See
	// badgerstore_rel_type_epoch.go.
	relTypeEpochs  [relTypeEpochStripes]atomic.Uint64
	relEpochCoarse atomic.Uint64
	// relAppendDeltas records pure inserts per rel type since that type's snapshot
	// was built, letting a read EXTEND instead of rebuilding.
	columnsOnDisk   bool
	relAppendMu     sync.Mutex
	relAppendDeltas map[uint16]*relAppendDelta
	docMu           sync.Mutex
	docColumns      map[uint16]*indexpkg.LabelDocValues
	// relColumns is the REL-TYPE keyed sibling of docColumns: one columnar snapshot
	// per relationship type, carrying endpoint columns alongside properties. Same
	// immutable epoch-stamped model, but stamped with the GLOBAL relEpoch, so any
	// edge write invalidates every type (see RelMutationEpochForType).
	relColumns map[uint16]*indexpkg.DocValues[types.RelID]
	// docColumnsMulti caches label-INTERSECTION columns (multi-label patterns like
	// (p:A:B)), keyed by the order-independent token-tuple key (MultiLabelKey).
	// Same docMu guard + lock-free epoch-keyed build as docColumns.
	docColumnsMulti map[string]*indexpkg.LabelDocValues
	// colOrds keeps, per label and per relationship type, the ordinals of the
	// rows of the column snapshot a column scan last read (aligned with its
	// IDs), so the scan's Ordinals column costs the ID → ordinal lookups once
	// per snapshot (badgerstore_ordinal.go). docMu guards it.
	colOrds columnOrdinalCache

	// Entity caches (internal sync, N-way sharded — see indexpkg.ShardedCache).
	// Typed as the EntityCache interface so the concrete sharded implementation
	// is the single swap point in newNodeCache / newRelCache.
	nodeCache indexpkg.EntityCache[*types.Node]
	relCache  indexpkg.EntityCache[*types.Relationship]
	resident  bool // ResidentCache: caches never evict; fetches skip LRU promotion
	heldRows  int  // ReadCosts.HeldRows: the entity caches' entry capacity, -1 resident, 0 byte-budgeted

	// Counters (atomic — persisted atomically via flush WriteBatch).
	nodeCount atomic.Int64
	relCount  atomic.Int64

	// Optional property-key registry — when set by Core.New, wire marshal /
	// unmarshal dictionary-encodes property keys into uint16 tokens. nil
	// preserves the pre-tokenization (V1) on-disk format.
	propKeyReg atomic.Pointer[registrypkg.PropertyKeyRegistry]

	// vdict holds this store's value dictionaries (DICT,
	// badgerstore_value_dict.go); never nil, so coded rows decode without a
	// plan. labelNames / relTypeNames name the plan's members
	// (SetNameRegistries); nil codes nothing.
	vdict        *valueDict
	labelNames   atomic.Pointer[registrypkg.LabelRegistry]
	relTypeNames atomic.Pointer[registrypkg.RelTypeRegistry]

	// onPropertyKeyGrow is the write-ahead hook fired from flush() when the
	// registry has grown since the last commit (see Config.OnPropertyKeyGrow).
	// Never nil after New: defaults to persisting this store's own registry when
	// none is supplied.
	onPropertyKeyGrow func() error

	// persistedKeyLen is the property-key-registry length last committed via
	// onPropertyKeyGrow — the write-ahead watermark. A flush whose registry is
	// longer commits it before writing the row WriteBatch. Read lock-free on the
	// hot path; advanced only under persistKeyMu after a successful commit.
	persistedKeyLen atomic.Int64
	persistKeyMu    sync.Mutex

	// Write buffer (own mutex, swapped on flush).
	// Map keyed by string(op.key) for last-write-wins deduplication.
	wbMu    sync.Mutex
	pending map[string]writeOp
	// pendingBytes is the badger transaction size of pending and pendingLog
	// (pendingTxnSize per entry), guarded by wbMu. It counts every insertion, a
	// coalesced key's earlier value too, so it can only overstate: an async
	// backpressure flush may come early, never too late.
	pendingBytes int64
	// flushing holds the snapshot a concurrent flush() is committing: parked from
	// pending under wbMu before the WriteBatch commit, cleared after it succeeds (merged
	// back on failure). In between a row lives only here, so history, adjacency and
	// label overlay scans must go through rangePending / lookupPending, which read both
	// maps. Entity point reads survive this window via the resident dirty cache;
	// overlay scans must use rangePending/lookupPending.
	flushing map[string]writeOp
	// commitGen counts the commits that move buffered rows into Badger (flush,
	// Clear): a read snapshot re-pins when it moved (badgerstore_read_snapshot.go).
	// Every path that writes entity rows to Badger outside the write buffer must
	// bump it before the rows leave the overlay.
	commitGen atomic.Uint64

	// flushMu serializes concurrent flush() executions end to end; otherwise two
	// flushes could snapshot counters and submit out-of-order WriteBatches, leaving
	// stale node/rel counts after restart. Matters only for SyncWrites callers.
	flushMu sync.Mutex

	// Per-label and per-type counters — O(1) reads via atomic.Int64.
	// Keys are uint16 tokens. Values are *atomic.Int64.
	// Rebuilt from index sizes in loadIndexes; maintained incrementally at runtime.
	labelCounts sync.Map // map[uint16]*atomic.Int64
	typeCounts  sync.Map // map[uint16]*atomic.Int64

	// Per-label property-key presence counters — O(1) reads via atomic.Int64.
	// Keys are indexpkg.PropertyIndexKey. Counts only indexable scalar property
	// values because the planner uses this to prune scalar equality lookups.
	propertyKeyCounts sync.Map // map[indexpkg.PropertyIndexKey]*atomic.Int64

	// Per-(label, property key) exact type-class node counters (*typeClassCounters).
	// Unlike propertyKeyCounts this covers every value (non-indexable ones are
	// types.ClassOther); maintained by adjustNodePropertyKeyCounts, rebuilt by
	// loadIndexes.
	propertyTypeClassCounts sync.Map // map[indexpkg.PropertyIndexKey]*typeClassCounters

	// relPropertyTypeClassCounts is the relationship mirror of
	// propertyTypeClassCounts (backlog 5B): incremented from the row a door makes
	// current, decremented from the row a delete or replace removes (backlog 53);
	// rebuilt by loadIndexes.
	relPropertyTypeClassCounts sync.Map // map[indexpkg.RelPropertyIndexKey]*typeClassCounters

	// Per-label property-key NDV + min/max accumulators, guarded by idxMu (HyperLogLog
	// registers are not atomic-friendly; NodePropertyStats is a cold path). Maintained
	// on the same doors as propertyKeyCounts (adjustNodePropertyKeyCounts in
	// badgerstore_property_key_counts.go). Backs store.NodePropertyStatsCapability.
	propertyStats map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator

	// Rel mirrors of propertyKeyCounts / propertyStats (backlog 21a): per-(relType,
	// property key) presence counter and NDV+min/max accumulator (idxMu); follow the rows
	// (backlog 53), rebuilt by loadIndexes. Backs store.RelPropertyStatsCapability.
	relPropertyKeyCounts sync.Map // map[indexpkg.RelPropertyIndexKey]*atomic.Int64
	relPropertyStats     map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator

	// rescanTestHook runs in NodePropertyStats between the unlocked collection and the
	// write-generation re-check, once per attempt. Test-only.
	rescanTestHook func(attempt int)

	// historyScanTestHook runs in the full-history prefix readers and the lazy builds
	// (membership sidecars, belief watermarks, relationshipIndexKeysForRel) after their
	// Badger scan and before the overlay merge / install, so tests land a flush() commit
	// there. Test-only; called under idxMu.Lock, so a hook must not take idxMu.
	historyScanTestHook func()
	// flushClearedTestHook (tests only) runs inside flush right after the
	// committed rows left the overlay (flushing cleared).
	flushClearedTestHook func()
	// propTxScanTestHook runs in a property membership build after the overlay capture
	// and before the badger view; propTxBuildTestHook after the scan, before the install.
	// Test-only.
	propTxScanTestHook  func()
	propTxBuildTestHook func()
	// propTxInstallLockedTestHook runs under idxMu at a build's install with the
	// scanned rows still waiting to be recorded (ledger row 62 review: 0).
	propTxInstallLockedTestHook func(pendingRows int)

	// bulkAsOfScanTestHook runs in NodesAsOf/RelsAsOf once per candidate before its
	// as-of read, so tests land a flush() commit mid-scan (backlog 18k). Test-only.
	bulkAsOfScanTestHook func(index int)

	// bulkAsOfKeyProbeTestHook runs in the bulk as-of history key check for each entity
	// the presence set (bulkPresence) did not answer. Test-only.
	bulkAsOfKeyProbeTestHook func()

	// bulkAsOfOverlayTestHook runs in NodesAsOf/RelsAsOf after the write-buffer overlay
	// is captured and before the shared transaction opens (a delete landing here is what
	// bulkPresence's delete counter must catch). Test-only.
	bulkAsOfOverlayTestHook func()

	// moveTestHook is called (moveHook) by every with-history door under idxMu.Lock
	// between the two halves of publishing a move and after each current-slot change, so
	// tests can prove lock-free point readers never see a half move (backlog 32).
	// Test-only.
	moveTestHook func()

	// nodeHashFillTestHook runs in NodeIntegrityHash after the row read and
	// before the lazy cache fill, so a test can land a write between them
	// (row 66). Test-only.
	nodeHashFillTestHook func()

	// asOfAfterCurrentTestHook runs in NodeAsOf / RelAsOf between the current-row read
	// and the history scan, whose order must be current first (backlog 32). Test-only.
	asOfAfterCurrentTestHook func()

	// relIndexKeysTestErr is consulted first by relationshipIndexKeysForRel; non-nil is
	// that read's error, to prove a fatal preflight error applies nothing. Test-only.
	relIndexKeysTestErr func() error

	// replaceRelPrefetchTestHook runs in ReplaceRelationship after prefetchRelWithRev,
	// before idxMu.Lock, to land a write that relRevs (backlog 18b) must detect.
	// Test-only.
	replaceRelPrefetchTestHook func()

	// relDeletePrefetchTestHook runs after a deferred relationship delete's prefetch and
	// before idxMu.Lock, to force the locked re-read fallback (backlog 53). Test-only.
	relDeletePrefetchTestHook func()

	// exactErasureScanTestHook runs before each exact-erasure preflight scan, to prove a
	// late failure leaves the destructive phase unstarted. Test-only.
	exactErasureScanTestHook func(stage string, id uint64) error

	// Property indexes — in-memory only. Definitions persisted, data rebuilt on startup.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Relationship property indexes: RAM-only value maps keyed by rel-type token.
	// Definitions persist under RelPropIndexDefsKey; data is rebuilt at open. Guarded by
	// idxMu like propertyIndexes.
	relPropertyIndexes map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex

	// Composite property indexes: in-memory only; definitions persist, data is rebuilt
	// at startup. compositeIndexesByLabel is the label->definitions index for the
	// maintainPropertyIndexesAdd/Remove/Purge seam.
	compositeIndexes        map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex
	compositeIndexesByLabel map[uint16][]indexpkg.CompositeIndexKey

	// Temporal indexes — in-memory only. Label tokens persisted, data rebuilt on startup.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Rel-type temporal indexes (backlog 21c), mirror of temporalIndexes in its own map
	// (label and rel-type tokens may collide). In memory; definitions persist under
	// RelTypeTemporalIndexDefsKey, data is rebuilt on open (loadRelTypeTemporalIndexes).
	relTypeTemporalIndexes map[uint16]*indexpkg.TemporalIndex
	// relTemporalBuilds counts rel temporal index builds, a timing-independent hook for
	// the tiered cold-shard tests (RelTemporalIndexBuildsForTest).
	relTemporalBuilds atomic.Int64

	// Index-rebuild diagnostics: node entries loadIndexes tolerated as missing/corrupt,
	// surfaced via IndexRebuildStats.
	indexRebuildPropertySkips  atomic.Int64
	indexRebuildCompositeSkips atomic.Int64
	indexRebuildTemporalSkips  atomic.Int64
	indexRebuildHFSkips        atomic.Int64
	indexRebuildVectorSkips    atomic.Int64

	// logger is captured from cfg.Logger so loadIndexes can warn about
	// skipped node records during the rebuild. Nil means no logging.
	logger badgerv4.Logger

	// High-frequency indexes — in-memory data. Definitions are persisted; entries are rebuilt on startup.
	hfIndexes map[uint16]*indexpkg.HighFrequencyIndex

	// Vector indexes — in-memory data. Definitions are persisted; entries are rebuilt on startup.
	vectorIndexes map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex

	// Lifecycle.
	inMemory   bool
	readOnly   bool
	syncWrites bool
	// groupCommit is set between BeginGroupCommit and EndGroupCommit
	// (store.GroupCommitCapability): flushIfNeeded holds per-mutation flushes and
	// EndGroupCommit issues one. Atomic so the read needs no lock.
	groupCommit atomic.Bool
	maxPending  int // async write-buffer bound; 0 disables (see Config.MaxPendingWrites)
	flushInt    time.Duration
	gcInt       time.Duration
	gcRatio     float64
	stopCh      chan struct{}
	flushDone   chan struct{}
	gcDone      chan struct{}
	closeOnce   sync.Once

	// pendingTxnBytes and pendingTxnEntries bound an async buffer (maxPending >
	// 0) below one badger transaction: half its MaxBatchSize and MaxBatchCount,
	// the other half the room for the mutation that crosses the bound, so a
	// backpressure flush commits as one transaction, not staged (ASYNCSTAGE).
	pendingTxnBytes   int64
	pendingTxnEntries int

	// closing is set at the start of Close(), before the final flush snapshots pending
	// writes; public operations fail closed after it so no mutation misses that flush.
	closing atomic.Bool
	// dbClosed is set just before bs.db.Close(); flush() checks it before
	// WriteBatch.Flush() because Badger v4 hangs in WaitForMark on a closed DB.
	dbClosed atomic.Bool
	// unsynced is set by each WriteBatch commit Badger did not fsync (SyncWrites off)
	// and cleared by DurableFlush's db.Sync, so a durable flush fsyncs exactly when
	// needed. durableSyncs / syncedFlushes feed the test sync counters (backlog 52).
	unsynced      atomic.Bool
	durableSyncs  atomic.Int64
	metaSyncs     atomic.Int64 // fsyncs issued by MetaSetDurable (MetaSyncCountForTest)
	syncedFlushes atomic.Int64

	// flushHold is set between HoldMutationFlushes and ReleaseMutationFlushes
	// (store.MutationFlushHoldCapability): flushIfNeeded leaves writes pending up to
	// holdMaxPending (resolved MaxPendingWrites; 0 = unbounded). Config.SharedFlushHold
	// when set, else the store's own.
	flushHold      *atomic.Bool
	holdMaxPending int

	// Test seams (nil in production): failNextFlush fails the next WriteBatch commit
	// through the real requeue path; testHookDurableAfterCheckOpen runs in DurableFlush
	// after checkOpen so a test can interleave Close.
	failNextFlush atomic.Pointer[error]
	// stagedFlushFault (tests only) fails a staged flush right after the step
	// it names (badgerstore_flush_staged.go).
	stagedFlushFault func(stagedFlushStep) error
	// stagedFailure is set when a staged flush committed but could not be
	// applied: the store fails closed until the next open applies it.
	stagedFailure atomic.Pointer[error]
	// maxEntryKey / maxEntryValue: the largest key and value a door may hand
	// badger (badgerstore_entry_size.go).
	maxEntryKey, maxEntryValue int
	// stageSeq is the last staging sequence of this session (nextStageSeq).
	stageSeq                      atomic.Uint64
	stageSeqOnce                  sync.Once
	failNextSync                  atomic.Pointer[error]
	testHookDurableAfterCheckOpen func()

	// Change-log (opt-in via Config.ChangeLog). logEnabled gates all record production;
	// atomic because ChangeLogEnabled() is a public lock-free accessor (backlog 18i).
	// logSeq is the LSN allocator, seeded at open (LastLSNKey or max KeyChangeLog key) and
	// advanced only under wbMu. pendingLog is the append-only buffer, guarded by wbMu.
	logEnabled atomic.Bool
	// logConfigured records the open-time change-log intent so EnableChangeLog
	// (recovery) only re-enables a store opened with the log. DisableChangeLog leaves it
	// set.
	logConfigured bool
	logSeq        atomic.Uint64
	pendingLog    []pendingLogRecord
	// logSeqSource, when non-nil (Config.ChangeLogSeqSource), supplies LSNs in place of
	// logSeq: nextLSN() draws from it and the open-time watermark is folded in via
	// Observe.
	logSeqSource ChangeLogSeqSource
	// onChangeLogFlush (Config.OnChangeLogFlush) persists the store-global
	// allocator watermark after a log-bearing flush commits. nil = standalone.
	onChangeLogFlush func() error
	// Commit floor (badgerstore_commit_floor.go): commitFloor is the newest row stamp
	// written (loaded at open); commitFloorDurable the value the persisted key holds
	// (flushMu; 0 after Clear); onCommitFloor is Config.OnCommitFloor; inDurableFlush
	// marks a DurableFlush's flush (flushMu).
	commitFloor        atomic.Int64
	commitFloorDurable int64
	onCommitFloor      func(types.Instant, bool) error
	beforeFlushRows    func(bool) error
	inDurableFlush     bool

	// Per-transaction change-log scope (store.TxChangeLogScope), guarded by wbMu. While
	// scopeActive, records go to scopeLog without an LSN; CommitLogScope mints contiguous
	// LSNs (a rolled-back tx burns none) and co-commits them via flush(); DiscardLogScope
	// drops them. The core toggles scopeActive only under c.mu.Lock.
	scopeActive bool
	scopeLog    [][]byte // framed record values (tag‖payload), LSN assigned at commit

	// Scoped change-log buffers (store.ScopedTxChangeLog, backlog 11f, changefeed_scoped.go):
	// each scope is addressed by its token, so several can be open concurrently. Guarded
	// by wbMu.
	scopedTokenSeq uint64
	scopedLogs     map[uint64][][]byte

	// relPurgeCursor (backlog 21, badgerstore_rel_retention_purge.go): per rel type,
	// where a chunked relationship purge resumes; cleared when it finishes.
	relPurgeMu     sync.Mutex
	relPurgeCursor map[uint16]relPurgeCursor
}

var (
	counterNodeCountKey = storepkg.MetaKey("node_count")
	counterRelCountKey  = storepkg.MetaKey("rel_count")
)

// New opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
// newNodeCache / newRelCache build the entity caches (byte-budgeted when budget > 0).
// cacheShards derives the shard count once from GOMAXPROCS (floor 16, power of two).
func cacheShards() int {
	// TKG_CACHE_SHARDS overrides the derived shard count (1 = single mutex, a kill
	// switch); invalid or unset uses the default.
	if v := os.Getenv("TKG_CACHE_SHARDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			return n // raw (NewShardedCache rounds to a power of two); 1 = no sharding
		}
	}
	return indexpkg.ShardHint(runtime.GOMAXPROCS(0))
}

func newNodeCache(capacity int, budget int64) indexpkg.EntityCache[*types.Node] {
	if budget <= 0 {
		return indexpkg.NewShardedCache[*types.Node](capacity, cacheShards())
	}
	return indexpkg.NewShardedCacheWithBudget(capacity, budget, func(n *types.Node) int64 {
		return int64(n.ApproxHeapBytes())
	}, cacheShards())
}

func newRelCache(capacity int, budget int64) indexpkg.EntityCache[*types.Relationship] {
	if budget <= 0 {
		return indexpkg.NewShardedCache[*types.Relationship](capacity, cacheShards())
	}
	return indexpkg.NewShardedCacheWithBudget(capacity, budget, func(r *types.Relationship) int64 {
		return int64(r.ApproxHeapBytes())
	}, cacheShards())
}

func New(cfg Config) (*Store, error) {
	if cfg.FlushInterval < 0 {
		return nil, fmt.Errorf("graph: FlushInterval must not be negative")
	}
	if cfg.GCInterval < 0 {
		return nil, fmt.Errorf("graph: GCInterval must not be negative")
	}
	if cfg.GCDiscardRatio != 0 && (cfg.GCDiscardRatio <= 0 || cfg.GCDiscardRatio >= 1) {
		return nil, fmt.Errorf("graph: GCDiscardRatio must be in (0, 1), got %g", cfg.GCDiscardRatio)
	}
	if !cfg.InMemory && cfg.Dir == "" {
		return nil, fmt.Errorf("graph: Dir required when InMemory is false")
	}
	if err := validateTuningConfig(cfg); err != nil {
		return nil, err
	}
	if err := validateEncryptionConfig(cfg); err != nil {
		return nil, err
	}
	vdPlan := cfg.ValueDictionaryPlan
	if vdPlan == nil {
		var err error
		if vdPlan, err = valuedict.NewPlan(cfg.ValueDictionaries); err != nil {
			return nil, fmt.Errorf("graph: %w", err)
		}
	}

	// Shrinking the effective MemTableSize on a dir with WALs written under a larger
	// memtable fails the open ("Arena too small", unrecoverable), so flush such WALs at
	// their original size first. Gated on a writable on-disk store, not on MemTableSize >
	// 0 (backlog 18m): reverting to stock can also be a shrink. The migration helpers
	// no-op when nothing needs migrating.
	if !cfg.InMemory && !cfg.ReadOnly {
		if err := MigrateOversizedWAL(cfg); err != nil {
			return nil, err
		}
	}
	// A read-only open cannot migrate an oversized WAL, and replaying one would
	// os.Exit ("Arena too small"); fail closed with an error. (The tiered store migrates
	// before its read-only probe.)
	if !cfg.InMemory && cfg.ReadOnly {
		if err := guardReadOnlyOversizedWAL(cfg); err != nil {
			return nil, err
		}
	}

	// A writable open may cut the newest value-log file at a bad entry, and
	// badger says nothing: note where its written data ends, report a cut.
	var vlogPath string
	var vlogWritten int64
	vlogKnown := false
	if !cfg.InMemory && !cfg.ReadOnly {
		if vlogPath = newestVlog(cfg.Dir); vlogPath != "" {
			vlogWritten, vlogKnown = writtenEnd(vlogPath)
		}
	}
	db, err := badgerv4.Open(buildBadgerOptions(cfg))
	if err != nil {
		return nil, fmt.Errorf("graph: badger open: %w", err)
	}
	if vlogKnown {
		reportVlogCut(badgerLogger(cfg), vlogPath, vlogWritten)
	}

	// Enforce the on-disk format contract before decoding a row: a directory stamped by
	// a newer release fails closed here, not as per-row decode failures that loadIndexes
	// would conflate with corruption.
	formatMarker, err := verifyAndStampWireFormatVersion(db, cfg.ReadOnly)
	if err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, err
	}
	// A staged flush that committed before a crash is applied (or uncommitted
	// staging deleted) before any row, counter or index is read.
	if err := recoverStagedFlush(db, cfg.ReadOnly, badgerLogger(cfg)); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, err
	}
	if err := verifyAndStampHistoryAnchorInterval(db, resolveHistoryAnchorInterval(cfg.HistoryAnchorInterval), cfg.HistoryDeltaEncoding, cfg.ReadOnly); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, err
	}

	capacity := cfg.CacheCapacity
	if capacity <= 0 {
		capacity = DefaultCacheCapacity
		if cfg.CacheBudgetBytes > 0 {
			// Byte budget governs; don't let the default count capacity
			// evict underneath it.
			capacity = math.MaxInt
		}
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = DefaultFlushInterval
	}
	if cfg.SyncWrites && !cfg.ReadOnly {
		flushInt = 0 // disable periodic flush; each write flushes synchronously
	}
	maxPending := cfg.MaxPendingWrites
	if maxPending == 0 {
		maxPending = DefaultMaxPendingWrites
	}
	holdMaxPending := max(maxPending, 0) // bounds a held durable group under SyncWrites
	if maxPending < 0 || (cfg.SyncWrites && !cfg.ReadOnly) {
		maxPending = 0 // explicitly unbounded, or moot under SyncWrites
	}
	gcInt := cfg.GCInterval
	if gcInt == 0 && !cfg.InMemory {
		gcInt = DefaultGCInterval
	}
	gcRatio := cfg.GCDiscardRatio
	if gcRatio == 0 {
		gcRatio = DefaultGCDiscardRatio
	}

	maxEntryKey, maxEntryValue := entryLimits(cfg)
	bs := &Store{
		vdict:         newValueDict(vdPlan),
		db:            db,
		maxEntryKey:   maxEntryKey,
		maxEntryValue: maxEntryValue,

		historyPresenceProbeOnly: cfg.HistoryPresenceProbeOnly,
		nodeHashes:               make(map[types.NodeID]string),
		nodeRevs:                 newRevTable(),
		relRevs:                  newRevTable(),
		labelIdx:                 make(map[uint16]map[types.NodeID]struct{}),
		typeIdx:                  make(map[uint16]*indexpkg.IDSet),
		outIdx:                   make(map[types.NodeID]map[types.RelID]types.NodeID),
		inIdx:                    make(map[types.NodeID]map[types.RelID]inEdge),
		// relValidIdx is built lazily on the first temporal traversal.
		nodeCache:               newNodeCache(capacity, cfg.CacheBudgetBytes),
		relCache:                newRelCache(capacity, cfg.CacheBudgetBytes),
		resident:                cfg.ResidentCache,
		heldRows:                heldRowsOf(capacity, cfg.ResidentCache),
		pending:                 make(map[string]writeOp),
		formatMarker:            formatMarker,
		propertyIndexes:         make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex),
		relPropertyIndexes:      make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex),
		compositeIndexes:        make(map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex),
		compositeIndexesByLabel: make(map[uint16][]indexpkg.CompositeIndexKey),
		propertyStats:           make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator),
		relPropertyStats:        make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator),
		temporalIndexes:         make(map[uint16]*indexpkg.TemporalIndex),
		relTypeTemporalIndexes:  make(map[uint16]*indexpkg.TemporalIndex),
		hfIndexes:               make(map[uint16]*indexpkg.HighFrequencyIndex),
		vectorIndexes:           make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
		inMemory:                cfg.InMemory,
		labelOnDisk:             cfg.LabelIndexOnDisk,
		adjOnDisk:               cfg.AdjacencyIndexOnDisk,
		propIdxOnDisk:           cfg.PropertyIndexOnDisk,
		temporalIdxOnDisk:       cfg.TemporalIndexOnDisk,
		columnsOnDisk:           cfg.ColumnsOnDisk,
		disablePlannerStats:     cfg.DisablePlannerStats,
		historyDelta:            cfg.HistoryDeltaEncoding,
		historyAnchorInterval:   resolveHistoryAnchorInterval(cfg.HistoryAnchorInterval),
		readOnly:                cfg.ReadOnly,
		syncWrites:              cfg.SyncWrites && !cfg.ReadOnly,
		logConfigured:           cfg.ChangeLog && !cfg.ReadOnly,
		logSeqSource:            cfg.ChangeLogSeqSource,
		sharedEpochs:            cfg.SharedMutationEpochs,
		ords:                    newOrdinals(cfg),
		onChangeLogFlush:        cfg.OnChangeLogFlush,
		onCommitFloor:           cfg.OnCommitFloor,
		beforeFlushRows:         cfg.BeforeFlushRows,
		maxPending:              maxPending,
		pendingTxnBytes:         db.MaxBatchSize() / 2,
		pendingTxnEntries:       int(db.MaxBatchCount() / 2),
		holdMaxPending:          holdMaxPending,
		flushHold:               cfg.SharedFlushHold,
		flushInt:                flushInt,
		gcInt:                   gcInt,
		gcRatio:                 gcRatio,
		stopCh:                  make(chan struct{}),
		flushDone:               make(chan struct{}),
		gcDone:                  make(chan struct{}),
		logger:                  badgerLogger(cfg),
	}
	// atomic.Bool cannot be set in a composite literal; set it before bs escapes.
	bs.logEnabled.Store(cfg.ChangeLog && !cfg.ReadOnly)
	if bs.flushHold == nil {
		bs.flushHold = new(atomic.Bool)
	}

	// Resident mode: a cache miss never re-decodes the same entity;
	// GetNode/GetRel additionally fetch via GetNoPromote.
	if bs.resident {
		bs.nodeCache.SetNoEvict()
		bs.relCache.SetNoEvict()
	}

	// Install the property-key registry BEFORE loadIndexes so row decoding can resolve
	// tokenized keys (else rows fail validation and silently drop from the liveness
	// map). An owner-supplied registry is used directly; otherwise load this store's
	// own meta copy.
	if cfg.PropertyKeyRegistry != nil {
		bs.propKeyReg.Store(cfg.PropertyKeyRegistry)
	} else if reg := readPropertyKeyRegistryFromMeta(bs); reg != nil {
		bs.propKeyReg.Store(reg)
	}
	// Seed the write-ahead watermark to the already-persisted length; only growth
	// beyond it needs a new commit.
	if reg := bs.propKeyReg.Load(); reg != nil {
		bs.persistedKeyLen.Store(int64(reg.Len()))
	}

	// Write-ahead registry hook (tiered store: commits the shared registry to the
	// reference shard; standalone: own meta). flush() calls it before the row
	// WriteBatch, so every token a durable row references is itself durable.
	bs.onPropertyKeyGrow = cfg.OnPropertyKeyGrow
	if bs.onPropertyKeyGrow == nil {
		bs.onPropertyKeyGrow = func() error {
			if reg := bs.propKeyReg.Load(); reg != nil {
				return bs.SavePropertyKeyRegistry(reg)
			}
			return nil
		}
	}

	// The valid axis (round 11) is fixed before any row is read: the store's
	// own persisted untimed mode decides it, else the composite's axis (a
	// tiered or sharded shard without the key), else the legacy axis.
	if err := bs.openValidAxis(cfg.ValidAxis); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, err
	}

	// A cleanly closed store opens from its keys and the persisted open image
	// (badgerstore_open_image.go) instead of decoding every row.
	img := bs.readOpenImage()
	if err := bs.loadIndexesRecovering(img); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, fmt.Errorf("graph: load indexes: %w", err)
	}

	// loadIndexes rebuilds temporal indexes from current node state only; fold each
	// node's history versions into the per-node valid-time envelope so the sound-superset
	// property survives restart.
	bs.idxMu.RLock()
	temporalToks := make([]uint16, 0, len(bs.temporalIndexes))
	for tok := range bs.temporalIndexes {
		if img == nil || !img.restoredTemporal[tok] { // a restored envelope already folds its history
			temporalToks = append(temporalToks, tok)
		}
	}
	bs.idxMu.RUnlock()
	if len(temporalToks) > 0 {
		if err := bs.foldTemporalHistoryEnvelopes(temporalToks); err != nil {
			_ = db.Close() // best-effort cleanup
			return nil, fmt.Errorf("graph: fold temporal history envelopes: %w", err)
		}
	}

	relTemporalLoad := func() error { return bs.loadRelTypeTemporalIndexes(img) }
	if cfg.DropRelTemporalIndexesAtOpen {
		relTemporalLoad = bs.discardRelTypeTemporalIndexDefs
	}
	if err := relTemporalLoad(); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, err
	}

	// Start background goroutines (skip when read-only or no flush interval).
	if flushInt > 0 && !cfg.ReadOnly {
		go bs.flushLoop()
	} else {
		close(bs.flushDone)
	}
	if gcInt > 0 && !cfg.InMemory && !cfg.ReadOnly {
		go bs.gcLoop()
	} else {
		close(bs.gcDone)
	}

	return bs, nil
}

// loadIndexes rebuilds in-memory indexes and counters from Badger in one db.View()
// key scan. With a trusted open image (img != nil) entity, label, type and adjacency
// state come from keys alone, index contents from the image, and planner statistics
// are deferred; no row is decoded unless an index is missing from the image.
func (bs *Store) loadIndexes(img *openImage) error {
	err := bs.loadIndexesScan(img)
	if err != nil {
		return err
	}
	if img != nil && !bs.disablePlannerStats {
		if img.stats != nil {
			bs.installPlannerStatsLocked(img.stats) // the closing process's statistics
		} else {
			bs.statsDeferred.Store(true) // built at the first statistics read
		}
	}
	if bs.labelOnDisk {
		// Disk-resident label index: the map was built transiently for the open-path
		// rebuilds; drop it for the steady-state RAM win (label snapshots come from
		// badgerstore_label_disk.go). Open peak memory still includes it.
		bs.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	}
	return nil
}

// openLabelMembers returns tok's members during the open: the label map when
// the scan built it, else (lazy: a trusted image with the label index on disk,
// where the scan only counts) tok's label keys, read once and kept in labelIdx
// for the rest of the open (loadIndexes drops the map in disk mode). ok is
// false for a label without members, as a map lookup answers.
func (bs *Store) openLabelMembers(txn *badgerv4.Txn, tok uint16, lazy bool) (map[types.NodeID]struct{}, bool) {
	if set, ok := bs.labelIdx[tok]; ok || !lazy {
		return set, ok
	}
	prefix := binary.BigEndian.AppendUint16([]byte{storepkg.KeyLabel}, tok)
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = prefix
	it := txn.NewIterator(opts)
	defer it.Close()
	set := make(map[types.NodeID]struct{})
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		key := it.Item().Key()
		if len(key) != storepkg.SizeLabelIdx {
			continue
		}
		if nid := types.NodeID(storepkg.ParseIDFromKey(key, 3)); bs.nodeIDs.Has(int64(nid)) {
			set[nid] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil, false
	}
	bs.labelIdx[tok] = set
	return set, true
}

// openWalkTestHook (tests only) is called with the keyspace prefix of each
// walk the open's index scan starts.
var openWalkTestHook func(prefix byte)

func (bs *Store) loadIndexesScan(img *openImage) error {
	// propIdxNeedsBuild / propIdxBackfillOps: one-time backfill of the new 0x0A
	// keyspace for a directory with property-index definitions when PropertyIndexOnDisk
	// is first enabled. Computed in the View closure below, committed via WriteBatch
	// after it, guarded by storeutil.PropertyIndexOnDiskBuiltKey.
	var propIdxNeedsBuild bool
	var propIdxBackfillOps []writeOp
	propIdxDefined := false // any property index definition persisted
	// temporalIdxNeedsBuild / temporalIdxBackfillOps mirror the above for the 0x0B
	// temporal-index keyspace, guarded by storeutil.TemporalIndexOnDiskBuiltKey.
	var temporalIdxNeedsBuild bool
	var temporalIdxBackfillOps []writeOp
	// kr collects the keys a full rebuild finds disagreeing with the rows
	// (badgerstore_index_repair.go); a trusted image needs none.
	kr := &keyRepair{}

	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false

		if bs.propIdxOnDisk {
			_, merr := txn.Get(storepkg.PropertyIndexOnDiskBuiltKey)
			switch {
			case merr == nil:
				propIdxNeedsBuild = false
			case errors.Is(merr, badgerv4.ErrKeyNotFound):
				propIdxNeedsBuild = true
			default:
				return fmt.Errorf("graph: read property-index-on-disk marker: %w", merr)
			}
		}
		if bs.temporalIdxOnDisk {
			// Trusted only when built under the TxFrom start (v2 marker) and no
			// older release rebuilt it since (that rebuild writes the v1 marker
			// and keys rows by the mint start, leaving v2 in place) — backlog 62.
			v2, err := metaKeyPresent(txn, storepkg.TemporalIndexOnDiskBuiltKey)
			if err != nil {
				return fmt.Errorf("graph: read temporal-index-on-disk marker: %w", err)
			}
			v1, err := metaKeyPresent(txn, storepkg.TemporalIndexOnDiskBuiltV1Key)
			if err != nil {
				return fmt.Errorf("graph: read temporal-index-on-disk marker: %w", err)
			}
			temporalIdxNeedsBuild = !v2 || v1
		}

		// nodeEntityIDs holds every node key, decodable or not (the rebuild keeps
		// an undecodable row's keys); with a trusted image it is the image's set.
		nodeEntityIDs := &indexpkg.IDSet{}
		decodedNodeLabels := make(map[types.NodeID]map[uint16]struct{})

		// Scan node entities. The node row is authoritative for liveness: stale label keys
		// must not manufacture live nodeIDs, missing ones can be rebuilt from the row.
		if img != nil && (img.nodeIDs != nil || img.relIDs != nil) {
			// The counters are persisted with the rows and match them at a clean
			// close (invariant 15): an ID set of another size disagrees with the
			// keys, so walk them instead.
			pn, nerr := getCounter(txn, counterNodeCountKey)
			pr, rerr := getCounter(txn, counterRelCountKey)
			if nerr != nil || rerr != nil || img.nodeIDs == nil || img.relIDs == nil ||
				int64(img.nodeIDs.Len()) != pn || int64(img.relIDs.Len()) != pr {
				img.nodeIDs, img.relIDs = nil, nil
			}
		}
		var it *badgerv4.Iterator
		var prefix []byte
		var loadErr error
		if img != nil && img.nodeIDs != nil {
			// Clean close with the ID set (EXISTMAP): no node-key walk.
			bs.nodeIDs = *img.nodeIDs
			nodeEntityIDs = &bs.nodeIDs
			if bs.ords != nil && !bs.ords.disabled {
				bs.nodeIDs.Ascend(func(id int64) bool {
					bs.ords.node(types.NodeID(id), true) // ID order, as the walk numbers them
					return true
				})
			}
			// The walk's per-node revision bump advanced the epoch once per node.
			bs.nodeEpoch.Add(uint64(bs.nodeIDs.Len())) // #nosec G115 -- a length
			bs.sharedEpochs.node()
		} else {
			it = txn.NewIterator(opts) // no prefetch: values are inline (PREFETCH)
			prefix = []byte{storepkg.KeyNode}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) != storepkg.SizeNodeKey {
					continue
				}
				nid := types.NodeID(storepkg.ParseIDFromKey(key, 1))
				nodeEntityIDs.Add(int64(nid))
				if img != nil {
					// Clean close: the row is live; labels come from the label keys, hash and
					// statistics on first use.
					bs.nodeIDs.Add(int64(nid))
					bs.ords.node(nid, true)
					bs.bumpNodeRevLocked(nid)
					continue
				}

				var n *types.Node
				if err := item.Value(func(val []byte) error {
					var w storepkg.NodeWire
					if err := storepkg.SafeUnmarshal(val, &w); err != nil {
						return fmt.Errorf("graph: unmarshal node: %w", err)
					}
					decoded, err := bs.decodeNodeWireForKey(w, nid.SnowflakeID())
					if err != nil {
						return fmt.Errorf("graph: decode node: %w", err)
					}
					n = decoded
					return nil
				}); err != nil {
					// Corrupt rows are skipped and counted, but a future wire-format version is a
					// newer release's data: dropping it would be silent data loss, so fail closed
					// (break first: the iterator must close before returning from the View closure).
					if errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
						loadErr = fmt.Errorf("graph: node %d: %w", nid.SnowflakeID(), err)
						break
					}
					if bs.logger != nil {
						bs.logger.Warningf("graph: node-index rebuild skipped node %d: %v", nid.SnowflakeID(), err)
					}
					continue
				}
				if n.ID() != nid {
					if bs.logger != nil {
						bs.logger.Warningf("graph: node-index rebuild skipped node key %d with mismatched row id %d", nid.SnowflakeID(), n.ID().SnowflakeID())
					}
					continue
				}
				bs.nodeIDs.Add(int64(nid))
				bs.ords.node(nid, true) // ID order: the open numbers the stored nodes 1..N
				bs.bumpNodeRevLocked(nid)
				labels := bs.addNodeIndexesFromRow(nid, collectNodeLabelTokens(n))
				bs.addNodePropertyKeyCounts(n)
				decodedNodeLabels[nid] = labels
				kr.expectedLabels += len(labels)
			}
			it.Close()
			if loadErr != nil {
				return loadErr
			}
		}

		// Scan label index: keyLabel(1B) + token(2B) + nodeID(8B). Only rows with a local
		// node entity may populate labelIdx; ignore labels that disagree with a cleanly
		// decoded row. A trusted image with the label index on disk only counts the
		// members (EXISTMAP: the map was built and dropped, ~33 B per node of open
		// peak); an index the image lacks reads its label's keys (openLabelMembers).
		lazyLabels := img != nil && bs.labelOnDisk
		labelCounts := map[uint16]int64{}
		if lazyLabels && img.nodeIDs != nil && img.labelCounts != nil {
			// The image holds the counts the walk below would make (OPENWALK),
			// over the same trusted node set.
			for token, n := range img.labelCounts {
				labelCounts[token] = n
				bs.nodeLabelEpochs[token%nodeLabelEpochStripes].Add(uint64(n)) // #nosec G115 -- a positive count
			}
		} else {
			it = txn.NewIterator(opts)
			prefix = []byte{storepkg.KeyLabel}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != storepkg.SizeLabelIdx {
					continue
				}
				token := binary.BigEndian.Uint16(key[1:3])
				nid := types.NodeID(storepkg.ParseIDFromKey(key, 3))
				if !nodeEntityIDs.Has(int64(nid)) {
					if img == nil {
						kr.drop(key) // no row
					}
					continue
				}
				if labels, decoded := decodedNodeLabels[nid]; decoded {
					if _, ok := labels[token]; !ok {
						kr.drop(key) // the row does not carry the label
						continue
					}
					kr.acceptedLabels++
				}
				if lazyLabels {
					labelCounts[token]++
				} else {
					bs.addLabelIdxLocked(token, nid)
				}
				if img != nil {
					// The epoch bump the full path makes per (node, label) through
					// addNodePropertyKeyCounts.
					bs.nodeLabelEpochs[token%nodeLabelEpochStripes].Add(1)
				}
			}
			it.Close()
		}

		relEntityIDs := &indexpkg.IDSet{} // as nodeEntityIDs
		decodedRelInfo := make(map[types.RelID]RelDeleteInfo)

		// Scan relationship entities. The entity row is authoritative for liveness: stale
		// reltype/outgoing keys (interrupted repair/delete) must not manufacture live relIDs.
		if img != nil && img.relIDs != nil {
			// Clean close with the ID set (EXISTMAP): no rel-key walk.
			bs.relIDs = *img.relIDs
			relEntityIDs = &bs.relIDs
			if bs.ords != nil && !bs.ords.disabled {
				bs.relIDs.Ascend(func(id int64) bool {
					bs.ords.rel(types.RelID(id), true)
					return true
				})
			}
		} else {
			it = txn.NewIterator(opts)
			prefix = []byte{storepkg.KeyRel}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				key := item.Key()
				if len(key) != storepkg.SizeRelKey {
					continue
				}
				rid := types.RelID(storepkg.ParseIDFromKey(key, 1))
				relEntityIDs.Add(int64(rid))
				if img != nil {
					// Clean close: type and adjacency come from their keys below.
					bs.relIDs.Add(int64(rid))
					bs.ords.rel(rid, true)
					bs.seedRelRevAtOpenLocked(rid)
					continue
				}

				var r *types.Relationship
				if err := item.Value(func(val []byte) error {
					var w storepkg.RelWire
					if err := storepkg.SafeUnmarshal(val, &w); err != nil {
						return fmt.Errorf("graph: unmarshal relationship: %w", err)
					}
					decoded, err := bs.decodeRelWireForKey(w, rid.SnowflakeID())
					if err != nil {
						return fmt.Errorf("graph: decode relationship: %w", err)
					}
					r = decoded
					return nil
				}); err != nil {
					// Same contract as the node scan: tolerate corruption, fail
					// closed on a future per-row format version.
					if errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
						loadErr = fmt.Errorf("graph: relationship %d: %w", rid.SnowflakeID(), err)
						break
					}
					if bs.logger != nil {
						bs.logger.Warningf("graph: relationship-index rebuild skipped rel %d: %v", rid.SnowflakeID(), err)
					}
					continue
				}
				if r.ID() != rid {
					if bs.logger != nil {
						bs.logger.Warningf("graph: relationship-index rebuild skipped rel key %d with mismatched row id %d", rid.SnowflakeID(), r.ID().SnowflakeID())
					}
					continue
				}
				bs.relIDs.Add(int64(rid))
				bs.ords.rel(rid, true) // ID order: the open numbers the stored relationships 1..N
				bs.seedRelRevAtOpenLocked(rid)
				bs.addRelPropertyTypeClassCounts(r) // rebuild rel type-class counters (BACKLOG 5B)
				bs.addRelPropertyStatsCounts(r)     // rebuild rel NDV+min/max counters (BACKLOG 21a)
				info := relDeleteInfoFromRelationship(r)
				decodedRelInfo[rid] = info
				bs.addRelationshipIndexesFromRow(info)
				// relValidIdx is built lazily on the first temporal traversal,
				// not during load — see ensureRelValidIdxBuilt.
			}
			it.Close()
			if loadErr != nil {
				return loadErr
			}
		}

		// Scan reltype index: keyRelType(1B) + token(2B) + relID(8B). Only rows with a
		// local entity key may populate it; ignore keys that disagree with a decoded row.
		if img != nil && img.relIDs != nil && img.relTypes != nil {
			// The image holds the index over the same trusted rel set (OPENWALK).
			bs.typeIdx = img.relTypes
		} else {
			it = txn.NewIterator(opts)
			prefix = []byte{storepkg.KeyRelType}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != storepkg.SizeRelTypeIdx {
					continue
				}
				token := binary.BigEndian.Uint16(key[1:3])
				rid := types.RelID(storepkg.ParseIDFromKey(key, 3))
				if img != nil {
					if local := relEntityIDs.Has(int64(rid)); local {
						bs.addTypeIdxLocked(token, rid)
					}
					continue
				}
				info, decoded := decodedRelInfo[rid]
				if !decoded {
					if local := relEntityIDs.Has(int64(rid)); !local {
						kr.drop(key) // no row (an undecodable row keeps its keys)
					}
					continue
				}
				if info.RelType != token {
					kr.drop(key)
					continue
				}
				kr.acceptedTypes++
				bs.addTypeIdxLocked(token, rid)
			}
			it.Close()
		}

		// A trusted image vouches for the adjacency keys, and the disk mode
		// answers from them: the walks below would keep nothing (OPENWALK).
		if img == nil || !bs.adjOnDisk {
			// Scan outgoing adjacency: keyOut(1B) + startID(8B) + relType(2B) + endID(8B) +
			// relID(8B). Ignore keys without a local relationship entity. A key with one
			// is kept whether or not its start node is local, as the write path keeps it:
			// a tiered mint shard holds rels of nodes that live on the reference shard
			// (tiered.Config.RelsByMint).
			it = txn.NewIterator(opts)
			prefix = []byte{storepkg.KeyOut}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != storepkg.SizeAdjacency {
					continue
				}
				startID := storepkg.ParseIDFromKey(key, 1)
				relType := binary.BigEndian.Uint16(key[9:11])
				endID := storepkg.ParseIDFromKey(key, 11)
				relID := types.RelID(storepkg.ParseRelIDFromAdjKey(key))
				if img != nil {
					if local := relEntityIDs.Has(int64(relID)); !local {
						continue
					}
				} else if info, decoded := decodedRelInfo[relID]; !decoded ||
					info.StartID != startID || info.EndID != endID || info.RelType != relType {
					if local := relEntityIDs.Has(int64(relID)); decoded || !local {
						kr.drop(key) // the row disagrees, or there is none
					}
					continue
				} else {
					kr.acceptedOut++
				}
				if bs.adjOnDisk {
					continue // disk mode answers from these keys; no RAM map, not even transient
				}
				startNID := types.NodeID(startID)
				if bs.outIdx[startNID] == nil {
					bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
				}
				bs.outIdx[startNID][relID] = types.NodeID(endID)
			}
			it.Close()

			if img == nil {
				for _, info := range decodedRelInfo {
					if bs.inKeyHereLocked(info) {
						kr.expectedIn++
					}
				}
			}
			// Scan incoming adjacency: keyIn(1B) + endID(8B) + relType(2B) + startID(8B) +
			// relID(8B). Incoming-only rows are valid in tiered cross-shard layouts, so keep keys
			// with no local entity; with a local row accept only the canonical entry.
			it = txn.NewIterator(opts)
			prefix = []byte{storepkg.KeyIn}
			if openWalkTestHook != nil {
				openWalkTestHook(prefix[0])
			}
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().Key()
				if len(key) != storepkg.SizeAdjacency {
					continue
				}
				endID := storepkg.ParseIDFromKey(key, 1)
				relType := binary.BigEndian.Uint16(key[9:])
				startID := storepkg.ParseIDFromKey(key, 11)
				relID := types.RelID(storepkg.ParseRelIDFromAdjKey(key))
				hasLocalEntity := relEntityIDs.Has(int64(relID))
				info, decoded := decodedRelInfo[relID]
				if img != nil && hasLocalEntity {
					decoded = true // clean close: the key agrees with the row
					info = RelDeleteInfo{ID: relID.SnowflakeID(), StartID: startID, EndID: endID, RelType: relType}
				}
				if decoded {
					if info.StartID != startID || info.EndID != endID || info.RelType != relType {
						kr.drop(key) // only reached on a rebuild: an image's keys agree
						continue
					}
					// A key backed by the local row is kept whether or not its end
					// node is local (the outgoing scan's rule); it counts toward the
					// shortfall check where the row wants it, as expectedIn does.
					if img == nil && bs.inKeyHereLocked(info) {
						kr.acceptedIn++
					}
				} else if hasLocalEntity {
					continue
				}
				if bs.adjOnDisk {
					continue
				}
				endNID := types.NodeID(endID)
				if bs.inIdx[endNID] == nil {
					bs.inIdx[endNID] = make(map[types.RelID]inEdge)
				}
				bs.inIdx[endNID][relID] = inEdge{start: types.NodeID(startID), typ: relType}
			}
			it.Close()
		}

		if img == nil {
			kr.addMissingKeys(txn, decodedNodeLabels, decodedRelInfo, bs.inKeyHereLocked)
		}

		// Load counters from meta keys, or count from live row maps if missing.
		nodeCount, err := getCounter(txn, counterNodeCountKey)
		if err != nil {
			return err
		}
		liveNodeCount := int64(bs.nodeIDs.Len())
		nodeCount, err = reconcilePersistedCounter("node", nodeCount, liveNodeCount, int64(nodeEntityIDs.Len()), bs.logger)
		if err != nil {
			return err
		}
		bs.nodeCount.Store(nodeCount)

		relCount, err := getCounter(txn, counterRelCountKey)
		if err != nil {
			return err
		}
		liveRelCount := int64(bs.relIDs.Len())
		relCount, err = reconcilePersistedCounter("relationship", relCount, liveRelCount, int64(relEntityIDs.Len()), bs.logger)
		if err != nil {
			return err
		}
		bs.relCount.Store(relCount)

		if err := bs.loadCommitFloor(txn); err != nil {
			return err
		}

		// Seed the LSN allocator so LSNs stay monotonic across restart. LastLSNKey commits
		// with the records it covers; fall back to scanning the max key if absent.
		lastLSN, err := seedLogLSN(txn)
		if err != nil {
			return err
		}
		if bs.logSeqSource != nil {
			// Tiered: fold this shard's watermark into the store-global allocator so the shared
			// sequence resumes above every shard's max, even a cold shard opened read-only later.
			bs.logSeqSource.Observe(lastLSN)
		} else {
			bs.logSeq.Store(lastLSN)
		}

		// Rebuild per-label and per-type counters from index sizes.
		if lazyLabels {
			for token, n := range labelCounts {
				bs.getOrCreateLabelCounter(token).Store(n)
			}
		} else {
			for token, set := range bs.labelIdx {
				bs.getOrCreateLabelCounter(token).Store(int64(len(set)))
			}
		}
		for token, set := range bs.typeIdx {
			bs.getOrCreateTypeCounter(token).Store(int64(set.Len()))
		}

		// Load property index definitions and rebuild index data.
		item, err := txn.Get(storepkg.PropIndexDefsKey)
		if err == nil {
			var defs []propIdxDef
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &defs)
			}); e != nil {
				return fmt.Errorf("graph: load property index definitions: %w", e)
			}
			propIdxDefined = len(defs) > 0
			seenProperty := make(map[indexpkg.PropertyIndexKey]struct{}, len(defs))
			for _, def := range defs {
				if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
					return fmt.Errorf("graph: load property index definition label %d property %q: %w",
						def.LabelToken, def.PropertyKey, err)
				}
				if err := storecontract.ValidateIndexPropertyKey(def.PropertyKey); err != nil {
					return fmt.Errorf("graph: load property index definition label %d property %q: %w",
						def.LabelToken, def.PropertyKey, err)
				}
				if def.Elements && bs.propIdxOnDisk {
					// The disk keyspace keys scalars only and is shared by
					// every label indexing the key: an Elements index would
					// answer equality. Open it without PropertyIndexOnDisk.
					return fmt.Errorf("graph: load property index definition label %d property %q: an elements index needs the RAM index, not PropertyIndexOnDisk: %w",
						def.LabelToken, def.PropertyKey, storecontract.ErrCapabilityNotSupported)
				}
				key := indexpkg.PropertyIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
				if _, exists := seenProperty[key]; exists {
					continue
				}
				seenProperty[key] = struct{}{}
				idx := indexpkg.NewPropertyIndexWith(def.RangeCounts, def.Elements)
				if entries, ok := img.takeNodeProp(key); ok && !bs.propIdxOnDisk {
					installValueIDs(entries, idx.AddKey)
				} else if bs.propIdxOnDisk && !propIdxNeedsBuild {
					// Disk mode keeps the entries in the 0x0A keyspace, already
					// built: nothing to read from the rows.
				} else if nodeIDs, ok := bs.openLabelMembers(txn, def.LabelToken, lazyLabels); ok {
					build := indexpkg.NewPostingsBuild(def.Elements)
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							// Tolerate missing/corrupt rows during rebuild, but count and warn
							// (IndexRebuildStats).
							bs.indexRebuildPropertySkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: property-index rebuild skipped node %d (label %d, property %q): %v", rawID, def.LabelToken, def.PropertyKey, nerr)
							}
							continue
						}
						if !bs.propIdxOnDisk {
							build.AddEntityKeys(rawID, def.PropertyKey, n.ForEachPropertyIndexKey)
							continue
						}
						// Disk mode: do NOT populate idx.Entries/numBuckets (entries stay
						// off the RAM heap); collect the one-time backfill op instead,
						// only when this directory has not been backfilled before
						// (propIdxNeedsBuild).
						valueKey, found := n.IndexablePropertyValueKey(def.PropertyKey)
						if found && propIdxNeedsBuild {
							if op, ok := bs.propertyIndexDiskOp(def.PropertyKey, valueKey, rawID, writeOpSet); ok {
								propIdxBackfillOps = append(propIdxBackfillOps, op)
							}
						}
					}
					fillFromBuild(build, idx.AddKey)
				}
				bs.propertyIndexes[key] = idx
				bs.restoreNodePropTx(img, key)
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no indexes defined yet.

		// Load rel property index definitions and rebuild the RAM value maps from current
		// relationship state (mirrors the non-disk node rebuild above).
		item, err = txn.Get(storepkg.RelPropIndexDefsKey)
		if err == nil {
			var defs []relPropIdxDef
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &defs)
			}); e != nil {
				return fmt.Errorf("graph: load relationship property index definitions: %w", e)
			}
			seenRelProperty := make(map[indexpkg.RelPropertyIndexKey]struct{}, len(defs))
			for _, def := range defs {
				if err := storecontract.ValidateRelTypeToken(def.RelTypeToken); err != nil {
					return fmt.Errorf("graph: load relationship property index definition type %d property %q: %w",
						def.RelTypeToken, def.PropertyKey, err)
				}
				if err := storecontract.ValidateIndexPropertyKey(def.PropertyKey); err != nil {
					return fmt.Errorf("graph: load relationship property index definition type %d property %q: %w",
						def.RelTypeToken, def.PropertyKey, err)
				}
				key := indexpkg.RelPropertyIndexKey{RelTypeToken: def.RelTypeToken, PropertyKey: def.PropertyKey}
				if _, exists := seenRelProperty[key]; exists {
					continue
				}
				seenRelProperty[key] = struct{}{}
				idx := indexpkg.NewPropertyIndexWith(def.RangeCounts, def.Elements)
				if entries, ok := img.takeRelProp(key); ok {
					installValueIDs(entries, idx.AddKey)
				} else if relIDs := bs.typeIdx[def.RelTypeToken]; relIDs != nil {
					build := indexpkg.NewPostingsBuild(def.Elements)
					for id := range relIDs.All() {
						relID := types.RelID(id)
						rawID := relID.SnowflakeID()
						r, rerr := bs.loadRelFromBadger(txn, rawID)
						if rerr != nil {
							// Tolerate missing/corrupt during rebuild, mirroring the
							// node property-index rebuild's F9 skip accounting.
							bs.indexRebuildPropertySkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: rel-property-index rebuild skipped rel %d (type %d, property %q): %v", rawID, def.RelTypeToken, def.PropertyKey, rerr)
							}
							continue
						}
						build.AddEntityKeys(rawID, def.PropertyKey, r.ForEachPropertyIndexKey)
					}
					fillFromBuild(build, idx.AddKey)
				}
				bs.relPropertyIndexes[key] = idx
				bs.restoreRelPropTx(img, key)
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no rel property indexes defined yet.

		// Load composite property index definitions and rebuild index data.
		// RAM-only — v1 has no on-disk mode, unlike PropertyIndexOnDisk.
		item, err = txn.Get(storepkg.CompositeIndexDefsKey)
		if err == nil {
			var compositeDefs []compositeIdxDef
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &compositeDefs)
			}); e != nil {
				return fmt.Errorf("graph: load composite index definitions: %w", e)
			}
			seenComposite := make(map[indexpkg.CompositeIndexKey]struct{}, len(compositeDefs))
			for _, def := range compositeDefs {
				if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
					return fmt.Errorf("graph: load composite index definition label %d: %w", def.LabelToken, err)
				}
				if err := storecontract.ValidateCompositeIndexKeys(def.Keys); err != nil {
					return fmt.Errorf("graph: load composite index definition label %d: %w", def.LabelToken, err)
				}
				key := indexpkg.CompositeIndexKey{LabelToken: def.LabelToken, Keys: indexpkg.EncodeCompositeKeyTuple(def.Keys)}
				if _, exists := seenComposite[key]; exists {
					continue
				}
				seenComposite[key] = struct{}{}
				idx := indexpkg.NewCompositePropertyIndex(def.Keys)
				if entries, ok := img.takeComposite(key); ok {
					installValueIDs(entries, idx.AddKey)
				} else if nodeIDs, ok := bs.openLabelMembers(txn, def.LabelToken, lazyLabels); ok {
					build := indexpkg.NewPostingsBuild(false)
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							// Tolerate missing/corrupt during rebuild, but
							// record + warn (F9), same as the single-key path.
							bs.indexRebuildCompositeSkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: composite-index rebuild skipped node %d (label %d, keys %v): %v", rawID, def.LabelToken, def.Keys, nerr)
							}
							continue
						}
						if vk, found := indexpkg.NodeCompositeValueKey(idx.Keys, n); found {
							build.AddKey(rawID, vk)
						}
					}
					fillFromBuild(build, idx.AddKey)
				}
				indexpkg.RegisterCompositeIndex(bs.compositeIndexes, bs.compositeIndexesByLabel, key, idx)
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no composite indexes defined yet.

		// Load temporal index label tokens and rebuild index data.
		item, err = txn.Get(storepkg.TemporalIndexDefsKey)
		if err == nil {
			var tokens []uint16
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &tokens)
			}); e != nil {
				return fmt.Errorf("graph: load temporal index definitions: %w", e)
			}
			seenTemporal := make(map[uint16]struct{}, len(tokens))
			for _, tok := range tokens {
				if err := storecontract.ValidateLabelToken(tok); err != nil {
					return fmt.Errorf("graph: load temporal index definition label %d: %w", tok, err)
				}
				if _, exists := seenTemporal[tok]; exists {
					continue
				}
				seenTemporal[tok] = struct{}{}
				ti := indexpkg.NewTemporalIndex()
				var restored []indexpkg.IntervalEntry
				haveImage := false
				if !temporalIdxNeedsBuild { // a pending 0x0B backfill needs the row walk below
					restored, haveImage = img.takeTemporal(tok)
				}
				if haveImage {
					// The runtime envelope also keeps nodes that left the label; the open keeps only
					// current members, as the row walk below does.
					members, _ := bs.openLabelMembers(txn, tok, lazyLabels)
					for _, e := range restored {
						if _, ok := members[types.NodeID(e.ID)]; ok {
							ti.AddKnownAbsent(e.ID, e.From, e.To)
						}
					}
				} else if bs.temporalIdxOnDisk && !temporalIdxNeedsBuild {
					// Fast path: stream (from, to, id) triples from the 0x0B keyspace without decoding
					// nodes; key order matches TemporalIndex.Entries (storeutil.TemporalIndexEntryKey).
					valueOpts := opts
					valueOpts.PrefetchValues = false // values are inline (PREFETCH)
					prefix := storepkg.TemporalIndexTokenPrefix(tok)
					it := txn.NewIterator(valueOpts)
					for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
						key := it.Item().Key()
						if len(key) != storepkg.SizeTemporalIndexEntryKey {
							continue
						}
						id := storepkg.TemporalIndexNodeIDFromKey(key)
						from := storepkg.TemporalIndexFromFromKey(key)
						var to types.Instant
						if verr := it.Item().Value(func(val []byte) error {
							to = storepkg.TemporalIndexEntryValueDecode(val)
							return nil
						}); verr != nil {
							it.Close()
							return fmt.Errorf("graph: read temporal-index-on-disk entry: %w", verr)
						}
						ti.AddKnownAbsent(id, from, to)
					}
					it.Close()
				} else if nodeIDs, ok := bs.openLabelMembers(txn, tok, lazyLabels); ok {
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							// Tolerate missing/corrupt during rebuild,
							// but record + warn (F9).
							bs.indexRebuildTemporalSkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: temporal-index rebuild skipped node %d (label %d): %v", rawID, tok, nerr)
							}
							continue
						}
						from, to := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), rawID, n.Temporal())
						ti.AddKnownAbsent(rawID, from, to)
						if bs.temporalIdxOnDisk && temporalIdxNeedsBuild {
							// Rebuild-on-enable: collect the one-time 0x0B backfill row (committed after the
							// View, see commitTemporalIndexOnDiskBackfill).
							temporalIdxBackfillOps = append(temporalIdxBackfillOps, writeOp{
								opType: writeOpSet,
								key:    storepkg.TemporalIndexEntryKey(tok, from, rawID),
								value:  storepkg.TemporalIndexEntryValue(to),
							})
						}
					}
				}
				bs.temporalIndexes[tok] = ti
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no temporal indexes defined yet.

		// Load high-frequency temporal index definitions and rebuild bucket data.
		item, err = txn.Get(storepkg.HighFrequencyIndexDefsKey)
		if err == nil {
			var defs []hfIdxDef
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &defs)
			}); e != nil {
				return fmt.Errorf("graph: load high-frequency index definitions: %w", e)
			}
			seenHF := make(map[uint16]time.Duration, len(defs))
			for _, def := range defs {
				if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
					return fmt.Errorf("graph: load high-frequency index definition label %d: %w",
						def.LabelToken, err)
				}
				if _, exists := bs.temporalIndexes[def.LabelToken]; exists {
					return fmt.Errorf("graph: load high-frequency index definition label %d: %w",
						def.LabelToken, ErrTemporalIndexExists)
				}
				bucketSize, err := highFrequencyBucketDuration(def.BucketSizeMillis)
				if err != nil {
					return fmt.Errorf("graph: load high-frequency index definition label %d: %w",
						def.LabelToken, err)
				}
				if existing, exists := seenHF[def.LabelToken]; exists {
					if existing != bucketSize {
						return fmt.Errorf("graph: load high-frequency index definition label %d: %w",
							def.LabelToken, ErrTemporalIndexExists)
					}
					continue
				}
				seenHF[def.LabelToken] = bucketSize
				hfi := indexpkg.NewHighFrequencyIndex(bucketSize, 0)
				if entries, ok := img.takeHF(def.LabelToken); ok {
					for _, e := range entries {
						hfi.Add(e.id, e.start)
					}
				} else if nodeIDs, ok := bs.openLabelMembers(txn, def.LabelToken, lazyLabels); ok {
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							bs.indexRebuildHFSkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: high-frequency-index rebuild skipped node %d (label %d): %v", rawID, def.LabelToken, nerr)
							}
							continue
						}
						from, _ := indexpkg.NodeTemporalBounds(bs.validAxis.Get(), rawID, n.Temporal())
						hfi.Add(types.NodeID(rawID), from)
					}
				}
				bs.hfIndexes[def.LabelToken] = hfi
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no high-frequency indexes defined yet.

		// Load vector index definitions and rebuild index data.
		item, err = txn.Get(storepkg.VectorIndexDefsKey)
		if err == nil {
			var defs []vectorIdxDef
			if e := item.Value(func(val []byte) error {
				return storepkg.SafeUnmarshal(val, &defs)
			}); e != nil {
				return fmt.Errorf("graph: load vector index definitions: %w", e)
			}
			seenVector := make(map[indexpkg.VectorIndexKey]vectorIdxDef, len(defs))
			for _, def := range defs {
				if err := storecontract.ValidateLabelToken(def.LabelToken); err != nil {
					return fmt.Errorf("graph: load vector index definition label %d property %q: %w",
						def.LabelToken, def.PropertyKey, err)
				}
				if err := storecontract.ValidateIndexPropertyKey(def.PropertyKey); err != nil {
					return fmt.Errorf("graph: load vector index definition label %d property %q: %w",
						def.LabelToken, def.PropertyKey, err)
				}
				if err := indexpkg.ValidateVectorIndexConfig(def.Dims, def.Metric); err != nil {
					return fmt.Errorf("graph: load vector index definition label %d property %q: %w",
						def.LabelToken, def.PropertyKey, err)
				}
				key := indexpkg.VectorIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
				if existing, exists := seenVector[key]; exists {
					if existing.Dims != def.Dims || existing.Metric != def.Metric {
						return fmt.Errorf("graph: load vector index definition label %d property %q: %w",
							def.LabelToken, def.PropertyKey, ErrVectorIndexExists)
					}
					continue
				}
				seenVector[key] = def
				vi := &indexpkg.VectorIndex{Dims: def.Dims, Metric: def.Metric}
				indexpkg.ApplyVectorIndexOptions(vi, def.vectorIndexOptions())
				if entries, ok := img.takeVector(key, def.Dims); ok {
					for _, e := range entries {
						if addErr := vi.AddOwned(e.id, e.vec); addErr != nil {
							return fmt.Errorf("graph: load vector index definition label %d property %q restore node %d: %w",
								def.LabelToken, def.PropertyKey, e.id, addErr)
						}
					}
				} else if nodeIDs, ok := bs.openLabelMembers(txn, def.LabelToken, lazyLabels); ok {
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							bs.indexRebuildVectorSkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: vector-index rebuild skipped node %d (label %d, property %q): %v", rawID, def.LabelToken, def.PropertyKey, nerr)
							}
							continue
						}
						vec, ok := n.Float32SlicePropertyCopy(def.PropertyKey)
						if !ok {
							continue
						}
						if addErr := vi.AddOwned(rawID, vec); addErr != nil {
							return fmt.Errorf("graph: load vector index definition label %d property %q rebuild node %d: %w",
								def.LabelToken, def.PropertyKey, rawID, addErr)
						}
					}
				}
				bs.vectorIndexes[key] = vi
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no vector indexes defined yet.

		return nil
	})
	if err != nil {
		return err
	}

	bs.commitKeyRepair(kr)

	if bs.propIdxOnDisk && propIdxNeedsBuild {
		if bs.propKeyReg.Load() == nil && propIdxDefined {
			// No property-key registry wired (direct badger.Store user): skip the backfill and
			// leave the marker unset so a later open retries. With no definition there is nothing
			// to backfill: mark it built, or every open re-walks the rows (backlog 54).
		} else if err := bs.commitPropertyIndexOnDiskBackfill(propIdxBackfillOps); err != nil {
			return err
		}
	}
	if bs.temporalIdxOnDisk && temporalIdxNeedsBuild && !bs.readOnly {
		if err := bs.commitTemporalIndexOnDiskBackfill(temporalIdxBackfillOps); err != nil {
			return err
		}
	}
	if bs.temporalIdxOnDisk && !bs.readOnly {
		if err := bs.purgeUndefinedTemporalDiskRows(); err != nil {
			return err
		}
	}
	return nil
}

// reconcilePersistedCounter resolves a persisted entity counter against the rows
// present after an index rebuild; rows are authoritative:
//
//   - persisted == 0: never written (pre-counter store), trust rows.
//   - persisted == liveRows: consistent.
//   - persisted == rawEntityRows > liveRows: some rows soft-deleted/undecodable, trust
//     liveRows.
//   - persisted < liveRows == rawEntityRows: counter lost increments in an unclean
//     shutdown; heal up to liveRows and warn.
//
// Anything else, notably persisted > liveRows (rows missing, data loss), stays fatal.
func reconcilePersistedCounter(name string, persisted, liveRows, rawEntityRows int64, logger badgerv4.Logger) (int64, error) {
	if persisted == 0 {
		return liveRows, nil
	}
	if persisted == liveRows {
		return liveRows, nil
	}
	if persisted == rawEntityRows && rawEntityRows > liveRows {
		return liveRows, nil
	}
	if persisted < liveRows && liveRows == rawEntityRows {
		if logger != nil {
			logger.Warningf("graph: %s counter %d undercounts %d clean live rows — healing up to the live row count (lost increments from an unclean shutdown)",
				name, persisted, liveRows)
		}
		return liveRows, nil
	}
	return 0, fmt.Errorf("%w: %s counter %d does not match %d live rows",
		ErrInvalidStoreMutation, name, persisted, liveRows)
}

func (bs *Store) addNodeIndexesFromRow(nid types.NodeID, labelTokens []uint16) map[uint16]struct{} {
	labels := make(map[uint16]struct{}, len(labelTokens))
	for _, tok := range labelTokens {
		labels[tok] = struct{}{}
		bs.addLabelIdxLocked(tok, nid)
	}
	return labels
}

// inKeyHereLocked reports whether the in-key of a local rel row lives on this
// store: its end node is local, or neither endpoint is (a co-located rel: a
// tiered mint shard, tiered.Config.RelsByMint, or a sharded rel slot). A local
// start with a remote end is a tiered split write, whose in-key lives on the
// end node's shard. Caller holds idxMu or is the open.
func (bs *Store) inKeyHereLocked(info RelDeleteInfo) bool {
	if bs.nodeIDs.Has(int64(types.NodeID(info.EndID))) {
		return true
	}
	startLocal := bs.nodeIDs.Has(int64(types.NodeID(info.StartID)))
	return !startLocal
}

func (bs *Store) addRelationshipIndexesFromRow(info RelDeleteInfo) {
	rid := types.RelID(info.ID)
	relType := info.RelType

	bs.addTypeIdxLocked(relType, rid)
	if bs.adjOnDisk {
		return // adjacency answered from the out/in keys (badgerstore_adjacency_disk.go)
	}

	// The out-key lives with the row, the in-key where inKeyHereLocked says
	// (the rules addMissingKeys repairs by): legs from the row answer a key
	// the repair re-creates at this open in this session too.
	startNID := types.NodeID(info.StartID)
	if bs.outIdx[startNID] == nil {
		bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
	}
	bs.outIdx[startNID][rid] = types.NodeID(info.EndID)

	if bs.inKeyHereLocked(info) {
		endNID := types.NodeID(info.EndID)
		if bs.inIdx[endNID] == nil {
			bs.inIdx[endNID] = make(map[types.RelID]inEdge)
		}
		bs.inIdx[endNID][rid] = inEdge{start: types.NodeID(info.StartID), typ: relType}
	}
}

// Clear removes all entities, indexes, history, counters and secondary indexes,
// leaving the Store as freshly opened. Registries are a Graph-layer concern and are
// not cleared.
//
// flushMu is taken first to drain any in-flight flush, which could otherwise race
// ahead of the wipe and resurrect pre-Clear entities, and to block new flushes. In
// SyncWrites mode concurrent mutations block for the duration of the wipe (accepted:
// Clear is rare and admin-scoped). Releasing flushMu before the wipe would not help:
// flush() also needs idxMu.RLock, which Clear holds via idxMu.Lock.
func (bs *Store) Clear() (err error) {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	// Clear wipes the persisted commit floor with the meta keys; the next flush
	// re-persists the (never lowered) commitFloor with its rows.
	bs.commitFloorDurable = 0

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// No history-presence build may scan across the wipe (lock order: flushMu -> idxMu
	// -> presence buildMu -> wbMu; a build takes only buildMu -> wbMu).
	bs.histNodePresence.buildMu.Lock()
	defer bs.histNodePresence.buildMu.Unlock()
	bs.histRelPresence.buildMu.Lock()
	defer bs.histRelPresence.buildMu.Unlock()
	bs.histNodeStamps.buildMu.Lock() // the stamps sidecars likewise (buildMu -> wbMu)
	defer bs.histNodeStamps.buildMu.Unlock()
	bs.histRelStamps.buildMu.Lock()
	defer bs.histRelStamps.buildMu.Unlock()
	bs.resetValidEndsLocked() // the valid-end indexes (tracked under idxMu)

	// Clear in-memory indexes.
	bs.nodeIDs = indexpkg.IDSet{}
	bs.nodeHashes = make(map[types.NodeID]string)
	// Every node revision moves past any captured before Clear: a lazy hash
	// read that started before it must not match a re-created node's revision
	// (fillNodeHash).
	bs.nodeRevs.bumpAll()
	bs.nodeEpoch.Add(1) // invalidate cached columns built before Clear
	bs.sharedEpochs.node()
	bs.nodeEpochSalt.Add(1) // label-less event: invalidate every per-label column too (BACKLOG 4b)
	bs.poisonAllLabels()    // label-less event: no per-label append record can describe it (R3)
	bs.relEpoch.Add(1)      // and the adjacency view (expand path)
	bs.sharedEpochs.rel()
	bs.docMu.Lock()
	bs.docColumns = nil
	bs.docColumnsMulti = nil
	bs.colOrds = columnOrdinalCache{}
	bs.docMu.Unlock()
	bs.relIDs = indexpkg.IDSet{}
	bs.relRevs.bumpAll()
	bs.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	bs.typeIdx = make(map[uint16]*indexpkg.IDSet)
	bs.labelOrder.Reset()
	bs.typeOrder.Reset()
	bs.ords.reset()
	bs.outIdx = make(map[types.NodeID]map[types.RelID]types.NodeID)
	bs.inIdx = make(map[types.NodeID]map[types.RelID]inEdge)
	bs.relValidIdx = nil // drop the lazy stamp index; rebuilt on next temporal traversal
	bs.relValidIdxBuilt.Store(false)
	bs.labelTxMembers = nil // drop the lazy membership sidecar; rebuilt on next pinned scan
	bs.labelTxMembersBuilt.Store(false)
	bs.relTypeTxMembers = nil // rel-type mirror
	bs.relTypeMembersBuilt.Store(false)
	bs.relPurgeMu.Lock()
	bs.relPurgeCursor = nil // backlog 21: a cleared store has nothing to resume
	bs.relPurgeMu.Unlock()
	bs.dropPropertyTxMembersLocked() // property sidecars; an in-flight build discards its scan
	bs.nodeBeliefWatermark = nil     // drop the lazy belief-watermark sidecar; rebuilt on next use
	bs.nodeBeliefWatermarkBuilt.Store(false)
	bs.relBeliefWatermark = nil // rel mirror
	bs.relBeliefWatermarkBuilt.Store(false)

	// Reset counters. Clear sync.Map contents via Range+Delete, not struct
	// reassignment: NodeCountByLabel / RelCountByType Load without idxMu, so replacing
	// the sync.Map value would race on the field.
	bs.nodeCount.Store(0)
	bs.relCount.Store(0)
	bs.labelCounts.Range(func(k, _ any) bool {
		bs.labelCounts.Delete(k)
		return true
	})
	bs.typeCounts.Range(func(k, _ any) bool {
		bs.typeCounts.Delete(k)
		return true
	})
	bs.propertyKeyCounts.Range(func(k, _ any) bool {
		bs.propertyKeyCounts.Delete(k)
		return true
	})
	bs.propertyTypeClassCounts.Range(func(k, _ any) bool {
		bs.propertyTypeClassCounts.Delete(k)
		return true
	})
	bs.relPropertyTypeClassCounts.Range(func(k, _ any) bool {
		bs.relPropertyTypeClassCounts.Delete(k)
		return true
	})
	bs.propertyStats = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	bs.relPropertyKeyCounts.Range(func(k, _ any) bool {
		bs.relPropertyKeyCounts.Delete(k)
		return true
	})
	bs.relPropertyStats = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	bs.statsDeferred.Store(false) // the empty statistics are exact for the empty store

	// Re-create LRU caches with same capacity and byte budget.
	cap := bs.nodeCache.Cap()
	bs.nodeCache = newNodeCache(cap, bs.nodeCache.Budget())
	bs.relCache = newRelCache(cap, bs.relCache.Budget())

	// Clear the pending buffer (flushMu serializes against flush()) and any buffered
	// change-log records: the wipe drops KeyChangeLog, so a stale record would be
	// re-flushed as a phantom. logSeq stays monotonic so post-Clear LSNs never collide
	// with a consumer's pre-Clear watermark.
	bs.commitGen.Add(1) // read snapshots re-pin after the wipe
	bs.wbMu.Lock()
	bs.pending = make(map[string]writeOp)
	bs.pendingLog = nil
	bs.pendingBytes = 0
	bs.histNodeEpoch.Add(1)
	bs.histRelEpoch.Add(1)
	bs.histNodePresence.resetLocked()
	bs.histRelPresence.resetLocked()
	bs.histNodeStamps.resetLocked()
	bs.histRelStamps.resetLocked()
	bs.histNodeEpochs.NoteAll()
	bs.histRelEpochs.NoteAll()
	// Drop any snapshot a just-completed flush parked, or rangePending would resurface
	// pre-Clear history keys as phantom IDs.
	bs.flushing = nil
	bs.wbMu.Unlock()

	// Clear all secondary index maps; leftovers would make CreateTemporalIndex /
	// CreateHighFrequencyIndex / CreateVectorIndex report "already exists" on an empty
	// store and leave stale vector entries in SearchNearestNodes.
	bs.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	bs.relPropertyIndexes = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex)
	// The wipe below may drop the format marker: the next Elements definition
	// re-raises it rather than trusting the cached value.
	bs.wbMu.Lock()
	bs.formatMarker = 0
	bs.wbMu.Unlock()
	// Rows written after the wipe carry binary hashes (backlog 61): the
	// wiped marker is stamped again once the wipe has succeeded.
	defer func() {
		if err == nil {
			err = bs.restampBinaryHashMarker()
		}
	}()
	// The value dictionaries outlive the wipe (a token handed out before it
	// may reach a row written after it): their entries go back into the batch.
	defer bs.vdict.reappend(bs)
	bs.compositeIndexes = make(map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex)
	bs.compositeIndexesByLabel = make(map[uint16][]indexpkg.CompositeIndexKey)
	bs.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	bs.relTypeTemporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	bs.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	bs.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)

	if bs.clearDropTestHook != nil {
		if err := bs.clearDropTestHook(); err != nil {
			return err
		}
	}
	// Change-log enabled: wipe via DropPrefix keeping LastLSNKey continuously durable
	// (clearAndReanchorChangeLog), so a crash mid-Clear cannot reseed LSNs to 0 (lesson 53).
	if bs.logEnabled.Load() {
		return bs.clearAndReanchorChangeLog()
	}
	// Change-log is off now, but a prior enabled session may have left a durable
	// LastLSNKey; wiping it would let a later enabled reopen reseed LSNs to 0 and reuse
	// LSNs a consumer checkpointed past (lesson 53), so it is kept when present. A
	// never-logged store is wiped the same way, never by DropAll (DROPALL): DropAll
	// deletes the value-log files under a read that already found a key, and badger
	// returns an empty value for a value-log read it cannot do (iterator.go:187-188).
	// DropPrefix leaves them to the value-log GC, so such a read gets its row.
	return bs.clearDataPreservingLastLSN()
}

// Close stops background goroutines, performs a final flush (including counters)
// and closes the Badger database. Safe to call multiple times.
func (bs *Store) Close() error {
	if bs == nil {
		return ErrNilStore
	}
	if bs.db == nil || bs.stopCh == nil || bs.flushDone == nil || bs.gcDone == nil {
		return ErrStoreClosed
	}
	var err error
	bs.closeOnce.Do(func() {
		bs.closing.Store(true)
		close(bs.stopCh)
		<-bs.flushDone // wait for flushLoop exit (or immediate if never started)
		<-bs.gcDone    // wait for GC exit
		// Explicit final flush — ensures pending ops are persisted even when
		// flushLoop was never spawned. No-op if already drained.
		e := bs.flush()
		err = e
		// flushMu from the open image's drained check until dbClosed is set: a write that
		// passed checkWritable before closing waits here, then finds the DB closed, so it
		// cannot commit between the image's encode and its header (badgerstore_open_image.go).
		bs.flushMu.Lock()
		if e == nil {
			bs.persistOpenImageLocked() // the next open reads keys + this image, not every row
		}
		// Mark closed BEFORE db.Close() so a concurrent flush() returns an error instead of
		// blocking in Badger's WaitForMark.
		bs.dbClosed.Store(true)
		bs.flushMu.Unlock()
		err = errors.Join(err, bs.db.Close())
	})
	return err
}

func (bs *Store) checkOpen() error {
	if bs == nil {
		return ErrNilStore
	}
	if bs.db == nil {
		return ErrStoreClosed
	}
	if bs.closing.Load() {
		return ErrStoreClosed
	}
	if bs.dbClosed.Load() {
		return ErrStoreClosed
	}
	if err := bs.stagedFailureErr(); err != nil {
		return err
	}
	return nil
}

// isClosingOrClosed is the shared "silently return zero" guard for stats accessors
// whose signature cannot propagate checkOpen()'s error (backlog 18q). A nil *Store is
// handled by the caller.
func (bs *Store) isClosingOrClosed() bool {
	return bs.closing.Load() || bs.dbClosed.Load()
}

func (bs *Store) checkWritable() error {
	if err := bs.checkOpen(); err != nil {
		return err
	}
	if bs.readOnly {
		return fmt.Errorf("%w: read-only store", ErrInvalidStoreMutation)
	}
	return nil
}

// IndexRebuildStats reports how many node entries loadIndexes (at Open) tolerated
// as missing or corrupt while rebuilding the property, temporal, high-frequency and
// vector indexes. A nonzero count means the persisted indexes are degraded; run an
// explicit repair pass before relying on index-backed queries.
type IndexRebuildStats struct {
	PropertySkipped  int64
	CompositeSkipped int64
	TemporalSkipped  int64
	HFSkipped        int64
	VectorSkipped    int64
}

// IndexRebuildStats returns the diagnostic counters captured during the last
// loadIndexes pass. Zero means a clean rebuild.
func (bs *Store) IndexRebuildStats() IndexRebuildStats {
	if bs == nil || bs.isClosingOrClosed() {
		return IndexRebuildStats{}
	}
	return IndexRebuildStats{
		PropertySkipped:  bs.indexRebuildPropertySkips.Load(),
		CompositeSkipped: bs.indexRebuildCompositeSkips.Load(),
		TemporalSkipped:  bs.indexRebuildTemporalSkips.Load(),
		HFSkipped:        bs.indexRebuildHFSkips.Load(),
		VectorSkipped:    bs.indexRebuildVectorSkips.Load(),
	}
}

// SetPropertyKeyRegistry installs the property-key registry: subsequent writes
// dictionary-encode property keys and reads resolve tokens back. Safe to call before
// any writes; effective from the next marshal/unmarshal. Nil disables tokenization
// and keeps the pre-tokenization wire format.
func (bs *Store) SetPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) {
	if bs == nil {
		return
	}
	bs.propKeyReg.Store(reg)
	// Re-seed the write-ahead watermark to the new registry's length (Core.New installs
	// the canonical registry loaded from durable meta); only later growth needs a commit.
	bs.persistKeyMu.Lock()
	if reg != nil {
		bs.persistedKeyLen.Store(int64(reg.Len()))
	} else {
		bs.persistedKeyLen.Store(0)
	}
	bs.persistKeyMu.Unlock()
}

// PropertyKeyRegistry returns the store's currently-installed property-key
// registry (nil if none). The tiered store reads the reference shard's registry
// via this to obtain the single canonical instance shared across all shards.
func (bs *Store) PropertyKeyRegistry() *registrypkg.PropertyKeyRegistry {
	if bs == nil {
		return nil
	}
	return bs.propKeyReg.Load()
}

// marshalNodeBytes encodes a node via MarshalNodeWireWithKeys using the
// currently-installed property-key registry (nil-safe), refusing a row the
// store cannot hold (ErrEntryTooLarge).
func (bs *Store) marshalNodeBytes(n *types.Node) ([]byte, error) {
	w, err := bs.nodeWire(n)
	var data []byte
	if err == nil {
		data, err = storepkg.MarshalNodeWireStruct(w)
	}
	if err == nil {
		err = bs.checkEntryValue("node row", len(data))
	}
	return data, err
}

// marshalRelBytes encodes a relationship via MarshalRelWireWithKeys, refusing a
// row the store cannot hold (ErrEntryTooLarge).
func (bs *Store) marshalRelBytes(r *types.Relationship) ([]byte, error) {
	w, err := bs.relWire(r)
	var data []byte
	if err == nil {
		data, err = storepkg.MarshalRelWireStruct(w)
	}
	if err == nil {
		err = bs.checkEntryValue("relationship row", len(data))
	}
	return data, err
}

// persistRegistryIfGrew write-ahead commits the property-key registry whenever it
// grew since the last commit. Called from flush() before the row WriteBatch, so a
// crash never leaves a tokenized row durable without its token (undecodable on
// reload, fatal counter mismatch).
//
// Growth is measured against the bs.persistedKeyLen watermark, not a per-marshal
// diff: the registry is shared with the core engine, which allocates tokens before
// the store marshals, so a diff is always zero. One persist per flush cycle keeps the
// fsync cost O(flush cycles). A failure aborts the flush and requeues the rows.
func (bs *Store) persistRegistryIfGrew() error {
	reg := bs.propKeyReg.Load()
	if reg == nil || bs.onPropertyKeyGrow == nil {
		return nil
	}
	// Skip during shutdown: the closing flush runs with closing=true, when
	// SavePropertyKeyRegistry's checkWritable rejects writes. Core.Close persists all
	// registries before store.Close, so those rows' tokens are already durable.
	if bs.closing.Load() {
		return nil
	}
	if int64(reg.Len()) <= bs.persistedKeyLen.Load() {
		return nil // fast path: nothing new since last commit
	}
	bs.persistKeyMu.Lock()
	defer bs.persistKeyMu.Unlock()
	cur := int64(reg.Len()) // re-read under lock; may have grown further
	if cur <= bs.persistedKeyLen.Load() {
		return nil // another writer committed it while we waited
	}
	if err := bs.onPropertyKeyGrow(); err != nil {
		return err
	}
	// The hook exported names after we read cur, so it persisted at least cur keys;
	// advancing the watermark only to cur keeps it a safe lower bound (an under-count
	// costs a redundant commit, never a skipped one).
	bs.persistedKeyLen.Store(cur)
	return nil
}

// resolveNodeWireKeys is a no-op when no registry is installed; otherwise
// it walks the wire's property slice and replaces any tokenized keys with
// their resolved strings. Call after msgpack.Unmarshal of NodeWire.
func (bs *Store) resolveNodeWireKeys(w *storepkg.NodeWire) error {
	if err := storepkg.ResolvePropertyKeyTokens(w.Properties, bs.propKeyReg.Load()); err != nil {
		return err
	}
	return bs.resolveValueTokens(w.Properties, w.FormatVersion)
}

func (bs *Store) resolveRelWireKeys(w *storepkg.RelWire) error {
	if err := storepkg.ResolvePropertyKeyTokens(w.Properties, bs.propKeyReg.Load()); err != nil {
		return err
	}
	return bs.resolveValueTokens(w.Properties, w.FormatVersion)
}

// loadIndexesRecovering is loadIndexes with a badger panic (a damaged table,
// recoverBadgerPanic) turned into an error: the open fails closed.
func (bs *Store) loadIndexesRecovering(img *openImage) (err error) {
	defer recoverBadgerPanic(&err)
	return bs.loadIndexes(img)
}
