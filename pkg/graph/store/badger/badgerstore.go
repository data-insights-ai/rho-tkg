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

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
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
	// DefaultMaxPendingWrites bounds the async write buffer: when the
	// pending-op count reaches this threshold the writing goroutine flushes
	// synchronously instead of growing the buffer (and the never-evictable
	// dirty cache entries behind it) without limit. Generous enough that a
	// workload must sustain ~1M ops/s against the 100ms flush interval to
	// ever hit it.
	DefaultMaxPendingWrites = 100_000
)

// Config configures a Store instance.
type Config struct {
	// Dir is the Badger data directory. Required unless InMemory is true.
	Dir string
	// InMemory enables memory-only mode (no disk I/O). Useful for testing.
	InMemory bool
	// Logger is the Badger logger. Nil uses Badger's default logger.
	Logger badgerv4.Logger
	// CacheCapacity is the per-cache (nodes, rels) soft limit. Default: 10,000.
	CacheCapacity int
	// CacheBudgetBytes bounds each entity cache (nodes, rels) by estimated
	// resident BYTES instead of entry count alone — entries vary 100B-64KB,
	// so a count capacity alone cannot bound memory under mixed payloads.
	// Clean LRU entries are evicted while the estimate exceeds the budget;
	// like CacheCapacity this is a soft limit (dirty entries are never
	// evicted). 0 disables byte accounting. When set and CacheCapacity is 0,
	// the count limit is effectively unbounded and the byte budget governs.
	CacheBudgetBytes int64
	// ResidentCache keeps every decoded node/rel resident (clean entries are
	// never evicted, and fetches skip LRU promotion). For an in-memory store
	// the backing data already lives in RAM, so re-decoding on cache miss is
	// pure waste that makes graph-larger-than-cache traversal scale
	// super-linearly; resident mode restores linear (Memgraph-like) big-O at
	// the cost of holding the decoded working set resident. Ignores
	// CacheCapacity/CacheBudgetBytes for eviction.
	ResidentCache bool
	// LabelIndexOnDisk keeps the label→nodes index OUT of RAM: label
	// snapshots are answered from the persisted label keyspace (written
	// transactionally with node rows since the format's inception — no
	// migration needed) via badger prefix iterators, with unflushed writes
	// overlaid. Saves the in-memory labelIdx map (~50-100B per label entry
	// — THE memory ceiling at hundreds of millions of nodes) at the cost
	// of disk iteration per label scan. See badgerstore_label_disk.go.
	LabelIndexOnDisk bool
	// AdjacencyIndexOnDisk is the adjacency sibling of LabelIndexOnDisk:
	// outgoing/incoming snapshots are answered from the persisted
	// OutKey/InKey keyspaces (also written transactionally with rel rows
	// since inception — no migration). Saves the outIdx/inIdx maps (~2
	// entries per relationship — the largest index maps) at the cost of a
	// disk prefix iteration per adjacency read; typed queries use an
	// 11-byte prefix that replaces the in-memory typeIdx intersection.
	// See badgerstore_adjacency_disk.go.
	AdjacencyIndexOnDisk bool
	// PropertyIndexOnDisk is the property-index sibling of LabelIndexOnDisk:
	// entries created by CreatePropertyIndex live in the persisted 0x0A
	// keyspace (see badgerstore_property_disk.go) instead of the in-memory
	// PropertyIndex.Entries/numBuckets maps — each entry is one
	// (propertyKeyToken, order-preserving value bytes, nodeID) row, written
	// transactionally with the node row it describes. Saves the per-value
	// RAM the in-memory index would otherwise hold at large scale, at the
	// cost of a disk prefix/range iteration per equality/range read. An
	// existing data directory with property-index definitions but no prior
	// 0x0A rows is backfilled from current node state exactly once, on the
	// first open with this flag set (guarded by
	// storeutil.PropertyIndexOnDiskBuiltKey, mirroring the wire-format
	// marker pattern) — no manual migration step is required. Requires a
	// wired property-key registry (see Config.PropertyKeyRegistry — always
	// present when opened via pkg/graph); CreatePropertyIndex fails closed
	// with ErrInvalidStoreMutation without one. Ignored when Store is
	// provided explicitly.
	PropertyIndexOnDisk bool
	// TemporalIndexOnDisk is a REBUILD-AT-OPEN accelerator for the
	// maxTo-augmented temporal interval index (g.Index().CreateTemporalIndex),
	// NOT a RAM-vs-disk trade-off like LabelIndexOnDisk / AdjacencyIndexOnDisk /
	// PropertyIndexOnDisk above: the index always stays fully resident in RAM
	// at runtime (its stabbing/overlap queries need the in-memory subMax
	// augmentation, which has no on-disk analogue). Off (default), reopening a
	// store with an existing temporal index definition rebuilds it via a full
	// node fetch+decode (a Badger point-get plus msgpack decode of the ENTIRE
	// row) for every node carrying the indexed label, just to extract two
	// int64 fields. On, a compact 19-byte-key/8-byte-value row is maintained
	// per (labelToken, entity) alongside the node row (see
	// badgerstore_temporal_disk.go), and loadIndexesScan streams straight from
	// a prefix iteration over it instead — trading a small amount of extra
	// write-path I/O for eliminating the O(N) full-row rebuild at open. An
	// existing data directory with temporal-index definitions but no prior
	// 0x0B rows is backfilled from current node state exactly once, on the
	// first open with this flag set (guarded by
	// storeutil.TemporalIndexOnDiskBuiltKey, mirroring the
	// PropertyIndexOnDiskBuiltKey pattern) — no manual migration step is
	// required. Ignored when Store is provided explicitly.
	TemporalIndexOnDisk bool
	// DisablePlannerStats turns OFF maintenance of the query-planner statistics —
	// the per-(label, property key) presence counts, NDV + min/max
	// range-cardinality accumulator, and exact type-class counts. These are
	// maintained on EVERY node write (a full per-property sweep in
	// adjustNodePropertyKeyCounts, under idxMu) and rebuilt by the loadIndexes
	// open scan, yet are consumed ONLY by query-planning APIs
	// (NodeCountByLabelAndPropertyKey, NodeRangeCardinality,
	// NodePropertyTypeClassCounts + rel mirrors). A pure-ingest or non-planning
	// deployment pays that write-path CPU (and open-scan work) for data it never
	// reads. When set, the maintenance is skipped and those stat methods fail
	// closed with ErrCapabilityNotSupported (range-cardinality returns exact=false)
	// — the SAME signal a backend that never implemented the capability returns,
	// so planners already fall back gracefully. NO correctness path reads these
	// counters (unique constraints use the property index, not the stats), so
	// disabling them changes only planner estimate availability. Default false =
	// stats maintained (unchanged behavior). Ignored when Store is provided
	// explicitly.
	DisablePlannerStats bool
	// HistoryDeltaEncoding turns ON anchor+delta storage for version-history rows
	// (badger 0x07/0x08). When set, a version V with V%HistoryAnchorInterval == 0
	// is stored as a full ANCHOR and the rest as DELTAS carrying only the
	// properties that changed vs the interval anchor — eliding large unchanged
	// values that a full snapshot would re-serialize every version (less
	// history storage post-Snappy on wide, history-heavy entities). Reads
	// reconstruct transparently and always accept BOTH forms, so the flag may be
	// toggled on an existing store with no migration (legacy full rows are
	// anchors; new deltas carry a 1-byte 'D' tag). Opt-in (default false) while the
	// path soaks; the current row (0x01/0x02) is always full. See ADR-0009.
	HistoryDeltaEncoding bool
	// HistoryAnchorInterval overrides the anchor spacing for HistoryDeltaEncoding
	// (0 = the default 16). A version V with V%interval == 0 is a full anchor; the
	// rest are deltas against the nearest lower anchor. A larger interval stores more
	// deltas per anchor (less storage, more reconstruction reads); a smaller one the
	// reverse. **The interval is baked into the on-disk delta layout**, so a store
	// that wrote deltas at interval N MUST be reopened at N — a persisted marker is
	// verified at open and a mismatch FAILS CLOSED (ErrHistoryAnchorIntervalMismatch),
	// because a delta reconstructed against the wrong anchor is a SILENT misread. To
	// change it on an existing delta store, rewrite history (not an inline migration).
	// Validated at New: 0 or in [2, 4096]. Moot when HistoryDeltaEncoding is off.
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
	// SyncWrites enables synchronous disk writes for every mutation.
	// When true, each write is flushed to Badger immediately (no async buffer).
	// Eliminates the async flush window at the cost of higher write latency.
	// Ignored in ReadOnly mode.
	SyncWrites bool
	// MaxPendingWrites bounds the async write buffer. When the pending-op
	// count reaches this threshold, the mutating call flushes synchronously
	// (backpressure) instead of letting the buffer — and the dirty,
	// never-evictable cache entries behind it — grow without limit under a
	// write burst faster than FlushInterval. Default:
	// DefaultMaxPendingWrites. Negative disables the bound (pre-4.5
	// unbounded behavior). Ignored when SyncWrites is true (every write
	// already flushes).
	MaxPendingWrites int
	// Compression sets the SSTable compression algorithm.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15).
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
	// ValueLogFileSize / MemTableSize / BlockCacheSize / NumCompactors tune
	// Badger's per-instance footprint. ZERO KEEPS BADGER'S STOCK DEFAULTS
	// (1GB vlog / 64MB memtable / 256MB block cache / 4 compactors) — a
	// deliberate choice for this library: silent default changes would be a
	// behavioral surprise for external consumers, so the owner (e.g. the
	// tiered store, or a downstream service) opts in explicitly. One Badger
	// instance opens per shard, so stock sizes multiply by shard count: a
	// handful of nearly-empty shards still pre-create GBs of apparent vlog and
	// allocate tens of MB of memtable arena each. These knobs bound that.
	//
	// ValueLogFileSize is validated to [1MB, 2GB) (Badger's own range). The
	// vlog FILE appears at 2x this while open (sparse — apparent, not
	// allocated) and is truncated to content on clean close.
	ValueLogFileSize int64 // bytes; valid [1MB, 2GB)
	// MemTableSize is the RAM knob: a heap arena of ~size+slack is allocated
	// upfront per open instance, and the WAL file appears at 2x this. Validated
	// to [8MB, 1GB] — the 8MB floor is a real Badger constraint (Open fails
	// unless ValueThreshold (1MB) <= 15% of MemTableSize). Shrinking this on an
	// existing data dir triggers a one-time WAL migration (see New).
	MemTableSize int64 // bytes; valid [8MB, 1GB]
	// BlockCacheSize bounds the Ristretto block cache (filled lazily, not
	// pre-allocated). Must be >= 0; 0 keeps the stock default.
	BlockCacheSize int64 // bytes; >= 0
	// IndexCacheSize bounds the Ristretto table-index/bloom-filter cache. Must
	// be >= 0; 0 keeps Badger's stock default (0 — indices decoded and kept
	// resident on the table object with NO cache). REQUIRED (> 0) whenever
	// EncryptionKey is set: an encrypted table's index is stored encrypted on
	// disk, and Badger's per-table fetchIndex() unconditionally PANICS
	// ("Index Cache must be set for encrypted workloads") the first time an
	// encrypted SSTable is created (flush or compaction) if this cache is
	// nil — a real, empirically-reproduced failure mode distinct from (and in
	// addition to) the BlockCacheSize requirement below. See
	// ErrEncryptionRequiresIndexCache.
	IndexCacheSize int64 // bytes; >= 0
	// NumCompactors sets the compactor goroutine count. 0 keeps the stock
	// default (4); any non-zero value must be >= 2 (Badger's minimum).
	NumCompactors int
	// ChangeLog enables the durable, ordered change-log (op-log): every
	// committed mutation appends a framed record under the KeyChangeLog
	// keyspace in the SAME WriteBatch as the data, tagged with a monotonic
	// cluster LSN. Off by default (zero overhead — the write path is
	// byte-for-byte identical). Surfaces store.ChangeFeedCapability for
	// change-data-capture / audit / point-in-time recovery and as the
	// foundation for read-replica streaming. Ignored in ReadOnly mode (a
	// read-only shard never mutates). See badgerstore_changelog.go.
	ChangeLog bool
	// ChangeLogSeqSource, when non-nil, replaces this shard's self-owned
	// change-log LSN counter (logSeq) with an injected store-global allocator.
	// A sharded owner (the tiered store) injects ONE allocator so every shard
	// draws change-log LSNs from a single monotonic sequence — the global
	// commit order the merged feed depends on. nil (the default) keeps the
	// self-owned counter, so a standalone badger store is byte-for-byte
	// unchanged. Ignored when ChangeLog is off or in ReadOnly mode. See
	// ChangeLogSeqSource and badgerstore_changelog.go.
	ChangeLogSeqSource ChangeLogSeqSource
	// PropertyKeyRegistry, when non-nil, is the property-key token registry the
	// store uses to tokenize on write and resolve tokens on read — supplied by
	// an owner (e.g. the tiered store) that holds ONE canonical registry for all
	// shards. When non-nil it is installed BEFORE loadIndexes so row decoding can
	// resolve tokenized property keys, and the store does NOT load its own copy
	// from meta. When nil, the store loads its own registry from its meta
	// (standalone behavior). The store never persists a registry it was handed;
	// persistence is the owner's responsibility (single canonical copy).
	PropertyKeyRegistry *registrypkg.PropertyKeyRegistry
	// OnPropertyKeyGrow, when non-nil, is invoked from flush() — BEFORE the row
	// WriteBatch — whenever the registry has grown since the last commit, i.e.
	// write-ahead persistence of the registry to its canonical location. The
	// tiered store sets this to commit the shared registry to the reference
	// shard (with fsync). When nil, the store persists the registry to its own
	// meta (standalone behavior). A non-nil error aborts the flush so a batch of
	// rows can never become durable before the registry entries it depends on.
	OnPropertyKeyGrow func() error
	// OnChangeLogFlush, when non-nil, is invoked from flush() AFTER a WriteBatch
	// carrying change-log records commits durably. The tiered store sets it to
	// persist the store-global allocator high-water to the reference shard's
	// catalog watermark (ADR-0005 §2.1-reseed), so reseed at open reads ONE key
	// and never opens a cold shard. A non-nil error surfaces to the writer (the
	// records are already durable; the watermark is belt-and-braces, so the store
	// logs but does not roll back). nil = no watermark persistence (standalone).
	OnChangeLogFlush func() error
	// EncryptionKey enables AES encryption-at-rest (SSTables, value log, WAL,
	// and Badger's own key registry). Length must be 0 (disabled — the
	// default, byte-for-byte the pre-encryption on-disk format), 16, 24, or 32
	// bytes (AES-128/192/256); any other length is rejected at New() with
	// ErrInvalidEncryptionKeyLength.
	//
	// Badger REQUIRES a non-zero BlockCacheSize whenever compression or
	// encryption is enabled and PANICS (not a returned error) on Open
	// otherwise — verified directly against the Badger v4.9.2 source (it is
	// BlockCacheSize, not IndexCacheSize, that gates the check). New() fails
	// closed with ErrEncryptionRequiresBlockCache instead of letting that
	// panic escape when EncryptionKey is set and BlockCacheSize == 0.
	//
	// Reopening an encrypted dir with the WRONG key, or opening an existing
	// PLAINTEXT dir with a non-empty key, fails Open with an error wrapping
	// badgerv4.ErrEncryptionKeyMismatch (errors.Is-able) — Badger detects
	// both by decrypting a sanity marker in its KEYREGISTRY file at open,
	// before any row is read.
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

// pendingLogRecord is a change-log record buffered for the next flush. value is
// the fully framed on-disk value (tag byte || msgpack body); lsn is its
// monotonic cluster sequence. Unlike pending (the entity-op map, which
// last-write-wins coalesces by key), pendingLog is an append-only slice — every
// committed mutation appears in the feed, never coalesced. Guarded by wbMu, the
// same lock as pending, so a record and its entity ops are snapshotted together
// by flush() (no committed-but-unlogged window).
type pendingLogRecord struct {
	lsn   uint64
	value []byte
}

// Store implements the Store interface using Badger as the durable backing store.
//
// Architecture: in-memory state is the source of truth while running. The LRU caches
// hold recently accessed entities with dirty tracking. In-memory indexes (label, type,
// adjacency) provide O(1) lookups. Write operations update in-memory state immediately
// and queue writeOps for async batch persistence.
//
// A background flush loop drains the write buffer to Badger via WriteBatch every
// FlushInterval. A GC loop runs RunValueLogGC periodically. On shutdown, a final
// flush ensures all pending writes are persisted.
//
// Counters are atomic int64 fields, persisted atomically in the flush WriteBatch. No OCC contention.
type Store struct {
	db *badgerv4.DB

	// In-memory indexes (source of truth while running).
	// Protected by idxMu for concurrent read/write access.
	idxMu                 sync.RWMutex
	nodeIDs               map[types.NodeID]struct{} // O(1) node existence check
	nodeHashes            map[types.NodeID]string   // current node integrity hash for live endpoint validation
	nodeRevs              map[types.NodeID]uint64   // live node row revision for safe prefetch handoff
	nextNodeRev           uint64
	relIDs                map[types.RelID]struct{}                      // O(1) rel existence check
	relRevs               map[types.RelID]uint64                        // live rel row revision for safe prefetch handoff (BACKLOG 18b — relRevs/nextRelRev mirror nodeRevs/nextNodeRev)
	nextRelRev            uint64
	labelIdx              map[uint16]map[types.NodeID]struct{}          // labelToken → set(nodeID); EMPTY in labelOnDisk mode
	labelOnDisk           bool                                          // answer label snapshots from the persisted keyspace
	adjOnDisk             bool                                          // answer adjacency snapshots from the persisted keyspaces
	propIdxOnDisk         bool                                          // maintain/answer property-index entries via the persisted 0x0A keyspace
	temporalIdxOnDisk     bool                                          // maintain the persisted 0x0B raw-entry log so loadIndexesScan can rebuild without a full node fetch per entity
	disablePlannerStats   bool                                          // skip planner-stat maintenance (presence/NDV/min-max/type-class) on writes + open; the stat capabilities fail closed with ErrCapabilityNotSupported
	historyDelta          bool                                          // store version-history rows as anchor+delta (ADR-0009); reads accept both forms regardless
	historyAnchorInterval uint64                                        // anchor spacing for historyDelta (>=1, default 16); baked into the on-disk layout — verified against a persisted marker at open
	typeIdx               map[uint16]map[types.RelID]struct{}           // relTypeToken → set(relID)
	outIdx                map[types.NodeID]map[types.RelID]types.NodeID // startNodeID → relID → endNodeID
	inIdx                 map[types.NodeID]map[types.RelID]inEdge       // endNodeID → relID → {startNodeID, typeToken}
	relValidIdx           map[types.RelID]relValidStamp                 // relID → effective {validFrom, validTo} for inline-stamp temporal traversal; nil until lazily built on the first temporal traversal
	relValidIdxBuilt      atomic.Bool                                   // fast-path "already built" check outside idxMu

	// Transaction-time membership sidecars (store.LabelTxMembershipCapability /
	// RelTypeTxMembershipCapability). labelTxMembers maps a label token to the set
	// of node IDs that EVER carried it (current OR any historical version) tagged
	// with a lower bound on their earliest acquisition transaction time;
	// relTypeTxMembers is the rel-type mirror. Both APPEND-ONLY (removal/delete
	// never drops a member) and nil until lazily built on the first pinned label/
	// type scan (mirrors relValidIdx). Guarded by idxMu.
	labelTxMembers      map[uint16]map[types.NodeID]types.Instant
	labelTxMembersBuilt atomic.Bool
	relTypeTxMembers    map[uint16]map[types.RelID]types.Instant
	relTypeMembersBuilt atomic.Bool

	// DocValues: cached per-label columnar snapshots + a global node-mutation
	// epoch bumped on EVERY node write (incl. deletes). nextNodeRev above misses
	// deletes, so DocValues keeps its own counter. docMu guards docColumns only
	// (the build itself runs lock-free, keyed on nodeEpoch — see
	// ForEachDocValues). atomic so the epoch reads need no lock.
	nodeEpoch atomic.Uint64
	// PER-LABEL node-mutation epochs (BACKLOG 4b): a node write bumps only the epochs
	// of the labels it carries, so a cached column for an UNRELATED label survives
	// write-active ingest of other labels (the global nodeEpoch invalidates every
	// label's column on any write). labelEpoch(token) = nodeLabelEpochs[token%256] +
	// nodeEpochSalt. The sharded array (256 stripes) trades exactness for lock-free
	// O(1): a hash collision over-invalidates two labels together (SAFE — never
	// stale). nodeEpochSalt is bumped on the label-LESS invalidation events (Clear,
	// retention purge) so they invalidate every label. Both counters are monotonic, so
	// a stamp-and-recheck is a total-order freshness test (and the multi-label cache
	// uses the monotonic SUM of member epochs). Bumped in add/removeNodePropertyKeyCounts
	// (the ungated wrappers every node-content write funnels through, with the node).
	nodeLabelEpochs [nodeLabelEpochStripes]atomic.Uint64
	nodeEpochSalt   atomic.Uint64
	// relEpoch: DISTINCT generation counter bumped on every relationship write. The
	// expand-aggregation column path reads ADJACENCY, so its Gate-2 re-check must
	// see edge mutations (nodeEpoch alone would wave through a torn aggregate from a
	// concurrent edge insert). Separate from nodeEpoch so node-only column caches do
	// not rebuild on edge-heavy writes.
	relEpoch   atomic.Uint64
	docMu      sync.Mutex
	docColumns map[uint16]*indexpkg.LabelDocValues
	// docColumnsMulti caches label-INTERSECTION columns (multi-label patterns like
	// (p:A:B)), keyed by the order-independent token-tuple key (MultiLabelKey).
	// Same docMu guard + lock-free epoch-keyed build as docColumns.
	docColumnsMulti map[string]*indexpkg.LabelDocValues

	// Entity caches (internal sync, N-way sharded — see indexpkg.ShardedCache).
	// Typed as the EntityCache interface so the concrete sharded implementation
	// is the single swap point in newNodeCache / newRelCache.
	nodeCache indexpkg.EntityCache[*types.Node]
	relCache  indexpkg.EntityCache[*types.Relationship]
	resident  bool // ResidentCache: caches never evict; fetches skip LRU promotion

	// Counters (atomic — persisted atomically via flush WriteBatch).
	nodeCount atomic.Int64
	relCount  atomic.Int64

	// Optional property-key registry — when set by Core.New, wire marshal /
	// unmarshal dictionary-encodes property keys into uint16 tokens. nil
	// preserves the pre-tokenization (V1) on-disk format.
	propKeyReg atomic.Pointer[registrypkg.PropertyKeyRegistry]

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
	// flushing holds the snapshot a concurrent flush() is mid-committing to
	// Badger. flush() parks `pending` here under wbMu BEFORE the WriteBatch
	// commit and clears it only AFTER the commit succeeds (or merges it back on
	// failure). Readers must consult BOTH maps: between the swap and the commit a
	// row lives ONLY in `flushing` — it is no longer in `pending` and not yet
	// visible in a Badger View. Entity point reads survive this window via the
	// resident dirty cache, but history / adjacency / label-index overlay scans
	// read the buffer directly and would otherwise drop in-flight rows. Always go
	// through rangePending / lookupPending so `flushing` is included.
	flushing map[string]writeOp

	// flushMu serializes concurrent flush() executions end-to-end.
	// Without this, two concurrent flush() calls can both snapshot atomic
	// counters under idxMu.RLock() and then submit out-of-order WriteBatches,
	// causing the later-completing batch to overwrite newer counter values with
	// stale ones — corrupting node/rel counts on restart.
	// The background flushLoop goroutine is single-threaded, so flushMu adds no
	// contention there. It serializes callers in SyncWrites mode only.
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

	// Per-(label, property key) EXACT type-class node counters — O(1) reads.
	// Keys are indexpkg.PropertyIndexKey, values *typeClassCounters. Unlike
	// propertyKeyCounts this covers EVERY property value (non-indexable ones
	// classify as types.ClassOther); maintained on the SAME mutation call
	// (adjustNodePropertyKeyCounts) and rebuilt by the same loadIndexes pass.
	propertyTypeClassCounts sync.Map // map[indexpkg.PropertyIndexKey]*typeClassCounters

	// relPropertyTypeClassCounts is the RELATIONSHIP mirror of propertyTypeClassCounts
	// (BACKLOG 5B): exact per-(relTypeToken, propertyKey) rel counts by
	// types.PropertyTypeClass. relTypeClassContrib memoizes each rel's per-property
	// classification by rel ID so the read-free deleteRelByInfo (which carries no
	// property values) can decrement precisely by ID — the single delete seam. Both
	// maintained at the full-rel-write ADD sites + rebuilt by loadIndexes.
	relPropertyTypeClassCounts sync.Map // map[indexpkg.RelPropertyIndexKey]*typeClassCounters
	relTypeClassContrib        sync.Map // map[snowflake.ID][]relClassEntry

	// Per-label property-key NDV + exact min/max accumulators, protected by
	// idxMu (unlike propertyKeyCounts's lock-free sync.Map — the accumulator's
	// HyperLogLog registers and min/max fields are not atomic-friendly, and
	// NodePropertyStats is a cold-path planner call, so it takes idxMu instead
	// of a bespoke lock-free structure). Maintained on the SAME node-mutation
	// doors as propertyKeyCounts — see adjustNodePropertyKeyCounts in
	// badgerstore_property_key_counts.go. Backs the optional
	// store.NodePropertyStatsCapability.
	propertyStats map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator

	// relPropertyKeyCounts / relPropertyStats / relStatsContrib are the
	// RELATIONSHIP mirror of propertyKeyCounts / propertyStats (BACKLOG 21a),
	// following the exact same shape as relPropertyTypeClassCounts /
	// relTypeClassContrib above: relPropertyKeyCounts is the per-(relType,
	// property key) presence counter (indexable scalar values only, same
	// convention as propertyKeyCounts); relPropertyStats is the per-(relType,
	// property key) NDV+min/max accumulator, protected by idxMu for the same
	// reason propertyStats is; relStatsContrib memoizes each rel's per-property
	// (key, valueKey, value) triples by rel ID so the read-free deleteRelByInfo
	// (which carries no property values) can Forget() precisely by ID — the
	// same memoized-delete-seam shape relTypeClassContrib already uses. Both
	// maintained at the full-rel-write ADD sites alongside
	// addRelPropertyTypeClassCounts + rebuilt by loadIndexes. Backs the
	// optional store.RelPropertyStatsCapability.
	relPropertyKeyCounts sync.Map // map[indexpkg.RelPropertyIndexKey]*atomic.Int64
	relPropertyStats     map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator
	relStatsContrib      sync.Map // map[snowflake.ID][]relStatsEntry

	// rescanTestHook, when non-nil, is invoked by NodePropertyStats right after
	// the unlocked value collection and BEFORE the write-generation re-check /
	// Rescan commit, once per rescan attempt. Production leaves it nil (zero
	// overhead); tests use it to deterministically land a concurrent mutation
	// inside the collect→commit window that the stale-rescan-overwrite guard
	// must catch. Set only from the owning test before starting workers.
	rescanTestHook func(attempt int)

	// historyScanTestHook, when non-nil, is invoked by the full-history prefix
	// readers (getNodeHistoryByPrefix / getRelHistoryByPrefix) right AFTER the
	// Badger scan and BEFORE the overlay merge. Production leaves it nil (zero
	// overhead); tests use it to deterministically land a concurrent flush()
	// commit (flushing rows -> Badger, `flushing` cleared) inside the
	// scan->merge window — the window in which a scan-first reader drops a row
	// that has left `flushing` but was not in the reader's older Badger
	// snapshot. Set only from the owning test.
	historyScanTestHook func()

	// replaceRelPrefetchTestHook, when non-nil, is invoked by ReplaceRelationship
	// right after prefetchRelWithRev returns and BEFORE idxMu.Lock() is acquired.
	// Production leaves it nil (zero overhead); tests use it to deterministically
	// land a concurrent property-changing write inside the prefetch->lock window
	// that relRevs (BACKLOG 18b) must detect via currentRelForPrefetchLocked's
	// rev check. Set only from the owning test.
	replaceRelPrefetchTestHook func()

	// Property indexes — in-memory only. Definitions persisted, data rebuilt on startup.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Relationship property indexes — RAM-only value maps, keyed by
	// rel-type token. Definitions persisted under RelPropIndexDefsKey; data
	// rebuilt from current relationships at open. There is no on-disk value
	// keyspace (RAM-only v1; a 0x0C rel keyspace disk mode is a documented
	// follow-up mirroring the node 0x0A PropertyIndexOnDisk mode). Guarded by
	// idxMu like propertyIndexes.
	relPropertyIndexes map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex

	// Composite property indexes — in-memory only (v1 has no on-disk mode,
	// unlike PropertyIndexOnDisk). Definitions persisted, data rebuilt on
	// startup. compositeIndexesByLabel is the label->definitions secondary
	// index the node-mutation maintenance seam
	// (maintainPropertyIndexesAdd/Remove/Purge) needs.
	compositeIndexes        map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex
	compositeIndexesByLabel map[uint16][]indexpkg.CompositeIndexKey

	// Temporal indexes — in-memory only. Label tokens persisted, data rebuilt on startup.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Relationship-type temporal indexes (BACKLOG 21c) — the rel-side mirror of
	// temporalIndexes, keyed by rel-type token in its own independent map (a
	// label token and a rel-type token are different registries and may
	// numerically collide). Deliberately RAM-only and NOT persisted across
	// reopen — no definitions record, no loadIndexes rebuild — unlike
	// temporalIndexes/relPropertyIndexes. Safe: PruneRelTypeTemporalCandidates
	// is a sound-superset optimization, so a reopened store simply starts
	// unaccelerated for rel-type temporal queries until CreateRelTemporal is
	// called again; no query ever returns a wrong answer as a result. See
	// CHANGELOG BACKLOG 21c for the scope rationale.
	relTypeTemporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Index-rebuild diagnostics — record count of node entries that the
	// loadIndexes pass tolerated as missing/corrupt. Surfaced via
	// IndexRebuildStats so operators can detect partially rebuilt indexes
	// instead of the previous silent skip (F9 in the maintainability review).
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
	maxPending int // async write-buffer bound; 0 disables (see Config.MaxPendingWrites)
	flushInt   time.Duration
	gcInt      time.Duration
	gcRatio    float64
	stopCh     chan struct{}
	flushDone  chan struct{}
	gcDone     chan struct{}
	closeOnce  sync.Once
	// closing is set at the start of Close(), before background goroutines are
	// stopped and before the final flush snapshots pending writes. Public
	// operations must fail closed after this point so no mutation can enqueue
	// work that misses the final flush.
	closing atomic.Bool
	// dbClosed is set to true immediately before bs.db.Close() in Close().
	// flush() checks this before calling WriteBatch.Flush() to avoid
	// blocking indefinitely — Badger v4 hangs in WaitForMark when the DB
	// is closed while a WriteBatch is in progress.
	dbClosed atomic.Bool

	// Change-log (op-log) — opt-in via Config.ChangeLog. logEnabled gates ALL
	// record production (zero overhead when off). logSeq is the monotonic LSN
	// allocator, seeded at open from LastLSNKey (or the max KeyChangeLog key),
	// advanced only under wbMu so LSN order is a total commit order. pendingLog
	// is the append-only buffer of framed records awaiting flush, guarded by
	// wbMu so a record and its entity ops snapshot together. logEnabled is
	// atomic (not a plain bool guarded by wbMu) because ChangeLogEnabled() is a
	// PUBLIC accessor called externally with no lock of its own (BACKLOG 18i) —
	// every write-path check still runs under wbMu as before, but the flag type
	// itself must be safe for the unsynchronized external read.
	logEnabled atomic.Bool
	// logConfigured records the open-time change-log intent, so EnableChangeLog
	// (recovery) only re-enables a store that was actually opened with the log —
	// it never turns the log on for a store opened without it. DisableChangeLog
	// leaves it set; EnableChangeLog restores logEnabled to it.
	logConfigured bool
	logSeq        atomic.Uint64
	pendingLog    []pendingLogRecord
	// logSeqSource, when non-nil (Config.ChangeLogSeqSource), supplies change-log
	// LSNs in place of logSeq — the tiered store's store-global allocator. When
	// set, nextLSN() draws from it and the open-time watermark is folded into it
	// via Observe (instead of stored in logSeq). nil = self-owned counter.
	logSeqSource ChangeLogSeqSource
	// onChangeLogFlush (Config.OnChangeLogFlush) persists the store-global
	// allocator watermark after a log-bearing flush commits. nil = standalone.
	onChangeLogFlush func() error

	// Per-transaction change-log scope (store.TxChangeLogScope). When a tx/batch
	// opens a scope, scopeActive diverts record production into scopeLog — the
	// records are buffered WITHOUT an LSN (logSeq is NOT advanced) and NOT placed
	// in pendingLog. On CommitLogScope they get contiguous LSNs minted at commit
	// time (so a rolled-back tx burns no LSN and leaves no gap) and co-commit with
	// the tx's data via flush(); on DiscardLogScope they are dropped (a rolled-back
	// tx emits nothing). All guarded by wbMu. scopeActive is toggled by the core
	// ONLY while it holds c.mu.Lock around a tx mutation, so a concurrent standalone
	// mutation (c.mu.RLock) can never observe it true and misroute its own record.
	scopeActive bool
	scopeLog    [][]byte // framed record values (tag‖payload), LSN assigned at commit
}

var (
	counterNodeCountKey = storepkg.MetaKey("node_count")
	counterRelCountKey  = storepkg.MetaKey("rel_count")
)

// New opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
// newNodeCache / newRelCache construct the entity caches, byte-budgeted
// when budget > 0 (sized by the entities' approximate resident heap
// footprint — see types.ApproxHeapBytes).
// cacheShards is the shard count for the entity caches, derived once from
// GOMAXPROCS so concurrent label scans (one Get per node) spread across shards
// instead of serializing on a single mutex. Floored at 16 and rounded to a
// power of two (see indexpkg.ShardHint). The TOTAL capacity/budget passed to the
// sharded constructors is split evenly across shards, so the configured soft
// limit is preserved.
func cacheShards() int {
	// TKG_CACHE_SHARDS overrides the derived shard count (1 disables
	// sharding — the pre-sharding single-mutex behaviour — for A/B
	// measurement and as a kill switch). Invalid / unset uses the
	// GOMAXPROCS-derived default.
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

	// Shrinking the EFFECTIVE MemTableSize on a data dir that still holds WAL
	// files written under a LARGER memtable bricks the open: Badger replays
	// each WAL into an arena sized by the current memtable and fails with
	// "Arena too small" (not a recoverable sentinel) before a single row is
	// read. Flush such WALs at their original size first. Gated on a writable
	// on-disk store (the read-only probe must not write — the tiered recovery
	// path migrates explicitly before its probe) — NOT on MemTableSize > 0
	// (BACKLOG 18m): reverting an explicitly-tuned dir back to MemTableSize: 0
	// ("use Badger's stock default") is itself a shrink whenever stock is
	// smaller than the previous tuning, so MigrateOversizedWAL/
	// guardReadOnlyOversizedWAL must run for that case too — they compare
	// against the resolved effective size internally and no-op when nothing
	// needs migrating (clean dirs, stock sizes with no oversized WAL, etc.).
	if !cfg.InMemory && !cfg.ReadOnly {
		if err := MigrateOversizedWAL(cfg); err != nil {
			return nil, err
		}
	}
	// A read-only open cannot flush, so it cannot migrate an oversized WAL —
	// and replaying one into the tuned arena would os.Exit (log.Fatal "Arena
	// too small"). Fail closed with a returned error instead of crashing the
	// process. (The tiered store never reaches here for a recoverable shard: it
	// migrates before its read-only probe.)
	if !cfg.InMemory && cfg.ReadOnly {
		if err := guardReadOnlyOversizedWAL(cfg); err != nil {
			return nil, err
		}
	}

	db, err := badgerv4.Open(buildBadgerOptions(cfg))
	if err != nil {
		return nil, fmt.Errorf("graph: badger open: %w", err)
	}

	// Enforce the on-disk format contract before decoding a single row: a
	// directory stamped by a newer release must fail closed here, not surface
	// as per-row decode failures (which loadIndexes would conflate with
	// corruption / counter mismatch).
	if err := verifyAndStampWireFormatVersion(db, cfg.ReadOnly); err != nil {
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

	bs := &Store{
		db:         db,
		nodeIDs:    make(map[types.NodeID]struct{}),
		nodeHashes: make(map[types.NodeID]string),
		nodeRevs:   make(map[types.NodeID]uint64),
		relIDs:     make(map[types.RelID]struct{}),
		relRevs:    make(map[types.RelID]uint64),
		labelIdx:   make(map[uint16]map[types.NodeID]struct{}),
		typeIdx:    make(map[uint16]map[types.RelID]struct{}),
		outIdx:     make(map[types.NodeID]map[types.RelID]types.NodeID),
		inIdx:      make(map[types.NodeID]map[types.RelID]inEdge),
		// relValidIdx is built LAZILY on the first temporal traversal — a graph
		// that never does temporal adjacency (or a tiered store, which does not
		// expose the capability) pays nothing for the per-rel stamps.
		nodeCache:               newNodeCache(capacity, cfg.CacheBudgetBytes),
		relCache:                newRelCache(capacity, cfg.CacheBudgetBytes),
		resident:                cfg.ResidentCache,
		pending:                 make(map[string]writeOp),
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
		disablePlannerStats:     cfg.DisablePlannerStats,
		historyDelta:            cfg.HistoryDeltaEncoding,
		historyAnchorInterval:   resolveHistoryAnchorInterval(cfg.HistoryAnchorInterval),
		readOnly:                cfg.ReadOnly,
		syncWrites:              cfg.SyncWrites && !cfg.ReadOnly,
		logConfigured:           cfg.ChangeLog && !cfg.ReadOnly,
		logSeqSource:            cfg.ChangeLogSeqSource,
		onChangeLogFlush:        cfg.OnChangeLogFlush,
		maxPending:              maxPending,
		flushInt:                flushInt,
		gcInt:                   gcInt,
		gcRatio:                 gcRatio,
		stopCh:                  make(chan struct{}),
		flushDone:               make(chan struct{}),
		gcDone:                  make(chan struct{}),
		logger:                  cfg.Logger,
	}
	// atomic.Bool cannot be set via a composite literal (unexported internal
	// field) — set it here, before any concurrent access is possible (bs is
	// still local to this constructor).
	bs.logEnabled.Store(cfg.ChangeLog && !cfg.ReadOnly)

	// Resident mode: keep every decoded node/rel resident so a cache miss never
	// re-decodes (msgpack unmarshal + wire-decode) the same entity twice — the
	// per-fetch decode is what makes graph-larger-than-cache traversal scale
	// super-linearly. GetNode/GetRel additionally fetch via GetNoPromote here.
	if bs.resident {
		bs.nodeCache.SetNoEvict()
		bs.relCache.SetNoEvict()
	}

	// Load the property-key registry BEFORE loadIndexes so the index
	// rebuild can resolve tokenized property keys when decoding stored
	// node/rel rows. Without this, rows written under tokenization would
	// fail validation during loadIndexes and silently drop from the
	// in-memory liveness map.
	// An owner-supplied registry (tiered store) is the single canonical instance
	// shared by all shards; use it directly so loadIndexes can resolve tokenized
	// property keys. Otherwise (standalone store) load this store's own meta copy.
	if cfg.PropertyKeyRegistry != nil {
		bs.propKeyReg.Store(cfg.PropertyKeyRegistry)
	} else if reg := readPropertyKeyRegistryFromMeta(bs); reg != nil {
		bs.propKeyReg.Store(reg)
	}
	// Seed the write-ahead watermark to the already-persisted length: the
	// canonical registry handed in by the tiered store is loaded from durable
	// meta, and a standalone store's own meta copy is durable too. Only growth
	// beyond this needs a new commit.
	if reg := bs.propKeyReg.Load(); reg != nil {
		bs.persistedKeyLen.Store(int64(reg.Len()))
	}

	// Write-ahead registry hook. The tiered store supplies one that commits the
	// shared registry to the reference shard; a standalone store defaults to
	// committing its own registry to its own meta. flush() invokes it before the
	// row WriteBatch, so every token a durable row references is itself durable —
	// recovery can always resolve every persisted row.
	bs.onPropertyKeyGrow = cfg.OnPropertyKeyGrow
	if bs.onPropertyKeyGrow == nil {
		bs.onPropertyKeyGrow = func() error {
			if reg := bs.propKeyReg.Load(); reg != nil {
				return bs.SavePropertyKeyRegistry(reg)
			}
			return nil
		}
	}

	if err := bs.loadIndexes(); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, fmt.Errorf("graph: load indexes: %w", err)
	}

	// loadIndexes rebuilds each temporal index from CURRENT node state only.
	// Fold every node's history versions into the per-node valid-time ENVELOPE so
	// the sound-superset property survives restart (a past interval differing from
	// the current one stays a candidate for the core resolver's temporal narrowing).
	bs.idxMu.RLock()
	temporalToks := make([]uint16, 0, len(bs.temporalIndexes))
	for tok := range bs.temporalIndexes {
		temporalToks = append(temporalToks, tok)
	}
	bs.idxMu.RUnlock()
	if len(temporalToks) > 0 {
		if err := bs.foldTemporalHistoryEnvelopes(temporalToks); err != nil {
			_ = db.Close() // best-effort cleanup
			return nil, fmt.Errorf("graph: fold temporal history envelopes: %w", err)
		}
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

// loadIndexes rebuilds in-memory indexes and counters from Badger.
// Single db.View() scan of all index key prefixes (keys-only, no values).
func (bs *Store) loadIndexes() error {
	err := bs.loadIndexesScan()
	if err != nil {
		return err
	}
	if bs.labelOnDisk {
		// Disk-resident label index: the map was built transiently above
		// because the open-path rebuilds (per-label counters, property/
		// temporal/HF/vector index backfills) walk it — drop it now for the
		// steady-state RAM win. Runtime label snapshots come from the
		// persisted keyspace (badgerstore_label_disk.go). Open-path peak
		// memory still includes the transient map; making open fully
		// streaming is the documented follow-up.
		bs.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	}
	if bs.adjOnDisk {
		// Same transient-then-drop discipline for the adjacency maps
		// (badgerstore_adjacency_disk.go) — the open-path counter rebuilds
		// walked them above; runtime snapshots come from the OutKey/InKey
		// keyspaces.
		bs.outIdx = make(map[types.NodeID]map[types.RelID]types.NodeID)
		bs.inIdx = make(map[types.NodeID]map[types.RelID]inEdge)
	}
	return nil
}

func (bs *Store) loadIndexesScan() error {
	// propIdxNeedsBuild / propIdxBackfillOps support item (d) — rebuild-on-
	// enable: the 0x0A property-index keyspace is NEW (unlike label/adjacency,
	// which have always been written transactionally), so an existing
	// directory that already has property-index DEFINITIONS needs an explicit
	// one-time backfill the first time PropertyIndexOnDisk is turned on.
	// Computed inside the View closure below (which already walks every
	// definition's current node set); committed via a real WriteBatch AFTER
	// the read-only View returns, guarded by
	// storeutil.PropertyIndexOnDiskBuiltKey so it runs exactly once.
	var propIdxNeedsBuild bool
	var propIdxBackfillOps []writeOp
	// temporalIdxNeedsBuild / temporalIdxBackfillOps mirror propIdxNeedsBuild /
	// propIdxBackfillOps above for the 0x0B temporal-index raw-entry log (also
	// a new keyspace): an existing directory with temporal-index definitions
	// but no prior 0x0B rows is backfilled from current node state exactly
	// once, the first time TemporalIndexOnDisk is turned on. Guarded by
	// storeutil.TemporalIndexOnDiskBuiltKey so it runs exactly once.
	var temporalIdxNeedsBuild bool
	var temporalIdxBackfillOps []writeOp

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
			_, merr := txn.Get(storepkg.TemporalIndexOnDiskBuiltKey)
			switch {
			case merr == nil:
				temporalIdxNeedsBuild = false
			case errors.Is(merr, badgerv4.ErrKeyNotFound):
				temporalIdxNeedsBuild = true
			default:
				return fmt.Errorf("graph: read temporal-index-on-disk marker: %w", merr)
			}
		}

		nodeEntityIDs := make(map[types.NodeID]struct{})
		decodedNodeLabels := make(map[types.NodeID]map[uint16]struct{})

		// Scan node entities. The node row is authoritative for liveness:
		// stale label index keys must not manufacture live nodeIDs after
		// restart, while missing label keys can be rebuilt from the row.
		valueOpts := opts
		valueOpts.PrefetchValues = true
		it := txn.NewIterator(valueOpts)
		prefix := []byte{storepkg.KeyNode}
		var loadErr error
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) != storepkg.SizeNodeKey {
				continue
			}
			nid := types.NodeID(storepkg.ParseIDFromKey(key, 1))
			nodeEntityIDs[nid] = struct{}{}

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
				// Corrupt rows are tolerated (skipped + counted) so one
				// damaged value cannot brick the store. A FUTURE format
				// version is not damage — it is a newer release's data and
				// silently dropping it would be data loss masquerading as a
				// clean open. Fail closed instead (break first: the iterator
				// must be closed before returning from the View closure).
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
			bs.nodeIDs[nid] = struct{}{}
			bs.nodeHashes[nid] = badgerNodeIntegrityHash(n)
			bs.bumpNodeRevLocked(nid)
			labels := bs.addNodeIndexesFromRow(nid, collectNodeLabelTokens(n))
			bs.addNodePropertyKeyCounts(n)
			decodedNodeLabels[nid] = labels
		}
		it.Close()
		if loadErr != nil {
			return loadErr
		}

		// Scan label index: keyLabel(1B) + token(2B) + nodeID(8B).
		// Only rows with a local node entity key may populate labelIdx. If
		// the entity row decoded cleanly, ignore labels that disagree with
		// the canonical node labels.
		it = txn.NewIterator(opts)
		prefix = []byte{storepkg.KeyLabel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeLabelIdx {
				continue
			}
			token := binary.BigEndian.Uint16(key[1:3])
			nid := types.NodeID(storepkg.ParseIDFromKey(key, 3))
			if _, exists := nodeEntityIDs[nid]; !exists {
				continue
			}
			if labels, decoded := decodedNodeLabels[nid]; decoded {
				if _, ok := labels[token]; !ok {
					continue
				}
			}
			if bs.labelIdx[token] == nil {
				bs.labelIdx[token] = make(map[types.NodeID]struct{})
			}
			bs.labelIdx[token][nid] = struct{}{}
		}
		it.Close()

		relEntityIDs := make(map[types.RelID]struct{})
		decodedRelInfo := make(map[types.RelID]RelDeleteInfo)

		// Scan relationship entities. The entity row is authoritative for
		// relationship liveness: reltype/outgoing index keys can be stale after
		// interrupted repair/delete paths and must not manufacture live relIDs.
		relValueOpts := opts
		relValueOpts.PrefetchValues = true
		it = txn.NewIterator(relValueOpts)
		prefix = []byte{storepkg.KeyRel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.Key()
			if len(key) != storepkg.SizeRelKey {
				continue
			}
			rid := types.RelID(storepkg.ParseIDFromKey(key, 1))
			relEntityIDs[rid] = struct{}{}

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
			bs.relIDs[rid] = struct{}{}
			bs.bumpRelRevLocked(rid) // seed a non-zero rev so a pre-first-write prefetch doesn't fall back needlessly (mirrors nodeRevs seeding above)
			bs.addRelPropertyTypeClassCounts(r) // rebuild rel type-class counters + contrib (BACKLOG 5B)
			bs.addRelPropertyStatsCounts(r)     // rebuild rel NDV+min/max counters + contrib (BACKLOG 21a)
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

		// Scan reltype index: keyRelType(1B) + token(2B) + relID(8B).
		// Only rows with a local entity key may populate the type index.
		// If the entity row decoded cleanly, ignore keys that disagree with
		// the canonical row type.
		it = txn.NewIterator(opts)
		prefix = []byte{storepkg.KeyRelType}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeRelTypeIdx {
				continue
			}
			token := binary.BigEndian.Uint16(key[1:3])
			rid := types.RelID(storepkg.ParseIDFromKey(key, 3))
			info, decoded := decodedRelInfo[rid]
			if !decoded {
				continue
			}
			if info.RelType != token {
				continue
			}
			if bs.typeIdx[token] == nil {
				bs.typeIdx[token] = make(map[types.RelID]struct{})
			}
			bs.typeIdx[token][rid] = struct{}{}
		}
		it.Close()

		// Scan outgoing adjacency: keyOut(1B) + startID(8B) + relType(2B) + endID(8B) + relID(8B).
		// Outgoing entries belong on the relationship entity shard. Ignore
		// outgoing keys without a local relationship entity.
		it = txn.NewIterator(opts)
		prefix = []byte{storepkg.KeyOut}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeAdjacency {
				continue
			}
			startID := storepkg.ParseIDFromKey(key, 1)
			relType := binary.BigEndian.Uint16(key[9:11])
			endID := storepkg.ParseIDFromKey(key, 11)
			relID := types.RelID(storepkg.ParseRelIDFromAdjKey(key))
			info, decoded := decodedRelInfo[relID]
			if !decoded {
				continue
			}
			if info.StartID != startID || info.EndID != endID || info.RelType != relType {
				continue
			}
			if _, startLocal := bs.nodeIDs[types.NodeID(startID)]; !startLocal {
				continue
			}
			startNID := types.NodeID(startID)
			if bs.outIdx[startNID] == nil {
				bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
			}
			bs.outIdx[startNID][relID] = types.NodeID(endID)
		}
		it.Close()

		// Scan incoming adjacency: keyIn(1B) + endID(8B) + relType(2B) + startID(8B) + relID(8B).
		// Incoming-only rows are valid in TieredStore cross-shard layouts, so
		// preserve keys with no local relationship entity. If there is a local
		// decoded relationship row, only accept the canonical same-shard entry.
		it = txn.NewIterator(opts)
		prefix = []byte{storepkg.KeyIn}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) != storepkg.SizeAdjacency {
				continue
			}
			endID := storepkg.ParseIDFromKey(key, 1)
			relType := binary.BigEndian.Uint16(key[9:])
			startID := storepkg.ParseIDFromKey(key, 11)
			relID := types.RelID(storepkg.ParseRelIDFromAdjKey(key))
			if info, decoded := decodedRelInfo[relID]; decoded {
				if info.StartID != startID || info.EndID != endID || info.RelType != relType {
					continue
				}
				if _, endLocal := bs.nodeIDs[types.NodeID(endID)]; !endLocal {
					continue
				}
			} else if _, hasLocalEntity := relEntityIDs[relID]; hasLocalEntity {
				continue
			}
			endNID := types.NodeID(endID)
			if bs.inIdx[endNID] == nil {
				bs.inIdx[endNID] = make(map[types.RelID]inEdge)
			}
			bs.inIdx[endNID][relID] = inEdge{start: types.NodeID(startID), typ: relType}
		}
		it.Close()

		// Load counters from meta keys, or count from live row maps if missing.
		nodeCount, err := getCounter(txn, counterNodeCountKey)
		if err != nil {
			return err
		}
		liveNodeCount := int64(len(bs.nodeIDs))
		nodeCount, err = reconcilePersistedCounter("node", nodeCount, liveNodeCount, int64(len(nodeEntityIDs)), bs.logger)
		if err != nil {
			return err
		}
		bs.nodeCount.Store(nodeCount)

		relCount, err := getCounter(txn, counterRelCountKey)
		if err != nil {
			return err
		}
		liveRelCount := int64(len(bs.relIDs))
		relCount, err = reconcilePersistedCounter("relationship", relCount, liveRelCount, int64(len(relEntityIDs)), bs.logger)
		if err != nil {
			return err
		}
		bs.relCount.Store(relCount)

		// Seed the change-log LSN allocator so new LSNs continue strictly
		// monotonic across restart and no LSN is ever reissued. LastLSNKey
		// commits in the same WriteBatch as the records it covers, so it is
		// crash-consistent with the maximum KeyChangeLog key; fall back to
		// scanning the max key when the marker is absent (defensive — a fresh
		// store has neither and seeds 0).
		lastLSN, err := seedLogLSN(txn)
		if err != nil {
			return err
		}
		if bs.logSeqSource != nil {
			// Tiered: fold this shard's durable watermark into the store-global
			// allocator (belt-and-braces above the refShard catalog watermark),
			// so the shared sequence resumes strictly above every shard's max —
			// even a cold shard that lazily opens read-only later.
			bs.logSeqSource.Observe(lastLSN)
		} else {
			bs.logSeq.Store(lastLSN)
		}

		// Rebuild per-label and per-type counters from index sizes.
		for token, set := range bs.labelIdx {
			bs.getOrCreateLabelCounter(token).Store(int64(len(set)))
		}
		for token, set := range bs.typeIdx {
			bs.getOrCreateTypeCounter(token).Store(int64(len(set)))
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
				key := indexpkg.PropertyIndexKey{LabelToken: def.LabelToken, PropertyKey: def.PropertyKey}
				if _, exists := seenProperty[key]; exists {
					continue
				}
				seenProperty[key] = struct{}{}
				idx := indexpkg.NewPropertyIndex()
				if nodeIDs, ok := bs.labelIdx[def.LabelToken]; ok {
					for nodeID := range nodeIDs {
						rawID := nodeID.SnowflakeID()
						n, nerr := bs.loadNodeFromBadger(txn, rawID)
						if nerr != nil {
							// Tolerate missing/corrupt during rebuild,
							// but record + warn (F9). Operators can
							// inspect via IndexRebuildStats() and trigger
							// an explicit repair pass if the count is
							// nonzero.
							bs.indexRebuildPropertySkips.Add(1)
							if bs.logger != nil {
								bs.logger.Warningf("graph: property-index rebuild skipped node %d (label %d, property %q): %v", rawID, def.LabelToken, def.PropertyKey, nerr)
							}
							continue
						}
						valueKey, found := n.IndexablePropertyValueKey(def.PropertyKey)
						if !found {
							continue
						}
						if bs.propIdxOnDisk {
							// Disk mode: do NOT populate idx.Entries/numBuckets
							// (the whole point is to keep entries off the RAM
							// heap) — collect the one-time backfill op instead,
							// only when this directory hasn't been backfilled
							// before (propIdxNeedsBuild).
							if propIdxNeedsBuild {
								if op, ok := bs.propertyIndexDiskOp(def.PropertyKey, valueKey, rawID, writeOpSet); ok {
									propIdxBackfillOps = append(propIdxBackfillOps, op)
								}
							}
							continue
						}
						idx.AddKey(rawID, valueKey)
					}
				}
				bs.propertyIndexes[key] = idx
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no indexes defined yet.

		// Load relationship property index definitions and rebuild RAM value maps.
		// RAM-only: no on-disk value keyspace, so the maps are rebuilt from
		// current relationship state by scanning each type's members. Mirrors the
		// non-disk node property index rebuild above.
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
				idx := indexpkg.NewPropertyIndex()
				if relIDs, ok := bs.typeIdx[def.RelTypeToken]; ok {
					for relID := range relIDs {
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
						if valueKey, found := r.IndexablePropertyValueKey(def.PropertyKey); found {
							idx.AddKey(rawID, valueKey)
						}
					}
				}
				bs.relPropertyIndexes[key] = idx
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
				if nodeIDs, ok := bs.labelIdx[def.LabelToken]; ok {
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
							idx.AddKey(rawID, vk)
						}
					}
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
				if bs.temporalIdxOnDisk && !temporalIdxNeedsBuild {
					// Fast path: stream (from, to, id) triples directly from the
					// compact 0x0B keyspace — no per-node full-row fetch/decode.
					// Iteration order over one label's sub-keyspace is already
					// (From ASC, ID ASC) by key construction (see
					// storeutil.TemporalIndexEntryKey), matching
					// TemporalIndex.Entries' required order exactly — this is
					// the O(N) full-node-fetch rebuild this eliminates.
					valueOpts := opts
					valueOpts.PrefetchValues = true
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
				} else if nodeIDs, ok := bs.labelIdx[tok]; ok {
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
						from, to := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
						ti.AddKnownAbsent(rawID, from, to)
						if bs.temporalIdxOnDisk && temporalIdxNeedsBuild {
							// Rebuild-on-enable: collect the one-time backfill row
							// for the NEW 0x0B keyspace (committed after the View
							// returns — see commitTemporalIndexOnDiskBackfill).
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
				if nodeIDs, ok := bs.labelIdx[def.LabelToken]; ok {
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
						from, _ := indexpkg.NodeTemporalBounds(rawID, n.Temporal())
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
				if nodeIDs, ok := bs.labelIdx[def.LabelToken]; ok {
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

	if bs.propIdxOnDisk && propIdxNeedsBuild {
		if bs.propKeyReg.Load() == nil {
			// No property-key registry wired yet (a direct badger.Store user
			// without Config.PropertyKeyRegistry and no meta-persisted
			// registry) — skip the backfill AND leave the marker unset so a
			// later open (once the registry is available) retries. A graph
			// opened via pkg/graph always wires a registry before this runs.
		} else if err := bs.commitPropertyIndexOnDiskBackfill(propIdxBackfillOps); err != nil {
			return err
		}
	}
	if bs.temporalIdxOnDisk && temporalIdxNeedsBuild {
		if err := bs.commitTemporalIndexOnDiskBackfill(temporalIdxBackfillOps); err != nil {
			return err
		}
	}
	return nil
}

// reconcilePersistedCounter resolves a persisted entity counter against the rows
// actually present after an index rebuild. Entity rows are authoritative:
//
//   - persisted == 0: counter never written (pre-counter store) — trust rows.
//   - persisted == liveRows: consistent.
//   - persisted == rawEntityRows && rawEntityRows > liveRows: counter matches the
//     raw row count but some rows are soft-deleted / undecodable — trust liveRows.
//   - persisted < liveRows && liveRows == rawEntityRows: every entity row decoded
//     cleanly and is current, yet the counter undercounts. No data is missing —
//     the counter lost increments in an unclean shutdown. Heal UP to liveRows
//     (warn so the operator knows a crash was recovered).
//
// Any other case — notably persisted > liveRows, where the counter claims more
// rows than exist (rows genuinely missing → data loss) — stays fatal: it surfaces
// real corruption instead of silently masking it.
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
		if bs.labelIdx[tok] == nil {
			bs.labelIdx[tok] = make(map[types.NodeID]struct{})
		}
		bs.labelIdx[tok][nid] = struct{}{}
	}
	return labels
}

func (bs *Store) addRelationshipIndexesFromRow(info RelDeleteInfo) {
	rid := types.RelID(info.ID)
	relType := info.RelType

	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[types.RelID]struct{})
	}
	bs.typeIdx[relType][rid] = struct{}{}

	if _, startLocal := bs.nodeIDs[types.NodeID(info.StartID)]; startLocal {
		startNID := types.NodeID(info.StartID)
		if bs.outIdx[startNID] == nil {
			bs.outIdx[startNID] = make(map[types.RelID]types.NodeID)
		}
		bs.outIdx[startNID][rid] = types.NodeID(info.EndID)
	}

	if _, endLocal := bs.nodeIDs[types.NodeID(info.EndID)]; endLocal {
		endNID := types.NodeID(info.EndID)
		if bs.inIdx[endNID] == nil {
			bs.inIdx[endNID] = make(map[types.RelID]inEdge)
		}
		bs.inIdx[endNID][rid] = inEdge{start: types.NodeID(info.StartID), typ: relType}
	}
}

// Clear removes all entities, indexes, history, counters, and secondary
// indexes. After Clear(), the Store is in the same state as a freshly
// opened store. Registries are a Graph-layer concern — not cleared here.
//
// flushMu is acquired first to drain any in-flight async flush. Without this
// barrier, a flush goroutine that has already snapshotted dirty cache
// versions, pending ops, and counter values (under idxMu.RLock) but has not
// yet submitted its WriteBatch could race ahead of DropAll() and resurrect
// pre-Clear entities into Badger after the namespace is wiped — silent
// post-restart data corruption. Holding flushMu for the duration of Clear
// also blocks any new flush() from snapshotting state we're about to reset.
//
// Cost note: in SyncWrites mode every concurrent mutation
// blocks while Clear runs DropAll, which can be slow on large stores.
// This is an intentional accepted trade-off — Clear is rare and admin-
// scoped, and the alternative (release flushMu before DropAll) does not
// actually reduce the blocking window because concurrent flush() also
// needs idxMu.RLock which Clear holds via idxMu.Lock for the same
// duration. Releasing flushMu would only save a sync.Mutex acquisition
// that is already serialised behind idxMu anyway.
func (bs *Store) Clear() error {
	if err := bs.checkWritable(); err != nil {
		return err
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// Clear in-memory indexes.
	bs.nodeIDs = make(map[types.NodeID]struct{})
	bs.nodeHashes = make(map[types.NodeID]string)
	bs.nodeRevs = make(map[types.NodeID]uint64)
	bs.nextNodeRev = 0
	bs.nodeEpoch.Add(1)     // invalidate cached columns built before Clear
	bs.nodeEpochSalt.Add(1) // label-less event: invalidate every per-label column too (BACKLOG 4b)
	bs.relEpoch.Add(1)      // and the adjacency view (expand path)
	bs.docMu.Lock()
	bs.docColumns = nil
	bs.docColumnsMulti = nil
	bs.docMu.Unlock()
	bs.relIDs = make(map[types.RelID]struct{})
	bs.relRevs = make(map[types.RelID]uint64)
	bs.nextRelRev = 0
	bs.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	bs.typeIdx = make(map[uint16]map[types.RelID]struct{})
	bs.outIdx = make(map[types.NodeID]map[types.RelID]types.NodeID)
	bs.inIdx = make(map[types.NodeID]map[types.RelID]inEdge)
	bs.relValidIdx = nil // drop the lazy stamp index; rebuilt on next temporal traversal
	bs.relValidIdxBuilt.Store(false)
	bs.labelTxMembers = nil // drop the lazy membership sidecar; rebuilt on next pinned scan
	bs.labelTxMembersBuilt.Store(false)
	bs.relTypeTxMembers = nil // rel-type mirror
	bs.relTypeMembersBuilt.Store(false)

	// Reset atomic counters. Clear sync.Map contents via Range+Delete
	// rather than struct reassignment: concurrent readers
	// at NodeCountByLabel / RelCountByType call labelCounts.Load /
	// typeCounts.Load WITHOUT holding idxMu, and replacing the
	// sync.Map struct value while a reader is mid-Load races on the
	// field itself. Deleting individual keys is safe because sync.Map
	// is concurrency-safe by contract.
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
	bs.relTypeClassContrib.Range(func(k, _ any) bool {
		bs.relTypeClassContrib.Delete(k)
		return true
	})
	bs.propertyStats = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	bs.relPropertyKeyCounts.Range(func(k, _ any) bool {
		bs.relPropertyKeyCounts.Delete(k)
		return true
	})
	bs.relStatsContrib.Range(func(k, _ any) bool {
		bs.relStatsContrib.Delete(k)
		return true
	})
	bs.relPropertyStats = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator)

	// Re-create LRU caches with same capacity and byte budget.
	cap := bs.nodeCache.Cap()
	bs.nodeCache = newNodeCache(cap, bs.nodeCache.Budget())
	bs.relCache = newRelCache(cap, bs.relCache.Budget())

	// Clear pending buffer. (flushMu serializes us against flush(), so any
	// snapshot that happens after this point sees an empty buffer.) Drop any
	// buffered change-log records too — DropAll below wipes the KeyChangeLog
	// keyspace, so a stale record left in pendingLog would be re-flushed as a
	// phantom after the wipe. logSeq is intentionally left monotonic so
	// post-Clear LSNs never collide with a consumer's pre-Clear watermark.
	bs.wbMu.Lock()
	bs.pending = make(map[string]writeOp)
	bs.pendingLog = nil
	// Drop any snapshot a just-completed flush parked (the success path clears it,
	// but a leaked/in-flight snapshot must not survive the wipe below — otherwise
	// rangePending would resurface pre-Clear history keys as phantom IDs).
	bs.flushing = nil
	bs.wbMu.Unlock()

	// Clear all secondary index maps. Leaving these populated leaks
	// pre-Clear state into the post-Clear store: CreateTemporalIndex /
	// CreateHighFrequencyIndex / CreateVectorIndex would return "already
	// exists" on a logically empty store, and stale vector entries would
	// occupy top-k slots in SearchNearestNodes results.
	bs.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	bs.relPropertyIndexes = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyIndex)
	bs.compositeIndexes = make(map[indexpkg.CompositeIndexKey]*indexpkg.CompositePropertyIndex)
	bs.compositeIndexesByLabel = make(map[uint16][]indexpkg.CompositeIndexKey)
	bs.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	bs.relTypeTemporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	bs.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	bs.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)

	// When the change-log is enabled, wipe via DropPrefix while keeping
	// LastLSNKey continuously durable (clearAndReanchorChangeLog), so a crash
	// mid-Clear cannot reseed the LSN allocator to 0 and collide with a tailing
	// consumer's pre-Clear watermark (lesson 53).
	if bs.logEnabled.Load() {
		return bs.clearAndReanchorChangeLog()
	}
	// Change-log disabled now — but a PRIOR change-log-enabled session may have
	// left a durable LastLSNKey. A bare DropAll would wipe it, and a FUTURE
	// change-log-enabled reopen would then reseed the LSN allocator to 0 and
	// REUSE LSNs a consumer already checkpointed past (the same silent-divergence
	// hole lesson 53 closes, reached through the log-disabled Clear door). So
	// preserve LastLSNKey whenever it is present; only a never-logged store
	// (no watermark to protect) takes the single atomic DropAll.
	hasLSN, err := bs.lastLSNKeyPresent()
	if err != nil {
		return err
	}
	if hasLSN {
		return bs.clearDataPreservingLastLSN()
	}
	return bs.db.DropAll()
}

// Close stops background goroutines, performs a final flush (including counters),
// and closes the Badger database. Safe to call multiple times.
//
// The explicit flush() handles the case where flushLoop was never started
// (InMemory mode, FlushInterval==0). If flushLoop already drained pending,
// this is a no-op. Counters are included in the WriteBatch atomically.
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
		if e := bs.flush(); e != nil {
			err = e
		}
		// Mark DB as closed BEFORE db.Close() so any concurrent flush()
		// calls (e.g., from tests that close the DB directly) return an error
		// instead of blocking in Badger's WaitForMark.
		bs.dbClosed.Store(true)
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
	return nil
}

// isClosingOrClosed is the shared "silently return zero" guard for stats
// accessors whose signature (plain value, no error return) can't propagate
// checkOpen()'s error — a nil *Store is handled by the caller's own `bs ==
// nil` check, since a nil receiver can't have this method called on it via a
// non-nil-checked call chain in the first place (BACKLOG 18q: deduplicates
// the identical `bs.closing.Load() || bs.dbClosed.Load()` inline check that
// was repeated at 5 call sites).
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

// IndexRebuildStats reports the number of node entries that the loadIndexes
// pass (called once at Open) tolerated as missing or corrupt while rebuilding
// the property, temporal, high-frequency, and vector in-memory indexes. A nonzero count after a fresh
// Open means the persisted indexes are degraded — operators should run an
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

// SetPropertyKeyRegistry installs the property-key registry on the Store.
// Once set, subsequent writes dictionary-encode property keys via the
// registry; reads resolve tokens back to key strings. Safe to call before
// any writes; effective from the next marshal/unmarshal onward. Pass nil
// to disable tokenization (preserves the pre-tokenization wire format).
func (bs *Store) SetPropertyKeyRegistry(reg *registrypkg.PropertyKeyRegistry) {
	if bs == nil {
		return
	}
	bs.propKeyReg.Store(reg)
	// Re-seed the write-ahead watermark to the new registry's length. Core.New
	// installs the canonical registry here after loading it from durable meta
	// (or freshly empty), so its current length is already persisted — only
	// later growth needs a commit.
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
// currently-installed property-key registry (nil-safe).
func (bs *Store) marshalNodeBytes(n *types.Node) ([]byte, error) {
	return storepkg.MarshalNodeWireWithKeys(n, bs.propKeyReg.Load())
}

// marshalRelBytes encodes a relationship via MarshalRelWireWithKeys.
func (bs *Store) marshalRelBytes(r *types.Relationship) ([]byte, error) {
	return storepkg.MarshalRelWireWithKeys(r, bs.propKeyReg.Load())
}

// persistRegistryIfGrew write-ahead commits the property-key registry to durable
// storage whenever it has grown since the last commit. Called from flush()
// BEFORE the row WriteBatch is written, so every batch of rows is preceded by a
// durable registry covering all of their tokens — a crash can never leave a
// tokenized row durable while its token is absent from the registry (which would
// make the row undecodable on reload → counter mismatch → fatal).
//
// Growth is measured against bs.persistedKeyLen (a watermark), NOT against any
// per-marshal diff: the registry is SHARED with the core engine, which allocates
// tokens during property validation — strictly before the store marshals — so a
// before/after diff at marshal time is always zero. The watermark captures "has
// any token been allocated since we last committed", regardless of which layer
// allocated it.
//
// Persisting here (once per ~100ms flush cycle) rather than per row keeps the
// fsync cost at O(flush cycles), not O(keys) — critical for high-cardinality
// key workloads. A failure is propagated so the caller aborts the flush and
// requeues the rows rather than committing rows ahead of their registry.
func (bs *Store) persistRegistryIfGrew() error {
	reg := bs.propKeyReg.Load()
	if reg == nil || bs.onPropertyKeyGrow == nil {
		return nil
	}
	// Skip during shutdown. The closing flush (flushLoop on stopCh + the explicit
	// final flush) runs with closing=true, when SavePropertyKeyRegistry's
	// checkWritable rejects writes. Close-time durability is already guaranteed:
	// Core.Close persists all registries (core.go) BEFORE store.Close, so every
	// row in the closing flush has its tokens durable. Re-persisting here would
	// only fail with ErrStoreClosed.
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
	// The hook exported names AFTER we read cur, so it persisted at least cur
	// keys. Advancing the watermark to cur (never beyond what we verified) keeps
	// it a safe lower bound — an under-count only risks a redundant future
	// commit, never a skipped one.
	bs.persistedKeyLen.Store(cur)
	return nil
}

// resolveNodeWireKeys is a no-op when no registry is installed; otherwise
// it walks the wire's property slice and replaces any tokenized keys with
// their resolved strings. Call after msgpack.Unmarshal of NodeWire.
func (bs *Store) resolveNodeWireKeys(w *storepkg.NodeWire) error {
	return storepkg.ResolvePropertyKeyTokens(w.Properties, bs.propKeyReg.Load())
}

func (bs *Store) resolveRelWireKeys(w *storepkg.RelWire) error {
	return storepkg.ResolvePropertyKeyTokens(w.Properties, bs.propKeyReg.Load())
}
