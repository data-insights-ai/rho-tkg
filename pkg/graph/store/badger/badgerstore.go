// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/vmihailenco/msgpack/v5"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storecontract "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	// Compression sets the SSTable compression algorithm.
	// Valid values: options.None (0), options.Snappy (1), options.ZSTD (2).
	// Zero keeps the Badger default (Snappy).
	Compression options.CompressionType
	// ZSTDCompressionLevel sets the ZSTD compression level (1-15).
	// Only effective when Compression is options.ZSTD.
	// Zero keeps the Badger default (1).
	ZSTDCompressionLevel int
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
	idxMu       sync.RWMutex
	nodeIDs     map[types.NodeID]struct{} // O(1) node existence check
	nodeHashes  map[types.NodeID]string   // current node integrity hash for live endpoint validation
	nodeRevs    map[types.NodeID]uint64   // live node row revision for safe prefetch handoff
	nextNodeRev uint64
	relIDs      map[types.RelID]struct{}                  // O(1) rel existence check
	labelIdx    map[uint16]map[types.NodeID]struct{}      // labelToken → set(nodeID)
	typeIdx     map[uint16]map[types.RelID]struct{}       // relTypeToken → set(relID)
	outIdx      map[types.NodeID]map[types.RelID]struct{} // startNodeID → set(relID)
	inIdx       map[types.NodeID]map[types.RelID]uint16   // endNodeID → relID → typeToken

	// Entity caches (internal sync via entityLRU mutex).
	nodeCache *indexpkg.Cache[*types.Node]
	relCache  *indexpkg.Cache[*types.Relationship]

	// Counters (atomic — persisted atomically via flush WriteBatch).
	nodeCount atomic.Int64
	relCount  atomic.Int64

	// Write buffer (own mutex, swapped on flush).
	// Map keyed by string(op.key) for last-write-wins deduplication.
	wbMu    sync.Mutex
	pending map[string]writeOp

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

	// Property indexes — in-memory only. Definitions persisted, data rebuilt on startup.
	propertyIndexes map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex

	// Temporal indexes — in-memory only. Label tokens persisted, data rebuilt on startup.
	temporalIndexes map[uint16]*indexpkg.TemporalIndex

	// Index-rebuild diagnostics — record count of node entries that the
	// loadIndexes pass tolerated as missing/corrupt. Surfaced via
	// IndexRebuildStats so operators can detect partially rebuilt indexes
	// instead of the previous silent skip (F9 in the maintainability review).
	indexRebuildPropertySkips atomic.Int64
	indexRebuildTemporalSkips atomic.Int64
	indexRebuildHFSkips       atomic.Int64
	indexRebuildVectorSkips   atomic.Int64

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
}

var (
	counterNodeCountKey = storepkg.MetaKey("node_count")
	counterRelCountKey  = storepkg.MetaKey("rel_count")
)

// New opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
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

	opts := badgerv4.DefaultOptions(cfg.Dir)
	if cfg.InMemory {
		opts = opts.WithInMemory(true)
	}
	if cfg.ReadOnly {
		opts = opts.WithReadOnly(true)
	}
	if cfg.Logger != nil {
		opts = opts.WithLogger(cfg.Logger)
	} else {
		opts = opts.WithLogger(nil) // suppress default Badger logs
	}
	if cfg.SyncWrites && !cfg.ReadOnly {
		opts = opts.WithSyncWrites(true)
	}
	if cfg.Compression != 0 {
		opts = opts.WithCompression(cfg.Compression)
	}
	if cfg.ZSTDCompressionLevel > 0 {
		opts = opts.WithZSTDCompressionLevel(cfg.ZSTDCompressionLevel)
	}

	db, err := badgerv4.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("graph: badger open: %w", err)
	}

	capacity := cfg.CacheCapacity
	if capacity <= 0 {
		capacity = DefaultCacheCapacity
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = DefaultFlushInterval
	}
	if cfg.SyncWrites && !cfg.ReadOnly {
		flushInt = 0 // disable periodic flush; each write flushes synchronously
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
		db:              db,
		nodeIDs:         make(map[types.NodeID]struct{}),
		nodeHashes:      make(map[types.NodeID]string),
		nodeRevs:        make(map[types.NodeID]uint64),
		relIDs:          make(map[types.RelID]struct{}),
		labelIdx:        make(map[uint16]map[types.NodeID]struct{}),
		typeIdx:         make(map[uint16]map[types.RelID]struct{}),
		outIdx:          make(map[types.NodeID]map[types.RelID]struct{}),
		inIdx:           make(map[types.NodeID]map[types.RelID]uint16),
		nodeCache:       indexpkg.NewCache[*types.Node](capacity),
		relCache:        indexpkg.NewCache[*types.Relationship](capacity),
		pending:         make(map[string]writeOp),
		propertyIndexes: make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex),
		temporalIndexes: make(map[uint16]*indexpkg.TemporalIndex),
		hfIndexes:       make(map[uint16]*indexpkg.HighFrequencyIndex),
		vectorIndexes:   make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex),
		inMemory:        cfg.InMemory,
		readOnly:        cfg.ReadOnly,
		syncWrites:      cfg.SyncWrites && !cfg.ReadOnly,
		flushInt:        flushInt,
		gcInt:           gcInt,
		gcRatio:         gcRatio,
		stopCh:          make(chan struct{}),
		flushDone:       make(chan struct{}),
		gcDone:          make(chan struct{}),
		logger:          cfg.Logger,
	}

	if err := bs.loadIndexes(); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, fmt.Errorf("graph: load indexes: %w", err)
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
	return bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false

		nodeEntityIDs := make(map[types.NodeID]struct{})
		decodedNodeLabels := make(map[types.NodeID]map[uint16]struct{})

		// Scan node entities. The node row is authoritative for liveness:
		// stale label index keys must not manufacture live nodeIDs after
		// restart, while missing label keys can be rebuilt from the row.
		valueOpts := opts
		valueOpts.PrefetchValues = true
		it := txn.NewIterator(valueOpts)
		prefix := []byte{storepkg.KeyNode}
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
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal node: %w", err)
				}
				decoded, err := decodeNodeWireForKey(w, nid.SnowflakeID())
				if err != nil {
					return fmt.Errorf("graph: decode node: %w", err)
				}
				n = decoded
				return nil
			}); err != nil {
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
			decodedNodeLabels[nid] = labels
		}
		it.Close()

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
				if err := msgpack.Unmarshal(val, &w); err != nil {
					return fmt.Errorf("graph: unmarshal relationship: %w", err)
				}
				decoded, err := decodeRelWireForKey(w, rid.SnowflakeID())
				if err != nil {
					return fmt.Errorf("graph: decode relationship: %w", err)
				}
				r = decoded
				return nil
			}); err != nil {
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
			info := relDeleteInfoFromRelationship(r)
			decodedRelInfo[rid] = info
			bs.addRelationshipIndexesFromRow(info)
		}
		it.Close()

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
				bs.outIdx[startNID] = make(map[types.RelID]struct{})
			}
			bs.outIdx[startNID][relID] = struct{}{}
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
				bs.inIdx[endNID] = make(map[types.RelID]uint16)
			}
			bs.inIdx[endNID][relID] = relType
		}
		it.Close()

		// Load counters from meta keys, or count from live row maps if missing.
		nodeCount, err := getCounter(txn, counterNodeCountKey)
		if err != nil {
			return err
		}
		liveNodeCount := int64(len(bs.nodeIDs))
		nodeCount, err = reconcilePersistedCounter("node", nodeCount, liveNodeCount, int64(len(nodeEntityIDs)))
		if err != nil {
			return err
		}
		bs.nodeCount.Store(nodeCount)

		relCount, err := getCounter(txn, counterRelCountKey)
		if err != nil {
			return err
		}
		liveRelCount := int64(len(bs.relIDs))
		relCount, err = reconcilePersistedCounter("relationship", relCount, liveRelCount, int64(len(relEntityIDs)))
		if err != nil {
			return err
		}
		bs.relCount.Store(relCount)

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
				return msgpack.Unmarshal(val, &defs)
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
						if valueKey, found := n.IndexablePropertyValueKey(def.PropertyKey); found {
							idx.AddKey(rawID, valueKey)
						}
					}
				}
				bs.propertyIndexes[key] = idx
			}
		}
		// badgerv4.ErrKeyNotFound is OK — no indexes defined yet.

		// Load temporal index label tokens and rebuild index data.
		item, err = txn.Get(storepkg.TemporalIndexDefsKey)
		if err == nil {
			var tokens []uint16
			if e := item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &tokens)
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
				if nodeIDs, ok := bs.labelIdx[tok]; ok {
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
				return msgpack.Unmarshal(val, &defs)
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
				return msgpack.Unmarshal(val, &defs)
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
}

func reconcilePersistedCounter(name string, persisted, liveRows, rawEntityRows int64) (int64, error) {
	if persisted == 0 {
		return liveRows, nil
	}
	if persisted == liveRows {
		return liveRows, nil
	}
	if persisted == rawEntityRows && rawEntityRows > liveRows {
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
			bs.outIdx[startNID] = make(map[types.RelID]struct{})
		}
		bs.outIdx[startNID][rid] = struct{}{}
	}

	if _, endLocal := bs.nodeIDs[types.NodeID(info.EndID)]; endLocal {
		endNID := types.NodeID(info.EndID)
		if bs.inIdx[endNID] == nil {
			bs.inIdx[endNID] = make(map[types.RelID]uint16)
		}
		bs.inIdx[endNID][rid] = relType
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
// Cost note (review M3): in SyncWrites mode every concurrent mutation
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
	bs.relIDs = make(map[types.RelID]struct{})
	bs.labelIdx = make(map[uint16]map[types.NodeID]struct{})
	bs.typeIdx = make(map[uint16]map[types.RelID]struct{})
	bs.outIdx = make(map[types.NodeID]map[types.RelID]struct{})
	bs.inIdx = make(map[types.NodeID]map[types.RelID]uint16)

	// Reset atomic counters. Clear sync.Map contents via Range+Delete
	// rather than struct reassignment (review L1): concurrent readers
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

	// Re-create LRU caches with same capacity.
	cap := bs.nodeCache.Cap()
	bs.nodeCache = indexpkg.NewCache[*types.Node](cap)
	bs.relCache = indexpkg.NewCache[*types.Relationship](cap)

	// Clear pending buffer. (flushMu serializes us against flush(), so any
	// snapshot that happens after this point sees an empty buffer.)
	bs.wbMu.Lock()
	bs.pending = make(map[string]writeOp)
	bs.wbMu.Unlock()

	// Clear all secondary index maps. Leaving these populated leaks
	// pre-Clear state into the post-Clear store: CreateTemporalIndex /
	// CreateHighFrequencyIndex / CreateVectorIndex would return "already
	// exists" on a logically empty store, and stale vector entries would
	// occupy top-k slots in SearchNearestNodes results.
	bs.propertyIndexes = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyIndex)
	bs.temporalIndexes = make(map[uint16]*indexpkg.TemporalIndex)
	bs.hfIndexes = make(map[uint16]*indexpkg.HighFrequencyIndex)
	bs.vectorIndexes = make(map[indexpkg.VectorIndexKey]*indexpkg.VectorIndex)

	// Drop all data from Badger — atomically removes all KV pairs.
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
	PropertySkipped int64
	TemporalSkipped int64
	HFSkipped       int64
	VectorSkipped   int64
}

// IndexRebuildStats returns the diagnostic counters captured during the last
// loadIndexes pass. Zero means a clean rebuild.
func (bs *Store) IndexRebuildStats() IndexRebuildStats {
	if bs == nil || bs.closing.Load() || bs.dbClosed.Load() {
		return IndexRebuildStats{}
	}
	return IndexRebuildStats{
		PropertySkipped: bs.indexRebuildPropertySkips.Load(),
		TemporalSkipped: bs.indexRebuildTemporalSkips.Load(),
		HFSkipped:       bs.indexRebuildHFSkips.Load(),
		VectorSkipped:   bs.indexRebuildVectorSkips.Load(),
	}
}
