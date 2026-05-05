package graph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// Default configuration values for BadgerStore.
const (
	defaultCacheCapacity  = 10_000
	defaultFlushInterval  = 100 * time.Millisecond
	defaultGCInterval     = 5 * time.Minute
	defaultGCDiscardRatio = 0.5
)

// BadgerStoreConfig configures a BadgerStore instance.
type BadgerStoreConfig struct {
	// Dir is the Badger data directory. Required unless InMemory is true.
	Dir string
	// InMemory enables memory-only mode (no disk I/O). Useful for testing.
	InMemory bool
	// Logger is the Badger logger. Nil uses Badger's default logger.
	Logger badger.Logger
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

// BadgerStore implements the Store interface using Badger as the durable backing store.
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
type BadgerStore struct {
	db *badger.DB

	// In-memory indexes (source of truth while running).
	// Protected by idxMu for concurrent read/write access.
	idxMu    sync.RWMutex
	nodeIDs  map[snowflake.ID]struct{}                  // O(1) node existence check
	relIDs   map[snowflake.ID]struct{}                  // O(1) rel existence check
	labelIdx map[uint16]map[snowflake.ID]struct{}       // labelToken → set(nodeID)
	typeIdx  map[uint16]map[snowflake.ID]struct{}       // relTypeToken → set(relID)
	outIdx   map[snowflake.ID]map[snowflake.ID]struct{} // startNodeID → set(relID)
	inIdx    map[snowflake.ID]map[snowflake.ID]uint16   // endNodeID → relID → typeToken

	// Entity caches (internal sync via entityLRU mutex).
	nodeCache *entityLRU[*types.Node]
	relCache  *entityLRU[*types.Relationship]

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
	propertyIndexes map[propertyIndexKey]*propertyIndex

	// Temporal indexes — in-memory only. Label tokens persisted, data rebuilt on startup.
	temporalIndexes map[uint16]*temporalIndex

	// High-frequency indexes — in-memory only. Not persisted; rebuilt via CreateHighFrequencyIndex after restart.
	hfIndexes map[uint16]*highFrequencyIndex

	// Vector indexes — in-memory only. Not persisted; rebuilt via CreateVectorIndex after restart.
	vectorIndexes map[vectorIndexKey]*vectorIndex

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
	// dbClosed is set to true immediately before bs.db.Close() in Close().
	// flush() checks this before calling WriteBatch.Flush() to avoid
	// blocking indefinitely — Badger v4 hangs in WaitForMark when the DB
	// is closed while a WriteBatch is in progress.
	dbClosed atomic.Bool
}

var (
	counterNodeCountKey = metaKey("node_count")
	counterRelCountKey  = metaKey("rel_count")
)

// NewBadgerStore opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
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

	opts := badger.DefaultOptions(cfg.Dir)
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

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("graph: badger open: %w", err)
	}

	capacity := cfg.CacheCapacity
	if capacity <= 0 {
		capacity = defaultCacheCapacity
	}
	flushInt := cfg.FlushInterval
	if flushInt == 0 {
		flushInt = defaultFlushInterval
	}
	if cfg.SyncWrites && !cfg.ReadOnly {
		flushInt = 0 // disable periodic flush; each write flushes synchronously
	}
	gcInt := cfg.GCInterval
	if gcInt == 0 && !cfg.InMemory {
		gcInt = defaultGCInterval
	}
	gcRatio := cfg.GCDiscardRatio
	if gcRatio == 0 {
		gcRatio = defaultGCDiscardRatio
	}

	bs := &BadgerStore{
		db:              db,
		nodeIDs:         make(map[snowflake.ID]struct{}),
		relIDs:          make(map[snowflake.ID]struct{}),
		labelIdx:        make(map[uint16]map[snowflake.ID]struct{}),
		typeIdx:         make(map[uint16]map[snowflake.ID]struct{}),
		outIdx:          make(map[snowflake.ID]map[snowflake.ID]struct{}),
		inIdx:           make(map[snowflake.ID]map[snowflake.ID]uint16),
		nodeCache:       newEntityLRU[*types.Node](capacity),
		relCache:        newEntityLRU[*types.Relationship](capacity),
		pending:         make(map[string]writeOp),
		propertyIndexes: make(map[propertyIndexKey]*propertyIndex),
		temporalIndexes: make(map[uint16]*temporalIndex),
		hfIndexes:       make(map[uint16]*highFrequencyIndex),
		vectorIndexes:   make(map[vectorIndexKey]*vectorIndex),
		inMemory:        cfg.InMemory,
		readOnly:        cfg.ReadOnly,
		syncWrites:      cfg.SyncWrites && !cfg.ReadOnly,
		flushInt:        flushInt,
		gcInt:           gcInt,
		gcRatio:         gcRatio,
		stopCh:          make(chan struct{}),
		flushDone:       make(chan struct{}),
		gcDone:          make(chan struct{}),
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
func (bs *BadgerStore) loadIndexes() error {
	return bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		// Scan label index: keyLabel(1B) + token(2B) + nodeID(8B)
		it := txn.NewIterator(opts)
		prefix := []byte{keyLabel}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < sizeLabelIdx {
				continue
			}
			token := binary.BigEndian.Uint16(key[1:3])
			nid := parseIDFromKey(key, 3)
			bs.nodeIDs[nid] = struct{}{}
			if bs.labelIdx[token] == nil {
				bs.labelIdx[token] = make(map[snowflake.ID]struct{})
			}
			bs.labelIdx[token][nid] = struct{}{}
		}
		it.Close()

		// Also scan node entities to catch nodes without label indexes
		// (shouldn't happen, but defensive).
		it = txn.NewIterator(opts)
		prefix = []byte{keyNode}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < sizeNodeKey {
				continue
			}
			nid := parseIDFromKey(key, 1)
			bs.nodeIDs[nid] = struct{}{}
		}
		it.Close()

		// Scan reltype index: keyRelType(1B) + token(2B) + relID(8B)
		it = txn.NewIterator(opts)
		prefix = []byte{keyRelType}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < sizeRelTypeIdx {
				continue
			}
			token := binary.BigEndian.Uint16(key[1:3])
			rid := parseIDFromKey(key, 3)
			bs.relIDs[rid] = struct{}{}
			if bs.typeIdx[token] == nil {
				bs.typeIdx[token] = make(map[snowflake.ID]struct{})
			}
			bs.typeIdx[token][rid] = struct{}{}
		}
		it.Close()

		// Scan outgoing adjacency: keyOut(1B) + startID(8B) + relType(2B) + endID(8B) + relID(8B)
		it = txn.NewIterator(opts)
		prefix = []byte{keyOut}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < sizeAdjacency {
				continue
			}
			startID := parseIDFromKey(key, 1)
			relID := parseRelIDFromAdjKey(key)
			if bs.outIdx[startID] == nil {
				bs.outIdx[startID] = make(map[snowflake.ID]struct{})
			}
			bs.outIdx[startID][relID] = struct{}{}
		}
		it.Close()

		// Scan incoming adjacency: keyIn(1B) + endID(8B) + relType(2B) + startID(8B) + relID(8B)
		it = txn.NewIterator(opts)
		prefix = []byte{keyIn}
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			if len(key) < sizeAdjacency {
				continue
			}
			endID := parseIDFromKey(key, 1)
			relType := binary.BigEndian.Uint16(key[9:])
			relID := parseRelIDFromAdjKey(key)
			if bs.inIdx[endID] == nil {
				bs.inIdx[endID] = make(map[snowflake.ID]uint16)
			}
			bs.inIdx[endID][relID] = relType
		}
		it.Close()

		// Load counters from meta keys, or count from indexes if missing.
		nodeCount, err := getCounter(txn, counterNodeCountKey)
		if err != nil {
			return err
		}
		if nodeCount == 0 && len(bs.nodeIDs) > 0 {
			nodeCount = int64(len(bs.nodeIDs))
		}
		bs.nodeCount.Store(nodeCount)

		relCount, err := getCounter(txn, counterRelCountKey)
		if err != nil {
			return err
		}
		if relCount == 0 && len(bs.relIDs) > 0 {
			relCount = int64(len(bs.relIDs))
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
		item, err := txn.Get(propIndexDefsKey)
		if err == nil {
			var defs []propIdxDef
			if e := item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &defs)
			}); e == nil {
				for _, def := range defs {
					key := propertyIndexKey{labelToken: def.LabelToken, propertyKey: def.PropertyKey}
					idx := newPropertyIndex()
					if nodeIDs, ok := bs.labelIdx[def.LabelToken]; ok {
						for nodeID := range nodeIDs {
							n, nerr := bs.loadNodeFromBadger(txn, nodeID)
							if nerr != nil {
								continue // tolerate missing/corrupt during rebuild
							}
							if val, found := n.GetProperty(def.PropertyKey); found {
								idx.add(nodeID, val)
							}
						}
					}
					bs.propertyIndexes[key] = idx
				}
			}
		}
		// badger.ErrKeyNotFound is OK — no indexes defined yet.

		// Load temporal index label tokens and rebuild index data.
		item, err = txn.Get(temporalIndexDefsKey)
		if err == nil {
			var tokens []uint16
			if e := item.Value(func(val []byte) error {
				return msgpack.Unmarshal(val, &tokens)
			}); e == nil {
				for _, tok := range tokens {
					ti := newTemporalIndex()
					if nodeIDs, ok := bs.labelIdx[tok]; ok {
						for nodeID := range nodeIDs {
							n, nerr := bs.loadNodeFromBadger(txn, nodeID)
							if nerr != nil {
								continue // tolerate missing/corrupt during rebuild
							}
							from, to := nodeTemporalBounds(nodeID, n.Temporal())
							ti.add(nodeID, from, to)
						}
					}
					bs.temporalIndexes[tok] = ti
				}
			}
		}
		// badger.ErrKeyNotFound is OK — no temporal indexes defined yet.

		return nil
	})
}

// getCounter reads a big-endian int64 counter from the given key within txn.
// Returns 0 if the key does not exist.
func getCounter(txn *badger.Txn, key []byte) (int64, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var val int64
	err = item.Value(func(v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("graph: counter value size %d, want 8", len(v))
		}
		val = int64(binary.BigEndian.Uint64(v)) // #nosec G115 — inverse of counter encoding
		return nil
	})
	return val, err
}

// --- Write buffer operations ---

// appendOps adds write operations to the pending buffer.
// Last-write-wins: if the same key is written multiple times, only the
// latest operation is retained.
func (bs *BadgerStore) appendOps(ops ...writeOp) {
	bs.wbMu.Lock()
	for _, op := range ops {
		bs.pending[string(op.key)] = op
	}
	bs.wbMu.Unlock()
}

// --- Node operations ---

// PutNode stores a node with its label index entries.
// Updates in-memory state immediately; Badger write is queued for async flush.
// Returns ErrNodeExists if a node with the same ID already exists.
func (bs *BadgerStore) PutNode(n *types.Node) error {
	id := n.InternalID().SnowflakeID()

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	bs.idxMu.Lock()

	// Check for duplicate.
	if _, exists := bs.nodeIDs[id]; exists {
		bs.idxMu.Unlock()
		return ErrNodeExists
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, n.DeepCopy())
	bs.nodeIDs[id] = struct{}{}

	// Build write ops.
	ops := []writeOp{{opType: writeOpSet, key: nodeKey(id), value: data}}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if bs.labelIdx[tv] == nil {
			bs.labelIdx[tv] = make(map[snowflake.ID]struct{})
		}
		bs.labelIdx[tv][id] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpSet, key: labelIndexKey(tv, id)})
		bs.getOrCreateLabelCounter(tv).Add(1)
	}

	addNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	addNodeToVectorIndexes(bs.vectorIndexes, n, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Cache-first: checks LRU cache, then nodeIDs (O(1) existence check),
// then falls through to Badger only if the node is confirmed to exist.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) GetNode(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	// Check cache first.
	v, status := bs.nodeCache.Get(id)
	switch status {
	case cacheHit:
		return v.DeepCopy(), nil
	case cacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Short-circuit: nodeIDs is the authoritative set of all node IDs.
	// Avoids opening a Badger transaction for non-existent nodes.
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[id]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Cache miss, node exists — read from Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w nodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = wireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean (evictable).
	bs.nodeCache.LoadClean(id, n)
	return n.DeepCopy(), nil
}

// DeleteNode removes a node and its label index entries.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNode(nid types.NodeID) error {
	id := nid.SnowflakeID()

	// Pre-fetch node state before acquiring the write lock to avoid holding
	// idxMu.Lock() during Badger disk I/O on cache misses (B3: lock scope rule).
	// prefetchNode checks the cache and falls through to db.View without any lock.
	n, err := bs.prefetchNode(nid)
	if err != nil {
		return err
	}

	bs.idxMu.Lock()

	// TOCTOU guard: re-verify existence after acquiring write lock.
	// A concurrent delete may have removed the node between prefetchNode and here.
	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Build delete ops using pre-fetched node (labels needed for index cleanup).
	ops := []writeOp{{opType: writeOpDelete, key: nodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	removeNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
	removeNodeFromTemporalIndexes(bs.temporalIndexes, n, id)
	removeNodeFromVectorIndexes(bs.vectorIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// ReplaceNode overwrites an existing node's data in-place.
// Returns ErrNodeNotFound if the node does not exist.
// No label index changes — labels are immutable after creation.
// Property indexes are updated to reflect property changes.
func (bs *BadgerStore) ReplaceNode(n *types.Node) error {
	id := n.InternalID().SnowflakeID()

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(n.InternalID())

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes: remove old entries, add new.
	if old != nil {
		removeNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		removeNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		removeNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		// Pre-fetch failed (concurrent delete between prefetch and write lock, or
		// cache miss on a just-opened store) — brute-force purge to avoid orphans.
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		purgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}
	bs.nodeCache.Put(id, n.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, n, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, n, id)
	addNodeToVectorIndexes(bs.vectorIndexes, n, id)
	bs.appendOps(writeOp{opType: writeOpSet, key: nodeKey(id), value: data})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// RemoveNodeLabelToken removes tok from the label index for id and persists updatedNode.
// updatedNode must already have the label removed (via RemoveLabelTokenRaw) and have its
// version bumped. Version history must be written by the caller before this call.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) RemoveNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	id := nid.SnowflakeID()
	w := nodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Pre-fetch old state before the write lock to avoid Badger I/O under idxMu.Lock().
	// Errors here are non-fatal: the write lock path falls back to brute-force purge.
	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes using pre-fetched old node state.
	if old != nil {
		removeNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		removeNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		removeNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		purgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	// Remove tok from the in-memory label index.
	if set, ok := bs.labelIdx[tok]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.labelIdx, tok)
		}
	}
	bs.getOrCreateLabelCounter(tok).Add(-1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	addNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	// Queue: set node data + delete label index entry.
	bs.appendOps(
		writeOp{opType: writeOpSet, key: nodeKey(id), value: data},
		writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// RemoveNodeLabelTokenWithHistory atomically removes tok from the label index,
// writes a version history entry, and persists updatedNode via a single appendOps call.
func (bs *BadgerStore) RemoveNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	id := nid.SnowflakeID()
	w := nodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	hw := nodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}

	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes using pre-fetched old node state.
	if old != nil {
		removeNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		removeNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		removeNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		purgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	// Remove tok from the in-memory label index.
	if set, ok := bs.labelIdx[tok]; ok {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.labelIdx, tok)
		}
	}
	bs.getOrCreateLabelCounter(tok).Add(-1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	addNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	// Single appendOps call — node data + history + label index delete — atomic in the pending buffer.
	histKey := histNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: nodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// AddNodeLabelToken adds tok to the label index for id and persists updatedNode.
// No version bump; no history entry. Used by transaction rollback.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) AddNodeLabelToken(nid types.NodeID, tok uint16, updatedNode *types.Node) error {
	id := nid.SnowflakeID()
	w := nodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	if old != nil {
		removeNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		removeNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		removeNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		purgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[snowflake.ID]struct{})
		bs.labelIdx[tok] = set
	}
	set[id] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	addNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	bs.appendOps(
		writeOp{opType: writeOpSet, key: nodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: labelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// AddNodeLabelTokenWithHistory atomically adds tok to the label index,
// writes a version history entry, and persists updatedNode via a single appendOps call.
func (bs *BadgerStore) AddNodeLabelTokenWithHistory(nid types.NodeID, tok uint16, updatedNode *types.Node,
	prevVersion uint32, prevState *types.Node) error {
	id := nid.SnowflakeID()
	w := nodeToWire(updatedNode)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	hw := nodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}

	old, _ := bs.prefetchNode(nid)

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property, temporal, and vector indexes using pre-fetched old node state.
	if old != nil {
		removeNodeFromPropertyIndexes(bs.propertyIndexes, old, id)
		removeNodeFromTemporalIndexes(bs.temporalIndexes, old, id)
		removeNodeFromVectorIndexes(bs.vectorIndexes, old, id)
	} else {
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)
		purgeNodeFromAllVectorIndexes(bs.vectorIndexes, id)
	}

	// Add tok to the in-memory label index.
	set, ok := bs.labelIdx[tok]
	if !ok {
		set = make(map[snowflake.ID]struct{})
		bs.labelIdx[tok] = set
	}
	set[id] = struct{}{}
	bs.getOrCreateLabelCounter(tok).Add(1)

	// Update cache and property/temporal/vector indexes for the new node state.
	bs.nodeCache.Put(id, updatedNode.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, updatedNode, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, updatedNode, id)
	addNodeToVectorIndexes(bs.vectorIndexes, updatedNode, id)

	// Single appendOps call — node data + history + label index set — atomic in the pending buffer.
	histKey := histNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: nodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
		writeOp{opType: writeOpSet, key: labelIndexKey(tok, id)},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// --- Relationship operations ---

// PutRelationship stores a relationship with type index and adjacency entries.
// Returns ErrNodeNotFound if the start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (bs *BadgerStore) PutRelationship(r *types.Relationship) error {
	id := r.InternalID().SnowflakeID()
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	// Verify endpoints exist.
	if _, exists := bs.nodeIDs[startID]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}
	if _, exists := bs.nodeIDs[endID]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Check for duplicate via O(1) relIDs.
	if _, exists := bs.relIDs[id]; exists {
		bs.idxMu.Unlock()
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, r.DeepCopy())
	bs.relIDs[id] = struct{}{}

	// Type index.
	if bs.typeIdx[relType] == nil {
		bs.typeIdx[relType] = make(map[snowflake.ID]struct{})
	}
	bs.typeIdx[relType][id] = struct{}{}

	// Adjacency indexes.
	if bs.outIdx[startID] == nil {
		bs.outIdx[startID] = make(map[snowflake.ID]struct{})
	}
	bs.outIdx[startID][id] = struct{}{}
	if bs.inIdx[endID] == nil {
		bs.inIdx[endID] = make(map[snowflake.ID]uint16)
	}
	bs.inIdx[endID][id] = relType

	// Build write ops.
	ops := []writeOp{
		{opType: writeOpSet, key: relKey(id), value: data},
		{opType: writeOpSet, key: relTypeIndexKey(relType, id)},
		{opType: writeOpSet, key: outKey(startID, relType, endID, id)},
		{opType: writeOpSet, key: inKey(endID, relType, startID, id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	bs.getOrCreateTypeCounter(relType).Add(1)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Cache-first: checks LRU cache before falling through to Badger.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	// Check cache first.
	v, status := bs.relCache.Get(id)
	switch status {
	case cacheHit:
		return v.DeepCopy(), nil
	case cacheDeleted:
		return nil, ErrRelNotFound
	}

	// Short-circuit: relIDs is the authoritative set of all relationship IDs.
	// Avoids opening a Badger transaction for non-existent relationships.
	bs.idxMu.RLock()
	_, exists := bs.relIDs[id]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrRelNotFound
	}

	// Cache miss, rel exists — read from Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(relKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w relWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r = wireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	// Populate cache as clean.
	bs.relCache.LoadClean(id, r)
	return r.DeepCopy(), nil
}

// ReplaceNodeWithHistory atomically replaces a node and writes a version history entry.
// Both operations are queued in a single appendOps call — the flush loop cannot
// snapshot one without the other.
func (bs *BadgerStore) ReplaceNodeWithHistory(current *types.Node, prevVersion uint32, prevState *types.Node) error {
	id := current.InternalID().SnowflakeID()

	// Serialize current state.
	w := nodeToWire(current)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	// Serialize history snapshot.
	hw := nodeToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.nodeIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrNodeNotFound
	}

	// Update property and temporal indexes: remove old entries based on prevState, add new from current.
	removeNodeFromPropertyIndexes(bs.propertyIndexes, prevState, id)
	removeNodeFromTemporalIndexes(bs.temporalIndexes, prevState, id)
	bs.nodeCache.Put(id, current.DeepCopy())
	addNodeToPropertyIndexes(bs.propertyIndexes, current, id)
	addNodeToTemporalIndexes(bs.temporalIndexes, current, id)

	// Single appendOps call — atomic in the pending buffer.
	histKey := histNodeKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: nodeKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// ReplaceRelWithHistory atomically replaces a relationship and writes a version history entry.
// Both operations are queued in a single appendOps call.
func (bs *BadgerStore) ReplaceRelWithHistory(current *types.Relationship, prevVersion uint32, prevState *types.Relationship) error {
	id := current.InternalID().SnowflakeID()

	// Serialize current state.
	w := relToWire(current)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	// Serialize history snapshot.
	hw := relToWire(prevState)
	histData, err := msgpack.Marshal(hw)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}

	bs.relCache.Put(id, current.DeepCopy())

	// Single appendOps call — atomic in the pending buffer.
	histKey := histRelKey(id, uint64(prevVersion))
	bs.appendOps(
		writeOp{opType: writeOpSet, key: relKey(id), value: data},
		writeOp{opType: writeOpSet, key: histKey, value: histData},
	)
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// ReplaceRelationship overwrites an existing relationship's data in-place.
// Returns ErrRelNotFound if the relationship does not exist.
// No index changes — type and endpoints are immutable after creation.
func (bs *BadgerStore) ReplaceRelationship(r *types.Relationship) error {
	id := r.InternalID().SnowflakeID()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()

	if _, exists := bs.relIDs[id]; !exists {
		bs.idxMu.Unlock()
		return ErrRelNotFound
	}

	bs.relCache.Put(id, r.DeepCopy())
	bs.appendOps(writeOp{opType: writeOpSet, key: relKey(id), value: data})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) DeleteRelationship(rid types.RelID) error {
	id := rid.SnowflakeID()
	bs.idxMu.Lock()
	err := bs.deleteRelLocked(id)
	bs.idxMu.Unlock()

	if err != nil {
		return err
	}
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// relDeleteInfo holds pre-read relationship metadata for two-phase cascade delete.
type relDeleteInfo struct {
	id      snowflake.ID
	relType uint16
	startID snowflake.ID
	endID   snowflake.ID
}

// deleteRelLocked removes a relationship and cleans up indexes.
// Caller must hold bs.idxMu write lock.
func (bs *BadgerStore) deleteRelLocked(id snowflake.ID) error {
	// Read phase.
	r, err := bs.getRelLocked(types.RelID(id))
	if err != nil {
		return err
	}

	// Mutation phase.
	bs.deleteRelByInfo(relDeleteInfo{
		id:      id,
		relType: r.TypeToken().Value(),
		startID: r.StartNodeID().SnowflakeID(),
		endID:   r.EndNodeID().SnowflakeID(),
	})

	return nil
}

// deleteRelByInfo applies relationship deletion mutations using pre-read metadata.
// Caller must hold bs.idxMu write lock. This method performs no reads — it cannot fail.
func (bs *BadgerStore) deleteRelByInfo(info relDeleteInfo) {
	// Update in-memory state.
	bs.relCache.MarkDeleted(info.id)
	delete(bs.relIDs, info.id)

	// Type index cleanup.
	if set, exists := bs.typeIdx[info.relType]; exists {
		delete(set, info.id)
		if len(set) == 0 {
			delete(bs.typeIdx, info.relType)
		}
	}

	// Adjacency cleanup.
	if set, exists := bs.outIdx[info.startID]; exists {
		delete(set, info.id)
		if len(set) == 0 {
			delete(bs.outIdx, info.startID)
		}
	}
	if set, exists := bs.inIdx[info.endID]; exists {
		delete(set, info.id)
		if len(set) == 0 {
			delete(bs.inIdx, info.endID)
		}
	}

	// Build delete ops.
	ops := []writeOp{
		{opType: writeOpDelete, key: relKey(info.id)},
		{opType: writeOpDelete, key: relTypeIndexKey(info.relType, info.id)},
		{opType: writeOpDelete, key: outKey(info.startID, info.relType, info.endID, info.id)},
		{opType: writeOpDelete, key: inKey(info.endID, info.relType, info.startID, info.id)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	bs.getOrCreateTypeCounter(info.relType).Add(-1)
}

// --- Index queries ---

// NodesByLabel returns nodes with the given label token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
// Uses the temporal index fast path when available and a temporal filter is set.
func (bs *BadgerStore) NodesByLabel(token uint16, opts QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()

	// Temporal index fast path: avoids iterating the full label set when a
	// temporal index exists and a temporal filter is active.
	// When a temporal query is requested, the index result is always authoritative
	// — nil means 0 matches, not "index not consulted." Do not fall through to
	// the full label scan in that case.
	if ti, ok := bs.temporalIndexes[token]; ok {
		var ids []snowflake.ID
		temporalQuery := false
		if opts.ValidAt != 0 {
			ids = ti.queryAt(opts.ValidAt)
			temporalQuery = true
		} else if opts.ValidStart > 0 && opts.ValidEnd > 0 {
			ids = ti.queryOverlap(opts.ValidStart, opts.ValidEnd)
			temporalQuery = true
		}
		if temporalQuery {
			bs.idxMu.RUnlock()
			ids = paginateIDs(ids, opts.After, opts.Limit)
			if len(ids) == 0 {
				return nil, nil
			}
			return bs.fetchNodesWithTemporalFilter(ids, opts)
		}
	}

	set := bs.labelIdx[token]
	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter via Peek (zero allocation for cache hits).
	ids = bs.filterNodeIDsByTemporalPeek(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(ids, opts)
}

// RelationshipsByType returns relationships with the given type token, with optional pagination
// and temporal filtering. Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) RelationshipsByType(token uint16, opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.typeIdx[token]
	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter via Peek.
	ids = bs.filterRelIDsByTemporalPeek(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(ids, opts)
}

// --- Adjacency queries ---

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) OutgoingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	nodeID := nid.SnowflakeID()
	bs.idxMu.RLock()
	set := bs.outIdx[nodeID]
	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(types.RelID(id))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", id, err)
		}
		if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
			rels = append(rels, r)
		}
	}

	sortRelsByID(rels)
	return rels, nil
}

// OutgoingRelationshipsForNodes returns outgoing relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots all relIDs under one idxMu.RLock;
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *BadgerStore) OutgoingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}
	nodeIDs := make([]snowflake.ID, len(typedNodeIDs))
	for i, n := range typedNodeIDs {
		nodeIDs[i] = n.SnowflakeID()
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	bs.idxMu.RLock()
	perNode := make(map[snowflake.ID][]snowflake.ID, len(nodeIDs))
	for _, nid := range nodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.outIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]snowflake.ID, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		perNode[nid] = ids
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, type-filter, group by source node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(types.RelID(rid))
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid, err)
			}
			if typeToken == 0 || r.HasTypeTokenRaw(typeToken) {
				rels = append(rels, r)
			}
		}
		if len(rels) > 0 {
			sortRelsByID(rels)
			result[types.NodeID(nid)] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) IncomingRelationships(nid types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	nodeID := nid.SnowflakeID()
	bs.idxMu.RLock()
	set := bs.inIdx[nodeID]
	ids := make([]snowflake.ID, 0, len(set))
	for id, tok := range set {
		if typeToken == 0 || tok == typeToken {
			ids = append(ids, id)
		}
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(types.RelID(id))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", id, err)
		}
		rels = append(rels, r)
	}

	sortRelsByID(rels)
	return rels, nil
}

// IncomingRelationshipsForNodes returns incoming relationships for multiple nodes
// in a single batched operation. Phase 1 snapshots relIDs from inIdx under one
// idxMu.RLock (with early type filtering since inIdx stores typeToken);
// phase 2 fetches entities outside the lock via the LRU cache.
func (bs *BadgerStore) IncomingRelationshipsForNodes(typedNodeIDs []types.NodeID, typeToken uint16) (map[types.NodeID][]*types.Relationship, error) {
	if len(typedNodeIDs) == 0 {
		return nil, nil
	}
	nodeIDs := make([]snowflake.ID, len(typedNodeIDs))
	for i, n := range typedNodeIDs {
		nodeIDs[i] = n.SnowflakeID()
	}

	// Phase 1: snapshot relIDs per node under single read lock.
	// inIdx stores relID -> typeToken, enabling early type filtering.
	bs.idxMu.RLock()
	perNode := make(map[snowflake.ID][]snowflake.ID, len(nodeIDs))
	for _, nid := range nodeIDs {
		if _, done := perNode[nid]; done {
			continue // deduplicate input
		}
		set := bs.inIdx[nid]
		if len(set) == 0 {
			continue
		}
		ids := make([]snowflake.ID, 0, len(set))
		for id, tok := range set {
			if typeToken == 0 || tok == typeToken {
				ids = append(ids, id)
			}
		}
		if len(ids) > 0 {
			perNode[nid] = ids
		}
	}
	bs.idxMu.RUnlock()

	if len(perNode) == 0 {
		return nil, nil
	}

	// Phase 2: fetch entities, group by target node.
	result := make(map[types.NodeID][]*types.Relationship, len(perNode))
	for nid, relIDs := range perNode {
		rels := make([]*types.Relationship, 0, len(relIDs))
		for _, rid := range relIDs {
			r, err := bs.GetRelationship(types.RelID(rid))
			if err != nil {
				if errors.Is(err, ErrRelNotFound) {
					continue // index orphan
				}
				return nil, fmt.Errorf("graph: query relationship %d: %w", rid, err)
			}
			rels = append(rels, r)
		}
		if len(rels) > 0 {
			sortRelsByID(rels)
			result[types.NodeID(nid)] = rels
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// --- Bulk queries ---

// AllNodes returns all stored nodes, with optional pagination and temporal filtering.
// Snapshot nodeIDs under lock, sort + paginate, then fetch via GetNode.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) AllNodes(opts QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()
	ids := make([]snowflake.ID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter via Peek.
	ids = bs.filterNodeIDsByTemporalPeek(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	return bs.fetchNodesWithTemporalFilter(ids, opts)
}

// AllRelationships returns all stored relationships, with optional pagination
// and temporal filtering. Snapshot relIDs under lock, sort + paginate, then
// fetch via GetRelationship. Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) AllRelationships(opts QueryOpts) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	ids := make([]snowflake.ID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	// Temporal pre-filter via Peek.
	ids = bs.filterRelIDsByTemporalPeek(ids, opts)

	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}

	return bs.fetchRelsWithTemporalFilter(ids, opts)
}

// GetNodesByIDs returns nodes matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (bs *BadgerStore) GetNodesByIDs(ids []types.NodeID) ([]*types.Node, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: get nodes by IDs %d: %w", id, err)
		}
		nodes = append(nodes, n)
	}

	if len(nodes) == 0 {
		return nil, nil
	}
	sortNodesByID(nodes)
	return nodes, nil
}

// GetRelationshipsByIDs returns relationships matching the given IDs.
// Missing IDs are silently skipped. Results are sorted by snowflake.ID.
func (bs *BadgerStore) GetRelationshipsByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: get relationships by IDs %d: %w", id, err)
		}
		rels = append(rels, r)
	}

	if len(rels) == 0 {
		return nil, nil
	}
	sortRelsByID(rels)
	return rels, nil
}

// --- Cascade operations ---

// DeleteNodeCascade atomically removes a node and all connected relationships.
// Phases 1+2 (preflight + in-memory mutations) run under idxMu write lock.
// Version history is preserved — temporal queries can still reconstruct past state.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNodeCascade(nid types.NodeID) error {
	_, corruptErr, err := bs.cascadeDeleteLocked(nid)
	if err != nil {
		return err
	}
	if corruptErr == nil && bs.syncWrites {
		return bs.flush()
	}
	return corruptErr
}

// cascadeDeleteInner performs Phases 1+2 of DeleteNodeCascade.
// Caller MUST hold bs.idxMu.Lock(). All ops are appended to pending under the same lock
// so that the caller can append additional ops (e.g. tombstone history) before releasing.
// Returns (toDelete, corruptErr, fatalErr):
//   - fatalErr != nil: aborted with no mutations applied.
//   - corruptErr != nil: cleanup completed but node data was unreadable (indexes brute-force purged).
//   - Otherwise: clean success.
func (bs *BadgerStore) cascadeDeleteInner(nid types.NodeID) ([]relDeleteInfo, error, error) {
	id := nid.SnowflakeID()
	if _, exists := bs.nodeIDs[id]; !exists {
		return nil, nil, ErrNodeNotFound
	}

	// Collect all connected relIDs (dedup self-loops).
	relIDs := make(map[snowflake.ID]struct{})
	for relID := range bs.outIdx[id] {
		relIDs[relID] = struct{}{}
	}
	for relID := range bs.inIdx[id] {
		relIDs[relID] = struct{}{}
	}

	// Phase 1 — Preflight: read all relationship metadata before any mutations.
	// If any read fails (corruption), we abort without partial state changes.
	toDelete := make([]relDeleteInfo, 0, len(relIDs))
	for relID := range relIDs {
		r, err := bs.getRelLocked(types.RelID(relID))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // tolerate already-deleted rels
			}
			return nil, nil, fmt.Errorf("graph: cascade read relationship: %w", err)
		}
		toDelete = append(toDelete, relDeleteInfo{
			id:      relID,
			relType: r.TypeToken().Value(),
			startID: r.StartNodeID().SnowflakeID(),
			endID:   r.EndNodeID().SnowflakeID(),
		})
	}

	// Phase 2 — Apply: all mutations use pre-read data, no reads, cannot fail.
	for _, info := range toDelete {
		bs.deleteRelByInfo(info)
	}

	// Get node data for label cleanup.
	n, err := bs.getNodeLocked(nid)
	if err != nil {
		// Node was in nodeIDs but can't be loaded (data corruption or cache miss
		// with closed DB). Still proceed with cleanup — scrub labelIdx by scanning
		// ALL label sets to prevent orphaned index entries (perma-leak).
		// O(L) where L is total distinct labels — bounded, corruption-only path.
		ops := []writeOp{{opType: writeOpDelete, key: nodeKey(id)}}
		for tok, set := range bs.labelIdx {
			if _, exists := set[id]; exists {
				delete(set, id)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
				ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)})
				bs.getOrCreateLabelCounter(tok).Add(-1)
			}
		}
		// Property and temporal indexes: node data unavailable, brute-force purge.
		purgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		purgeNodeFromAllTemporalIndexes(bs.temporalIndexes, id)

		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, id)
		bs.appendOps(ops...)
		bs.nodeCount.Add(-1)
		return toDelete, fmt.Errorf("graph: cascade completed with corrupt node data: %w", err), nil
	}

	// Build delete ops for node.
	ops := []writeOp{{opType: writeOpDelete, key: nodeKey(id)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
		bs.getOrCreateLabelCounter(tok).Add(-1)
	}

	removeNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
	removeNodeFromTemporalIndexes(bs.temporalIndexes, n, id)

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)

	return toDelete, nil, nil
}

// cascadeDeleteLocked acquires idxMu.Lock() and delegates to cascadeDeleteInner.
// Used by DeleteNodeCascade — same contract as before the refactor.
func (bs *BadgerStore) cascadeDeleteLocked(nid types.NodeID) ([]relDeleteInfo, error, error) {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	return bs.cascadeDeleteInner(nid)
}

// DeleteRelWithHistory atomically writes a relationship tombstone history entry
// and deletes the live relationship in one batch flush.
//
// Serializes tombstone data outside the lock (B3), then holds idxMu.Lock() across
// both the live delete and the tombstone history append so both ops land in the
// same pending map before the next flush. Atomic within this shard.
func (bs *BadgerStore) DeleteRelWithHistory(rid types.RelID, prevVersion uint32, tombstone *types.Relationship) error {
	id := rid.SnowflakeID()
	// Serialize tombstone OUTSIDE lock (B3: no I/O under write lock).
	w := relToWire(tombstone)
	tombData, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal rel tombstone: %w", err)
	}
	histKey := histRelKey(id, uint64(prevVersion))

	bs.idxMu.Lock()
	r, err := bs.getRelLocked(rid)
	if err != nil {
		bs.idxMu.Unlock()
		return err
	}
	info := relDeleteInfo{
		id:      id,
		relType: r.TypeToken().Value(),
		startID: r.StartNodeID().SnowflakeID(),
		endID:   r.EndNodeID().SnowflakeID(),
	}
	bs.deleteRelByInfo(info) // appends delete ops to pending under lock
	bs.appendOps(writeOp{opType: writeOpSet, key: histKey, value: tombData})
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteNodeWithHistory atomically combines PutRelVersion×N + PutNodeVersion +
// DeleteNodeCascade into a single batch flush.
//
// Serializes all tombstone data outside the lock (B3), then holds idxMu.Lock()
// across cascadeDeleteInner AND the tombstone history appends so all ops land in
// the same pending map. The background flush goroutine acquires idxMu.RLock() for
// its snapshot phase, so it is blocked until we release — guaranteeing all ops
// commit atomically.
//
// Cross-shard atomicity: per-shard only (same B7 limitation as DeleteNodeCascade).
func (bs *BadgerStore) DeleteNodeWithHistory(nid types.NodeID, prevNodeVersion uint32, nodeTombstone *types.Node, relTombstones []RelTombstone) error {
	id := nid.SnowflakeID()
	// Serialize all tombstones OUTSIDE lock (B3).
	nodeData, err := marshalNodeToBytes(nodeTombstone)
	if err != nil {
		return fmt.Errorf("graph: marshal node tombstone: %w", err)
	}
	nodeHistKey := histNodeKey(id, uint64(prevNodeVersion))

	type histEntry struct{ key, data []byte }
	relEntries := make([]histEntry, 0, len(relTombstones))
	for _, rt := range relTombstones {
		data, err := marshalRelToBytes(rt.Tombstone)
		if err != nil {
			return fmt.Errorf("graph: marshal rel tombstone: %w", err)
		}
		relEntries = append(relEntries, histEntry{
			key:  histRelKey(rt.ID.SnowflakeID(), uint64(rt.PrevVersion)),
			data: data,
		})
	}

	// Acquire lock ONCE — hold it across cascade + tombstone appends (B3 + lock ordering rule).
	bs.idxMu.Lock()
	_, corruptErr, fatalErr := bs.cascadeDeleteInner(nid)
	if fatalErr != nil {
		bs.idxMu.Unlock()
		return fatalErr
	}
	// Append tombstone history ops to SAME pending map before releasing lock.
	ops := make([]writeOp, 0, 1+len(relEntries))
	ops = append(ops, writeOp{opType: writeOpSet, key: nodeHistKey, value: nodeData})
	for _, e := range relEntries {
		ops = append(ops, writeOp{opType: writeOpSet, key: e.key, value: e.data})
	}
	bs.appendOps(ops...)
	bs.idxMu.Unlock()

	if corruptErr == nil && bs.syncWrites {
		return bs.flush()
	}
	return corruptErr
}

// --- Batch operations ---

// PutNodesBatch stores multiple nodes atomically using two-phase validation.
// Phase 1: check for duplicates vs existing store AND within the batch.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any duplicate → error, zero mutations. Nil/empty input → nil error.
func (bs *BadgerStore) PutNodesBatch(nodes []*types.Node) error {
	if len(nodes) == 0 {
		return nil
	}

	// Pre-serialize all nodes outside the lock.
	type nodeData struct {
		id   snowflake.ID
		data []byte
	}
	serialized := make([]nodeData, len(nodes))
	for i, n := range nodes {
		w := nodeToWire(n)
		data, err := msgpack.Marshal(w)
		if err != nil {
			return fmt.Errorf("graph: marshal node: %w", err)
		}
		serialized[i] = nodeData{id: n.InternalID().SnowflakeID(), data: data}
	}

	bs.idxMu.Lock()

	// Phase 1: validate — no duplicates in store or within batch.
	seen := make(map[snowflake.ID]struct{}, len(nodes))
	for _, nd := range serialized {
		if _, exists := bs.nodeIDs[nd.id]; exists {
			bs.idxMu.Unlock()
			return ErrNodeExists
		}
		if _, exists := seen[nd.id]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate node ID %d in batch", nd.id)
		}
		seen[nd.id] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(nodes)*3) // entity + avg ~2 label indexes
	for i, n := range nodes {
		nd := serialized[i]

		bs.nodeCache.Put(nd.id, n.DeepCopy())
		bs.nodeIDs[nd.id] = struct{}{}

		ops = append(ops, writeOp{opType: writeOpSet, key: nodeKey(nd.id), value: nd.data})
		for _, tok := range n.AllLabelTokens() {
			tv := tok.Value()
			if bs.labelIdx[tv] == nil {
				bs.labelIdx[tv] = make(map[snowflake.ID]struct{})
			}
			bs.labelIdx[tv][nd.id] = struct{}{}
			ops = append(ops, writeOp{opType: writeOpSet, key: labelIndexKey(tv, nd.id)})
			bs.getOrCreateLabelCounter(tv).Add(1)
		}
		addNodeToPropertyIndexes(bs.propertyIndexes, n, nd.id)
		addNodeToTemporalIndexes(bs.temporalIndexes, n, nd.id)
	}

	bs.appendOps(ops...)
	bs.nodeCount.Add(int64(len(nodes)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// PutRelationshipsBatch stores multiple relationships atomically using two-phase validation.
// Phase 1: check endpoints exist, check for duplicate rel IDs.
// Phase 2: serialize, cache, index, and queue each for async flush.
// Any failure → error, zero mutations. Nil/empty input → nil error.
func (bs *BadgerStore) PutRelationshipsBatch(rels []*types.Relationship) error {
	if len(rels) == 0 {
		return nil
	}

	// Pre-serialize all relationships outside the lock.
	type relData struct {
		id      snowflake.ID
		startID snowflake.ID
		endID   snowflake.ID
		relType uint16
		data    []byte
	}
	serialized := make([]relData, len(rels))
	for i, r := range rels {
		w := relToWire(r)
		data, err := msgpack.Marshal(w)
		if err != nil {
			return fmt.Errorf("graph: marshal relationship: %w", err)
		}
		serialized[i] = relData{
			id:      r.InternalID().SnowflakeID(),
			startID: r.StartNodeID().SnowflakeID(),
			endID:   r.EndNodeID().SnowflakeID(),
			relType: r.TypeToken().Value(),
			data:    data,
		}
	}

	bs.idxMu.Lock()

	// Phase 1: validate — endpoints exist, no duplicates.
	seen := make(map[snowflake.ID]struct{}, len(rels))
	for _, rd := range serialized {
		if _, exists := bs.nodeIDs[rd.startID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if _, exists := bs.nodeIDs[rd.endID]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		if _, exists := bs.relIDs[rd.id]; exists {
			bs.idxMu.Unlock()
			return ErrRelExists
		}
		if _, exists := seen[rd.id]; exists {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: duplicate relationship ID %d in batch", rd.id)
		}
		seen[rd.id] = struct{}{}
	}

	// Phase 2: apply — all validated, safe to mutate.
	ops := make([]writeOp, 0, len(rels)*4) // entity + type + out + in
	for i, r := range rels {
		rd := serialized[i]

		bs.relCache.Put(rd.id, r.DeepCopy())
		bs.relIDs[rd.id] = struct{}{}

		if bs.typeIdx[rd.relType] == nil {
			bs.typeIdx[rd.relType] = make(map[snowflake.ID]struct{})
		}
		bs.typeIdx[rd.relType][rd.id] = struct{}{}

		if bs.outIdx[rd.startID] == nil {
			bs.outIdx[rd.startID] = make(map[snowflake.ID]struct{})
		}
		bs.outIdx[rd.startID][rd.id] = struct{}{}

		if bs.inIdx[rd.endID] == nil {
			bs.inIdx[rd.endID] = make(map[snowflake.ID]uint16)
		}
		bs.inIdx[rd.endID][rd.id] = rd.relType

		ops = append(ops, writeOp{opType: writeOpSet, key: relKey(rd.id), value: rd.data})
		ops = append(ops, writeOp{opType: writeOpSet, key: relTypeIndexKey(rd.relType, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: outKey(rd.startID, rd.relType, rd.endID, rd.id)})
		ops = append(ops, writeOp{opType: writeOpSet, key: inKey(rd.endID, rd.relType, rd.startID, rd.id)})
		bs.getOrCreateTypeCounter(rd.relType).Add(1)
	}

	bs.appendOps(ops...)
	bs.relCount.Add(int64(len(rels)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteNodesBatch deletes multiple nodes atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read node data for label cleanup.
// Phase 2: remove from cache, indexes, queue delete ops.
// Missing ID → ErrNodeNotFound, zero mutations. Nil/empty input → nil error.
func (bs *BadgerStore) DeleteNodesBatch(typedIDs []types.NodeID) error {
	if len(typedIDs) == 0 {
		return nil
	}
	ids := make([]snowflake.ID, len(typedIDs))
	for i, id := range typedIDs {
		ids[i] = id.SnowflakeID()
	}

	bs.idxMu.Lock()

	// Phase 1: validate — all must exist + pre-read for label cleanup.
	nodeData := make([]*types.Node, len(ids))
	for i, id := range ids {
		if _, exists := bs.nodeIDs[id]; !exists {
			bs.idxMu.Unlock()
			return ErrNodeNotFound
		}
		n, err := bs.getNodeLocked(types.NodeID(id))
		if err != nil {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: batch read node %d: %w", id, err)
		}
		nodeData[i] = n
	}

	// Phase 2: apply — all validated, safe to mutate.
	for i, id := range ids {
		n := nodeData[i]

		ops := []writeOp{{opType: writeOpDelete, key: nodeKey(id)}}

		allTokens := collectNodeLabelTokens(n)
		for _, tok := range allTokens {
			ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, id)})
			if set, exists := bs.labelIdx[tok]; exists {
				delete(set, id)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
			}
			bs.getOrCreateLabelCounter(tok).Add(-1)
		}

		removeNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, id)
		bs.appendOps(ops...)
	}

	bs.nodeCount.Add(-int64(len(ids)))
	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// DeleteRelationshipsBatch deletes multiple relationships atomically using two-phase validation.
// Phase 1: check all IDs exist, pre-read relationship metadata.
// Phase 2: delete via deleteRelByInfo (mutation-only), clean up history.
// Missing ID → ErrRelNotFound, zero mutations. Nil/empty input → nil error.
func (bs *BadgerStore) DeleteRelationshipsBatch(typedIDs []types.RelID) error {
	if len(typedIDs) == 0 {
		return nil
	}
	ids := make([]snowflake.ID, len(typedIDs))
	for i, id := range typedIDs {
		ids[i] = id.SnowflakeID()
	}

	bs.idxMu.Lock()

	// Phase 1: validate — all must exist + pre-read metadata.
	infos := make([]relDeleteInfo, len(ids))
	for i, id := range ids {
		if _, exists := bs.relIDs[id]; !exists {
			bs.idxMu.Unlock()
			return ErrRelNotFound
		}
		r, err := bs.getRelLocked(types.RelID(id))
		if err != nil {
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: batch read relationship %d: %w", id, err)
		}
		infos[i] = relDeleteInfo{
			id:      id,
			relType: r.TypeToken().Value(),
			startID: r.StartNodeID().SnowflakeID(),
			endID:   r.EndNodeID().SnowflakeID(),
		}
	}

	// Phase 2: apply — all validated, mutations cannot fail.
	for _, info := range infos {
		bs.deleteRelByInfo(info)
	}

	bs.idxMu.Unlock()

	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// --- Marshal helpers (shared by PutNodeVersion, PutRelVersion, DeleteNodeWithHistory, DeleteRelWithHistory) ---

// marshalNodeToBytes serializes a Node to msgpack bytes via the wire format.
func marshalNodeToBytes(n *types.Node) ([]byte, error) {
	return msgpack.Marshal(nodeToWire(n))
}

// marshalRelToBytes serializes a Relationship to msgpack bytes via the wire format.
func marshalRelToBytes(r *types.Relationship) ([]byte, error) {
	return msgpack.Marshal(relToWire(r))
}

// --- Version history ---

// PutNodeVersion stores a node snapshot at the given version.
// Serializes via nodeToWire (deep copy at serialization boundary).
func (bs *BadgerStore) PutNodeVersion(nid types.NodeID, version uint32, n *types.Node) error {
	id := nid.SnowflakeID()
	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node version: %w", err)
	}
	key := histNodeKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetNodeVersion retrieves a node snapshot at the given version.
// Checks the pending buffer first (unflushed writes), then Badger.
// Returns ErrVersionNotFound if the version does not exist.
func (bs *BadgerStore) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	id := nid.SnowflakeID()
	key := histNodeKey(id, uint64(version))
	keyStr := string(key)

	// Check pending buffer for unflushed writes.
	bs.wbMu.Lock()
	op, found := bs.pending[keyStr]
	bs.wbMu.Unlock()

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w nodeWire
		if err := msgpack.Unmarshal(op.value, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n := wireToNode(w)
		return n.DeepCopy(), nil
	}

	// Fall through to Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w nodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node version: %w", err)
			}
			n = wireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

// GetNodeHistory returns all node version snapshots in ascending version order.
// Merges persisted Badger entries with unflushed pending buffer entries.
func (bs *BadgerStore) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	id := nid.SnowflakeID()
	prefix := histNodePrefix(id)
	return bs.getNodeHistoryByPrefix(prefix)
}

// getNodeHistoryByPrefix scans Badger and the pending buffer for node history entries.
func (bs *BadgerStore) getNodeHistoryByPrefix(prefix []byte) ([]*types.Node, error) {
	prefixStr := string(prefix)

	// Collect from Badger.
	entries := make(map[string][]byte) // key string -> value bytes
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := string(item.KeyCopy(nil))
			err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Merge pending buffer entries (pending wins).
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(entries, k)
			} else {
				cp := make([]byte, len(op.value))
				copy(cp, op.value)
				entries[k] = cp
			}
		}
	}
	bs.wbMu.Unlock()

	if len(entries) == 0 {
		return nil, nil
	}

	// Sort by key (big-endian version in bytes 9-17 gives natural ascending order).
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]*types.Node, 0, len(keys))
	for _, k := range keys {
		var w nodeWire
		if err := msgpack.Unmarshal(entries[k], &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n := wireToNode(w)
		result = append(result, n.DeepCopy())
	}
	return result, nil
}

// TruncateNodeHistory removes all but the N most recent node versions.
// If keepVersions <= 0, all history is cleared.
func (bs *BadgerStore) TruncateNodeHistory(nid types.NodeID, keepVersions int) error {
	id := nid.SnowflakeID()
	prefix := histNodePrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// PutRelVersion stores a relationship snapshot at the given version.
// Serializes via relToWire (deep copy at serialization boundary).
func (bs *BadgerStore) PutRelVersion(rid types.RelID, version uint32, r *types.Relationship) error {
	id := rid.SnowflakeID()
	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal rel version: %w", err)
	}
	key := histRelKey(id, uint64(version))
	bs.appendOps(writeOp{opType: writeOpSet, key: key, value: data})
	if bs.syncWrites {
		return bs.flush()
	}
	return nil
}

// GetRelVersion retrieves a relationship snapshot at the given version.
// Checks the pending buffer first, then Badger.
// Returns ErrVersionNotFound if the version does not exist.
func (bs *BadgerStore) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	key := histRelKey(id, uint64(version))
	keyStr := string(key)

	// Check pending buffer.
	bs.wbMu.Lock()
	op, found := bs.pending[keyStr]
	bs.wbMu.Unlock()

	if found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		var w relWire
		if err := msgpack.Unmarshal(op.value, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r := wireToRel(w)
		return r.DeepCopy(), nil
	}

	// Fall through to Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w relWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal rel version: %w", err)
			}
			r = wireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

// GetRelHistory returns all relationship version snapshots in ascending version order.
// Merges persisted Badger entries with unflushed pending buffer entries.
func (bs *BadgerStore) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	id := rid.SnowflakeID()
	prefix := histRelPrefix(id)
	return bs.getRelHistoryByPrefix(prefix)
}

// getRelHistoryByPrefix scans Badger and the pending buffer for rel history entries.
func (bs *BadgerStore) getRelHistoryByPrefix(prefix []byte) ([]*types.Relationship, error) {
	prefixStr := string(prefix)

	entries := make(map[string][]byte)
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := string(item.KeyCopy(nil))
			err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(entries, k)
			} else {
				cp := make([]byte, len(op.value))
				copy(cp, op.value)
				entries[k] = cp
			}
		}
	}
	bs.wbMu.Unlock()

	if len(entries) == 0 {
		return nil, nil
	}

	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]*types.Relationship, 0, len(keys))
	for _, k := range keys {
		var w relWire
		if err := msgpack.Unmarshal(entries[k], &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r := wireToRel(w)
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

// TruncateRelHistory removes all but the N most recent relationship versions.
// If keepVersions <= 0, all history is cleared.
func (bs *BadgerStore) TruncateRelHistory(rid types.RelID, keepVersions int) error {
	id := rid.SnowflakeID()
	prefix := histRelPrefix(id)
	return bs.truncateHistoryByPrefix(prefix, keepVersions)
}

// truncateHistoryByPrefix removes all but the N most recent history entries
// matching the given prefix. Scans both Badger and the pending buffer.
func (bs *BadgerStore) truncateHistoryByPrefix(prefix []byte, keepVersions int) error {
	prefixStr := string(prefix)

	// Collect all keys from Badger.
	var allKeys []string
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			allKeys = append(allKeys, string(it.Item().KeyCopy(nil)))
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Merge in pending buffer keys.
	keySet := make(map[string]struct{}, len(allKeys))
	for _, k := range allKeys {
		keySet[k] = struct{}{}
	}
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if len(k) >= len(prefixStr) && k[:len(prefixStr)] == prefixStr {
			if op.opType == writeOpDelete {
				delete(keySet, k)
			} else {
				keySet[k] = struct{}{}
			}
		}
	}
	bs.wbMu.Unlock()

	if len(keySet) == 0 {
		return nil
	}

	sorted := make([]string, 0, len(keySet))
	for k := range keySet {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var toDelete []string
	if keepVersions <= 0 {
		toDelete = sorted
	} else if len(sorted) > keepVersions {
		toDelete = sorted[:len(sorted)-keepVersions]
	}

	if len(toDelete) == 0 {
		return nil
	}

	ops := make([]writeOp, len(toDelete))
	for i, k := range toDelete {
		ops[i] = writeOp{opType: writeOpDelete, key: []byte(k)}
	}
	bs.appendOps(ops...)
	return nil
}

// --- Counts (O(1) via atomic counters) ---

// NodeCount returns the number of stored nodes. O(1).
func (bs *BadgerStore) NodeCount() (int, error) {
	return int(bs.nodeCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// RelationshipCount returns the number of stored relationships. O(1).
func (bs *BadgerStore) RelationshipCount() (int, error) {
	return int(bs.relCount.Load()), nil // #nosec G115 — count is always non-negative and within int range
}

// NodeCountByLabel returns the number of nodes with the given label token. O(1).
func (bs *BadgerStore) NodeCountByLabel(token uint16) (int, error) {
	if v, ok := bs.labelCounts.Load(token); ok {
		return int(v.(*atomic.Int64).Load()), nil // #nosec G115 — count is always non-negative and within int range
	}
	return 0, nil
}

// RelCountByType returns the number of relationships with the given type token. O(1).
func (bs *BadgerStore) RelCountByType(token uint16) (int, error) {
	if v, ok := bs.typeCounts.Load(token); ok {
		return int(v.(*atomic.Int64).Load()), nil // #nosec G115 — count is always non-negative and within int range
	}
	return 0, nil
}

// getOrCreateLabelCounter returns the atomic counter for the given label token,
// creating it if it doesn't exist.
func (bs *BadgerStore) getOrCreateLabelCounter(token uint16) *atomic.Int64 {
	if v, ok := bs.labelCounts.Load(token); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.labelCounts.LoadOrStore(token, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// getOrCreateTypeCounter returns the atomic counter for the given reltype token,
// creating it if it doesn't exist.
func (bs *BadgerStore) getOrCreateTypeCounter(token uint16) *atomic.Int64 {
	if v, ok := bs.typeCounts.Load(token); ok {
		return v.(*atomic.Int64)
	}
	v, _ := bs.typeCounts.LoadOrStore(token, &atomic.Int64{})
	return v.(*atomic.Int64)
}

// --- Background flush ---

// flushLoop periodically drains the write buffer to Badger.
func (bs *BadgerStore) flushLoop() {
	defer close(bs.flushDone)
	ticker := time.NewTicker(bs.flushInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			if err := bs.flush(); err != nil {
				slog.Error("graph: flush failed", "error", err)
			}
			return
		case <-ticker.C:
			if err := bs.flush(); err != nil {
				slog.Error("graph: flush failed", "error", err)
			}
		}
	}
}

// Flush synchronously drains the write buffer to Badger. Exported for testing.
func (bs *BadgerStore) Flush() error {
	return bs.flush()
}

// flush drains the write buffer to Badger via WriteBatch.
//
// flushMu is held for the entire duration to serialize concurrent flush() calls.
// Without this, two concurrent callers (e.g. two SyncWrites mutations running in
// parallel goroutines) can both snapshot counter values under idxMu.RLock, then
// submit their WriteBatches to Badger concurrently. If the older batch completes
// last, it overwrites the on-disk counter with a stale value, corrupting counts
// on the next restart.
//
// The flush holds idxMu.RLock during the snapshot+swap phase to prevent any
// writer from being between cache.Put and appendOps (all writers hold
// idxMu.Lock for their entire mutation). This guarantees that the dirty
// version snapshot, pending ops, and counter values are consistent.
//
// Counters are included in the same WriteBatch as entity ops — no TOCTOU
// window between data and counter persistence.
func (bs *BadgerStore) flush() error {
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()

	// Step 1: Atomically snapshot dirty cache versions, pending ops, and counters.
	// idxMu.RLock blocks writers (who hold idxMu.Lock) during this phase,
	// ensuring no writer is between cache.Put and appendOps.
	bs.idxMu.RLock()
	nodeDirty := bs.nodeCache.CollectDirty()
	relDirty := bs.relCache.CollectDirty()
	bs.wbMu.Lock()
	ops := bs.pending
	bs.pending = make(map[string]writeOp)
	bs.wbMu.Unlock()
	nc := bs.nodeCount.Load()
	rc := bs.relCount.Load()
	bs.idxMu.RUnlock()

	if len(ops) == 0 {
		return nil
	}

	// Step 2: Write all ops + counters to Badger via WriteBatch (blind writes, no OCC).
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel() // no-op if Flush already called

	for _, op := range ops {
		switch op.opType {
		case writeOpSet:
			if err := wb.SetEntry(badger.NewEntry(op.key, op.value)); err != nil {
				bs.requeueOps(ops)
				return fmt.Errorf("graph: write batch set: %w", err)
			}
		case writeOpDelete:
			if err := wb.Delete(op.key); err != nil {
				bs.requeueOps(ops)
				return fmt.Errorf("graph: write batch delete: %w", err)
			}
		}
	}

	// Include counters in the same atomic batch — no TOCTOU drift on crash.
	ncBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(ncBuf, uint64(nc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	if err := wb.SetEntry(badger.NewEntry(counterNodeCountKey, ncBuf)); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch set counter: %w", err)
	}
	rcBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(rcBuf, uint64(rc)) // #nosec G115 — intentional int64→uint64 for binary encoding
	if err := wb.SetEntry(badger.NewEntry(counterRelCountKey, rcBuf)); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch set counter: %w", err)
	}

	// Guard against blocking forever: Badger v4's WriteBatch.Flush() hangs
	// when called after db.Close() (WaitForMark blocks on a stopped oracle).
	if bs.dbClosed.Load() {
		wb.Cancel()
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch flush: %w", badger.ErrDBClosed)
	}
	if err := wb.Flush(); err != nil {
		bs.requeueOps(ops)
		return fmt.Errorf("graph: write batch flush: %w", err)
	}

	// Step 3: Mark cache entries clean — version-aware.
	// Only clears dirty on entries whose dirtyVer matches the snapshot.
	// Entries re-dirtied during the flush retain their dirty status.
	bs.markCacheFlushed(nodeDirty, relDirty)

	return nil
}

// requeueOps merges failed write ops back into the pending buffer.
// Only re-adds ops whose key is not already in pending (a newer concurrent
// write takes precedence over the failed one).
func (bs *BadgerStore) requeueOps(failed map[string]writeOp) {
	bs.wbMu.Lock()
	for k, op := range failed {
		if _, exists := bs.pending[k]; !exists {
			bs.pending[k] = op
		}
	}
	bs.wbMu.Unlock()
}

// markCacheFlushed builds flushed ID→version maps from the collected dirty
// entries and passes them to MarkFlushed on each cache.
func (bs *BadgerStore) markCacheFlushed(nodeDirty []lruEntry[*types.Node], relDirty []lruEntry[*types.Relationship]) {
	if len(nodeDirty) > 0 {
		nf := make(map[snowflake.ID]uint64, len(nodeDirty))
		for _, e := range nodeDirty {
			nf[e.key] = e.dirtyVer
		}
		bs.nodeCache.MarkFlushed(nf)
	}
	if len(relDirty) > 0 {
		rf := make(map[snowflake.ID]uint64, len(relDirty))
		for _, e := range relDirty {
			rf[e.key] = e.dirtyVer
		}
		bs.relCache.MarkFlushed(rf)
	}
}

// --- Background GC ---

// gcLoop periodically runs Badger value log GC.
func (bs *BadgerStore) gcLoop() {
	defer close(bs.gcDone)
	ticker := time.NewTicker(bs.gcInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			return
		case <-ticker.C:
			for bs.db.RunValueLogGC(bs.gcRatio) == nil {
				// Keep running until no more garbage to collect.
			}
		}
	}
}

// --- Property indexes ---

// CreatePropertyIndex creates a property index for the given label token and property key.
// Three-phase approach to prevent blocking concurrent reads/writes during slow I/O:
//
//	Phase 1 (write Lock): Install an empty live index so concurrent PutNode/ReplaceNode
//	writes are captured immediately. Snapshot existing node IDs.
//	Phase 2 (no lock): Fetch node data via public GetNode to build a backfill set.
//	Phase 3 (write Lock): Merge backfill entries into the live index, skipping IDs
//	that were already handled by concurrent writes during Phase 2.
//
// Returns ErrIndexExists if the index already exists.
func (bs *BadgerStore) CreatePropertyIndex(labelToken uint16, propertyKey string) error {
	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	// Write lock (not RLock) ensures the index is visible to concurrent mutations.
	bs.idxMu.Lock()
	key := propertyIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrIndexExists
	}
	liveIdx := newPropertyIndex()
	liveIdx.mutated = make(map[snowflake.ID]struct{})
	bs.propertyIndexes[key] = liveIdx
	var ids []snowflake.ID
	if nodeIDs, ok := bs.labelIdx[labelToken]; ok {
		ids = make([]snowflake.ID, 0, len(nodeIDs))
		for id := range nodeIDs {
			ids = append(ids, id)
		}
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock via public GetNode.
	// Builds a backfill index for nodes that existed before Phase 1.
	backfill := newPropertyIndex()
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			// Fatal error — remove the incomplete index.
			bs.idxMu.Lock()
			delete(bs.propertyIndexes, key)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create property index: %w", err)
		}
		if val, found := n.GetProperty(propertyKey); found {
			backfill.add(id, val)
		}
	}

	// Phase 3: Merge backfill into live index under write Lock.
	// Skip entries for IDs already handled by concurrent writes during Phase 2,
	// and entries for nodes deleted during Phase 2.
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	for vk, idSet := range backfill.entries {
		for id := range idSet {
			if _, mutated := liveIdx.mutated[id]; mutated {
				continue // concurrent write handled this ID during Phase 2
			}
			if _, alive := bs.nodeIDs[id]; !alive {
				continue // node deleted during Phase 2
			}
			if liveIdx.entries[vk] == nil {
				liveIdx.entries[vk] = make(map[snowflake.ID]struct{})
			}
			liveIdx.entries[vk][id] = struct{}{}
		}
	}
	liveIdx.mutated = nil // stop tracking — index creation complete
	bs.persistPropertyIndexDefs()
	return nil
}

// DropPropertyIndex removes a property index.
// Returns ErrIndexNotFound if the index does not exist.
func (bs *BadgerStore) DropPropertyIndex(labelToken uint16, propertyKey string) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	key := propertyIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := bs.propertyIndexes[key]; !exists {
		return ErrIndexNotFound
	}

	delete(bs.propertyIndexes, key)
	bs.persistPropertyIndexDefs()
	return nil
}

// propIdxDef is the serialization format for property index definitions.
type propIdxDef struct {
	LabelToken  uint16 `msgpack:"l"`
	PropertyKey string `msgpack:"p"`
}

// --- Temporal indexes ---

// CreateTemporalIndex creates a temporal index on nodes with the given label token.
// Three-phase approach (same as CreatePropertyIndex) for safe concurrent operation.
// Returns ErrTemporalIndexExists if the index already exists.
func (bs *BadgerStore) CreateTemporalIndex(labelToken uint16) error {
	// Phase 1: Install empty live index + snapshot IDs under write Lock.
	bs.idxMu.Lock()
	if _, exists := bs.temporalIndexes[labelToken]; exists {
		bs.idxMu.Unlock()
		return ErrTemporalIndexExists
	}
	liveTI := newTemporalIndex()
	bs.temporalIndexes[labelToken] = liveTI
	var ids []snowflake.ID
	if nodeIDs, ok := bs.labelIdx[labelToken]; ok {
		ids = make([]snowflake.ID, 0, len(nodeIDs))
		for id := range nodeIDs {
			ids = append(ids, id)
		}
	}
	bs.idxMu.Unlock()

	// Phase 2: Fetch node data OUTSIDE any lock via public GetNode.
	type nodeEntry struct {
		id   snowflake.ID
		from types.Instant
		to   types.Instant
	}
	backfill := make([]nodeEntry, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // deleted between snapshot and fetch
			}
			// Fatal error — remove the incomplete index.
			bs.idxMu.Lock()
			delete(bs.temporalIndexes, labelToken)
			bs.idxMu.Unlock()
			return fmt.Errorf("graph: create temporal index: %w", err)
		}
		from, to := nodeTemporalBounds(id, n.Temporal())
		backfill = append(backfill, nodeEntry{id: id, from: from, to: to})
	}

	// Phase 3: Merge backfill into live index under write Lock.
	// Skip IDs that were touched by concurrent mutations during Phase 2.
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	for _, entry := range backfill {
		if _, alive := bs.nodeIDs[entry.id]; !alive {
			continue // node deleted during Phase 2
		}
		// Only add if not already handled by a concurrent write.
		// The live index starts empty; any entry already present was added
		// by a concurrent PutNode/ReplaceNode that ran during Phase 2.
		found := false
		for _, e := range liveTI.entries {
			if e.id == entry.id {
				found = true
				break
			}
		}
		if !found {
			liveTI.add(entry.id, entry.from, entry.to)
		}
	}
	bs.persistTemporalIndexDefs()
	return nil
}

// DropTemporalIndex removes a temporal index.
// Returns ErrTemporalIndexNotFound if the index does not exist.
func (bs *BadgerStore) DropTemporalIndex(labelToken uint16) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.temporalIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}

	delete(bs.temporalIndexes, labelToken)
	bs.persistTemporalIndexDefs()
	return nil
}

// --- High-frequency indexes ---

// CreateHighFrequencyIndex creates a time-bucketed high-frequency index on nodes
// with the given label token. Only one temporal index type can exist per label —
// returns ErrTemporalIndexExists if a temporalIndex or highFrequencyIndex already
// exists for this label.
func (bs *BadgerStore) CreateHighFrequencyIndex(labelToken uint16, bucketSize time.Duration) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.temporalIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}
	if _, exists := bs.hfIndexes[labelToken]; exists {
		return ErrTemporalIndexExists
	}

	bs.hfIndexes[labelToken] = newHighFrequencyIndex(bucketSize, 0)
	return nil
}

// DropHighFrequencyIndex removes the high-frequency index for the given label token.
// Returns ErrTemporalIndexNotFound if no high-frequency index exists.
func (bs *BadgerStore) DropHighFrequencyIndex(labelToken uint16) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.hfIndexes[labelToken]; !exists {
		return ErrTemporalIndexNotFound
	}
	delete(bs.hfIndexes, labelToken)
	return nil
}

// persistTemporalIndexDefs serializes the current temporal index label tokens to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *BadgerStore) persistTemporalIndexDefs() {
	tokens := make([]uint16, 0, len(bs.temporalIndexes))
	for tok := range bs.temporalIndexes {
		tokens = append(tokens, tok)
	}
	if len(tokens) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: temporalIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(tokens)
	if err != nil {
		slog.Error("graph: persist temporal index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: temporalIndexDefsKey, value: data})
}

// CreateVectorIndex creates a vector similarity index for nodes with the given label token,
// on the given property key, expecting vectors of length dims.
// Scans existing nodes to populate the index. Returns ErrVectorIndexExists on duplicate.
// Vector indexes are in-memory only and are not persisted.
func (bs *BadgerStore) CreateVectorIndex(labelToken uint16, propertyKey string, dims int, metric DistanceMetric) error {
	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}

	// Phase 1: Install empty placeholder under write lock for concurrent-write visibility.
	bs.idxMu.Lock()
	if _, exists := bs.vectorIndexes[key]; exists {
		bs.idxMu.Unlock()
		return ErrVectorIndexExists
	}
	vi := &vectorIndex{dims: dims, metric: metric}
	bs.vectorIndexes[key] = vi

	// Snapshot existing node IDs for population scan.
	nodeIDs := make([]snowflake.ID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		nodeIDs = append(nodeIDs, id)
	}
	bs.idxMu.Unlock()

	// Phase 2: Populate from existing nodes (unlocked I/O).
	for _, id := range nodeIDs {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			continue // node may have been deleted concurrently
		}
		if !n.HasLabelTokenRaw(labelToken) {
			continue
		}
		val, ok := n.GetProperty(propertyKey)
		if !ok {
			continue
		}
		vec, ok := toFloat32Slice(val)
		if !ok {
			continue
		}
		_ = vi.add(id, vec)
	}
	return nil
}

// DropVectorIndex removes a vector index.
// Returns ErrVectorIndexNotFound if the index does not exist.
func (bs *BadgerStore) DropVectorIndex(labelToken uint16, propertyKey string) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	if _, exists := bs.vectorIndexes[key]; !exists {
		return ErrVectorIndexNotFound
	}
	delete(bs.vectorIndexes, key)
	return nil
}

// SearchNearestNodes returns the k nodes with vectors closest to query
// under the index defined for labelToken+propertyKey.
// Results are ordered by ascending distance (closest first).
// Returns ErrVectorIndexNotFound if no index exists.
// Returns ErrDimensionMismatch if query length differs from the index's dims.
// Returns nil, nil if the index exists but has no entries.
func (bs *BadgerStore) SearchNearestNodes(labelToken uint16, propertyKey string, query []float32, k int, opts QueryOpts) ([]*types.Node, error) {
	bs.idxMu.RLock()
	key := vectorIndexKey{labelToken: labelToken, propertyKey: propertyKey}
	vi, exists := bs.vectorIndexes[key]
	bs.idxMu.RUnlock()

	if !exists {
		return nil, ErrVectorIndexNotFound
	}

	ids, err := vi.searchNearest(query, k)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Fetch nodes in distance order — do NOT sort by ID (would destroy distance ranking).
	result := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			continue // node may have been deleted concurrently
		}
		result = append(result, n)
	}
	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// persistPropertyIndexDefs serializes the current property index definitions to Badger.
// Caller must hold bs.idxMu write lock.
func (bs *BadgerStore) persistPropertyIndexDefs() {
	var defs []propIdxDef
	for key := range bs.propertyIndexes {
		defs = append(defs, propIdxDef{LabelToken: key.labelToken, PropertyKey: key.propertyKey})
	}
	if len(defs) == 0 {
		bs.appendOps(writeOp{opType: writeOpDelete, key: propIndexDefsKey})
		return
	}
	data, err := msgpack.Marshal(defs)
	if err != nil {
		slog.Error("graph: persist property index defs: marshal failed", "error", err)
		return // index still works in-memory; will retry on next change
	}
	bs.appendOps(writeOp{opType: writeOpSet, key: propIndexDefsKey, value: data})
}

// loadNodeFromBadger reads and unmarshals a node within an existing Badger transaction.
// Does not interact with the LRU cache. Used during loadIndexes where the cache is
// not yet populated and concurrent access has not started.
func (bs *BadgerStore) loadNodeFromBadger(txn *badger.Txn, id snowflake.ID) (*types.Node, error) {
	item, err := txn.Get(nodeKey(id))
	if err == badger.ErrKeyNotFound {
		return nil, ErrNodeNotFound
	}
	if err != nil {
		return nil, err
	}
	var n *types.Node
	err = item.Value(func(val []byte) error {
		var w nodeWire
		if err := msgpack.Unmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal node: %w", err)
		}
		n = wireToNode(w)
		return nil
	})
	return n, err
}

// NodesByLabelAndProperty returns nodes matching the label and property value,
// with optional temporal filtering. Uses the property index if one exists;
// falls back to label scan + property filter.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) NodesByLabelAndProperty(labelToken uint16, propKey string, value any, opts QueryOpts) ([]*types.Node, error) {
	// Snapshot matching IDs under RLock, then release before entity I/O.
	bs.idxMu.RLock()
	key := propertyIndexKey{labelToken: labelToken, propertyKey: propKey}
	var ids []snowflake.ID

	if idx, ok := bs.propertyIndexes[key]; ok {
		// Indexed path: snapshot matching IDs.
		matchSet := idx.lookup(value)
		if len(matchSet) == 0 {
			bs.idxMu.RUnlock()
			return nil, nil
		}
		ids = make([]snowflake.ID, 0, len(matchSet))
		for id := range matchSet {
			ids = append(ids, id)
		}
		bs.idxMu.RUnlock()

		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		// Temporal pre-filter via Peek.
		ids = bs.filterNodeIDsByTemporalPeek(ids, opts)

		ids = paginateIDs(ids, opts.After, opts.Limit)
		if len(ids) == 0 {
			return nil, nil
		}

		return bs.fetchNodesWithTemporalFilter(ids, opts)
	}

	// Fallback: snapshot label IDs, release lock, then scan properties.
	slog.Debug("graph: NodesByLabelAndProperty using full label scan (no property index)",
		"labelToken", labelToken, "propertyKey", propKey)
	labelIDs := bs.labelIdx[labelToken]
	if len(labelIDs) == 0 {
		bs.idxMu.RUnlock()
		return nil, nil
	}

	ids = make([]snowflake.ID, 0, len(labelIDs))
	for id := range labelIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	targetKey := propertyValueKey(value)
	if targetKey == "" {
		return nil, nil
	}

	// Sort label IDs, apply cursor skip, scan in order for property matches.
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = paginateIDs(ids, opts.After, 0) // apply cursor, not limit yet
	if len(ids) == 0 {
		return nil, nil
	}

	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	var result []*types.Node
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // orphaned index entry
			}
			return nil, err
		}
		if v, found := n.GetProperty(propKey); found {
			if propertyValueKey(v) == targetKey {
				if hasTemporal && !matchesTemporalFilter(id, n.Temporal(), opts) {
					continue
				}
				result = append(result, n)
				if opts.Limit > 0 && len(result) >= opts.Limit {
					break
				}
			}
		}
	}

	if len(result) == 0 {
		return nil, nil
	}
	return result, nil
}

// --- ID-only queries ---

// AllNodeIDs returns the IDs of all current nodes, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy. O(N) in nodeIDs map size.
func (bs *BadgerStore) AllNodeIDs(opts QueryOpts) ([]types.NodeID, error) {
	bs.idxMu.RLock()
	ids := make([]snowflake.ID, 0, len(bs.nodeIDs))
	for id := range bs.nodeIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out, nil
}

// AllRelIDs returns the IDs of all current relationships, with optional pagination.
// Returns only IDs — no entity deserialization or deep copy. O(N) in relIDs map size.
func (bs *BadgerStore) AllRelIDs(opts QueryOpts) ([]types.RelID, error) {
	bs.idxMu.RLock()
	ids := make([]snowflake.ID, 0, len(bs.relIDs))
	for id := range bs.relIDs {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	ids = paginateIDs(ids, opts.After, opts.Limit)
	if len(ids) == 0 {
		return nil, nil
	}
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out, nil
}

// --- ForEach iterators ---

// ForEachNodeID iterates over all current node IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *BadgerStore) ForEachNodeID(fn func(types.NodeID) bool) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for id := range bs.nodeIDs {
		if !fn(types.NodeID(id)) {
			return nil
		}
	}
	return nil
}

// ForEachRelID iterates over all current relationship IDs, calling fn for each.
// Iteration stops early if fn returns false. No ordering guarantee.
func (bs *BadgerStore) ForEachRelID(fn func(types.RelID) bool) error {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	for id := range bs.relIDs {
		if !fn(types.RelID(id)) {
			return nil
		}
	}
	return nil
}

// ForEachNodeHistoryID iterates over all node IDs with version history entries.
// Scans both the pending buffer and Badger for 0x07 prefix keys.
// Iteration stops early if fn returns false.
func (bs *BadgerStore) ForEachNodeHistoryID(fn func(types.NodeID) bool) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: pending buffer.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= sizeHistKey && k[0] == keyHistNode {
			id := parseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Emit pending IDs.
	for id := range seen {
		if !fn(types.NodeID(id)) {
			return nil
		}
	}

	// Phase 2: Badger prefix scan.
	return bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{keyHistNode}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= sizeHistKey {
				id := parseIDFromKey(key, 1)
				if _, ok := seen[id]; ok {
					continue // already emitted
				}
				seen[id] = struct{}{}
				if !fn(types.NodeID(id)) {
					return nil
				}
			}
		}
		return nil
	})
}

// ForEachRelHistoryID iterates over all relationship IDs with version history entries.
// Scans both the pending buffer and Badger for 0x08 prefix keys.
// Iteration stops early if fn returns false.
func (bs *BadgerStore) ForEachRelHistoryID(fn func(types.RelID) bool) error {
	seen := make(map[snowflake.ID]struct{})

	// Phase 1: pending buffer.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= sizeHistKey && k[0] == keyHistRel {
			id := parseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Emit pending IDs.
	for id := range seen {
		if !fn(types.RelID(id)) {
			return nil
		}
	}

	// Phase 2: Badger prefix scan.
	return bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{keyHistRel}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= sizeHistKey {
				id := parseIDFromKey(key, 1)
				if _, ok := seen[id]; ok {
					continue // already emitted
				}
				seen[id] = struct{}{}
				if !fn(types.RelID(id)) {
					return nil
				}
			}
		}
		return nil
	})
}

// --- History ID scans ---

// AllNodeHistoryIDs returns the IDs of all nodes that have version history entries.
// Scans both the pending buffer and Badger for 0x07 prefix keys.
// The full ID slice is loaded into memory — acceptable for typical history populations.
// TODO(v3.1.0): add cursor-based AllNodeHistoryIDs(QueryOpts) to the Store interface
// to eliminate OOM risk at large history depths (10K nodes × 1K versions = 10M IDs).
func (bs *BadgerStore) AllNodeHistoryIDs() ([]types.NodeID, error) {
	seen := make(map[snowflake.ID]struct{})

	// Check pending buffer for unflushed history writes.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= sizeHistKey && k[0] == keyHistNode {
			id := parseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Scan Badger for persisted history keys.
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{keyHistNode}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= sizeHistKey {
				id := parseIDFromKey(key, 1)
				seen[id] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan node history IDs: %w", err)
	}

	if len(seen) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]types.NodeID, len(ids))
	for i, id := range ids {
		out[i] = types.NodeID(id)
	}
	return out, nil
}

// AllRelHistoryIDs returns the IDs of all relationships that have version history entries.
// Scans both the pending buffer and Badger for 0x08 prefix keys.
func (bs *BadgerStore) AllRelHistoryIDs() ([]types.RelID, error) {
	seen := make(map[snowflake.ID]struct{})

	// Check pending buffer for unflushed history writes.
	bs.wbMu.Lock()
	for k, op := range bs.pending {
		if op.opType == writeOpSet && len(k) >= sizeHistKey && k[0] == keyHistRel {
			id := parseIDFromKey([]byte(k), 1)
			seen[id] = struct{}{}
		}
	}
	bs.wbMu.Unlock()

	// Scan Badger for persisted history keys.
	err := bs.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		pfx := []byte{keyHistRel}
		for it.Seek(pfx); it.ValidForPrefix(pfx); it.Next() {
			key := it.Item().Key()
			if len(key) >= sizeHistKey {
				id := parseIDFromKey(key, 1)
				seen[id] = struct{}{}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: scan rel history IDs: %w", err)
	}

	if len(seen) == 0 {
		return nil, nil
	}
	ids := make([]snowflake.ID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := make([]types.RelID, len(ids))
	for i, id := range ids {
		out[i] = types.RelID(id)
	}
	return out, nil
}

// --- Clear ---

// Clear removes all entities, indexes, history, counters, and property indexes.
// After Clear(), the BadgerStore is in the same state as a freshly opened store.
// Registries are a Graph-layer concern — not cleared here.
func (bs *BadgerStore) Clear() error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// Clear in-memory indexes.
	bs.nodeIDs = make(map[snowflake.ID]struct{})
	bs.relIDs = make(map[snowflake.ID]struct{})
	bs.labelIdx = make(map[uint16]map[snowflake.ID]struct{})
	bs.typeIdx = make(map[uint16]map[snowflake.ID]struct{})
	bs.outIdx = make(map[snowflake.ID]map[snowflake.ID]struct{})
	bs.inIdx = make(map[snowflake.ID]map[snowflake.ID]uint16)

	// Reset atomic counters.
	bs.nodeCount.Store(0)
	bs.relCount.Store(0)
	bs.labelCounts = sync.Map{}
	bs.typeCounts = sync.Map{}

	// Re-create LRU caches with same capacity.
	cap := bs.nodeCache.Cap()
	bs.nodeCache = newEntityLRU[*types.Node](cap)
	bs.relCache = newEntityLRU[*types.Relationship](cap)

	// Clear pending buffer.
	bs.wbMu.Lock()
	bs.pending = make(map[string]writeOp)
	bs.wbMu.Unlock()

	// Clear property indexes.
	bs.propertyIndexes = make(map[propertyIndexKey]*propertyIndex)

	// Drop all data from Badger — atomically removes all KV pairs.
	return bs.db.DropAll()
}

// --- Lifecycle ---

// Close stops background goroutines, performs a final flush (including counters),
// and closes the Badger database. Safe to call multiple times.
//
// The explicit flush() handles the case where flushLoop was never started
// (InMemory mode, FlushInterval==0). If flushLoop already drained pending,
// this is a no-op. Counters are included in the WriteBatch atomically.
func (bs *BadgerStore) Close() error {
	var err error
	bs.closeOnce.Do(func() {
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

// --- StoreStats implementation ---

// NodeCacheHits returns the total number of node cache hits since store creation.
// Implements StoreStats. Both cacheHit and cacheDeleted (tombstone) results count
// as hits, because both avoid a Badger read.
func (bs *BadgerStore) NodeCacheHits() int64 { return bs.nodeCache.Hits() }

// NodeCacheMisses returns the total number of node cache misses since store creation.
// Implements StoreStats.
func (bs *BadgerStore) NodeCacheMisses() int64 { return bs.nodeCache.Misses() }

// RelCacheHits returns the total number of relationship cache hits since store creation.
// Implements StoreStats.
func (bs *BadgerStore) RelCacheHits() int64 { return bs.relCache.Hits() }

// RelCacheMisses returns the total number of relationship cache misses since store creation.
// Implements StoreStats.
func (bs *BadgerStore) RelCacheMisses() int64 { return bs.relCache.Misses() }

// --- Registry persistence ---

// SaveLabelRegistry persists the label registry to the Badger store.
func (bs *BadgerStore) SaveLabelRegistry(reg *labelRegistry) error {
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal label registry: %w", err)
	}
	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey("label_tokens"), data)
	})
}

// LoadLabelRegistry loads the label registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *BadgerStore) LoadLabelRegistry(reg *labelRegistry) (bool, error) {
	var names []string
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey("label_tokens"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load label registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// SaveRelTypeRegistry persists the relationship type registry to the Badger store.
func (bs *BadgerStore) SaveRelTypeRegistry(reg *relTypeRegistry) error {
	names := reg.ExportNames()
	data, err := msgpack.Marshal(names)
	if err != nil {
		return fmt.Errorf("graph: marshal reltype registry: %w", err)
	}
	return bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey("reltype_tokens"), data)
	})
}

// LoadRelTypeRegistry loads the relationship type registry from the Badger store.
// Returns false if no saved data exists (fresh database).
func (bs *BadgerStore) LoadRelTypeRegistry(reg *relTypeRegistry) (bool, error) {
	var names []string
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey("reltype_tokens"))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return msgpack.Unmarshal(val, &names)
		})
	})
	if err != nil {
		return false, fmt.Errorf("graph: load reltype registry: %w", err)
	}
	if names == nil {
		return false, nil
	}
	return true, reg.ImportNames(names)
}

// --- Internal helpers ---

// prefetchNode retrieves a node from cache or Badger WITHOUT holding idxMu.
// Used as a pre-fetch step before acquiring idxMu.Lock() in write operations
// (DeleteNode, ReplaceNode, RemoveNodeLabelToken) to avoid holding the global
// write lock during slow disk I/O on cache misses.
//
// Callers MUST re-verify node existence under idxMu.Lock() after calling this
// (TOCTOU guard). The returned node may be stale if a concurrent delete occurred
// between the pre-fetch and the write lock acquisition — the re-verify catches this.
//
// Safety: nodeCache has its own internal mutex; db.View opens a read-only Badger
// transaction — neither requires idxMu. Dirty (unflushed) nodes are always retained
// in the LRU (soft capacity never evicts dirty entries), so a newly Put node that
// has not yet been flushed to Badger will always be found in the cache.
func (bs *BadgerStore) prefetchNode(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	switch status {
	case cacheHit:
		return v, nil
	case cacheDeleted:
		return nil, ErrNodeNotFound
	}

	// Cache miss — check existence before incurring Badger I/O.
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[id]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}

	// Node exists in-memory but not in cache (flushed + evicted from LRU).
	// Read from Badger without holding any lock.
	var n *types.Node
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w nodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = wireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}

// getNodeLocked retrieves a node from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *BadgerStore) getNodeLocked(nid types.NodeID) (*types.Node, error) {
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	if status == cacheHit {
		return v, nil
	}
	if status == cacheDeleted {
		return nil, ErrNodeNotFound
	}

	// Cache miss — read from Badger.
	var n *types.Node
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrNodeNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w nodeWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal node: %w", err)
			}
			n = wireToNode(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.nodeCache.LoadClean(id, n)
	return n, nil
}

// getRelLocked retrieves a relationship from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *BadgerStore) getRelLocked(rid types.RelID) (*types.Relationship, error) {
	id := rid.SnowflakeID()
	v, status := bs.relCache.Get(id)
	if status == cacheHit {
		return v, nil
	}
	if status == cacheDeleted {
		return nil, ErrRelNotFound
	}

	// Cache miss — read from Badger.
	var r *types.Relationship
	err := bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(relKey(id))
		if err == badger.ErrKeyNotFound {
			return ErrRelNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			var w relWire
			if err := msgpack.Unmarshal(val, &w); err != nil {
				return fmt.Errorf("graph: unmarshal relationship: %w", err)
			}
			r = wireToRel(w)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	bs.relCache.LoadClean(id, r)
	return r, nil
}

// --- Temporal filtering helpers ---

// filterNodeIDsByTemporalPeek removes IDs that don't match the temporal filter
// using Peek (zero allocation for cache hits). Cache misses are kept as candidates
// to be post-filtered after GetNode.
func (bs *BadgerStore) filterNodeIDsByTemporalPeek(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := make([]snowflake.ID, 0, len(ids))
	for _, id := range ids {
		v, status := bs.nodeCache.Peek(id)
		switch status {
		case cacheHit:
			if matchesTemporalFilter(id, v.Temporal(), opts) {
				filtered = append(filtered, id)
			}
		case cacheDeleted:
			// skip — entity is deleted
		case cacheMiss:
			// Keep as candidate — will be post-filtered after GetNode.
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// filterRelIDsByTemporalPeek removes IDs that don't match the temporal filter
// using Peek. Cache misses are kept as candidates.
func (bs *BadgerStore) filterRelIDsByTemporalPeek(ids []snowflake.ID, opts QueryOpts) []snowflake.ID {
	if opts.ValidAt == 0 && (opts.ValidStart == 0 || opts.ValidEnd == 0) {
		return ids // no filter
	}
	filtered := make([]snowflake.ID, 0, len(ids))
	for _, id := range ids {
		v, status := bs.relCache.Peek(id)
		switch status {
		case cacheHit:
			if matchesTemporalFilter(id, v.Temporal(), opts) {
				filtered = append(filtered, id)
			}
		case cacheDeleted:
			// skip
		case cacheMiss:
			filtered = append(filtered, id)
		}
	}
	return filtered
}

// fetchNodesWithTemporalFilter fetches nodes by ID and post-filters for temporal
// match. Cache-miss candidates that were speculatively included are filtered here.
func (bs *BadgerStore) fetchNodesWithTemporalFilter(ids []snowflake.ID, opts QueryOpts) ([]*types.Node, error) {
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(types.NodeID(id))
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: query node %d: %w", id, err)
		}
		if hasTemporal && !matchesTemporalFilter(id, n.Temporal(), opts) {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// fetchRelsWithTemporalFilter fetches relationships by ID and post-filters for
// temporal match.
func (bs *BadgerStore) fetchRelsWithTemporalFilter(ids []snowflake.ID, opts QueryOpts) ([]*types.Relationship, error) {
	hasTemporal := opts.ValidAt != 0 || (opts.ValidStart > 0 && opts.ValidEnd > 0)
	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(types.RelID(id))
		if err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue
			}
			return nil, fmt.Errorf("graph: query relationship %d: %w", id, err)
		}
		if hasTemporal && !matchesTemporalFilter(id, r.Temporal(), opts) {
			continue
		}
		rels = append(rels, r)
	}
	return rels, nil
}

// collectNodeLabelTokens returns all label token values from a node.
func collectNodeLabelTokens(n *types.Node) []uint16 {
	tokens := n.AllLabelTokens()
	result := make([]uint16, len(tokens))
	for i, t := range tokens {
		result[i] = t.Value()
	}
	return result
}
