package graph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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
	inIdx    map[snowflake.ID]map[snowflake.ID]struct{} // endNodeID → set(relID)

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

	// Lifecycle.
	inMemory  bool
	flushInt  time.Duration
	gcInt     time.Duration
	gcRatio   float64
	stopCh    chan struct{}
	flushDone chan struct{}
	gcDone    chan struct{}
	closeOnce sync.Once
}

var (
	counterNodeCountKey = metaKey("node_count")
	counterRelCountKey  = metaKey("rel_count")
)

// NewBadgerStore opens a Badger database with the given configuration and
// rebuilds in-memory indexes from persisted data.
func NewBadgerStore(cfg BadgerStoreConfig) (*BadgerStore, error) {
	opts := badger.DefaultOptions(cfg.Dir)
	if cfg.InMemory {
		opts = opts.WithInMemory(true)
	}
	if cfg.Logger != nil {
		opts = opts.WithLogger(cfg.Logger)
	} else {
		opts = opts.WithLogger(nil) // suppress default Badger logs
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
	if flushInt == 0 && !cfg.InMemory {
		flushInt = defaultFlushInterval
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
		db:        db,
		nodeIDs:   make(map[snowflake.ID]struct{}),
		relIDs:    make(map[snowflake.ID]struct{}),
		labelIdx:  make(map[uint16]map[snowflake.ID]struct{}),
		typeIdx:   make(map[uint16]map[snowflake.ID]struct{}),
		outIdx:    make(map[snowflake.ID]map[snowflake.ID]struct{}),
		inIdx:     make(map[snowflake.ID]map[snowflake.ID]struct{}),
		nodeCache: newEntityLRU[*types.Node](capacity),
		relCache:  newEntityLRU[*types.Relationship](capacity),
		pending:   make(map[string]writeOp),
		inMemory:  cfg.InMemory,
		flushInt:  flushInt,
		gcInt:     gcInt,
		gcRatio:   gcRatio,
		stopCh:    make(chan struct{}),
		flushDone: make(chan struct{}),
		gcDone:    make(chan struct{}),
	}

	if err := bs.loadIndexes(); err != nil {
		_ = db.Close() // best-effort cleanup
		return nil, fmt.Errorf("graph: load indexes: %w", err)
	}

	// Start background goroutines (skip for InMemory mode with no flush interval).
	if flushInt > 0 {
		go bs.flushLoop()
	} else {
		close(bs.flushDone)
	}
	if gcInt > 0 && !cfg.InMemory {
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
			nid := snowflake.ID(parseIDFromKey(key, 3))
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
			nid := snowflake.ID(parseIDFromKey(key, 1))
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
			rid := snowflake.ID(parseIDFromKey(key, 3))
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
			startID := snowflake.ID(parseIDFromKey(key, 1))
			relID := snowflake.ID(parseRelIDFromAdjKey(key))
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
			endID := snowflake.ID(parseIDFromKey(key, 1))
			relID := snowflake.ID(parseRelIDFromAdjKey(key))
			if bs.inIdx[endID] == nil {
				bs.inIdx[endID] = make(map[snowflake.ID]struct{})
			}
			bs.inIdx[endID][relID] = struct{}{}
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
	intID := int64(id)

	w := nodeToWire(n)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal node: %w", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// Check for duplicate.
	if _, exists := bs.nodeIDs[id]; exists {
		return ErrNodeExists
	}

	// Update in-memory state.
	bs.nodeCache.Put(id, n)
	bs.nodeIDs[id] = struct{}{}

	// Build write ops.
	ops := []writeOp{{opType: writeOpSet, key: nodeKey(intID), value: data}}
	for _, tok := range n.AllLabelTokens() {
		tv := tok.Value()
		if bs.labelIdx[tv] == nil {
			bs.labelIdx[tv] = make(map[snowflake.ID]struct{})
		}
		bs.labelIdx[tv][id] = struct{}{}
		ops = append(ops, writeOp{opType: writeOpSet, key: labelIndexKey(tv, intID)})
	}

	bs.appendOps(ops...)
	bs.nodeCount.Add(1)
	return nil
}

// GetNode retrieves a node by its snowflake ID.
// Cache-first: checks LRU cache, then nodeIDs (O(1) existence check),
// then falls through to Badger only if the node is confirmed to exist.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) GetNode(id snowflake.ID) (*types.Node, error) {
	// Check cache first.
	v, status := bs.nodeCache.Get(id)
	switch status {
	case cacheHit:
		return v, nil
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
		item, err := txn.Get(nodeKey(int64(id)))
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
	return n, nil
}

// DeleteNode removes a node and its label index entries.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNode(id snowflake.ID) error {
	intID := int64(id)

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.nodeIDs[id]; !exists {
		return ErrNodeNotFound
	}

	// Get node data for label token cleanup.
	n, err := bs.getNodeLocked(id)
	if err != nil {
		return err
	}

	// Build delete ops.
	ops := []writeOp{{opType: writeOpDelete, key: nodeKey(intID)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, intID)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
	}

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)
	return nil
}

// --- Relationship operations ---

// PutRelationship stores a relationship with type index and adjacency entries.
// Returns ErrNodeNotFound if the start or end node does not exist.
// Returns ErrRelExists if a relationship with the same ID already exists.
func (bs *BadgerStore) PutRelationship(r *types.Relationship) error {
	id := r.InternalID().SnowflakeID()
	intID := int64(id)
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()
	relType := r.TypeToken().Value()

	w := relToWire(r)
	data, err := msgpack.Marshal(w)
	if err != nil {
		return fmt.Errorf("graph: marshal relationship: %w", err)
	}

	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	// Verify endpoints exist.
	if _, exists := bs.nodeIDs[startID]; !exists {
		return ErrNodeNotFound
	}
	if _, exists := bs.nodeIDs[endID]; !exists {
		return ErrNodeNotFound
	}

	// Check for duplicate via O(1) relIDs.
	if _, exists := bs.relIDs[id]; exists {
		return ErrRelExists
	}

	// Update in-memory state.
	bs.relCache.Put(id, r)
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
		bs.inIdx[endID] = make(map[snowflake.ID]struct{})
	}
	bs.inIdx[endID][id] = struct{}{}

	// Build write ops.
	ops := []writeOp{
		{opType: writeOpSet, key: relKey(intID), value: data},
		{opType: writeOpSet, key: relTypeIndexKey(relType, intID)},
		{opType: writeOpSet, key: outKey(int64(startID), relType, int64(endID), intID)},
		{opType: writeOpSet, key: inKey(int64(endID), relType, int64(startID), intID)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(1)
	return nil
}

// GetRelationship retrieves a relationship by its snowflake ID.
// Cache-first: checks LRU cache before falling through to Badger.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) GetRelationship(id snowflake.ID) (*types.Relationship, error) {
	// Check cache first.
	v, status := bs.relCache.Get(id)
	switch status {
	case cacheHit:
		return v, nil
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
		item, err := txn.Get(relKey(int64(id)))
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
	return r, nil
}

// DeleteRelationship removes a relationship and cleans up type + adjacency indexes.
// Returns ErrRelNotFound if the relationship does not exist.
func (bs *BadgerStore) DeleteRelationship(id snowflake.ID) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	return bs.deleteRelLocked(id)
}

// deleteRelLocked removes a relationship and cleans up indexes.
// Caller must hold bs.idxMu write lock.
func (bs *BadgerStore) deleteRelLocked(id snowflake.ID) error {
	intID := int64(id)

	// Get rel data (cache or Badger).
	r, err := bs.getRelLocked(id)
	if err != nil {
		return err
	}

	relType := r.TypeToken().Value()
	startID := r.StartNodeID().SnowflakeID()
	endID := r.EndNodeID().SnowflakeID()

	// Update in-memory state.
	bs.relCache.MarkDeleted(id)
	delete(bs.relIDs, id)

	// Type index cleanup.
	if set, exists := bs.typeIdx[relType]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.typeIdx, relType)
		}
	}

	// Adjacency cleanup.
	if set, exists := bs.outIdx[startID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.outIdx, startID)
		}
	}
	if set, exists := bs.inIdx[endID]; exists {
		delete(set, id)
		if len(set) == 0 {
			delete(bs.inIdx, endID)
		}
	}

	// Build delete ops.
	ops := []writeOp{
		{opType: writeOpDelete, key: relKey(intID)},
		{opType: writeOpDelete, key: relTypeIndexKey(relType, intID)},
		{opType: writeOpDelete, key: outKey(int64(startID), relType, int64(endID), intID)},
		{opType: writeOpDelete, key: inKey(int64(endID), relType, int64(startID), intID)},
	}

	bs.appendOps(ops...)
	bs.relCount.Add(-1)
	return nil
}

// --- Index queries ---

// NodesByLabel returns all nodes with the given label token.
// Results are sorted by snowflake.ID (chronological) for deterministic output.
func (bs *BadgerStore) NodesByLabel(token uint16) ([]*types.Node, error) {
	bs.idxMu.RLock()
	set := bs.labelIdx[token]
	ids := make([]snowflake.ID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	bs.idxMu.RUnlock()

	if len(ids) == 0 {
		return nil, nil
	}

	nodes := make([]*types.Node, 0, len(ids))
	for _, id := range ids {
		n, err := bs.GetNode(id)
		if err != nil {
			if errors.Is(err, ErrNodeNotFound) {
				continue // index orphan or tombstone
			}
			return nil, fmt.Errorf("graph: query node %d: %w", id, err)
		}
		nodes = append(nodes, n)
	}

	sortNodesByID(nodes)
	return nodes, nil
}

// RelationshipsByType returns all relationships with the given type token.
// Results are sorted by snowflake.ID (chronological) for deterministic output.
func (bs *BadgerStore) RelationshipsByType(token uint16) ([]*types.Relationship, error) {
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

	rels := make([]*types.Relationship, 0, len(ids))
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
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

// --- Adjacency queries ---

// OutgoingRelationships returns relationships starting from the given node.
// If typeToken is 0, returns all outgoing; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) OutgoingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
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
		r, err := bs.GetRelationship(id)
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

// IncomingRelationships returns relationships ending at the given node.
// If typeToken is 0, returns all incoming; otherwise filters by type.
// Results are sorted by snowflake.ID for deterministic output.
func (bs *BadgerStore) IncomingRelationships(nodeID snowflake.ID, typeToken uint16) ([]*types.Relationship, error) {
	bs.idxMu.RLock()
	set := bs.inIdx[nodeID]
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
		r, err := bs.GetRelationship(id)
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

// --- Cascade operations ---

// DeleteNodeCascade atomically removes a node and all connected relationships.
// In-memory state is updated atomically under idxMu write lock. Badger writes
// are queued for the next flush.
// Returns ErrNodeNotFound if the node does not exist.
func (bs *BadgerStore) DeleteNodeCascade(id snowflake.ID) error {
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()

	if _, exists := bs.nodeIDs[id]; !exists {
		return ErrNodeNotFound
	}

	// Collect all connected relIDs (dedup self-loops).
	relIDs := make(map[snowflake.ID]struct{})
	for relID := range bs.outIdx[id] {
		relIDs[relID] = struct{}{}
	}
	for relID := range bs.inIdx[id] {
		relIDs[relID] = struct{}{}
	}

	// Delete each relationship.
	for relID := range relIDs {
		if err := bs.deleteRelLocked(relID); err != nil {
			if errors.Is(err, ErrRelNotFound) {
				continue // tolerate already-deleted rels
			}
			return fmt.Errorf("graph: cascade delete relationship: %w", err)
		}
	}

	// Get node data for label cleanup.
	n, err := bs.getNodeLocked(id)
	if err != nil {
		// Node was in nodeIDs but can't be loaded (data corruption or cache miss
		// with closed DB). Still proceed with cleanup — scrub labelIdx by scanning
		// ALL label sets to prevent orphaned index entries (perma-leak).
		intID := int64(id)
		ops := []writeOp{{opType: writeOpDelete, key: nodeKey(intID)}}
		for tok, set := range bs.labelIdx {
			if _, exists := set[id]; exists {
				delete(set, id)
				if len(set) == 0 {
					delete(bs.labelIdx, tok)
				}
				ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, intID)})
			}
		}
		bs.nodeCache.MarkDeleted(id)
		delete(bs.nodeIDs, id)
		bs.appendOps(ops...)
		bs.nodeCount.Add(-1)
		return nil
	}

	// Build delete ops for node.
	intID := int64(id)
	ops := []writeOp{{opType: writeOpDelete, key: nodeKey(intID)}}

	// Remove label index entries.
	allTokens := collectNodeLabelTokens(n)
	for _, tok := range allTokens {
		ops = append(ops, writeOp{opType: writeOpDelete, key: labelIndexKey(tok, intID)})
		if set, exists := bs.labelIdx[tok]; exists {
			delete(set, id)
			if len(set) == 0 {
				delete(bs.labelIdx, tok)
			}
		}
	}

	// Update in-memory state.
	bs.nodeCache.MarkDeleted(id)
	delete(bs.nodeIDs, id)
	bs.appendOps(ops...)
	bs.nodeCount.Add(-1)

	// Note: relCount was already adjusted by deleteRelLocked for each rel.
	// The cascade counter is handled independently by deleteRelLocked's relCount.Add(-1).
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

// --- Background flush ---

// flushLoop periodically drains the write buffer to Badger.
func (bs *BadgerStore) flushLoop() {
	defer close(bs.flushDone)
	ticker := time.NewTicker(bs.flushInt)
	defer ticker.Stop()
	for {
		select {
		case <-bs.stopCh:
			_ = bs.flush() // #nosec G104 — best-effort final flush; failed ops are re-queued internally
			return
		case <-ticker.C:
			_ = bs.flush() // #nosec G104 — best-effort periodic flush; failed ops are re-queued internally
		}
	}
}

// Flush synchronously drains the write buffer to Badger. Exported for testing.
func (bs *BadgerStore) Flush() error {
	return bs.flush()
}

// flush drains the write buffer to Badger via WriteBatch.
//
// The flush holds idxMu.RLock during the snapshot+swap phase to prevent any
// writer from being between cache.Put and appendOps (all writers hold
// idxMu.Lock for their entire mutation). This guarantees that the dirty
// version snapshot, pending ops, and counter values are consistent.
//
// Counters are included in the same WriteBatch as entity ops — no TOCTOU
// window between data and counter persistence.
func (bs *BadgerStore) flush() error {
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
		if e := bs.db.Close(); e != nil && err == nil {
			err = e
		}
	})
	return err
}

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

// getNodeLocked retrieves a node from cache or Badger.
// Caller must hold bs.idxMu (read or write).
func (bs *BadgerStore) getNodeLocked(id snowflake.ID) (*types.Node, error) {
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
		item, err := txn.Get(nodeKey(int64(id)))
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
func (bs *BadgerStore) getRelLocked(id snowflake.ID) (*types.Relationship, error) {
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
		item, err := txn.Get(relKey(int64(id)))
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

// collectNodeLabelTokens returns all label token values from a node.
func collectNodeLabelTokens(n *types.Node) []uint16 {
	tokens := n.AllLabelTokens()
	result := make([]uint16, len(tokens))
	for i, t := range tokens {
		result[i] = t.Value()
	}
	return result
}
