package index

import (
	"cmp"
	"container/list"
	"slices"
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// CacheStatus indicates the result of an LRU cache lookup.
type CacheStatus int

// CacheStatus values returned by LRU lookups.
const (
	CacheMiss    CacheStatus = iota // key not in cache
	CacheHit                        // key found, value valid
	CacheDeleted                    // key tombstoned (deleted but not yet flushed)
)

// Entry is a single cached entity with dirty/tombstone tracking.
type Entry[V any] struct {
	Key      snowflake.ID
	Value    V
	Size     int64  // accounted bytes (sizer estimate + per-entry overhead); 0 when byte budgeting is off
	DirtyVer uint64 // 0 = clean; >0 = dirty (monotonic mutation version)
	Deleted  bool   // tombstone — entity has been deleted
}

// perEntryOverhead approximates the cache's own per-entry cost — the
// list.Element, the items/dirtySet map bucket shares, and the Entry struct.
// Added to every sizer estimate so a byte budget reflects resident memory,
// not just payload bytes.
const perEntryOverhead = 160

// Cache is a generic LRU cache with dirty tracking and tombstone support.
// Clean entries are evicted when capacity is exceeded; dirty entries are never
// evicted until they are flushed to durable storage.
//
// Dirty tracking uses a monotonic version counter. Each mutation (Put, MarkDeleted)
// increments the counter and stamps the entry with that version. CollectDirty
// returns snapshots including the version; MarkFlushed only clears dirty on entries
// whose version still matches — entries re-dirtied during a flush cycle are not
// affected.
//
// All methods are thread-safe via internal mutex.
type Cache[V any] struct {
	mu         sync.RWMutex
	capacity   int  // soft limit — dirty entries can exceed
	noEvict    bool // resident mode: clean entries are never evicted (see SetNoEvict)
	cleanCount int  // number of evictable entries (DirtyVer == 0, !Deleted)
	items      map[snowflake.ID]*list.Element
	dirtySet   map[snowflake.ID]*list.Element // entries with DirtyVer > 0 — flush-cycle index
	order      *list.List                     // front = most recent, back = LRU
	nextVer    uint64                         // monotonic dirty version counter
	budget     int64                          // byte budget; 0 = count-only eviction
	totalBytes int64                          // accounted bytes across all entries
	sizer      func(V) int64                  // value-size estimator; nil = byte budgeting off
	hits       atomic.Int64                   // total cache hits (CacheHit + CacheDeleted)
	misses     atomic.Int64                   // total cache misses (CacheMiss)
}

// NewCache creates an LRU cache with the given capacity.
// Capacity is a soft limit: dirty entries are never evicted, so the cache
// may temporarily exceed this size under write pressure.
func NewCache[V any](capacity int) *Cache[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &Cache[V]{
		capacity: capacity,
		items:    make(map[snowflake.ID]*list.Element, capacity),
		dirtySet: make(map[snowflake.ID]*list.Element),
		order:    list.New(),
	}
}

// NewCacheWithBudget creates an LRU cache bounded by BOTH an entry-count
// capacity and a byte budget: clean LRU entries are evicted while either
// limit is exceeded. sizer estimates a value's resident bytes (the cache
// adds perEntryOverhead per entry). Both limits are SOFT — dirty entries
// are never evicted, so the cache may exceed them under write pressure.
// budget <= 0 or a nil sizer disables byte accounting (capacity-only,
// identical to NewCache).
func NewCacheWithBudget[V any](capacity int, budget int64, sizer func(V) int64) *Cache[V] {
	c := NewCache[V](capacity)
	if budget > 0 && sizer != nil {
		c.budget = budget
		c.sizer = sizer
	}
	return c
}

// ensureInitializedLocked makes the zero value behave like NewCache(1).
// Must be called with c.mu held.
func (c *Cache[V]) ensureInitializedLocked() {
	if c.capacity < 1 {
		c.capacity = 1
	}
	if c.items == nil {
		c.items = make(map[snowflake.ID]*list.Element, c.capacity)
	}
	if c.dirtySet == nil {
		c.dirtySet = make(map[snowflake.ID]*list.Element)
	}
	if c.order == nil {
		c.order = list.New()
	}
}

// Get looks up a key in the cache.
// Returns the value and cache status (CacheMiss, CacheHit, or CacheDeleted).
// Moves the entry to the front (most recently used) on hit.
func (c *Cache[V]) Get(key snowflake.ID) (V, CacheStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		var zero V
		c.misses.Add(1)
		return zero, CacheMiss
	}

	entry := el.Value.(*Entry[V])
	// Resident mode keeps no eviction order, so the LRU promotion is pure cost.
	if !c.noEvict {
		c.order.MoveToFront(el)
	}

	if entry.Deleted {
		var zero V
		c.hits.Add(1)
		return zero, CacheDeleted
	}

	c.hits.Add(1)
	return entry.Value, CacheHit
}

// GetNoPromote looks up a key WITHOUT promoting it to most-recently-used.
// Unlike Get it takes only a READ lock and never touches the recency list, so
// concurrent callers do not serialize on the exclusive mutex. Intended for
// full-cardinality SCAN reads (every prefetch*Scan path — label, type, all,
// temporal, and numeric-range scans, not only ForEachByLabel / ForEachByType): a large scan
// must not pay one exclusive Lock + MoveToFront per row, and scanned rows are
// not "hot" — promoting them would evict genuinely hot point-read entries (the
// same rationale as the no-cache-fill scan path). Point reads keep using Get so
// revisited entries stay warm.
//
// Sound under RLock: it reads only c.items (the map) and the entry's
// Value/Deleted fields, which are mutated only by writers (Put, MarkDeleted,
// eviction, flush) that hold the EXCLUSIVE Lock. RLock excludes Lock, so no
// torn read of the map, the recency list, or an entry is possible. It never
// touches c.order / c.totalBytes / cleanCount; hits/misses are atomic.
func (c *Cache[V]) GetNoPromote(key snowflake.ID) (V, CacheStatus) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	el, ok := c.items[key]
	if !ok {
		var zero V
		c.misses.Add(1)
		return zero, CacheMiss
	}

	entry := el.Value.(*Entry[V])
	if entry.Deleted {
		var zero V
		c.hits.Add(1)
		return zero, CacheDeleted
	}

	c.hits.Add(1)
	return entry.Value, CacheHit
}

// entrySizeLocked estimates a value's accounted bytes. Returns 0 when byte
// budgeting is off so totalBytes stays 0 and overBudgetLocked never fires.
func (c *Cache[V]) entrySizeLocked(value V) int64 {
	if c.sizer == nil {
		return 0
	}
	return c.sizer(value) + perEntryOverhead
}

// resizeEntryLocked re-accounts an entry whose value changed.
func (c *Cache[V]) resizeEntryLocked(entry *Entry[V], value V) {
	size := c.entrySizeLocked(value)
	c.totalBytes += size - entry.Size
	entry.Size = size
}

func (c *Cache[V]) overBudgetLocked() bool {
	return c.budget > 0 && c.totalBytes > c.budget
}

// Put inserts or updates a value in the cache, marking it dirty.
// Moves the entry to the front. Triggers eviction of LRU clean entries
// if the cache exceeds capacity or the byte budget.
func (c *Cache[V]) Put(key snowflake.ID, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()

	c.nextVer++

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer == 0 && !entry.Deleted {
			c.cleanCount-- // was clean, now dirty
		}
		entry.Value = value
		c.resizeEntryLocked(entry, value)
		entry.DirtyVer = c.nextVer
		entry.Deleted = false
		c.dirtySet[key] = el
		c.order.MoveToFront(el)
		// Updates never change the entry COUNT, but growing the value can
		// exceed the byte budget — shed clean LRU entries to compensate.
		c.evictClean()
		return
	}

	entry := &Entry[V]{Key: key, Value: value, DirtyVer: c.nextVer, Size: c.entrySizeLocked(value)}
	c.totalBytes += entry.Size
	el := c.order.PushFront(entry)
	c.items[key] = el
	c.dirtySet[key] = el

	c.evictClean()
}

// MarkDeleted sets a tombstone for the key, marking it dirty.
// The tombstone prevents cache misses from falling through to Badger for
// entities that have been deleted but not yet flushed.
// If the key is not in the cache, a tombstone entry is inserted.
func (c *Cache[V]) MarkDeleted(key snowflake.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()

	c.nextVer++

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer == 0 && !entry.Deleted {
			c.cleanCount-- // was clean, now dirty tombstone
		}
		// Release the stale payload: Get/GetNoPromote/Peek never return
		// entry.Value once Deleted is set, so retaining it (and its
		// accounted Size) is pure waste — the value stays reachable via the
		// cache's own map (blocking GC) and, being dirty, is unevictable
		// until flush, so a large tombstoned payload would otherwise hold
		// its full byte-budget weight for the entire flush interval.
		// Shrink to the same tombstone-only footprint a fresh insert gets.
		var zero V
		entry.Value = zero
		if c.sizer != nil {
			newSize := int64(perEntryOverhead)
			c.totalBytes += newSize - entry.Size
			entry.Size = newSize
		}
		entry.Deleted = true
		entry.DirtyVer = c.nextVer
		c.dirtySet[key] = el
		c.order.MoveToFront(el)
		return
	}

	// Insert tombstone for a key not currently cached.
	var zero V
	entry := &Entry[V]{Key: key, Deleted: true, DirtyVer: c.nextVer, Value: zero}
	if c.sizer != nil {
		entry.Size = perEntryOverhead
		c.totalBytes += entry.Size
	}
	el := c.order.PushFront(entry)
	c.items[key] = el
	c.dirtySet[key] = el
	// The tombstone itself is dirty and unevictable, but its insertion can
	// push the cache over a limit — shed clean LRU entries to compensate.
	c.evictClean()
}

// LoadClean inserts an entry loaded from Badger (not dirty, immediately evictable).
// If the key already exists, this is a no-op (in-memory state takes precedence).
func (c *Cache[V]) LoadClean(key snowflake.ID, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()

	if _, ok := c.items[key]; ok {
		return // in-memory state takes precedence
	}

	entry := &Entry[V]{Key: key, Value: value, Size: c.entrySizeLocked(value)} // DirtyVer 0 = clean
	c.totalBytes += entry.Size
	el := c.order.PushFront(entry)
	c.items[key] = el
	c.cleanCount++

	c.evictClean()
}

// CollectDirty returns a snapshot of all dirty entries without modifying state.
// The returned entries include their DirtyVer, which must be passed to MarkFlushed
// after a successful write to durable storage. Calling CollectDirty multiple times
// without MarkFlushed returns the same (or superset of) entries.
func (c *Cache[V]) CollectDirty() []Entry[V] {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()

	// Iterate the dirty index, not the whole LRU list — pre-fix this walked
	// EVERY cached entry under c.mu per flush cycle (100ms): with a
	// multi-million-entry cache the walk starved writers and collapsed
	// ingestion (measured: 1M-node load stalled indefinitely at 2M capacity,
	// flat ~31k rows/s at 10k capacity). Sorted by key for deterministic
	// flush batches.
	dirty := make([]Entry[V], 0, len(c.dirtySet))
	for _, el := range c.dirtySet {
		entry := el.Value.(*Entry[V])
		dirty = append(dirty, *entry) // copy with DirtyVer snapshot
	}
	// slices.SortFunc, not sort.Slice — this runs once per 100ms flush
	// cycle and reflection sorting showed up in ingestion profiles.
	slices.SortFunc(dirty, func(a, b Entry[V]) int { return cmp.Compare(a.Key, b.Key) })
	return dirty
}

// MarkFlushed clears the dirty flag on entries whose DirtyVer matches the given
// version. Entries that were re-dirtied since collection (higher DirtyVer) retain
// their dirty status — they will be included in the next CollectDirty cycle.
// Clean tombstones (Deleted + DirtyVer cleared) are removed from the cache.
//
// This must only be called after the data for these entries has been successfully
// persisted to durable storage.
func (c *Cache[V]) MarkFlushed(flushed map[snowflake.ID]uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, ver := range flushed {
		el, ok := c.items[id]
		if !ok {
			continue
		}
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer != ver {
			continue // re-dirtied since collection; leave dirty
		}
		entry.DirtyVer = 0 // mark clean
		delete(c.dirtySet, id)
		// Clean tombstones can be removed — the delete has been persisted.
		if entry.Deleted {
			c.totalBytes -= entry.Size
			c.order.Remove(el)
			delete(c.items, id)
		} else {
			c.cleanCount++ // dirty → clean, not deleted
		}
	}
	c.evictClean() // newly clean entries may put the cache over a limit
}

// Peek returns a cached value WITHOUT deep-copy or MRU promotion.
// Useful for checking metadata on cached entities without allocation.
func (c *Cache[V]) Peek(key snowflake.ID) (V, CacheStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		var zero V
		return zero, CacheMiss
	}

	entry := el.Value.(*Entry[V])
	if entry.Deleted {
		var zero V
		return zero, CacheDeleted
	}

	return entry.Value, CacheHit
}

// Cap returns the configured capacity of the cache.
func (c *Cache[V]) Cap() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()
	return c.capacity
}

// Len returns the number of entries in the cache (including tombstones).
func (c *Cache[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// SetNoEvict puts the cache into resident mode: clean entries are never
// evicted, so a value decoded once stays decoded. Intended for in-memory stores
// where the backing data already lives in RAM and re-decoding on cache miss is
// pure waste that makes large-graph traversal scale super-linearly. The caller
// owns the memory trade-off (the decoded working set stays resident). Callers
// in resident mode should fetch via GetNoPromote — with no eviction there is no
// LRU order to maintain, so the per-fetch MoveToFront write-lock is pure cost.
func (c *Cache[V]) SetNoEvict() {
	c.mu.Lock()
	c.noEvict = true
	c.mu.Unlock()
}

// CleanCount returns the number of clean (evictable) entries in the cache.
func (c *Cache[V]) CleanCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cleanCount
}

// ResetForTest clears every entry from the cache without touching the
// underlying Store. Test-only — equivalent to dropping the in-memory
// cache view and forcing subsequent reads to fall through to Badger.
// Used by failure-injection tests that need to simulate a cold cache.
func (c *Cache[V]) ResetForTest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureInitializedLocked()
	c.items = make(map[snowflake.ID]*list.Element, c.capacity)
	c.dirtySet = make(map[snowflake.ID]*list.Element)
	c.order.Init()
	c.cleanCount = 0
	c.totalBytes = 0
}

// EvictForTest removes the entry for key without touching Badger or
// dirty/tombstone bookkeeping in the surrounding store. Test-only —
// production code uses Put/MarkDeleted/evictClean. Used by failure-
// injection tests that need a divergence between the in-memory cache
// and the underlying store (e.g. simulating a Phase-2 ErrRelNotFound
// race in RunRepair). No-op if the key is absent.
//
// Returns true if an entry was evicted. The cache's clean/dirty
// invariants are restored: the evicted entry's accounting is removed.
func (c *Cache[V]) EvictForTest(key snowflake.ID) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return false
	}
	entry := el.Value.(*Entry[V])
	if !entry.Deleted && entry.DirtyVer == 0 {
		c.cleanCount--
	}
	c.totalBytes -= entry.Size
	c.order.Remove(el)
	delete(c.items, key)
	delete(c.dirtySet, key)
	return true
}

// Bytes returns the accounted byte total across all entries (sizer
// estimates + per-entry overhead). Always 0 when byte budgeting is off.
func (c *Cache[V]) Bytes() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalBytes
}

// Budget returns the configured byte budget (0 = byte budgeting off).
func (c *Cache[V]) Budget() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.budget
}

// Hits returns the total number of cache hits (CacheHit + CacheDeleted) since creation.
func (c *Cache[V]) Hits() int64 { return c.hits.Load() }

// Misses returns the total number of cache misses (CacheMiss) since creation.
func (c *Cache[V]) Misses() int64 { return c.misses.Load() }

// evictClean removes LRU clean entries until the cache is within BOTH
// limits: the entry-count capacity and (when configured) the byte budget.
// Only clean (DirtyVer == 0, !Deleted) entries are candidates for eviction.
// Maintains cleanCount for O(1) early exit when no clean entries exist;
// otherwise O(N) single-pass backward scan.
// Must be called with c.mu held.
func (c *Cache[V]) evictClean() {
	// Resident mode: decoded entries are kept for the process lifetime. For an
	// in-memory store this turns the per-fetch decode-on-miss (msgpack unmarshal
	// + wire-decode) — which makes graph-larger-than-cache traversal scale
	// super-linearly — into a one-time decode, restoring linear (Memgraph-like)
	// big-O at the cost of holding the decoded working set resident.
	if c.noEvict {
		return
	}
	if c.cleanCount == 0 || (len(c.items) <= c.capacity && !c.overBudgetLocked()) {
		return
	}
	el := c.order.Back()
	for (len(c.items) > c.capacity || c.overBudgetLocked()) && el != nil && c.cleanCount > 0 {
		prev := el.Prev() // save before potential removal
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer == 0 && !entry.Deleted {
			c.totalBytes -= entry.Size
			c.order.Remove(el)
			delete(c.items, entry.Key)
			c.cleanCount--
		}
		el = prev
	}
}
