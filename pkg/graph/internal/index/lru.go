package index

import (
	"container/list"
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
	DirtyVer uint64 // 0 = clean; >0 = dirty (monotonic mutation version)
	Deleted  bool   // tombstone — entity has been deleted
}

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
	mu         sync.Mutex
	capacity   int // soft limit — dirty entries can exceed
	cleanCount int // number of evictable entries (DirtyVer == 0, !Deleted)
	items      map[snowflake.ID]*list.Element
	order      *list.List   // front = most recent, back = LRU
	nextVer    uint64       // monotonic dirty version counter
	hits       atomic.Int64 // total cache hits (CacheHit + CacheDeleted)
	misses     atomic.Int64 // total cache misses (CacheMiss)
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
		order:    list.New(),
	}
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
	c.order.MoveToFront(el)

	if entry.Deleted {
		var zero V
		c.hits.Add(1)
		return zero, CacheDeleted
	}

	c.hits.Add(1)
	return entry.Value, CacheHit
}

// Put inserts or updates a value in the cache, marking it dirty.
// Moves the entry to the front. Triggers eviction of LRU clean entries
// if the cache exceeds capacity.
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
		entry.DirtyVer = c.nextVer
		entry.Deleted = false
		c.order.MoveToFront(el)
		return
	}

	entry := &Entry[V]{Key: key, Value: value, DirtyVer: c.nextVer}
	el := c.order.PushFront(entry)
	c.items[key] = el

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
		entry.Deleted = true
		entry.DirtyVer = c.nextVer
		c.order.MoveToFront(el)
		return
	}

	// Insert tombstone for a key not currently cached.
	var zero V
	entry := &Entry[V]{Key: key, Deleted: true, DirtyVer: c.nextVer, Value: zero}
	el := c.order.PushFront(entry)
	c.items[key] = el
	// No eviction — this is a dirty entry.
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

	entry := &Entry[V]{Key: key, Value: value} // DirtyVer 0 = clean
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

	var dirty []Entry[V]
	for el := c.order.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer > 0 {
			dirty = append(dirty, *entry) // copy with DirtyVer snapshot
		}
	}
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
		// Clean tombstones can be removed — the delete has been persisted.
		if entry.Deleted {
			c.order.Remove(el)
			delete(c.items, id)
		} else {
			c.cleanCount++ // dirty → clean, not deleted
		}
	}
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
	c.order.Init()
	c.cleanCount = 0
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
	c.order.Remove(el)
	delete(c.items, key)
	return true
}

// Hits returns the total number of cache hits (CacheHit + CacheDeleted) since creation.
func (c *Cache[V]) Hits() int64 { return c.hits.Load() }

// Misses returns the total number of cache misses (CacheMiss) since creation.
func (c *Cache[V]) Misses() int64 { return c.misses.Load() }

// evictClean removes LRU clean entries until the cache is at capacity.
// Only clean (DirtyVer == 0, !Deleted) entries are candidates for eviction.
// Maintains cleanCount for O(1) early exit when no clean entries exist;
// otherwise O(N) single-pass backward scan.
// Must be called with c.mu held.
func (c *Cache[V]) evictClean() {
	if c.cleanCount == 0 || len(c.items) <= c.capacity {
		return
	}
	el := c.order.Back()
	for len(c.items) > c.capacity && el != nil && c.cleanCount > 0 {
		prev := el.Prev() // save before potential removal
		entry := el.Value.(*Entry[V])
		if entry.DirtyVer == 0 && !entry.Deleted {
			c.order.Remove(el)
			delete(c.items, entry.Key)
			c.cleanCount--
		}
		el = prev
	}
}
