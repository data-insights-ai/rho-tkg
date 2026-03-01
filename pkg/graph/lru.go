package graph

import (
	"container/list"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// cacheStatus indicates the result of an LRU cache lookup.
type cacheStatus int

const (
	cacheMiss    cacheStatus = iota // key not in cache
	cacheHit                        // key found, value valid
	cacheDeleted                    // key tombstoned (deleted but not yet flushed)
)

// lruEntry is a single cached entity with dirty/tombstone tracking.
type lruEntry[V any] struct {
	key      snowflake.ID
	value    V
	dirtyVer uint64 // 0 = clean; >0 = dirty (monotonic mutation version)
	deleted  bool   // tombstone — entity has been deleted
}

// entityLRU is a generic LRU cache with dirty tracking and tombstone support.
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
type entityLRU[V any] struct {
	mu         sync.Mutex
	capacity   int // soft limit — dirty entries can exceed
	cleanCount int // number of evictable entries (dirtyVer == 0, !deleted)
	items      map[snowflake.ID]*list.Element
	order      *list.List // front = most recent, back = LRU
	nextVer    uint64     // monotonic dirty version counter
}

// newEntityLRU creates an LRU cache with the given capacity.
// Capacity is a soft limit: dirty entries are never evicted, so the cache
// may temporarily exceed this size under write pressure.
func newEntityLRU[V any](capacity int) *entityLRU[V] {
	if capacity < 1 {
		capacity = 1
	}
	return &entityLRU[V]{
		capacity: capacity,
		items:    make(map[snowflake.ID]*list.Element, capacity),
		order:    list.New(),
	}
}

// Get looks up a key in the cache.
// Returns the value and cache status (cacheMiss, cacheHit, or cacheDeleted).
// Moves the entry to the front (most recently used) on hit.
func (c *entityLRU[V]) Get(key snowflake.ID) (V, cacheStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		var zero V
		return zero, cacheMiss
	}

	entry := el.Value.(*lruEntry[V])
	c.order.MoveToFront(el)

	if entry.deleted {
		var zero V
		return zero, cacheDeleted
	}

	return entry.value, cacheHit
}

// Put inserts or updates a value in the cache, marking it dirty.
// Moves the entry to the front. Triggers eviction of LRU clean entries
// if the cache exceeds capacity.
func (c *entityLRU[V]) Put(key snowflake.ID, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextVer++

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*lruEntry[V])
		if entry.dirtyVer == 0 && !entry.deleted {
			c.cleanCount-- // was clean, now dirty
		}
		entry.value = value
		entry.dirtyVer = c.nextVer
		entry.deleted = false
		c.order.MoveToFront(el)
		return
	}

	entry := &lruEntry[V]{key: key, value: value, dirtyVer: c.nextVer}
	el := c.order.PushFront(entry)
	c.items[key] = el

	c.evictClean()
}

// MarkDeleted sets a tombstone for the key, marking it dirty.
// The tombstone prevents cache misses from falling through to Badger for
// entities that have been deleted but not yet flushed.
// If the key is not in the cache, a tombstone entry is inserted.
func (c *entityLRU[V]) MarkDeleted(key snowflake.ID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.nextVer++

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*lruEntry[V])
		if entry.dirtyVer == 0 && !entry.deleted {
			c.cleanCount-- // was clean, now dirty tombstone
		}
		entry.deleted = true
		entry.dirtyVer = c.nextVer
		c.order.MoveToFront(el)
		return
	}

	// Insert tombstone for a key not currently cached.
	var zero V
	entry := &lruEntry[V]{key: key, deleted: true, dirtyVer: c.nextVer, value: zero}
	el := c.order.PushFront(entry)
	c.items[key] = el
	// No eviction — this is a dirty entry.
}

// LoadClean inserts an entry loaded from Badger (not dirty, immediately evictable).
// If the key already exists, this is a no-op (in-memory state takes precedence).
func (c *entityLRU[V]) LoadClean(key snowflake.ID, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.items[key]; ok {
		return // in-memory state takes precedence
	}

	entry := &lruEntry[V]{key: key, value: value} // dirtyVer 0 = clean
	el := c.order.PushFront(entry)
	c.items[key] = el
	c.cleanCount++

	c.evictClean()
}

// CollectDirty returns a snapshot of all dirty entries without modifying state.
// The returned entries include their dirtyVer, which must be passed to MarkFlushed
// after a successful write to durable storage. Calling CollectDirty multiple times
// without MarkFlushed returns the same (or superset of) entries.
func (c *entityLRU[V]) CollectDirty() []lruEntry[V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirty []lruEntry[V]
	for el := c.order.Front(); el != nil; el = el.Next() {
		entry := el.Value.(*lruEntry[V])
		if entry.dirtyVer > 0 {
			dirty = append(dirty, *entry) // copy with dirtyVer snapshot
		}
	}
	return dirty
}

// MarkFlushed clears the dirty flag on entries whose dirtyVer matches the given
// version. Entries that were re-dirtied since collection (higher dirtyVer) retain
// their dirty status — they will be included in the next CollectDirty cycle.
// Clean tombstones (deleted + dirtyVer cleared) are removed from the cache.
//
// This must only be called after the data for these entries has been successfully
// persisted to durable storage.
func (c *entityLRU[V]) MarkFlushed(flushed map[snowflake.ID]uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, ver := range flushed {
		el, ok := c.items[id]
		if !ok {
			continue
		}
		entry := el.Value.(*lruEntry[V])
		if entry.dirtyVer != ver {
			continue // re-dirtied since collection; leave dirty
		}
		entry.dirtyVer = 0 // mark clean
		// Clean tombstones can be removed — the delete has been persisted.
		if entry.deleted {
			c.order.Remove(el)
			delete(c.items, id)
		} else {
			c.cleanCount++ // dirty → clean, not deleted
		}
	}
}

// Len returns the number of entries in the cache (including tombstones).
func (c *entityLRU[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// CleanCount returns the number of clean (evictable) entries in the cache.
func (c *entityLRU[V]) CleanCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cleanCount
}

// evictClean removes LRU clean entries until the cache is at capacity.
// Only clean (dirtyVer == 0, !deleted) entries are candidates for eviction.
// Maintains cleanCount for O(1) early exit when no clean entries exist;
// otherwise O(N) single-pass backward scan.
// Must be called with c.mu held.
func (c *entityLRU[V]) evictClean() {
	if c.cleanCount == 0 || len(c.items) <= c.capacity {
		return
	}
	el := c.order.Back()
	for len(c.items) > c.capacity && el != nil && c.cleanCount > 0 {
		prev := el.Prev() // save before potential removal
		entry := el.Value.(*lruEntry[V])
		if entry.dirtyVer == 0 && !entry.deleted {
			c.order.Remove(el)
			delete(c.items, entry.key)
			c.cleanCount--
		}
		el = prev
	}
}
