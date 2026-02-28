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
	key     snowflake.ID
	value   V
	dirty   bool // modified since last flush
	deleted bool // tombstone — entity has been deleted
}

// entityLRU is a generic LRU cache with dirty tracking and tombstone support.
// Clean entries are evicted when capacity is exceeded; dirty entries are never
// evicted until they are flushed to durable storage.
//
// All methods are thread-safe via internal mutex.
type entityLRU[V any] struct {
	mu       sync.Mutex
	capacity int // soft limit — dirty entries can exceed
	items    map[snowflake.ID]*list.Element
	order    *list.List // front = most recent, back = LRU
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

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*lruEntry[V])
		entry.value = value
		entry.dirty = true
		entry.deleted = false
		c.order.MoveToFront(el)
		return
	}

	entry := &lruEntry[V]{key: key, value: value, dirty: true}
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

	if el, ok := c.items[key]; ok {
		entry := el.Value.(*lruEntry[V])
		entry.deleted = true
		entry.dirty = true
		c.order.MoveToFront(el)
		return
	}

	// Insert tombstone for a key not currently cached.
	var zero V
	entry := &lruEntry[V]{key: key, deleted: true, dirty: true, value: zero}
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

	entry := &lruEntry[V]{key: key, value: value, dirty: false}
	el := c.order.PushFront(entry)
	c.items[key] = el

	c.evictClean()
}

// CollectDirty returns all dirty entries and marks them clean.
// Clean tombstones are removed from the cache entirely.
// Called by the flush loop after successfully writing to Badger.
func (c *entityLRU[V]) CollectDirty() []lruEntry[V] {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirty []lruEntry[V]
	for el := c.order.Front(); el != nil; {
		entry := el.Value.(*lruEntry[V])
		next := el.Next()
		if entry.dirty {
			dirty = append(dirty, *entry) // copy
			entry.dirty = false
			// Clean tombstones can be removed — the delete has been persisted.
			if entry.deleted {
				c.order.Remove(el)
				delete(c.items, entry.key)
			}
		}
		el = next
	}
	return dirty
}

// Len returns the number of entries in the cache (including tombstones).
func (c *entityLRU[V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

// evictClean removes the LRU clean entry if the cache exceeds capacity.
// Only clean (non-dirty) entries are candidates for eviction.
// Must be called with c.mu held.
func (c *entityLRU[V]) evictClean() {
	for len(c.items) > c.capacity {
		// Walk from back (LRU) to find a clean entry.
		evicted := false
		for el := c.order.Back(); el != nil; el = el.Prev() {
			entry := el.Value.(*lruEntry[V])
			if !entry.dirty {
				c.order.Remove(el)
				delete(c.items, entry.key)
				evicted = true
				break
			}
		}
		if !evicted {
			break // all entries dirty — allow temporary overflow
		}
	}
}
