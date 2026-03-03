package graph

// GraphStats holds operation counters and optional cache metrics for a Graph.
// Cache metrics are populated only when the underlying store is a BadgerStore;
// they are zero for MemoryStore and TieredStore.
type GraphStats struct {
	// Operation counters — incremented on every successful operation.
	NodesAdded   int64
	NodesRead    int64
	NodesUpdated int64
	NodesDeleted int64
	RelsAdded    int64
	RelsRead     int64
	RelsUpdated  int64
	RelsDeleted  int64

	// Cache metrics — populated for BadgerStore only, zero otherwise.
	// Both cacheHit and cacheDeleted (tombstone) results count as hits,
	// because both avoid a Badger read. cacheMiss counts as a miss.
	NodeCacheHits   int64
	NodeCacheMisses int64
	RelCacheHits    int64
	RelCacheMisses  int64
}

// StoreStats is an optional interface for stores that expose LRU cache metrics.
// Graph.Stats() type-asserts the underlying store to this interface — implementing
// it does not affect the Store interface contract.
type StoreStats interface {
	NodeCacheHits() int64
	NodeCacheMisses() int64
	RelCacheHits() int64
	RelCacheMisses() int64
}
