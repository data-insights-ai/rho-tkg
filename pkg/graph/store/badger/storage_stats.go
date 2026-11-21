package badger

// CacheMetrics is one of badger's ristretto caches (the block cache or the
// index cache) as its counters stand: Hits and Misses are lookups since the
// open, KeysAdded/KeysEvicted the entries admitted and evicted, and
// CostAdded/CostEvicted their bytes (a block's cost is its decoded size), so
// CostAdded-CostEvicted is what the cache holds now. MaxCost is the size the
// cache was opened with; 0 with every counter 0: the store has no such cache.
type CacheMetrics struct {
	MaxCost                   int64
	Hits, Misses              uint64
	KeysAdded, KeysEvicted    uint64
	CostAdded, CostEvicted    uint64
	SetsDropped, SetsRejected uint64
}

// Add sums two caches' counters (MaxCost included).
func (m CacheMetrics) Add(o CacheMetrics) CacheMetrics {
	return CacheMetrics{
		MaxCost: m.MaxCost + o.MaxCost, Hits: m.Hits + o.Hits, Misses: m.Misses + o.Misses,
		KeysAdded: m.KeysAdded + o.KeysAdded, KeysEvicted: m.KeysEvicted + o.KeysEvicted,
		CostAdded: m.CostAdded + o.CostAdded, CostEvicted: m.CostEvicted + o.CostEvicted,
		SetsDropped: m.SetsDropped + o.SetsDropped, SetsRejected: m.SetsRejected + o.SetsRejected,
	}
}

// Held is the bytes the cache holds now (CostAdded less CostEvicted).
func (m CacheMetrics) Held() int64 {
	if m.CostEvicted >= m.CostAdded {
		return 0
	}
	return int64(m.CostAdded - m.CostEvicted) // #nosec G115 -- bounded by MaxCost
}

// StorageStats is one open store's LSM tree and caches (BLOCKCACHE): what
// its tables hold on disk and decoded, the bytes of their block indexes and
// bloom filters (resident while the store is open: badger keeps them in the
// mapped table, or in the index cache when IndexCacheSize is set), and the
// block and index caches' counters. Hits and misses of the block cache are
// the data-block reads of the open; a block cache sized below the blocks a
// workload reads again shows misses and evictions, a store without one
// (NoBlockCache) shows zeros.
type StorageStats struct {
	Tables            int
	LSMBytes          int64 // the tables' bytes on disk (compressed when compression is on)
	UncompressedBytes int64 // the tables' bytes decoded
	IndexBytes        int64 // the tables' block indexes
	FilterBytes       int64 // the tables' bloom filters
	BlockCache        CacheMetrics
	IndexCache        CacheMetrics
}

// StorageStats returns the store's LSM tree and cache counters. Cheap: it
// walks badger's table list (one entry per table) and reads atomic counters;
// it reads no table. Zero on a closed store; a Close waits for it.
func (bs *Store) StorageStats() StorageStats {
	var s StorageStats
	if bs == nil || bs.db == nil {
		return s
	}
	bs.statsMu.RLock()
	defer bs.statsMu.RUnlock()
	if bs.db.IsClosed() {
		return s
	}
	for _, t := range bs.db.Tables() {
		s.Tables++
		s.LSMBytes += int64(t.OnDiskSize)
		s.UncompressedBytes += int64(t.UncompressedSize)
		s.IndexBytes += int64(t.IndexSz)
		s.FilterBytes += int64(t.BloomFilterSize)
	}
	opts := bs.db.Opts()
	s.BlockCache = cacheMetrics(bs.db.BlockCacheMetrics(), opts.BlockCacheSize)
	s.IndexCache = cacheMetrics(bs.db.IndexCacheMetrics(), opts.IndexCacheSize)
	return s
}

// ristrettoMetrics is the part of *ristretto.Metrics StorageStats reads; its
// methods answer 0 on a nil receiver (no cache).
type ristrettoMetrics interface {
	Hits() uint64
	Misses() uint64
	KeysAdded() uint64
	KeysEvicted() uint64
	CostAdded() uint64
	CostEvicted() uint64
	SetsDropped() uint64
	SetsRejected() uint64
}

func cacheMetrics(m ristrettoMetrics, maxCost int64) CacheMetrics {
	c := CacheMetrics{
		Hits: m.Hits(), Misses: m.Misses(), KeysAdded: m.KeysAdded(), KeysEvicted: m.KeysEvicted(),
		CostAdded: m.CostAdded(), CostEvicted: m.CostEvicted(), SetsDropped: m.SetsDropped(), SetsRejected: m.SetsRejected(),
	}
	if c != (CacheMetrics{}) || maxCost > 0 {
		c.MaxCost = maxCost
	}
	return c
}
