package tiered

import (
	"os"
	"path/filepath"
	"strings"

	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
)

// BlockCacheOpen is what a badger open tells Config.BlockCacheSizeFor: the
// handle's shard name (an event shard's as ShardStorageStats names it, an
// owner index "index"), its kind ("" for the owner index), whether it
// opens read-only, and the bytes of the tables it holds on disk before the
// open (0 in memory or for a new shard; compressed when compression is on).
type BlockCacheOpen struct {
	Name       string
	Kind       ShardKind
	ReadOnly   bool
	TableBytes int64
}

// ShardStorageStats is one open shard's LSM tree and badger caches
// (badger.StorageStats), keyed by Name as ShardCacheStats is.
type ShardStorageStats struct {
	Name    string
	Kind    ShardKind
	Tier    ShardTier
	Storage badger.StorageStats
}

// StorageStats is a tiered store's badger storage by open shard (BLOCKCACHE),
// plus the cache counters of every handle closed since New
// (ClosedBlockCache, ClosedIndexCache: MaxCost is their sizes summed), so the
// sum of the rows' counters and the closed ones is every lookup since New.
type StorageStats struct {
	Shards           []ShardStorageStats
	ClosedBlockCache badger.CacheMetrics
	ClosedIndexCache badger.CacheMetrics
}

// StorageStats returns a row per open shard: the reference shard, the archive
// when open, then each open event shard (in no set order: key them by Name);
// a closed shard has no row, its counters are in the closed totals. It walks
// each open handle's table list and reads atomic counters: it reads no table,
// counts no shard access, opens nothing and keeps no shard open.
func (ts *Store) StorageStats() (StorageStats, error) {
	if err := ts.checkOpen(); err != nil {
		return StorageStats{}, err
	}
	var out StorageStats
	row := func(name string, kind ShardKind, tier ShardTier, s *BadgerStore) {
		out.Shards = append(out.Shards, ShardStorageStats{Name: name, Kind: kind, Tier: tier, Storage: s.StorageStats()})
	}
	row("reference", ShardReference, TierHot, ts.refShard)
	if archive := ts.refArchive.Load(); archive != nil {
		row("archive", ShardArchive, TierCold, archive)
	}
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}
	ts.mu.RUnlock()
	for _, es := range shards {
		es.shardMu.Lock()
		s := es.store
		es.shardMu.Unlock()
		if s != nil {
			row(es.name, ShardEvent, es.currentTier(), s)
		}
	}
	ts.closedCachesMu.Lock()
	out.ClosedBlockCache, out.ClosedIndexCache = ts.closedBlockCache, ts.closedIndexCache
	ts.closedCachesMu.Unlock()
	return out, nil
}

// foldClosedCaches keeps a closing handle's cache counters
// (badger.Config.OnCloseCaches).
func (ts *Store) foldClosedCaches(block, index badger.CacheMetrics) {
	ts.closedCachesMu.Lock()
	ts.closedBlockCache = ts.closedBlockCache.Add(block)
	ts.closedIndexCache = ts.closedIndexCache.Add(index)
	ts.closedCachesMu.Unlock()
}

// blockCacheName is the shard name of a handle's directory: an event
// shard's name without its "events/" directory, as ShardStorageStats names it.
func blockCacheName(dir string) string {
	return strings.TrimPrefix(filepath.ToSlash(dir), "events/")
}

// blockCacheKind is the kind of the handle a directory name opens.
func blockCacheKind(name string) ShardKind {
	switch name {
	case "reference":
		return ShardReference
	case "archive":
		return ShardArchive
	case ownerIndexDir:
		return ""
	}
	return ShardEvent
}

// tableBytes is the size of the badger tables (*.sst) in dir; 0 for "" or a
// missing directory.
func tableBytes(dir string) int64 {
	if dir == "" {
		return 0
	}
	ents, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var n int64
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".sst") {
			continue
		}
		if fi, err := e.Info(); err == nil {
			n += fi.Size()
		}
	}
	return n
}

// sizeBlockCache sets cfg's block cache from Config.BlockCacheSizeFor for the
// handle name opens at cfg.Dir (its tables' bytes are read there); without
// the callback cfg keeps BlockCacheSize.
func (ts *Store) sizeBlockCache(cfg *BadgerStoreConfig, name string) {
	if ts.blockCacheSizeFor == nil {
		return
	}
	size := ts.blockCacheSizeFor(BlockCacheOpen{Name: blockCacheName(name), Kind: blockCacheKind(name), ReadOnly: cfg.ReadOnly, TableBytes: tableBytes(cfg.Dir)})
	cfg.BlockCacheSize, cfg.NoBlockCache = size, size == 0
}
