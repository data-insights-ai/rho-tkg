package badger

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/dgraph-io/badger/v4/options"
)

// storageStatsFixture writes n nodes into a store at dir, closes it (badger
// flushes the memtable into a table), reopens it with cfg and reads every node
// once (the entity cache is empty after the open, so each read decodes a block
// row). It returns the reopened store's stats and the counters OnCloseCaches
// received at its Close.
func storageStatsFixture(t *testing.T, cfg Config, n int) (StorageStats, CacheMetrics) {
	t.Helper()
	cfg.Dir = t.TempDir()
	write := Config{Dir: cfg.Dir, Compression: cfg.Compression, NoCompression: cfg.NoCompression}
	bs, err := New(write)
	if err != nil {
		t.Fatalf("New (write): %v", err)
	}
	ids := make([]types.NodeID, n)
	for i := range ids {
		ids[i] = types.NodeID(snowflake.ID(1000 + i))
		node := types.NewNode(ids[i], 7, nil)
		if err := node.SetProperty("pad", "0123456789abcdef0123456789abcdef"); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	var closed CacheMetrics
	cfg.OnCloseCaches = func(block, _ CacheMetrics) { closed = block }
	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("New (read): %v", err)
	}
	for _, id := range ids {
		if _, err := bs.GetNode(id); err != nil {
			t.Fatalf("GetNode %v: %v", id, err)
		}
	}
	st := bs.StorageStats()
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if bs.StorageStats() != (StorageStats{}) {
		t.Fatal("a closed store reports stats")
	}
	return st, closed
}

// BLOCKCACHE: StorageStats reports the tables a store holds (count, bytes on
// disk and decoded, index and bloom filter bytes) and its block cache's
// counters; with compression the reads go through the cache (misses, then
// hits on blocks read again; the bytes it holds within its size), and the
// counters reach OnCloseCaches at Close.
func TestStorageStats_CompressedReadsCountInTheBlockCache(t *testing.T) {
	t.Parallel()
	st, closed := storageStatsFixture(t, Config{Compression: options.ZSTD, BlockCacheSize: 1 << 20}, 400)
	if st.Tables < 1 || st.LSMBytes <= 0 || st.UncompressedBytes <= 0 || st.IndexBytes <= 0 || st.FilterBytes <= 0 {
		t.Fatalf("tables: %+v, want at least one table with its bytes, index and filter", st)
	}
	if st.UncompressedBytes <= st.LSMBytes {
		t.Errorf("zstd: decoded %d not above on disk %d", st.UncompressedBytes, st.LSMBytes)
	}
	bc := st.BlockCache
	if bc.MaxCost != 1<<20 || bc.Misses == 0 || bc.Hits == 0 || bc.CostAdded == 0 || bc.Held() <= 0 || bc.Held() > bc.MaxCost {
		t.Fatalf("block cache %+v: want misses, hits and bytes held within 1 MiB", bc)
	}
	if st.IndexCache != (CacheMetrics{}) {
		t.Errorf("index cache %+v without IndexCacheSize, want none", st.IndexCache)
	}
	if closed.Hits < bc.Hits || closed.Misses < bc.Misses || closed.MaxCost != bc.MaxCost {
		t.Errorf("OnCloseCaches got %+v, want at least the open counters %+v", closed, bc)
	}
}

// BLOCKCACHE: NoBlockCache opens a store with plain tables without a block
// cache: the same reads answer, the tables are reported and every cache
// counter stays 0. With compression or encryption it is refused before badger
// would panic, and together with a BlockCacheSize it is refused as a conflict.
func TestNoBlockCache(t *testing.T) {
	t.Parallel()
	st, closed := storageStatsFixture(t, Config{NoCompression: true, NoBlockCache: true}, 400)
	if st.Tables < 1 || st.LSMBytes <= 0 || st.IndexBytes <= 0 {
		t.Fatalf("tables: %+v", st)
	}
	if st.BlockCache != (CacheMetrics{}) || closed != (CacheMetrics{}) {
		t.Fatalf("block cache %+v (at close %+v), want none", st.BlockCache, closed)
	}
	for _, c := range []Config{
		{NoBlockCache: true},
		{NoBlockCache: true, Compression: options.ZSTD},
		{NoBlockCache: true, NoCompression: true, EncryptionKey: make([]byte, 32), IndexCacheSize: 1 << 20},
	} {
		c.Dir = t.TempDir()
		if bs, err := New(c); !errors.Is(err, ErrNoBlockCacheNeedsPlainTables) {
			if err == nil {
				_ = bs.Close()
			}
			t.Errorf("NoBlockCache with compression %v / encryption %v: %v, want ErrNoBlockCacheNeedsPlainTables", c.Compression, len(c.EncryptionKey) > 0, err)
		}
	}
	if bs, err := New(Config{Dir: t.TempDir(), NoCompression: true, NoBlockCache: true, BlockCacheSize: 1 << 20}); err == nil {
		_ = bs.Close()
		t.Error("NoBlockCache with a BlockCacheSize is accepted")
	}
}
