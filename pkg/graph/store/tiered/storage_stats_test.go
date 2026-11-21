package tiered

import (
	"errors"
	"sync"
	"testing"

	badger "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/dgraph-io/badger/v4/options"
)

// BLOCKCACHE: BlockCacheSizeFor sizes each badger open of a tiered store:
// it is called with the handle's name and kind (reference, event, the owner
// index as ""), the handle opens with the size it returns, and StorageStats
// shows that size per open shard; a closed handle's counters move to the
// closed totals. 0 opens plain-table shards without a block cache; 0 with
// compression is refused at New.
func TestBlockCacheSizeForSizesEachOpen(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	kinds := map[string]ShardKind{}
	sizeFor := func(size int64) func(BlockCacheOpen) int64 {
		return func(o BlockCacheOpen) int64 {
			mu.Lock()
			kinds[o.Name] = o.Kind
			mu.Unlock()
			if o.TableBytes < 0 {
				t.Errorf("open %s: TableBytes %d", o.Name, o.TableBytes)
			}
			return size
		}
	}

	cfg := volumeCfg(t.TempDir(), "", "")
	cfg.Compression = options.ZSTD
	cfg.BlockCacheSizeFor = sizeFor(3 << 20)
	ts, _, _, cold, _ := closedColdFixtureWith(t, cfg)
	defer func() { _ = ts.Close() }()
	st, err := ts.StorageStats()
	if err != nil {
		t.Fatal(err)
	}
	hot := ts.HotShardForTest().Name()
	rows := map[string]ShardStorageStats{}
	for _, r := range st.Shards {
		rows[r.Name] = r
	}
	if _, ok := rows[cold]; ok {
		t.Fatalf("the closed shard %s has a row", cold)
	}
	for _, name := range []string{"reference", hot} {
		if r, ok := rows[name]; !ok || r.Storage.BlockCache.MaxCost != 3<<20 {
			t.Fatalf("%s: %+v, want an open row with a 3 MiB block cache", name, r)
		}
	}
	if st.ClosedBlockCache.MaxCost < 3<<20 {
		t.Fatalf("closed totals %+v: the closed cold shard's 3 MiB cache is missing", st.ClosedBlockCache)
	}
	mu.Lock()
	if kinds["reference"] != ShardReference || kinds[hot] != ShardEvent || kinds[cold] != ShardEvent {
		t.Fatalf("kinds seen %v", kinds)
	}
	if k, ok := kinds[ownerIndexDir]; ok && k != "" {
		t.Fatalf("owner index kind %q", k)
	}
	mu.Unlock()

	plain := volumeCfg(t.TempDir(), "", "")
	plain.NoCompression = true
	plain.BlockCacheSizeFor = sizeFor(0)
	ps, err := New(plain)
	if err != nil {
		t.Fatalf("New without block caches: %v", err)
	}
	defer func() { _ = ps.Close() }()
	pst, err := ps.StorageStats()
	if err != nil || len(pst.Shards) < 2 {
		t.Fatalf("StorageStats: %+v, %v", pst, err)
	}
	for _, r := range pst.Shards {
		if r.Storage.BlockCache != (badger.CacheMetrics{}) {
			t.Fatalf("%s: block cache %+v, want none", r.Name, r.Storage.BlockCache)
		}
	}

	zstd := volumeCfg(t.TempDir(), "", "")
	zstd.Compression = options.ZSTD
	zstd.BlockCacheSizeFor = sizeFor(0)
	if zs, err := New(zstd); !errors.Is(err, badger.ErrNoBlockCacheNeedsPlainTables) {
		if err == nil {
			_ = zs.Close()
		}
		t.Fatalf("zstd without a block cache: %v, want ErrNoBlockCacheNeedsPlainTables", err)
	}
}
