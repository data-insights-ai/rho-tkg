package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldOpenFixture is coldReadFixture with the config under the caller's
// control: a store whose only demoted shard is closed and sealed, one node n in
// it, a second ID (late) minted into its window for a late write.
func coldOpenFixture(t *testing.T, tune func(*Config)) (ts *Store, name string, n, late types.NodeID) {
	t.Helper()
	cfg := mintCfg(t.TempDir(), true)
	if tune != nil {
		tune(&cfg)
	}
	ts = openMintStore(t, cfg)
	gen := tieredNodeGen(t)
	n, late = types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
	putMintNode(t, ts, n, mintSignalTok)
	name = hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	if closed := closeColdStores(t, ts); closed != 1 {
		t.Fatalf("setup: closed %d cold shards, want 1", closed)
	}
	if ts.timestampToEventShardEntry(late.SnowflakeID()) != ts.EventShardsForTest()[name] {
		t.Fatal("setup: the late ID was not minted in the closed shard's window")
	}
	return ts, name, n, late
}

// readOnlyColdHandle opens the closed cold shard name the way a read does
// (checkoutStoreReadOnly: GetNode and the other read doors) and returns the
// pinned handle with its release. It fails the test when the handle is not
// the read-only one a read opens.
func readOnlyColdHandle(t *testing.T, ts *Store, name string) (*BadgerStore, func()) {
	t.Helper()
	es := ts.EventShardsForTest()[name]
	store, release, err := es.checkoutStoreReadOnly(ts)
	if err != nil {
		t.Fatalf("read-only checkout of %s: %v", name, err)
	}
	if _, ro := es.StoreStateForTest(); !ro {
		release()
		t.Fatalf("the read checkout of %s opened a writable handle", name)
	}
	return store, release
}

const (
	stockBlockCache = 256 << 20 // badger's default BlockCacheSize, what 0 keeps
	transientCache  = 16 << 20  // the most a transient read-only handle is given
)

// TestColdOpen_TransientHandleGetsASmallBlockCache: with MaxOpenColdShards 0 a
// read of a closed cold shard opens it read-only and closes it again when the
// read ends, so the handle is one read's and the block cache it allocates at
// open (ristretto sizes its counters and doorkeeper by the cache's capacity:
// 2.4 MB of the 7 MB an open allocated at the stock 256 MB, 0.85 MB at ai-soc's
// 64 MB) is paid by every read for blocks that serve that read only. Bytes
// configured per open, by the shard's handle (R3 coldread.txt, option c iii):
// a transient read-only handle gets at most 16 MB and never more than the
// store is configured with; a handle a cap keeps (MaxOpenColdShards > 0) and a
// writable one keep the configured size.
func TestColdOpen_TransientHandleGetsASmallBlockCache(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name       string
		maxOpen    int
		configured int64 // Config.BlockCacheSize; 0 keeps badger's stock 256 MB
		want       int64 // the read-only handle's block cache
	}{
		{"cap 0, stock size", 0, 0, transientCache},
		{"cap 0, 64 MB", 0, 64 << 20, transientCache},
		{"cap 0, 4 MB is not raised", 0, 4 << 20, 4 << 20},
		{"cap 2 retains: stock size kept", 2, 0, stockBlockCache},
		{"cap 2 retains: 64 MB kept", 2, 64 << 20, 64 << 20},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, _, _ := coldOpenFixture(t, func(cfg *Config) {
				cfg.MaxOpenColdShards = c.maxOpen
				cfg.BlockCacheSize = c.configured
			})
			store, release := readOnlyColdHandle(t, ts, name)
			got := store.DBForTest().Opts().BlockCacheSize
			release()
			if got != c.want {
				t.Fatalf("the read-only cold handle has a %d-byte block cache, want %d", got, c.want)
			}
		})
	}
}

// TestColdOpen_WritableHandleKeepsTheConfiguredBlockCache: a late write opens
// the closed cold shard writable (a read-only handle gives way to it); that
// handle stays open for the writes that follow and keeps the configured block
// cache, at cap 0 as with a cap.
func TestColdOpen_WritableHandleKeepsTheConfiguredBlockCache(t *testing.T) {
	t.Parallel()
	ts, name, _, late := coldOpenFixture(t, func(cfg *Config) { cfg.BlockCacheSize = 64 << 20 })
	if err := ts.PutNode(types.NewNode(late, mintSignalTok, nil)); err != nil {
		t.Fatalf("late write: %v", err)
	}
	store, ro := ts.EventShardsForTest()[name].StoreStateForTest()
	if store == nil || ro {
		t.Fatalf("the late write left store %v read-only %v, want a writable handle", store != nil, ro)
	}
	if got := store.DBForTest().Opts().BlockCacheSize; got != 64<<20 {
		t.Fatalf("the writable cold handle has a %d-byte block cache, want the configured %d", got, 64<<20)
	}
}
