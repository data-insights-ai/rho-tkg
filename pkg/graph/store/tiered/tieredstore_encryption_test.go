package tiered

import (
	"errors"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// tieredKey32 / tieredWrongKey32 mirror the badger package's key fixtures
// (kept local — tiered does not import the badger package's _test.go files).
var (
	tieredKey32      = []byte("0123456789abcdef0123456789abcdef")
	tieredWrongKey32 = []byte("ZYXWVUTSRQPONMLKJIHGFEDCBA098765")
)

// encTieredCfg returns a disk-backed Config with encryption on for EVERY
// shard, plus both caches Badger requires whenever EncryptionKey is set (see
// badger.ErrEncryptionRequiresBlockCache / ErrEncryptionRequiresIndexCache).
func encTieredCfg(dir string, key []byte) Config {
	return Config{
		DataDir:        dir,
		RefLabels:      []string{"Case", "User"},
		ShardWindow:    7 * 24 * time.Hour,
		FlushInterval:  1<<63 - 1, // manual flush
		EncryptionKey:  key,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
	}
}

// TestTieredEncryptionMultiShardSmoke proves encryption reaches EVERY shard
// kind the tiered store opens (reference, hot event, and — after rotation —
// warm event), not just a single-shard badger.Store: write reference and
// event data, force a real flush + rotation so a warm shard exists and an
// encrypted SSTable is actually created on each shard, close, and reopen with
// the SAME key. Every entity on every shard must still be readable.
func TestTieredEncryptionMultiShardSmoke(t *testing.T) {
	dir := t.TempDir()
	cfg := encTieredCfg(dir, tieredKey32)

	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Reference-shard node.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("put ref node: %v", err)
	}

	// Hot event-shard nodes.
	var hotIDs []types.NodeID
	for i := 0; i < 20; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put hot event node %d: %v", i, err)
		}
		hotIDs = append(hotIDs, n.ID())
	}

	// Force real flushes (actual encrypted SSTable creation) on both shards
	// currently open, then rotate hot -> warm so a THIRD encrypted shard
	// kind (warm, reopened via openBadgerStoreWithRecovery) is exercised.
	ts.MuForTest().RLock()
	if err := ts.RefShardForTest().Flush(); err != nil {
		ts.MuForTest().RUnlock()
		t.Fatalf("flush reference shard: %v", err)
	}
	if err := ts.HotShardForTest().Store().Flush(); err != nil {
		ts.MuForTest().RUnlock()
		t.Fatalf("flush hot shard: %v", err)
	}
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}

	// Post-rotation hot-shard nodes (the new hot shard, still encrypted).
	var newHotIDs []types.NodeID
	for i := 0; i < 10; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put post-rotation event node %d: %v", i, err)
		}
		newHotIDs = append(newHotIDs, n.ID())
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen with the SAME key; every entity on every shard kind must
	// still be readable.
	ts2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen with same key: %v", err)
	}
	defer ts2.Close()

	if _, err := ts2.GetNode(refNode.ID()); err != nil {
		t.Fatalf("reference-shard node lost across encrypted reopen: %v", err)
	}
	for _, id := range hotIDs {
		if _, err := ts2.GetNode(id); err != nil {
			t.Fatalf("warm-shard (post-rotation) node %d lost across encrypted reopen: %v", id.SnowflakeID(), err)
		}
	}
	for _, id := range newHotIDs {
		if _, err := ts2.GetNode(id); err != nil {
			t.Fatalf("hot-shard node %d lost across encrypted reopen: %v", id.SnowflakeID(), err)
		}
	}
}

// TestTieredEncryptionWrongKeyFailsClosed proves a wrong key fails the
// TIERED open (which opens the reference shard first) the same way it fails
// a standalone badger.Store — recognizable via errors.Is against Badger's own
// exported sentinel.
func TestTieredEncryptionWrongKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	ts, err := New(encTieredCfg(dir, tieredKey32))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	bad, err := New(encTieredCfg(dir, tieredWrongKey32))
	if bad != nil {
		_ = bad.Close()
	}
	if err == nil {
		t.Fatal("expected reopen with wrong key to fail, got nil error")
	}
	if !errors.Is(err, badgerv4.ErrEncryptionKeyMismatch) {
		t.Fatalf("reopen with wrong key: err = %v, want wrapped badgerv4.ErrEncryptionKeyMismatch", err)
	}
}

// TestTieredEncryptionValidationSurfacesAtNew confirms an invalid encryption
// config (bad key length, or a key missing a required cache) is rejected
// when the REFERENCE shard opens in New — the same shape as
// TestTieredTuningValidationSurfacesAtNew for the footprint knobs.
func TestTieredEncryptionValidationSurfacesAtNew(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"bad key length", func(c *Config) { c.EncryptionKey = []byte("too-short") }, "EncryptionKey"},
		{"key without BlockCacheSize", func(c *Config) { c.EncryptionKey = tieredKey32; c.IndexCacheSize = 1 << 20 }, "BlockCacheSize"},
		{"key without IndexCacheSize", func(c *Config) { c.EncryptionKey = tieredKey32; c.BlockCacheSize = 1 << 20 }, "IndexCacheSize"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{
				DataDir:     t.TempDir(),
				RefLabels:   []string{"Case"},
				ShardWindow: 7 * 24 * time.Hour,
			}
			tc.mutate(&cfg)
			ts, err := New(cfg)
			if err == nil {
				_ = ts.Close()
				t.Fatalf("expected New to reject %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not name %q", err, tc.wantErr)
			}
		})
	}
}

// TestTieredEncryptionKnobsReachEveryShardOptions is the application-level
// complement to the multi-shard smoke test: it proves EncryptionKey and
// IndexCacheSize reach the live Badger options on the reference shard, the
// hot shard, AND (after rotation) the warm shard — a badgerCfg that dropped
// either field for one shard kind would pass a single-shard check but fail
// here.
func TestTieredEncryptionKnobsReachEveryShardOptions(t *testing.T) {
	ts, err := New(Config{
		DataDir:        t.TempDir(),
		RefLabels:      []string{"Case", "User"},
		ShardWindow:    7 * 24 * time.Hour,
		FlushInterval:  1<<63 - 1,
		EncryptionKey:  tieredKey32,
		BlockCacheSize: 1 << 20,
		IndexCacheSize: 1 << 20,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	check := func(label string, bs *BadgerStore) {
		if bs == nil {
			t.Fatalf("%s shard store is nil", label)
		}
		opts := bs.DBForTest().Opts()
		if len(opts.EncryptionKey) != len(tieredKey32) {
			t.Errorf("%s shard EncryptionKey = %d bytes, want %d", label, len(opts.EncryptionKey), len(tieredKey32))
		}
		if opts.IndexCacheSize != 1<<20 {
			t.Errorf("%s shard IndexCacheSize = %d, want %d", label, opts.IndexCacheSize, 1<<20)
		}
	}

	check("reference", ts.RefShardForTest())
	ts.MuForTest().RLock()
	check("hot", ts.HotShardForTest().Store())
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	var warmChecked bool
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm && es.Store() != nil {
			check("warm", es.Store())
			warmChecked = true
		}
	}
	ts.MuForTest().RUnlock()
	if !warmChecked {
		t.Fatal("no open warm shard found to verify encryption knob passthrough")
	}
}
