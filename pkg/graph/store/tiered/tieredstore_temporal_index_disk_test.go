package tiered

import (
	"fmt"
	"sort"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// K4 — TemporalIndexOnDisk pass-through to tiered.Config.
//
// The tiered store delegates CreateTemporalIndex/DropTemporalIndex to EVERY
// shard's own badger.Store (see tieredstore_write.go), so the rebuild-at-open
// accelerator itself needs no tiered-specific logic — only the Config
// pass-through (badgerCfg) needs verifying, mirroring
// TestTieredPropertyIndexOnDisk_ReachesEveryShard, plus one functional
// reopen test proving a temporal index on the reference shard survives a
// close/reopen and answers correctly via the fast disk-stream path.

// TestTieredTemporalIndexOnDisk_ReachesEveryShard proves Config.
// TemporalIndexOnDisk reaches the reference, hot, and (post-rotation) warm
// shard's badger.Config, and that the zero-value Config leaves every shard
// in the default (no accelerator) mode.
func TestTieredTemporalIndexOnDisk_ReachesEveryShard(t *testing.T) {
	t.Parallel()

	t.Run("enabled reaches reference, hot, and warm shards", func(t *testing.T) {
		t.Parallel()
		ts, err := New(Config{
			InMemory:            true,
			RefLabels:           []string{"Case"},
			ShardWindow:         7 * 24 * time.Hour,
			FlushInterval:       1<<63 - 1,
			TemporalIndexOnDisk: true,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer ts.Close()

		if !ts.RefShardForTest().TemporalIndexOnDiskForTest() {
			t.Error("reference shard did not inherit TemporalIndexOnDisk=true")
		}
		ts.MuForTest().RLock()
		hot := ts.HotShardForTest().Store()
		ts.MuForTest().RUnlock()
		if !hot.TemporalIndexOnDiskForTest() {
			t.Error("hot shard did not inherit TemporalIndexOnDisk=true")
		}

		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		var warmChecked bool
		ts.MuForTest().RLock()
		for _, es := range ts.EventShardsForTest() {
			if es.Tier() == TierWarm && es.Store() != nil {
				if !es.Store().TemporalIndexOnDiskForTest() {
					t.Error("warm shard did not inherit TemporalIndexOnDisk=true")
				}
				warmChecked = true
			}
		}
		ts.MuForTest().RUnlock()
		if !warmChecked {
			t.Fatal("no open warm shard found to verify TemporalIndexOnDisk passthrough")
		}
	})

	t.Run("zero-value Config defaults every shard to the slow path", func(t *testing.T) {
		t.Parallel()
		ts := newTestTieredStore(t) // Config{} equivalent — TemporalIndexOnDisk unset
		if ts.RefShardForTest().TemporalIndexOnDiskForTest() {
			t.Error("reference shard defaulted to disk mode with TemporalIndexOnDisk unset")
		}
		ts.MuForTest().RLock()
		hot := ts.HotShardForTest().Store()
		ts.MuForTest().RUnlock()
		if hot.TemporalIndexOnDiskForTest() {
			t.Error("hot shard defaulted to disk mode with TemporalIndexOnDisk unset")
		}
	})
}

// TestTieredTemporalIndexOnDisk_ReopenPersistence creates a reference-shard
// temporal index and reference nodes with random-ish valid-time bounds under
// TemporalIndexOnDisk, closes, reopens with the SAME Config, and asserts
// NodesByLabel(ValidAt=...) answers correctly — the reference shard's own
// badger.Store rebuilds via the fast 0x0B-stream path (marker set at the
// first open), not a full node-fetch rescan.
func TestTieredTemporalIndexOnDisk_ReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:             dir,
		RefLabels:           []string{"Case"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		TemporalIndexOnDisk: true,
	}

	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	labelReg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(labelReg)
	caseTok, err := labelReg.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate label: %v", err)
	}

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	gen := tieredNodeGen(t)
	var ids []types.NodeID
	for i := int64(1); i <= 5; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(100 * i), ValidTo: 0})
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		ids = append(ids, n.ID())
	}

	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	if !ts2.RefShardForTest().TemporalIndexOnDiskForTest() {
		t.Fatal("reopen: reference shard lost TemporalIndexOnDisk=true")
	}

	// ValidAt(300) must match every node whose ValidFrom <= 300 (open-ended
	// ValidTo=0): nodes 1-3 (ValidFrom 100,200,300).
	nodes, err := ts2.NodesByLabel(caseTok, QueryOpts{ValidAt: types.Instant(300)})
	if err != nil {
		t.Fatalf("NodesByLabel(ValidAt=300): %v", err)
	}
	gotIDs := nodeIDsSortedInt64(nodes)
	wantIDs := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		wantIDs = append(wantIDs, int64(ids[i].SnowflakeID()))
	}
	sort.Slice(wantIDs, func(i, j int) bool { return wantIDs[i] < wantIDs[j] })
	if fmt.Sprint(gotIDs) != fmt.Sprint(wantIDs) {
		t.Fatalf("reopen: ValidAt=300 got=%v want=%v", gotIDs, wantIDs)
	}

	// Negative: a point before every node's ValidFrom returns empty.
	empty, err := ts2.NodesByLabel(caseTok, QueryOpts{ValidAt: types.Instant(1)})
	if err != nil {
		t.Fatalf("NodesByLabel(ValidAt=1): %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("ValidAt=1 returned %d matches, want 0", len(empty))
	}
}
