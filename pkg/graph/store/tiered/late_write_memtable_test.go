package tiered

import (
	"path/filepath"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// R3-3 LATEWRITE (L1, tasks/evidence/R3/design-latewrite-floor.txt section
// 3): with WarmMemTableSize 0 only a shard cold at the open takes the 8 MB
// memtable. A window shard made on demand that is warm (a past day inside
// ColdAfter, or a future day a rotation will make hot) keeps MemTableSize, so
// the hot shard never runs on a small handle. The WAL's apparent size is 2 x
// the open's memtable (badger memtable.go:117). The graph-level answers are
// TestLateWriteColdShardOpensAtSmallMemtable (pkg/graph).
func TestLateWriteMemtable_OnlyColdAtTheOpen(t *testing.T) {
	const hot = 16 << 20
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = hot
	cfg.ValueLogFileSize = 1 << 20
	cfg.CacheCapacity = 100
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	wal := func(name string) int64 { return maxMemFileSizeTiered(t, filepath.Join(dir, "events", name)) }

	warmMemPut(t, ts, tok, 22, 3)
	warmMemPut(t, ts, tok, 21, 3) // a past day inside ColdAfter: a warm window shard
	warmMemPut(t, ts, tok, 23, 3) // the next day: warm now, hot after the rotation
	cold := warmMemPut(t, ts, tok, 10, 3)
	for name, want := range map[string]int64{"2026-07-22": 2 * hot, "2026-07-21": 2 * hot, "2026-07-23": 2 * hot, "2026-07-10": 2 * coldMemTableSize} {
		if got := wal(name); got != want {
			t.Errorf("shard %s made on demand: WAL %d, want %d", name, got, want)
		}
	}
	if tier := ts.EventShardsForTest()["2026-07-10"].Tier(); tier != TierCold {
		t.Fatalf("day 10 is %s, want cold", tier)
	}

	clock.set(julDay(23, 8))
	warmMemPut(t, ts, tok, 23, 4) // rotates: day 23 is promoted, on its hot-size handle
	if hs := ts.HotShardForTest(); hs.Name() != "2026-07-23" || wal("2026-07-23") != 2*hot {
		t.Fatalf("hot shard %s, WAL %d; want 2026-07-23 at 2 x %d", hs.Name(), wal("2026-07-23"), hot)
	}

	// A late write into the closed cold day.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	if s, _ := ts.EventShardsForTest()["2026-07-10"].StoreStateForTest(); s != nil {
		t.Fatal("day 10 still open after the idle close")
	}
	warmMemPut(t, ts, tok, 10, 4)
	if got := wal("2026-07-10"); got != 2*coldMemTableSize {
		t.Fatalf("late write into the closed cold day: WAL %d, want 2 x %d", got, coldMemTableSize)
	}
	if _, err := ts.GetNode(cold); err != nil {
		t.Fatalf("cold row after the late write: %v", err)
	}
	// The read-only recovery probe and a cold read keep the hot size.
	if got := ts.shardCfg("events/2026-07-10", true, true).MemTableSize; got != hot {
		t.Fatalf("read-only cold config MemTableSize %d, want %d", got, hot)
	}
}
