package tiered

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// WARMMEM (R3, tasks/evidence/R3/measure.txt): every writable badger open
// allocates a memtable arena of 1.30 x MemTableSize on the Go heap (83.2 MB at
// 64 MB), whatever the shard's role. WarmMemTableSize sizes it for a shard
// opened writable in a non-hot role: warm at open, a window shard made on
// demand, a cold shard opened writable, a shard reopened after a move or Clear.
// Badger creates the WAL at exactly 2x the memtable size while a handle is
// open, so the .mem file's apparent size shows which size a handle took.

const (
	warmMemHot  = 16 << 20
	warmMemWarm = 8 << 20
)

func warmMemCfg(dir string, clock *tierClock) Config {
	cfg := tierClockCfg(dir, clock)
	cfg.MemTableSize = warmMemHot
	cfg.WarmMemTableSize = warmMemWarm
	cfg.ValueLogFileSize = 1 << 20
	cfg.CacheCapacity = 100
	return cfg
}

// warmMemWAL is the apparent size of the largest WAL file the open handle of
// the event shard name holds.
func warmMemWAL(t *testing.T, dir, name string) int64 {
	t.Helper()
	return maxMemFileSizeTiered(t, filepath.Join(dir, "events", name))
}

func warmMemPut(t *testing.T, ts *Store, tok uint16, day, hour int) types.NodeID {
	t.Helper()
	id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(day, hour)), true)
	if err != nil || !ok {
		t.Fatalf("MintNodeIDAt(day %d): %v, %v", day, ok, err)
	}
	if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
		t.Fatalf("PutNode(day %d): %v", day, err)
	}
	return id
}

// TestWarmMemTable_SizesNonHotWritableOpens: the hot shard and the reference
// shard keep MemTableSize; a window shard made on demand for a past day, a
// warm shard opened at New, and a cold shard a write opens writable take
// WarmMemTableSize; every row stays readable across the reopen.
func TestWarmMemTable_SizesNonHotWritableOpens(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(warmMemCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	var ids []types.NodeID
	ids = append(ids, warmMemPut(t, ts, tok, 22, 3))
	if got := warmMemWAL(t, dir, "2026-07-22"); got != 2*warmMemHot {
		t.Fatalf("hot shard WAL %d, want 2 x MemTableSize %d", got, 2*warmMemHot)
	}
	if got := maxMemFileSizeTiered(t, filepath.Join(dir, "reference")); got != 2*warmMemHot {
		t.Fatalf("reference shard WAL %d, want 2 x MemTableSize %d", got, 2*warmMemHot)
	}

	// A write for a past day with no shard makes its window shard (cold: it ends
	// before the clock minus ColdAfter): a non-hot role.
	ids = append(ids, warmMemPut(t, ts, tok, 10, 3))
	if got := warmMemWAL(t, dir, "2026-07-10"); got != 2*warmMemWarm {
		t.Fatalf("window shard made on demand: WAL %d, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}

	// Rotation: the new hot shard is hot, whatever the old one turned into.
	clock.set(julDay(23, 8))
	ids = append(ids, warmMemPut(t, ts, tok, 23, 3))
	if got := warmMemWAL(t, dir, "2026-07-23"); got != 2*warmMemHot {
		t.Fatalf("new hot shard WAL %d, want 2 x MemTableSize %d", got, 2*warmMemHot)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: the hot shard reopens hot, the warm one in its warm role.
	if ts, err = New(warmMemCfg(dir, clock)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	ts.SetLabelRegistry(reg)
	if got := warmMemWAL(t, dir, "2026-07-23"); got != 2*warmMemHot {
		t.Fatalf("hot shard after restart: WAL %d, want 2 x MemTableSize %d", got, 2*warmMemHot)
	}
	if got := warmMemWAL(t, dir, "2026-07-22"); got != 2*warmMemWarm {
		t.Fatalf("warm shard opened at New: WAL %d, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}

	// A late write into the cold shard (closed by the idle sweep, then read
	// read-only) upgrades it to a writable handle in its role.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	if _, err := ts.GetNode(ids[1]); err != nil {
		t.Fatalf("cold read: %v", err)
	}
	if got := warmMemWAL(t, dir, "2026-07-10"); got != 0 {
		t.Fatalf("a read-only cold handle holds a WAL of %d bytes", got)
	}
	ids = append(ids, warmMemPut(t, ts, tok, 10, 4))
	if got := warmMemWAL(t, dir, "2026-07-10"); got != 2*warmMemWarm {
		t.Fatalf("cold shard opened writable by a late write: WAL %d, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}
	for _, id := range ids {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("node %d: %v", id, err)
		}
	}

	// Clear reopens the warm shard in its role, and the hot shard stays hot.
	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if got := warmMemWAL(t, dir, "2026-07-22"); got != 2*warmMemWarm {
		t.Fatalf("warm shard reopened by Clear: WAL %d, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}
	if got := warmMemWAL(t, dir, "2026-07-23"); got != 2*warmMemHot {
		t.Fatalf("hot shard after Clear: WAL %d, want 2 x MemTableSize %d", got, 2*warmMemHot)
	}
}

// TestWarmMemTable_RowsBeyondTheWarmMemtableSurviveReopen: rows written into a
// warm-role shard past three of its memtables (the memtable rotates and
// flushes under the writes) are all there after Close and a reopen.
func TestWarmMemTable_RowsBeyondTheWarmMemtableSurviveReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 25 MB: not in -short")
	}
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(warmMemCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	pad := strings.Repeat("y", 100<<10)
	var ids []types.NodeID
	for i := 0; i < 250; i++ { // a backfill into the on-demand shard of 2026-07-10
		id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(10, 1+i%7)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
		}
		n := types.NewNode(id, tok, nil)
		if err := n.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": pad})); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if got := warmMemWAL(t, dir, "2026-07-10"); got != 2*warmMemWarm {
		t.Fatalf("on-demand shard WAL %d, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := warmMemCfg(dir, clock)
	cfg.MaxOpenColdShards = 1 // the reads keep the shard open instead of reopening it for each
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	for i, id := range ids {
		n, err := ts.GetNode(id)
		if err != nil {
			t.Fatalf("node %d lost across Close and reopen: %v", i, err)
		}
		if v, _ := n.GetProperty("i"); fmt.Sprint(v) != fmt.Sprint(i) {
			t.Fatalf("node %d reads i = %v", i, v)
		}
	}
}

// TestWarmMemTable_OversizedWALMigratedAtWarmOpen: a store copied while it
// ran holds a hot shard's WAL written under MemTableSize; once that shard is
// warm, WarmMemTableSize is smaller than the WAL fits, and the open migrates
// the WAL (badger.MigrateOversizedWAL) instead of failing, with every row
// served. It is TestTieredOversizedWALMigratedOnWarmReopen for the warm role.
func TestWarmMemTable_OversizedWALMigratedAtWarmOpen(t *testing.T) {
	if testing.Short() {
		t.Skip("copies a live data directory: not in -short")
	}
	srcDir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	cfg := warmMemCfg(srcDir, clock)
	cfg.MemTableSize = 20 << 20 // its WAL, 40 MB, is above 2 x WarmMemTableSize
	ts1, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts1.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	pad := strings.Repeat("y", 30<<10)
	var ids []types.NodeID
	for i := 0; i < 100; i++ { // 3 MB, inside one memtable: it lives in the WAL
		id, ok, err := ts1.MintNodeIDAt(tok, ms(julDay(22, 1+i%7)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
		}
		n := types.NewNode(id, tok, nil)
		if err := n.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": pad})); err != nil {
			t.Fatal(err)
		}
		if err := ts1.PutNode(n); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := ts1.HotShardForTest().Store().Flush(); err != nil {
		t.Fatal(err)
	}
	clock.set(julDay(23, 8))
	warmMemPut(t, ts1, tok, 23, 3) // rotates: 2026-07-22 turns warm
	if got := warmMemWAL(t, srcDir, "2026-07-22"); got <= 2*warmMemWarm {
		t.Fatalf("fixture: the shard's WAL is %d bytes, not above 2 x WarmMemTableSize", got)
	}
	dstDir := t.TempDir()
	copyTree(t, srcDir, dstDir) // a crash-consistent copy of the running store
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	cfg2 := warmMemCfg(dstDir, clock)
	cfg2.MemTableSize = 20 << 20
	ts2, err := New(cfg2)
	if err != nil {
		t.Fatalf("open with the oversized warm WAL: %v", err)
	}
	defer func() { _ = ts2.Close() }()
	for i, id := range ids {
		if _, err := ts2.GetNode(id); err != nil {
			t.Fatalf("node %d lost across the WAL migration: %v", i, err)
		}
	}
	if got := warmMemWAL(t, dstDir, "2026-07-22"); got != 2*warmMemWarm {
		t.Fatalf("warm shard WAL %d after the open, want 2 x WarmMemTableSize %d", got, 2*warmMemWarm)
	}
}

// TestWarmMemTable_RangeChecked: WarmMemTableSize has MemTableSize's bounds
// [8 MB, 1 GB] and may not exceed MemTableSize (the stock 64 MB when that is
// 0); New checks it, since a warm shard opens long after New, where a bad value
// would fail a read or a late write instead.
func TestWarmMemTable_RangeChecked(t *testing.T) {
	for _, tc := range []struct {
		hot, warm int64
		ok        bool
	}{
		{8 << 20, 0, true},
		{8 << 20, 8 << 20, true},
		{16 << 20, 8 << 20, true},
		{8 << 20, 8<<20 - 1, false}, // below the floor
		{8 << 20, -1, false},
		{1 << 30, 1<<30 + 1, false}, // above the cap
		{8 << 20, 8<<20 + 1, false}, // above the hot memtable
		{0, 128 << 20, false},       // above the stock 64 MB
	} {
		clock := &tierClock{}
		clock.set(julDay(22, 12))
		cfg := tierClockCfg(t.TempDir(), clock)
		cfg.MemTableSize, cfg.WarmMemTableSize = tc.hot, tc.warm
		ts, err := New(cfg)
		if tc.ok != (err == nil) {
			t.Errorf("MemTableSize %d, WarmMemTableSize %d: New = %v, want ok %v", tc.hot, tc.warm, err, tc.ok)
		}
		if err != nil {
			if !strings.Contains(err.Error(), "WarmMemTableSize") {
				t.Errorf("WarmMemTableSize %d: error %q does not name the field", tc.warm, err)
			}
			continue
		}
		_ = ts.Close()
	}
}
