//go:build measure

// Measurement level (`scripts/measure.sh`): what a burst of reads of one closed
// cold shard costs (R3 follow-up 3, tasks/evidence/R3/coldread.txt). The R3
// state measure found 100 GetNode on a closed day shard of 50,000 rows at 2.5 to
// 2.9 s and 707 MB of allocation. The count behind it is the cold opens: with
// MaxOpenColdShards 0 (the default) every read opens the shard read-only and
// closes it again at check-in; with a cap the burst pays one open. The run is by
// hand, the wall time and the allocation move with the host's load (logged,
// never a bound); the counts are asserted.
//
//	R3_PER_DAY=50000 go test -tags measure -v -run TestColdReadBurst ./pkg/graph/store/tiered/

package tiered

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestColdReadBurst(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	perDay := 5000
	if v, err := strconv.Atoi(os.Getenv("R3_PER_DAY")); err == nil {
		perDay = v
	}
	for _, c := range []struct {
		name    string
		maxOpen int
		asoc    bool // ai-soc's cold-read configuration: label and adjacency indexes on disk, a 64 MB block cache
	}{
		{"default (cap 0, label map in RAM)", 0, false},
		{"ai-soc config, cap 0", 0, true},
		{"default config, cap 2", 2, false},
		{"ai-soc config, cap 2", 2, true},
	} {
		t.Run(c.name, func(t *testing.T) { coldReadBurst(t, perDay, c.maxOpen, c.asoc) })
	}
}

// coldBurstFixture is the section E fixture of the burst: perDay rows on day 20,
// four more days so day 20 is cold, the idle sweep closes it. It returns the store,
// every 500th node of day 20 and the shard's name.
func coldBurstFixture(t *testing.T, perDay, maxOpen int, asoc bool) (ts *Store, ids []types.NodeID, day20 string) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.MemTableSize = 64 * mib
	cfg.CacheCapacity = 100
	cfg.MaxOpenColdShards = maxOpen
	if asoc {
		cfg.LabelIndexOnDisk = true
		cfg.AdjacencyIndexOnDisk = true
		cfg.BlockCacheSize = 64 * mib
		cfg.HistoryDeltaEncoding = true
		cfg.OpenImageInterval = 5 * time.Minute
	}
	reg := registrypkg.NewLabelRegistry()
	tok, _ := reg.GetOrCreate("Signal")
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(reg)
	t.Cleanup(func() { _ = ts.Close() })
	for day := 20; day <= 24; day++ { // day 20 is cold from day 23 on (ColdAfter 48 h)
		clock.set(julDay(day, 8))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatal(err)
		}
		ts.SweepHandlesForTest()
		n := 1
		if day == 20 {
			n = perDay
		}
		for i := 0; i < n; i++ {
			id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(day, 1+i%7)), true)
			if err != nil || !ok {
				t.Fatalf("MintNodeIDAt: %v %v", ok, err)
			}
			node := types.NewNode(id, tok, nil)
			_ = node.SetProperty("host", fmt.Sprintf("host-%d", i%97))
			_ = node.SetProperty("msg", fmt.Sprintf("event %d on day %d with some payload text", i, day))
			if err := ts.PutNode(node); err != nil {
				t.Fatal(err)
			}
			if day == 20 && i%500 == 0 {
				ids = append(ids, id)
			}
		}
	}
	day20 = "2026-07-20"
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	ts.SweepHandlesForTest()
	if ts.EventShardsForTest()[day20].Store() != nil {
		t.Fatal("the closed cold shard is still open before the burst")
	}
	return ts, ids, day20
}

func coldReadBurst(t *testing.T, perDay, maxOpen int, asoc bool) {
	ts, ids, day20 := coldBurstFixture(t, perDay, maxOpen, asoc)
	runtime.GC()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	savesBefore := ts.CatalogForTest().SavesForTest()
	u0, _ := rusageCPU()
	t0 := time.Now()
	for _, id := range ids {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("cold read: %v", err)
		}
	}
	took := time.Since(t0)
	u1, _ := rusageCPU()
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	saves := ts.CatalogForTest().SavesForTest() - savesBefore
	opens := coldOpensOf(t, ts, day20)
	retained := ts.EventShardsForTest()[day20].Store() != nil
	t.Logf("%d rows a day, %d GetNode on the closed day shard: %d cold opens, %d catalog saves, handle retained %v; %v wall, %v user CPU, "+
		"alloc %.1f MB, live heap kept %+.1f MB", perDay, len(ids), opens, saves, retained, took.Round(time.Millisecond),
		(u1 - u0).Round(time.Millisecond), float64(after.TotalAlloc-before.TotalAlloc)/mib,
		(float64(after.HeapAlloc)-float64(before.HeapAlloc))/mib)
	want := int64(len(ids)) // not retained: every read opens the shard and closes it at check-in
	if maxOpen > 0 {
		want = 1 // retained: the burst's first read opens it, the rest find it open
	}
	if opens != want {
		t.Fatalf("%d reads made %d cold opens with MaxOpenColdShards %d, want %d", len(ids), opens, maxOpen, want)
	}
	if retained != (maxOpen > 0) {
		t.Fatalf("the cold handle is retained = %v after the burst with MaxOpenColdShards %d", retained, maxOpen)
	}
}

// TestColdOpenConfigs times and counts what one raw read-only open and close of
// the closed day shard costs under the cold read config as it was (shardCfg: the
// store's own settings) and as it is (coldReadCfg: a 16 MB block cache, no label
// maps, encoded ID sets, no revision tables), in both configurations of the
// burst; 50 rounds each, no read, no catalog save, so the figures are the open
// and the close alone. Alloc and CPU are logged, never asserted (R3 cold open,
// tasks/evidence/R3/coldread.txt).
//
//	R3_PER_DAY=50000 go test -tags measure -v -run TestColdOpenConfigs ./pkg/graph/store/tiered/
func TestColdOpenConfigs(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("measurement: not in -short, no race detector")
	}
	perDay := 5000
	if v, err := strconv.Atoi(os.Getenv("R3_PER_DAY")); err == nil {
		perDay = v
	}
	for _, asoc := range []bool{false, true} {
		name := "default config"
		if asoc {
			name = "ai-soc config"
		}
		t.Run(name, func(t *testing.T) {
			ts, _, day20 := coldBurstFixture(t, perDay, 0, asoc)
			es := ts.EventShardsForTest()[day20]
			for _, c := range []struct {
				name string
				cfg  func() BadgerStoreConfig
			}{
				{"before (shardCfg)", func() BadgerStoreConfig { return ts.shardCfg(es.path, true, true) }},
				{"after (coldReadCfg)", func() BadgerStoreConfig { return ts.coldReadCfg(es.path) }},
			} {
				const rounds = 50
				var openTook, closeTook time.Duration
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				u0, _ := rusageCPU()
				for i := 0; i < rounds; i++ {
					t0 := time.Now()
					store, err := ts.newShardStore(c.cfg())
					if err != nil {
						t.Fatal(err)
					}
					openTook += time.Since(t0)
					t0 = time.Now()
					if err := store.Close(); err != nil {
						t.Fatal(err)
					}
					closeTook += time.Since(t0)
				}
				u1, _ := rusageCPU()
				runtime.ReadMemStats(&after)
				t.Logf("%d rows, %s, %s: open %.2f ms + close %.2f ms, user CPU %.2f ms, alloc %.2f MB per open and close",
					perDay, name, c.name, float64(openTook.Microseconds())/rounds/1000, float64(closeTook.Microseconds())/rounds/1000,
					float64((u1-u0).Microseconds())/rounds/1000, float64(after.TotalAlloc-before.TotalAlloc)/rounds/mib)
			}
		})
	}
}
