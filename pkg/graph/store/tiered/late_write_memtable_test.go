package tiered

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
)

// R3-3 LATEWRITE (L1, tasks/evidence/R3/design-latewrite-floor.txt section
// 3): with WarmMemTableSize 0 only a shard cold at the open takes the 8 MiB
// memtable. A window shard made on demand that is warm (a past day inside
// ColdAfter, or a future day a rotation will make hot) keeps MemTableSize, so
// the hot shard never runs on a small handle. The WAL's apparent size is 2 x
// the open's memtable (badger memtable.go:117). The graph-level answers are
// TestLateWriteColdShardOpensAtSmallMemtable (pkg/graph).
func TestLateWriteMemtable_OnlyColdAtTheOpen(t *testing.T) {
	testtier.Slow(t)
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

// TestLateWriteMemtable_InMemoryKeepsTheHotSize (L2): an in-memory store has
// no idle close, no handle sweep and so no grow: a window shard a late write
// makes cold keeps its first handle for good, so with WarmMemTableSize 0 it
// takes MemTableSize, not the small cold memtable. An explicit
// WarmMemTableSize is kept as set, in memory too. Control: the same write on
// disk opens at the cold size. The native counter is the handle's badger
// option (BadgerStore.MemTableSize); the row reads back in each case.
func TestLateWriteMemtable_InMemoryKeepsTheHotSize(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	for _, tc := range []struct {
		name     string
		inMemory bool
		warm     int64
		want     int64
	}{
		{"in memory", true, 0, cgHot},
		{"in memory, WarmMemTableSize 8 MiB", true, coldMemTableSize, coldMemTableSize},
		{"on disk (control)", false, 0, coldMemTableSize},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			clock := &tierClock{}
			clock.set(julDay(22, 12))
			cfg := tierClockCfg(t.TempDir(), clock)
			if tc.inMemory {
				cfg.DataDir = ""
				cfg.InMemory = true
			}
			cfg.MemTableSize = cgHot
			cfg.WarmMemTableSize = tc.warm
			ts, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			tok, _ := reg.GetOrCreate("Signal")
			id := warmMemPut(t, ts, tok, 10, 3)
			es := ts.EventShardsForTest()["2026-07-10"]
			if es == nil || es.Tier() != TierCold {
				t.Fatalf("day 10 is %+v, want a cold shard", es)
			}
			s, _ := es.StoreStateForTest()
			if s == nil {
				t.Fatal("day 10 has no handle after the late write")
			}
			if got := s.MemTableSize(); got != tc.want {
				t.Errorf("day 10's handle MemTableSize %d, want %d", got, tc.want)
			}
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("the late row: %v", err)
			}
		})
	}
}

// writableOpens records the MemTableSize of every writable open of day 10
// (newShardStore's config) until the returned func is called, which removes
// the hook and returns them.
func (f *coldGrowFix) writableOpens() func() []int64 {
	var mu sync.Mutex
	var opens []int64
	f.ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
		if !cfg.ReadOnly && filepath.Base(cfg.Dir) == "2026-07-10" {
			mu.Lock()
			opens = append(opens, cfg.MemTableSize)
			mu.Unlock()
		}
	})
	return func() []int64 {
		f.ts.SetShardOpenHookForTest(nil)
		mu.Lock()
		defer mu.Unlock()
		return append([]int64(nil), opens...)
	}
}

// TestLateWriteMemtable_ClearAndDropRollback (T8): the two other writable
// opens of a shard cold at the open take the cold size with WarmMemTableSize
// 0: Clear's open of cold day 10 (its MemTableSize read at newShardStore's
// config: the .mem is gone when Clear returns), after which no row is left
// and a late write reads back; and the retention drop's rollback when the
// catalog save fails (retention_purge_drop.go), whose handle serves every
// row. Control: WarmMemTableSize = MemTableSize, both at 16 MiB, the same
// rows.
func TestLateWriteMemtable_ClearAndDropRollback(t *testing.T) {
	testtier.Slow(t)
	if runtime.GOOS == "windows" {
		t.Skip("chmod-driven write-failure injection is unreliable on Windows")
	}
	for _, tc := range []struct {
		warm, want int64
	}{{0, coldMemTableSize}, {cgHot, cgHot}} {
		t.Run(fmt.Sprintf("WarmMemTableSize %d MiB", tc.warm>>20), func(t *testing.T) {
			t.Run("Clear", func(t *testing.T) {
				f := newColdGrow(t, tc.warm, nil)
				f.put(t, 3)
				f.idleClose(t)
				opens := f.writableOpens()
				if err := f.ts.Clear(); err != nil {
					t.Fatalf("Clear: %v", err)
				}
				if got := opens(); len(got) != 1 || got[0] != tc.want {
					t.Fatalf("Clear's writable opens of day 10: %v, want [%d]", got, tc.want)
				}
				if ns, err := f.ts.NodesByLabel(f.tok, QueryOpts{}); err != nil || len(ns) != 0 {
					t.Fatalf("after Clear: %d rows (%v), want none", len(ns), err)
				}
				f.next = 0
				f.put(t, 1)
				if w := f.wal(t); w != 2*tc.want {
					t.Fatalf("late write after Clear: WAL %d, want 2 x %d", w, tc.want)
				}
				if n := f.rows(t); n != 1 {
					t.Fatalf("late write after Clear: %d rows, want 1", n)
				}
			})
			t.Run("drop rollback", func(t *testing.T) {
				f := newColdGrow(t, tc.warm, nil)
				f.put(t, 3) // the late write leaves day 10 open (its unseal's catalog save before the fault)
				metaDir := filepath.Join(f.dir, "meta")
				if err := os.Chmod(metaDir, 0o500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(metaDir, 0o700) })
				opens := f.writableOpens()
				_, err := f.ts.PurgeNodesByLabelBefore(f.tok, ms(julDay(11, 0)), 8)
				t.Logf("purge with the catalog save failing: %v", err)
				if err == nil {
					t.Fatal("PurgeNodesByLabelBefore: nil error, want the catalog save's failure")
				}
				if err := os.Chmod(metaDir, 0o700); err != nil {
					t.Fatal(err)
				}
				if got := opens(); len(got) != 1 || got[0] != tc.want {
					t.Fatalf("the rollback's writable opens of day 10: %v, want [%d]", got, tc.want)
				}
				if n := f.rows(t); n != 4 {
					t.Fatalf("after the rollback: %d rows, want 4", n)
				}
			})
		})
	}
}
