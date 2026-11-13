package tiered

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R3 T-e. A process dies inside a handle swap: at its gate (the warm shard
// still on its hot-size handle, its WAL unclean and holding 20 MB of late rows,
// more than a warm arena) or between its close and its reopen (the shard
// closed cleanly). A reopen with WarmMemTableSize 8 MB keeps every row: the
// warm open migrates the oversized hot-size WAL first (without
// MigrateOversizedWAL, in the tiered warm open and in the badger open, the
// replay dies with "Arena too small"). A reopen that
// finds the shard cold opens it for a read: read-only on a clean directory,
// writable (the read-only open refuses an unclean WAL) after a crash at the
// gate. The child runs the real rotation and sweep and exits at the step
// without Close; only the exit point comes from the test seam.

const (
	hsEnvDir  = "RHO_TIERED_SWEEPCRASH_DIR"
	hsEnvStep = "RHO_TIERED_SWEEPCRASH_STEP"
	hsRows    = 40
)

func hsCfg(dir string, clock *tierClock) Config {
	cfg := tierClockCfg(dir, clock)
	cfg.WarmMemTableSize = 8 << 20
	cfg.MaxOpenColdShards = 4 // a read keeps its handle, so the test sees which kind it was
	return cfg
}

func hsOpen(t *testing.T, dir string, clock *tierClock) (*Store, uint16) {
	t.Helper()
	ts, err := New(hsCfg(dir, clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	return ts, tok
}

func hsPut(t *testing.T, ts *Store, tok uint16, day int) {
	t.Helper()
	for i := 0; i < hsRows; i++ {
		id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(day, 1+i%7)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v %v", ok, err)
		}
		n := types.NewNode(id, tok, nil)
		// 256 KB a row, 10 MB a day: more than a warm arena holds, so a
		// warm open must flush the hot-size WAL first (MigrateOversizedWAL).
		if err := n.SetProperties(mustTieredProps(t, map[string]any{"pad": strings.Repeat("p", 256<<10)})); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
}

// TestHandleSweepCrashChild is the child: day 20 turns warm at a rotation to
// day 21 (the sweep held at its gate, so day 20 keeps its hot-size handle),
// day 21 takes hot writes and is flushed, day 20 takes another 20 MB of late
// rows, not flushed (its WAL outgrows a warm arena), and the process dies
// when the sweep's swap of day 20 reaches step RHO_TIERED_SWEEPCRASH_STEP.
func TestHandleSweepCrashChild(t *testing.T) {
	dir := os.Getenv(hsEnvDir)
	if dir == "" {
		t.Skip("child of TestHandleSweep_CrashMidSwapKeepsRows")
	}
	step := os.Getenv(hsEnvStep)
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	ts, tok := hsOpen(t, dir, clock)
	holdHandleSweep(ts)
	hsPut(t, ts, tok, 20)
	clock.set(julDay(21, 8))
	if err := ts.CheckRotationForTest(); err != nil { // kicks the sweep, held at the gate
		t.Fatalf("rotation: %v", err)
	}
	hsPut(t, ts, tok, 21)
	if err := ts.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Late rows on day 20's hot-size handle, only in its WAL: 20 MB, about
	// twice a warm arena (10.4 MB at 8 MB), so its replay needs the migration.
	hsPut(t, ts, tok, 20)
	hsPut(t, ts, tok, 20)
	ts.SetSweepFaultForTest(func(s string) error {
		if s == step {
			os.Exit(3) // the crash: no Close, no further step
		}
		return nil
	})
	ts.SweepHandlesForTest()
	t.Logf("the sweep never reached %q", step)
	os.Exit(4)
}

func TestHandleSweep_CrashMidSwapKeepsRows(t *testing.T) {
	if testing.Short() {
		t.Skip("runs child processes: not in -short")
	}
	for _, step := range []string{"gated", "closed"} {
		t.Run("crash at "+step, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestHandleSweepCrashChild$", "-test.count=1")
			cmd.Env = append(os.Environ(), hsEnvDir+"="+dir, hsEnvStep+"="+step)
			out, _ := cmd.CombinedOutput()
			if code := cmd.ProcessState.ExitCode(); code != 3 {
				t.Fatalf("the child did not die at %q (exit %d):\n%s", step, code, out)
			}
			coldDir := t.TempDir()
			copyTree(t, dir, coldDir)

			count := func(ts *Store, tok uint16) int {
				t.Helper()
				ns, err := ts.NodesByLabel(tok, QueryOpts{})
				if err != nil {
					t.Fatalf("NodesByLabel after a crash at %q: %v", step, err)
				}
				return len(ns)
			}

			// Day 21: day 20 reopens warm at WarmMemTableSize.
			clock := &tierClock{}
			clock.set(julDay(21, 9))
			ts, tok := hsOpen(t, dir, clock)
			if got := count(ts, tok); got != 4*hsRows {
				t.Fatalf("warm reopen after a crash at %q: %d rows, want %d", step, got, 4*hsRows)
			}
			if es := ts.EventShardsForTest()["2026-07-20"]; es == nil || es.Tier() != TierWarm || es.HotArenaForTest() {
				t.Fatalf("day 20 after the reopen: %+v; want warm on a warm-size handle", es)
			}
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}

			// Day 23 on the copy: day 20 is cold and closed; a read opens it.
			clock.set(julDay(23, 9))
			ts, tok = hsOpen(t, coldDir, clock)
			defer func() { _ = ts.Close() }()
			es := ts.EventShardsForTest()["2026-07-20"]
			if es == nil || es.Tier() != TierCold {
				t.Fatalf("day 20 on the cold reopen: %+v; want cold", es)
			}
			if got := count(ts, tok); got != 4*hsRows {
				t.Fatalf("cold reopen after a crash at %q: %d rows, want %d", step, got, 4*hsRows)
			}
			s, ro := es.StoreStateForTest()
			if s == nil {
				t.Fatal("the read left day 20 closed; want its handle kept (MaxOpenColdShards)")
			}
			if wantRO := step == "closed"; ro != wantRO {
				t.Fatalf("crash at %q: the read opened day 20 read-only %v, want %v (an unclean WAL needs the writable fallback)", step, ro, wantRO)
			}
		})
	}
}
