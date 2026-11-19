package tiered

import (
	"os"
	"os/exec"
	"strings"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A process dies inside a handle swap: at its gate (the warm shard
// still on its hot-size handle, its WAL unclean and holding 20 MB of late rows,
// more than a warm arena) or between its close and its reopen (the shard
// closed cleanly). A reopen with WarmMemTableSize 8 MB keeps every row: the
// warm open migrates the oversized hot-size WAL first (without
// MigrateOversizedWAL, in the tiered warm open and in the badger open, the
// replay dies with "Arena too small"). A reopen that finds the shard cold
// finds it marked for recovery after either crash: at the gate its handle was
// open writable; at "closed" the swap closed it cleanly, but a run-time close
// clears the mark only for a cold shard (noteCloseLocked), so the warm shard
// keeps it until Store.Close, which the crash skipped. New repairs it before
// the first read (settleMarkedAtOpen: a writable open, sealed and closed;
// OpenStats.ColdRecovered 1). After a crash at the gate the WAL is unclean: the repair's
// recovery probe (read-only, unmetered) refuses it and its writable open cuts
// and replays it (OpenStats.WALTruncations 1 after New, before any read). The
// read then finds a clean directory and opens it read-only. The child runs
// the real rotation and sweep and exits at the step without Close; only the
// exit point comes from the test seam.

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
	// Long tier only (go test ./..., gate-major): it runs child processes that
	// crash on purpose, and a -race build adds nothing to a crash-and-reopen
	// check, so make race (-race -short) skips it too.
	if testing.Short() {
		t.Skip("runs child processes: long tier only")
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

			// Day 23 on the copy: day 20 is cold and closed, its catalog
			// entry marked for recovery after either crash (open writable at
			// the gate; at "closed" closed cleanly while warm, and a run-time
			// close clears the mark only for a cold shard).
			clock.set(julDay(23, 9))
			ts, tok = hsOpen(t, coldDir, clock)
			defer func() { _ = ts.Close() }()
			st := ts.OpenStats()
			t.Logf("crash at %q: after New ColdRecovered %d WALTruncations %d", step, st.ColdRecovered, st.WALTruncations)
			es := ts.EventShardsForTest()["2026-07-20"]
			if es == nil || es.Tier() != TierCold {
				t.Fatalf("day 20 on the cold reopen: %+v; want cold", es)
			}
			if s, _ := es.StoreStateForTest(); s != nil {
				t.Fatal("day 20 open after New; want New's repair closed")
			}
			// New repairs it before the first read (settleMarkedAtOpen).
			if st.ColdRecovered != 1 {
				t.Fatalf("crash at %q: OpenStats().ColdRecovered %d, want 1 (New repairs the marked shard)", step, st.ColdRecovered)
			}
			// After the crash at the gate the WAL is unclean, and it is the
			// repair that reads it: WALTruncations counts only the recovery
			// probe's refusal (openShardStoreWithRecovery: read-only and
			// unmetered, it refuses the unclean WAL), after which the
			// repair's writable open cuts and replays it. No read has run
			// yet, so no read-only open read that WAL. After the crash at
			// "closed" the WAL is clean.
			wantCut := int64(0)
			if step == "gated" {
				wantCut = 1
			}
			if st.WALTruncations != wantCut {
				t.Fatalf("crash at %q: OpenStats().WALTruncations %d after New, want %d", step, st.WALTruncations, wantCut)
			}

			// The read opens day 20.
			if got := count(ts, tok); got != 4*hsRows {
				t.Fatalf("cold reopen after a crash at %q: %d rows, want %d", step, got, 4*hsRows)
			}
			s, ro := es.StoreStateForTest()
			if s == nil {
				t.Fatal("the read left day 20 closed; want its handle kept (MaxOpenColdShards)")
			}
			if !ro {
				t.Fatalf("crash at %q: the read opened day 20 writable; want read-only (the directory is clean: New repaired it)", step)
			}
		})
	}
}
