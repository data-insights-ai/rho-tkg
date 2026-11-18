package tiered

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// CRASHFIX (tasks/evidence/INTEGRATE/takeover-review.txt H3, H4). A process
// dies on ai-soc's v6 path (arch-v6-rh/cycle/cycle.go openGraph: day windows,
// ColdAfter 48 h, MintAtValidFrom, RelsByMint, SyncWrites, MaxOpenColdShards
// 0, no on-disk index; every read of a cut under one read meter). The child
// runs the store's own code and exits without Close; the parent reopens with
// the same configuration and runs the cut's write and its metered read.

const (
	cfEnvDir  = "RHO_TIERED_CRASHFIX_DIR"
	cfEnvCase = "RHO_TIERED_CRASHFIX_CASE"
)

func cfCfg(dir string, clock *tierClock) Config {
	return Config{DataDir: dir, ShardWindow: 24 * time.Hour, ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour,
		SyncWrites: true, MintAtValidFrom: true, RelsByMint: true, Clock: clock.now, RefLabels: []string{"User", "Host"}}
}

func cfOpen(t *testing.T, dir string, clock *tierClock) (*Store, uint16) {
	t.Helper()
	ts, err := New(cfCfg(dir, clock))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Logon")
	return ts, tok
}

// cfPut creates a logon at its valid-from instant at, as ai-soc's cut does.
func cfPut(t *testing.T, ts *Store, tok uint16, at time.Time) (types.NodeID, error) {
	t.Helper()
	id, ok, err := ts.MintNodeIDAt(tok, ms(at), true)
	if err != nil || !ok {
		t.Fatalf("MintNodeIDAt: %v %v", ok, err)
	}
	return id, ts.PutNode(types.NewNode(id, tok, nil))
}

// cfMeteredGet reads id through a read snapshot under a count-only meter, as
// a cut's reads run.
func cfMeteredGet(t *testing.T, ts *Store, id types.NodeID) (*types.Node, error) {
	t.Helper()
	_, meter := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
	snap := ts.NewReadSnapshot()
	defer snap.Close()
	snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(meter.Tally("cut"))
	return snap.GetNode(id)
}

func cfRunChild(t *testing.T, dir, which string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashFixChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), cfEnvDir+"="+dir, cfEnvCase+"="+which)
	out, _ := cmd.CombinedOutput()
	if code := cmd.ProcessState.ExitCode(); code != 3 {
		t.Fatalf("the child did not die in %q (exit %d):\n%s", which, code, out)
	}
}

// TestCrashFixChild is the child. "rotation": on day 22 it takes a logon,
// then runs the rotation's first disk step for day 23 (openNewEventShard
// makes events/2026-07-23, as rotateHotShardLocked does before its catalog
// save), leaves one row in that directory, and dies before the save.
// "cold": day 20 takes logons, the clock moves to day 23 (day 20 cold), a
// late logon of day 20 opens day 20 writable, and the process dies with that
// handle open (its WAL holds the late row).
func TestCrashFixChild(t *testing.T) {
	dir := os.Getenv(cfEnvDir)
	if dir == "" {
		t.Skip("child of the CRASHFIX tests")
	}
	clock := &tierClock{}
	switch os.Getenv(cfEnvCase) {
	case "rotation":
		clock.set(julDay(22, 12))
		ts, tok := cfOpen(t, dir, clock)
		if _, err := cfPut(t, ts, tok, julDay(22, 12)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		ts.mu.Lock()
		s, err := ts.openNewEventShard("2026-07-23", filepath.Join("events", "2026-07-23"), TierHot)
		if err != nil {
			t.Fatalf("openNewEventShard: %v", err)
		}
		stale := types.NodeID(tieredNodeGen(t).Generate())
		if err := s.PutNode(types.NewNode(stale, tok, nil)); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(); err != nil {
			t.Fatal(err)
		}
		os.Exit(3) // the kill: before the catalog save
	case "cold":
		clock.set(julDay(20, 12))
		ts, tok := cfOpen(t, dir, clock)
		for h := 1; h <= 3; h++ {
			if _, err := cfPut(t, ts, tok, julDay(20, h)); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
		}
		clock.set(julDay(23, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation: %v", err)
		}
		ts.SweepHandlesForTest() // day 20's demoted handle closes
		if es := ts.EventShardsForTest()["2026-07-20"]; es == nil || es.Tier() != TierCold {
			t.Fatalf("day 20 after the rotation: %+v; want cold", es)
		}
		if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // the late logon opens day 20 writable
			t.Fatalf("late PutNode: %v", err)
		}
		if s, ro := ts.EventShardsForTest()["2026-07-20"].StoreStateForTest(); s == nil || ro {
			t.Fatalf("day 20 after the late write: open %v, read-only %v; want a writable handle", s != nil, ro)
		}
		os.Exit(3) // the kill: day 20's writable handle open
	}
	os.Exit(4)
}

// TestCrashFix_KillInsideRotationKeepsCreatesOpen (H3): after a kill between
// a rotation's new-shard directory and its catalog save, the next rotation
// onto that window still takes creates, and the leftover directory's row is
// not adopted.
func TestCrashFix_KillInsideRotationKeepsCreatesOpen(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "rotation")
	if _, err := os.Stat(filepath.Join(dir, "events", "2026-07-23")); err != nil {
		t.Fatalf("the kill left no directory for day 23: %v", err)
	}
	clock := &tierClock{}
	clock.set(julDay(22, 23))
	ts, tok := cfOpen(t, dir, clock)
	defer func() { _ = ts.Close() }()
	before, err := ts.AllNodes(QueryOpts{})
	if err != nil || len(before) != 1 {
		t.Fatalf("reopen on day 22: %d nodes, %v; want the one logon", len(before), err)
	}
	clock.set(julDay(23, 1))
	for i := 0; i < 4; i++ {
		id, err := cfPut(t, ts, tok, julDay(23, 1))
		if err != nil {
			t.Fatalf("create %d on day 23 after the kill: %v", i, err)
		}
		if n, err := cfMeteredGet(t, ts, id); err != nil || n == nil || n.ID() != id {
			t.Fatalf("metered read of create %d = %v, %v; want the node", i, n, err)
		}
	}
	all, err := ts.AllNodes(QueryOpts{})
	if err != nil || len(all) != 5 {
		t.Fatalf("after day 23's creates: %d nodes, %v; want 5 (the leftover row not adopted)", len(all), err)
	}
	if got := ts.HotShardForTest().Name(); got == "2026-07-23" {
		t.Fatalf("hot shard %q took the leftover directory", got)
	}
}
