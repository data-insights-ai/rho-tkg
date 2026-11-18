package tiered

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
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
	cfEnvLate = "RHO_TIERED_CRASHFIX_LATE" // "many": the cold days that take a late write, comma-separated
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

func cfRunChild(t *testing.T, dir, which string, env ...string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashFixChild$", "-test.count=1")
	cmd.Env = append(append(os.Environ(), cfEnvDir+"="+dir, cfEnvCase+"="+which), env...)
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
		cfChildLateWrite(t, dir, clock)
		os.Exit(3) // the kill: day 20's writable handle open
	case "warm":
		clock.set(julDay(20, 12))
		ts, _ := cfOpen(t, dir, clock)
		if err := ts.Close(); err != nil { // clean: day 20's mark is cleared
			t.Fatal(err)
		}
		ts, tok := cfOpen(t, dir, clock) // day 20 opens hot again
		id, err := cfPut(t, ts, tok, julDay(20, 1))
		if err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "late-id"), []byte(strconv.FormatUint(uint64(id), 10)), 0o600); err != nil {
			t.Fatal(err)
		}
		clock.set(julDay(21, 12))
		if err := ts.CheckRotationForTest(); err != nil { // day 20 warm on its writable handle
			t.Fatalf("rotation: %v", err)
		}
		os.Exit(3) // the kill: day 20 warm and open
	case "demoted":
		clock.set(julDay(20, 12))
		ts, tok := cfOpen(t, dir, clock)
		id, err := cfPut(t, ts, tok, julDay(20, 1))
		if err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "late-id"), []byte(strconv.FormatUint(uint64(id), 10)), 0o600); err != nil {
			t.Fatal(err)
		}
		clock.set(julDay(23, 12))
		if err := ts.CheckRotationForTest(); err != nil { // day 20 cold on its hot handle; no sweep pass
			t.Fatalf("rotation: %v", err)
		}
		if s, ro := ts.EventShardsForTest()["2026-07-20"].StoreStateForTest(); s == nil || ro {
			t.Fatalf("day 20 after the rotation: open %v, read-only %v; want its writable handle", s != nil, ro)
		}
		os.Exit(3) // the kill: day 20 cold on its open writable handle
	case "noimage":
		ts := cfChildLateWrite(t, dir, clock)
		s, _ := ts.EventShardsForTest()["2026-07-20"].StoreStateForTest()
		s.SuppressCloseImageForTest() // as an index-key repair that failed at the open
		if err := ts.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		os.Exit(3) // closed, day 20 without a close header at its newest version
	case "rotated":
		// Day 20's shard is made by a rotation, not by New: its mark is the
		// rotation entry's alone.
		clock.set(julDay(19, 12))
		ts, tok := cfOpen(t, dir, clock)
		clock.set(julDay(20, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation to day 20: %v", err)
		}
		cfChildWriteLateID(t, ts, tok, dir, julDay(20, 1))
		clock.set(julDay(21, 12))
		if err := ts.CheckRotationForTest(); err != nil { // day 20 warm on its writable handle
			t.Fatalf("rotation to day 21: %v", err)
		}
		os.Exit(3) // the kill: day 20 warm and open
	case "window":
		// A logon of day 18, which has no shard, makes day 18's window shard
		// cold and open writable: its mark is the window entry's alone.
		clock.set(julDay(23, 12))
		ts, tok := cfOpen(t, dir, clock)
		cfChildWriteLateID(t, ts, tok, dir, julDay(18, 1))
		if es := ts.EventShardsForTest()["2026-07-18"]; es == nil || es.Tier() != TierCold {
			t.Fatalf("day 18 after the logon: %+v; want a cold window shard", es)
		}
		os.Exit(3) // the kill: before the sweep closes day 18
	case "swapped":
		// Day 20 turns warm and the handle sweep swaps its hot-size handle for
		// a warm-size one (a run-time close and reopen of a warm shard); a
		// later catalog save writes what the swap left in the catalog.
		clock.set(julDay(20, 12))
		cfg := cfCfg(dir, clock)
		cfg.WarmMemTableSize = 8 << 20
		ts, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		reg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(reg)
		tok, _ := reg.GetOrCreate("Logon")
		cfChildWriteLateID(t, ts, tok, dir, julDay(20, 1))
		clock.set(julDay(21, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation to day 21: %v", err)
		}
		ts.SweepHandlesForTest() // the swap
		if s, ro := ts.EventShardsForTest()["2026-07-20"].StoreStateForTest(); s == nil || ro {
			t.Fatalf("day 20 after the swap: open %v, read-only %v; want its warm-size writable handle", s != nil, ro)
		}
		if err := ts.catalog.Save(); err != nil { // any later save (a seal, a rotation, a late write's open)
			t.Fatal(err)
		}
		os.Exit(3) // the kill: day 20 warm and open
	case "many":
		clock.set(julDay(14, 12))
		ts, tok := cfOpen(t, dir, clock)
		for d := 14; d <= 22; d++ {
			clock.set(julDay(d, 12))
			if err := ts.CheckRotationForTest(); err != nil {
				t.Fatalf("rotation to day %d: %v", d, err)
			}
			if _, err := cfPut(t, ts, tok, julDay(d, 1)); err != nil {
				t.Fatalf("PutNode day %d: %v", d, err)
			}
		}
		clock.set(julDay(23, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation: %v", err)
		}
		ts.SweepHandlesForTest() // the demoted handles close
		var ids []string
		for _, f := range strings.Split(os.Getenv(cfEnvLate), ",") {
			if f == "" {
				continue
			}
			d, err := strconv.Atoi(f)
			if err != nil {
				t.Fatal(err)
			}
			late, err := cfPut(t, ts, tok, julDay(d, 4)) // opens day d writable
			if err != nil {
				t.Fatalf("late PutNode day %d: %v", d, err)
			}
			ids = append(ids, strconv.FormatUint(uint64(late), 10))
		}
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "late-id"), []byte(strings.Join(ids, ",")), 0o600); err != nil {
			t.Fatal(err)
		}
		os.Exit(3) // the kill: each late day's writable handle open
	}
	os.Exit(4)
}

// cfChildLateWrite is the "cold" child up to its kill: day 20 takes logons,
// the clock moves to day 23 (day 20 cold, its demoted handle closed), and a
// late logon of day 20 opens day 20 writable.
func cfChildLateWrite(t *testing.T, dir string, clock *tierClock) *Store {
	t.Helper()
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
	late, err := cfPut(t, ts, tok, julDay(20, 4)) // the late logon opens day 20 writable
	if err != nil {
		t.Fatalf("late PutNode: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "late-id"), []byte(strconv.FormatUint(uint64(late), 10)), 0o600); err != nil {
		t.Fatal(err)
	}
	if s, ro := ts.EventShardsForTest()["2026-07-20"].StoreStateForTest(); s == nil || ro {
		t.Fatalf("day 20 after the late write: open %v, read-only %v; want a writable handle", s != nil, ro)
	}
	return ts
}

// cfChildWriteLateID creates a logon at at and records its ID for the parent.
func cfChildWriteLateID(t *testing.T, ts *Store, tok uint16, dir string, at time.Time) {
	t.Helper()
	id, err := cfPut(t, ts, tok, at)
	if err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "late-id"), []byte(strconv.FormatUint(uint64(id), 10)), 0o600); err != nil {
		t.Fatal(err)
	}
}

// cfLateIDs reads the late logons' IDs the child wrote.
func cfLateIDs(t *testing.T, dir string) []types.NodeID {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "late-id"))
	if err != nil {
		t.Fatal(err)
	}
	var ids []types.NodeID
	for _, f := range strings.Split(string(raw), ",") {
		if f == "" {
			continue
		}
		n, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, types.NodeID(n))
	}
	return ids
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

// TestCrashFix_KillWithColdWritableHandleKeepsMeteredReads (H4, run time):
// a cold shard left needing recovery that the catalog does not mark (a catalog
// an older release wrote; at run time, a shard that goes bad while the store
// runs) is not opened by New. A metered read of it is refused (its read-only
// open cannot replay the WAL, and the replay is not charged), and the refusal
// hands the shard to the handle sweep, which recovers it: after the sweep's
// pass (the idle tick's kick) the next metered read answers, with the late row.
func TestCrashFix_KillWithColdWritableHandleKeepsMeteredReads(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "cold")
	late := cfLateIDs(t, dir)[0]
	cfDropRecoverMarks(t, dir)
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, _ := cfOpen(t, dir, clock)
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()["2026-07-20"]
	if es == nil || es.Tier() != TierCold {
		t.Fatalf("day 20 after the reopen: %+v; want cold", es)
	}
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the reopen; want it closed")
	}
	var refused *storecontract.ReadBudgetError
	if got, err := cfMeteredGet(t, ts, late); !errors.As(err, &refused) || refused.Limit != "unsupported" {
		t.Fatalf("first metered read = %v, %v; want the unsupported refusal of the WAL replay", got, err)
	}
	ts.SweepHandlesForTest() // the idle tick's kick
	got, err := cfMeteredGet(t, ts, late)
	if err != nil || got == nil || got.ID() != late {
		t.Fatalf("metered read after the sweep pass = %v, %v; want the late logon", got, err)
	}
	all, err := ts.AllNodes(QueryOpts{})
	if err != nil || len(all) != 4 {
		t.Fatalf("after the recovery: %d nodes, %v; want 4", len(all), err)
	}
}

// cfDropRecoverMarks rewrites the saved catalog without any shard's recovery
// mark, as a release before the mark wrote it.
func cfDropRecoverMarks(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "meta", "shard_catalog.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cat map[string][]map[string]any
	if err := json.Unmarshal(raw, &cat); err != nil {
		t.Fatal(err)
	}
	for _, e := range cat["shards"] {
		delete(e, "recover")
	}
	if raw, err = json.Marshal(cat); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// cfReopenAndReadAtOnce reopens dir on day 23 and checks that day 20 is closed
// and that the first metered read of late answers with no sweep pass in
// between: New recovered the shard.
func cfReopenAndReadAtOnce(t *testing.T, dir string, late types.NodeID, nodes int) {
	t.Helper()
	cfReopenAndReadShardAtOnce(t, dir, "2026-07-20", late, nodes)
}

// cfReopenAndReadShardAtOnce is cfReopenAndReadAtOnce for shard name.
func cfReopenAndReadShardAtOnce(t *testing.T, dir, name string, late types.NodeID, nodes int) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, _ := cfOpen(t, dir, clock)
	defer func() { _ = ts.Close() }()
	es := ts.EventShardsForTest()[name]
	if es == nil || es.Tier() != TierCold {
		t.Fatalf("%s after the reopen: %+v; want cold", name, es)
	}
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatalf("%s open after the reopen; want it recovered and closed", name)
	}
	got, err := cfMeteredGet(t, ts, late)
	if err != nil || got == nil || got.ID() != late {
		t.Fatalf("first metered read after the reopen = %v, %v; want the late logon, no wait", got, err)
	}
	if n := ts.sweep.passes.Load(); n != 0 {
		t.Fatalf("handle sweep passes before the read: %d; want 0", n)
	}
	all, err := ts.AllNodes(QueryOpts{})
	if err != nil || len(all) != nodes {
		t.Fatalf("after the reopen: %d nodes, %v; want %d", len(all), err, nodes)
	}
}

// TestCrashFix_KillWithColdWritableHandleRecoversAtOpen (H4): after a kill
// with a cold shard's writable handle open (a late write), the reopen recovers
// that shard before it returns, so the cut's first metered read of it answers.
func TestCrashFix_KillWithColdWritableHandleRecoversAtOpen(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "cold")
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 4)
}

// TestCrashFix_KillWithDemotedHandleRecoversAtOpen (H4): a kill after a
// rotation demoted day 20 to cold with its writable handle still open (before
// the handle sweep closed it); the reopen recovers it before it returns.
func TestCrashFix_KillWithDemotedHandleRecoversAtOpen(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "demoted")
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 1)
}

// TestCrashFix_KillWithWarmShardDemotedAtOpenRecovers (H4): day 20 closes
// cleanly (its mark cleared), reopens hot (New marks it again before it
// opens), turns warm, and the process is killed; the reopen two days later
// opens it cold (ColdAfter) without its warm open, and recovers it before it
// returns.
func TestCrashFix_KillWithWarmShardDemotedAtOpenRecovers(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "warm")
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 1)
}

// TestCrashFix_CloseWithoutImageRecoversAtOpen (H4): the second unclean
// state. A cold shard's writable handle closes without writing its close image
// (the open's index-key repair failed; badgerstore_open_image.go), so the
// directory holds no WAL but no close header at its newest version, and a
// metered read-only open refuses it (prepareAsOfIndexOpen). The reopen
// recovers it before it returns, as after a kill.
func TestCrashFix_CloseWithoutImageRecoversAtOpen(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "noimage")
	if mems, _ := filepath.Glob(filepath.Join(dir, "events", "2026-07-20", "*.mem")); len(mems) != 0 {
		t.Fatalf("day 20 after the clean close holds WAL files %v; want none", mems)
	}
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 4)
}

// TestCrashFix_OpenRecoversOnlyMarkedColdShards (H4, the bound): New opens
// only the cold shards the catalog marks for recovery, the ones open writable
// at the kill. Seven cold days (14 to 20); the child takes late writes into
// none, one or three of them and is killed with their handles open. badger's
// own open count (badger.SetOpenHookForTest, which the tiered store does not
// keep) shows New opening exactly those cold shards, and each one's first
// metered read answers.
func TestCrashFix_OpenRecoversOnlyMarkedColdShards(t *testing.T) {
	testtier.Slow(t)
	for _, tc := range []struct {
		name string
		late []int
	}{{"none", nil}, {"one", []int{17}}, {"three", []int{15, 17, 19}}} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			var days []string
			for _, d := range tc.late {
				days = append(days, strconv.Itoa(d))
			}
			cfRunChild(t, dir, "many", cfEnvLate+"="+strings.Join(days, ","))
			lates := cfLateIDs(t, dir)
			events := filepath.Join(dir, "events") + string(filepath.Separator)
			var mu sync.Mutex
			opened := map[string]int{}
			restore := badger.SetOpenHookForTest(func(d string, _ bool) {
				if rest, ok := strings.CutPrefix(d, events); ok {
					mu.Lock()
					opened[rest]++
					mu.Unlock()
				}
			})
			clock := &tierClock{}
			clock.set(julDay(23, 13))
			ts, _ := cfOpen(t, dir, clock)
			restore()
			defer func() { _ = ts.Close() }()
			t.Logf("reopen with %d marked cold shards: %v (%+v)", len(tc.late), ts.OpenStats().Duration, ts.OpenStats())
			if got := ts.OpenStats().ColdRecovered; got != len(tc.late) {
				t.Errorf("OpenStats().ColdRecovered = %d; want %d", got, len(tc.late))
			}
			want := map[string]bool{}
			for _, d := range tc.late {
				want[fmt.Sprintf("2026-07-%02d", d)] = true
			}
			for d := 14; d <= 20; d++ {
				name := fmt.Sprintf("2026-07-%02d", d)
				es := ts.EventShardsForTest()[name]
				if es == nil || es.Tier() != TierCold {
					t.Fatalf("%s after the reopen: %+v; want cold", name, es)
				}
				if got := opened[name] > 0; got != want[name] {
					t.Errorf("%s opened %d times by New; want opened %v", name, opened[name], want[name])
				}
				if s, _ := es.StoreStateForTest(); s != nil {
					t.Errorf("%s open after the reopen; want closed", name)
				}
			}
			for i, late := range lates {
				got, err := cfMeteredGet(t, ts, late)
				if err != nil || got == nil || got.ID() != late {
					t.Fatalf("first metered read of day %d's late logon = %v, %v; want it", tc.late[i], got, err)
				}
			}
		})
	}
}

// cfSetRecoverMark rewrites the saved catalog with shard name's recovery
// mark set, as a kill with its writable handle open leaves it.
func cfSetRecoverMark(t *testing.T, dir, name string) {
	t.Helper()
	path := filepath.Join(dir, "meta", "shard_catalog.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var cat map[string][]map[string]any
	if err := json.Unmarshal(raw, &cat); err != nil {
		t.Fatal(err)
	}
	found := false
	for _, e := range cat["shards"] {
		if e["name"] == name {
			e["recover"], found = true, true
		}
	}
	if !found {
		t.Fatalf("no catalog entry %s", name)
	}
	if raw, err = json.Marshal(cat); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestCrashFix_RecoveryAtOpenWalksNoHistory (H4b M3): New's recovery of a
// marked cold shard seals its counts without building its former-label set,
// as every close on a read's or a write's path does: New is the foreground,
// and the build walks every history row of the shard. Day 20 holds 400 logons
// with one history row each and closes cleanly; its mark is set in the saved
// catalog, and badger's own count of index iterator steps (which the tiered
// store does not keep) over New is 0, the steps the seal with the build would
// take being about three a history row.
func TestCrashFix_RecoveryAtOpenWalksNoHistory(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(20, 12))
	cfg := cfCfg(dir, clock)
	cfg.SyncWrites = false // the setup only; the reopen runs cfCfg
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Logon")
	const rows = 400
	var last types.NodeID
	for i := 0; i < rows; i++ {
		id, err := cfPut(t, ts, tok, julDay(20, 1).Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
		if err := ts.PutNodeVersion(id, 0, types.NewNode(id, tok, nil)); err != nil {
			t.Fatalf("PutNodeVersion %d: %v", i, err)
		}
		last = id
	}
	clock.set(julDay(23, 12))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	ts.SweepHandlesForTest()
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	cfSetRecoverMark(t, dir, "2026-07-20")
	clock.set(julDay(23, 13))
	visited, stop := badger.CountIndexEntriesVisitedForTest()
	ts, _ = cfOpen(t, dir, clock)
	steps := visited()
	stop()
	defer func() { _ = ts.Close() }()
	if got := ts.OpenStats().ColdRecovered; got != 1 {
		t.Fatalf("OpenStats().ColdRecovered = %d; want 1 (day 20)", got)
	}
	if steps != 0 {
		t.Errorf("index iterator steps inside New with one recovered shard of %d history rows: %d; want 0", rows, steps)
	}
	if got, err := cfMeteredGet(t, ts, last); err != nil || got == nil || got.ID() != last {
		t.Fatalf("first metered read after the reopen = %v, %v; want the logon", got, err)
	}
}

// TestCrashFix_KillWithRotatedWarmShardDemotedAtOpenRecovers (H4b M2a): the
// usual production case. Day 20's shard is made by a rotation (the process
// started on day 19), turns warm and the process is killed; the reopen two
// days later opens it cold (ColdAfter) and recovers it before it returns, by
// the mark the rotation's catalog entry carries.
func TestCrashFix_KillWithRotatedWarmShardDemotedAtOpenRecovers(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "rotated")
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 1)
}

// TestCrashFix_KillWithColdWindowShardRecoversAtOpen (H4b M2b): a logon minted
// into a day without a shard makes that day's window shard, cold and open
// writable; a kill before the sweep closes it, and the reopen recovers it
// before it returns, by the mark the window entry carries.
func TestCrashFix_KillWithColdWindowShardRecoversAtOpen(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "window")
	cfReopenAndReadShardAtOnce(t, dir, "2026-07-18", cfLateIDs(t, dir)[0], 1)
}

// TestCrashFix_KillAfterWarmSwapDemotedAtOpenRecovers (H4b M2c): the run-time
// close clears the mark of a cold shard only. Day 20, warm, has its handle
// swapped by the handle sweep (closed cleanly and reopened writable at the
// warm size), a later save writes the catalog, and the process is killed; the
// reopen two days later opens day 20 cold and recovers it before it returns,
// by the mark the swap's close kept.
func TestCrashFix_KillAfterWarmSwapDemotedAtOpenRecovers(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "swapped")
	cfReopenAndReadAtOnce(t, dir, cfLateIDs(t, dir)[0], 1)
}

// TestCrashFix_CleanCloseClearsMarks (H4b M2d): Store.Close clears the mark
// of every writable handle it closes with its close image, so the reopen after
// a clean close recovers nothing. Day 20 is cold with a late write's writable
// handle open when the store closes; badger's own open count (which the
// tiered store does not keep) shows New opening no cold shard.
func TestCrashFix_CleanCloseClearsMarks(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	ts := cfChildLateWrite(t, dir, &tierClock{})
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	day20 := filepath.Join(dir, "events", "2026-07-20")
	var mu sync.Mutex
	opens := 0
	restore := badger.SetOpenHookForTest(func(d string, _ bool) {
		if d == day20 {
			mu.Lock()
			opens++
			mu.Unlock()
		}
	})
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, _ = cfOpen(t, dir, clock)
	restore()
	defer func() { _ = ts.Close() }()
	if got := ts.OpenStats().ColdRecovered; got != 0 {
		t.Errorf("OpenStats().ColdRecovered after a clean close = %d; want 0", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if opens != 0 {
		t.Errorf("day 20 opened %d times by New after a clean close; want 0", opens)
	}
}
