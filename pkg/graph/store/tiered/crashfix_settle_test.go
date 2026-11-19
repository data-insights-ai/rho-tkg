package tiered

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A cold shard's derived state after any close, on ai-soc's v6 configuration
// (cfCfg: day windows, ColdAfter 48 h, MaxOpenColdShards 0). A closed cold
// shard whose catalog entry is not settled (marked for recovery, not sealed,
// or sealed with rows and no former-label set) is owed, whichever close, kill
// or open left it so, and the handle sweep settles it off every reader's
// meter: afterwards a label scan's ever-member gather under a tight budget
// opens nothing and charges nothing, where before it was refused on every
// retry.

const (
	cfHistoryRows = 400 // day 20's logons, each with one history row
	cfFormerRows  = 10  // the history rows that carry the label Former
	cfDay20Name   = "2026-07-20"
)

// cfTight is a cut's budget below one shard's former-label build (about one
// history examination a history row).
var cfTight = storecontract.ReadBudget{HistoryExamined: 100}

// cfOpenHeld is cfOpen with the sweep's settle steps held from New on
// (holdSettleSteps): the queued shards stay queued, so a test looks at what
// New or a close left before the sweep settles it. cfRelease lets the steps
// run to the end.
func cfOpenHeld(t *testing.T, dir string, clock *tierClock) (*Store, uint16) {
	t.Helper()
	cfg := cfCfg(dir, clock)
	cfg.sweepFault = holdSettleSteps
	return cfOpenCfg(t, cfg)
}

func cfRelease(ts *Store) {
	ts.SetSweepFaultForTest(nil)
	ts.SweepHandlesForTest()
}

// cfSettleSteps is the number of settle steps the sweep has run.
func cfSettleSteps(ts *Store) int64 { return ts.sweep.settles.Load() }

// cfAfterIdleTimeout runs the sweep once IdleTimeout has passed since the
// last write's check-in close (the build of a shard still being written
// waits that long): it lowers the timeout to do so.
func cfAfterIdleTimeout(ts *Store) {
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.SweepHandlesForTest()
}

// cfWriteHistoryLogons installs the registry (Logon, then Former) and writes
// rows logons on day 20, each with one history row; the first cfFormerRows
// history rows carry Former, so Former has that many former members on day
// 20. Returns Logon's token (Former's is the next) and the logons.
func cfWriteHistoryLogons(t *testing.T, ts *Store, rows int) (uint16, []types.NodeID) {
	t.Helper()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Logon")
	former, _ := reg.GetOrCreate("Former")
	ids := make([]types.NodeID, 0, rows)
	for i := 0; i < rows; i++ {
		id, err := cfPut(t, ts, tok, julDay(20, 1).Add(time.Duration(i)*time.Second))
		if err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
		was := tok
		if i < cfFormerRows {
			was = former
		}
		if err := ts.PutNodeVersion(id, 0, types.NewNode(id, was, nil)); err != nil {
			t.Fatalf("PutNodeVersion %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	return tok, ids
}

// cfHistoryStore opens dir on day 20 for a setup (no SyncWrites: the reopen
// under test runs cfCfg) and writes day 20's history logons.
func cfHistoryStore(t *testing.T, dir string, clock *tierClock) (*Store, uint16, []types.NodeID) {
	t.Helper()
	clock.set(julDay(20, 12))
	cfg := cfCfg(dir, clock)
	cfg.SyncWrites = false
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tok, ids := cfWriteHistoryLogons(t, ts, cfHistoryRows)
	return ts, tok, ids
}

// cfHistoryColdDay is a store on day 23 whose day 20 holds the history
// logons, cold and closed, its demoted handle closed by the sweep.
func cfHistoryColdDay(t *testing.T, dir string, clock *tierClock) (*Store, uint16, []types.NodeID) {
	t.Helper()
	ts, tok, ids := cfHistoryStore(t, dir, clock)
	clock.set(julDay(23, 12))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	ts.SweepHandlesForTest()
	if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the sweep; want closed")
	}
	return ts, tok, ids
}

// cfSettleChild runs the settle tests' children; it returns for a case that
// is not one of them.
func cfSettleChild(t *testing.T, which, dir string, clock *tierClock) {
	t.Helper()
	switch which {
	case "history":
		// Day 20 takes the history logons and turns cold, and a late logon
		// opens it writable; the kill leaves it marked.
		ts, tok, _ := cfHistoryColdDay(t, dir, clock)
		cfChildWriteLateID(t, ts, tok, dir, julDay(20, 4))
		if s, ro := cfDay20(t, ts).StoreStateForTest(); s == nil || ro {
			t.Fatalf("day 20 after the late write: open %v, read-only %v; want a writable handle", s != nil, ro)
		}
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		os.Exit(3) // the kill: day 20's writable handle open
	case "emptywarm":
		// Day 20 opens hot with no row, turns warm, and the process is
		// killed: the reopen two days later recovers a shard that holds none.
		clock.set(julDay(20, 12))
		ts, _ := cfOpen(t, dir, clock)
		clock.set(julDay(21, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation to day 21: %v", err)
		}
		os.Exit(3) // the kill: day 20 warm and open, empty
	case "settlekill":
		// The reopen of a "many" directory (seven recovered shards owe their
		// build), killed as the sweep starts its second settle step: the first
		// step and its pass's save stand.
		clock.set(julDay(23, 13))
		cfg := cfCfg(dir, clock)
		var steps atomic.Int64
		cfg.sweepFault = func(step string) error {
			if step == "settle" && steps.Add(1) == 2 {
				os.Exit(3) // the kill: the second step
			}
			return nil
		}
		ts, _ := cfOpenCfg(t, cfg)
		ts.SweepHandlesForTest()
		cfChildLinger() // a pass of the sweep goroutine took the step
	case "buildkill":
		// The reopen of a "history" directory, killed inside the build step:
		// its read-only handle open, the walk about to start.
		clock.set(julDay(23, 13))
		cfg := cfCfg(dir, clock)
		cfg.sweepFault = func(step string) error {
			if step == "build" {
				os.Exit(3) // the kill: inside the build
			}
			return nil
		}
		ts, _ := cfOpenCfg(t, cfg)
		ts.SweepHandlesForTest()
		cfChildLinger()
	case "repairkill":
		// The reopen of a "history" directory whose catalog carries no mark,
		// killed inside the repair step: at the writable open of day 20, after
		// the step saved the mark.
		clock.set(julDay(23, 13))
		day20 := filepath.Join(dir, "events", cfDay20Name)
		ts, _ := cfOpenHeld(t, dir, clock)
		badger.SetOpenHookForTest(func(d string, readOnly bool) {
			if d == day20 && !readOnly {
				os.Exit(3) // the kill: inside the repair
			}
		})
		cfRelease(ts)
		cfChildLinger()
	case "clearkill":
		// Day 20, cold, closed and sealed with its history logons; Clear is
		// killed after the reference shard's clear (the pending marker is
		// gone) and before its last catalog save.
		ts, _, _ := cfHistoryColdDay(t, dir, clock)
		if err := ts.Flush(); err != nil {
			t.Fatal(err)
		}
		clearStepFaultForTest = func(stage string) error {
			if stage == "reference shard" {
				os.Exit(3) // the kill: inside Clear
			}
			return nil
		}
		_ = ts.Clear()
	}
}

// cfChildLinger keeps a child alive for the kill a pass of its sweep goroutine
// makes; a child that outlives it exits 4 (no kill).
func cfChildLinger() {
	time.Sleep(30 * time.Second)
}

// cfGatherProbe runs a cut's label scan, the ever-member gather of one label
// (ForEachLabelTxMemberMetered), under a budget, and counts the opens of day
// 20's directory over it at the store's own boundary (every shard open goes
// through newShardStore, whose hook is per store, so these tests run in
// parallel), a count the read meter under test does not keep.
type cfGatherProbe struct {
	ts    *Store
	opens atomic.Int64
}

func cfNewGatherProbe(t *testing.T, ts *Store, dir string) *cfGatherProbe {
	t.Helper()
	p := &cfGatherProbe{ts: ts}
	day20 := filepath.Join(dir, "events", cfDay20Name)
	ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
		if cfg.Dir == day20 {
			p.opens.Add(1)
		}
	})
	t.Cleanup(func() { ts.SetShardOpenHookForTest(nil) })
	return p
}

func (p *cfGatherProbe) gather(t *testing.T, token uint16, b storecontract.ReadBudget) (members int, opens int64, work storecontract.ReadWork, err error) {
	t.Helper()
	_, meter := storecontract.WithReadBudget(t.Context(), b)
	before := p.opens.Load()
	err = p.ts.ForEachLabelTxMemberMetered(token, meter.Tally("cut"), func(types.NodeID, types.Instant) bool {
		members++
		return true
	})
	return members, p.opens.Load() - before, meter.Work(), err
}

// wantSettled checks what a settled day 20 gives a cut: the ever-member gather
// of a label day 20 holds no member of, under the tight budget, opens nothing
// and charges nothing; and the exact, non-empty unlimited control, Former's
// former members, which the set keeps (so that gather opens day 20, once).
func (p *cfGatherProbe) wantSettled(t *testing.T, tok uint16, when string) {
	t.Helper()
	n, opens, work, err := p.gather(t, tok+7, cfTight)
	t.Logf("%s: tight gather of an absent label: err=%v members=%d opens=%d work=%+v", when, err, n, opens, work)
	if err != nil || n != 0 || opens != 0 || work != (storecontract.ReadWork{}) {
		t.Fatalf("%s: tight gather = %d members, %d opens, %+v, %v; want 0, 0 opens, no work, nil", when, n, opens, work, err)
	}
	if n, opens, _, err := p.gather(t, tok+1, storecontract.ReadBudget{}); err != nil || n != cfFormerRows || opens != 1 {
		t.Fatalf("%s: unlimited Former gather = %d members, %d opens, %v; want %d, 1 open", when, n, opens, err, cfFormerRows)
	}
	if n, opens, work, err := p.gather(t, tok+7, cfTight); err != nil || n != 0 || opens != 0 || work != (storecontract.ReadWork{}) {
		t.Fatalf("%s: tight gather after the control = %d members, %d opens, %+v, %v; want 0, 0 opens, no work, nil", when, n, opens, work, err)
	}
}

// cfSavedEntry loads shard name's entry from the saved catalog.
func cfSavedEntry(t *testing.T, dir, name string) ShardEntry {
	t.Helper()
	sc := NewShardCatalog(filepath.Join(dir, "meta", "shard_catalog.json"))
	if err := sc.Load(); err != nil {
		t.Fatal(err)
	}
	e, ok := sc.GetShard(name)
	if !ok {
		t.Fatalf("no saved catalog entry %s", name)
	}
	return *e
}

// cfEntrySettled is the design's "settled", read from an entry.
func cfEntrySettled(e ShardEntry) bool {
	return !e.Recover && e.CountsSealed && (e.NoRows || e.FormerLabelsKnown)
}

// cfWantSettledEntry checks that day 20 is closed and that its entry is
// settled in memory and in the saved catalog.
func cfWantSettledEntry(t *testing.T, ts *Store, dir, when string) {
	t.Helper()
	if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil {
		t.Fatalf("%s: day 20 open; want closed", when)
	}
	mem, _ := ts.CatalogForTest().GetShard(cfDay20Name)
	if saved := cfSavedEntry(t, dir, cfDay20Name); !cfEntrySettled(*mem) || !cfEntrySettled(saved) {
		t.Fatalf("%s: day 20 not settled: in memory %+v, saved %+v", when, *mem, saved)
	}
}

// cfEditSavedCatalog rewrites the saved catalog through edit, entry by entry.
func cfEditSavedCatalog(t *testing.T, dir string, edit func(entry map[string]any)) {
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
		edit(e)
	}
	if raw, err = json.Marshal(cat); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

// cfReopenDay23 reopens dir on day 23 with ai-soc's configuration.
func cfReopenDay23(t *testing.T, dir string) (*Store, uint16) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, tok := cfOpen(t, dir, clock)
	t.Cleanup(func() { _ = ts.Close() })
	return ts, tok
}

// TestCrashFix_KilledShardSettles: a kill with a late write's handle open on
// day 20. New recovers the shard before its first read and walks no history;
// the sweep then builds its former-label set. Before, nothing built it at
// MaxOpenColdShards 0 and the tight gather was refused on every retry.
func TestCrashFix_KilledShardSettles(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "history")
	ts, tok := cfReopenDay23(t, dir)
	if got := ts.OpenStats().ColdRecovered; got != 1 {
		t.Fatalf("OpenStats().ColdRecovered = %d; want 1 (day 20)", got)
	}
	ts.SweepHandlesForTest()
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
	cfWantSettledEntry(t, ts, dir, "after the sweep")
	if got, err := cfMeteredGet(t, ts, cfLateIDs(t, dir)[0]); err != nil || got == nil {
		t.Fatalf("metered read of the late logon = %v, %v; want it", got, err)
	}
}

// TestCrashFix_CleanCloseAcrossColdAfterSettles: day 20 is warm at a clean
// Close and past ColdAfter at the next New, which mounts it cold, closed and
// never sealed (a warm shard carries no seal). No kill is involved: this is
// every restart that crosses a ColdAfter boundary. The sweep settles it;
// before, it stayed unsealed and the tight gather was refused for the whole
// run.
func TestCrashFix_CleanCloseAcrossColdAfterSettles(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	clock := &tierClock{}
	ts, _, _ := cfHistoryStore(t, dir, clock)
	clock.set(julDay(21, 12))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	ts.SweepHandlesForTest()
	if es := ts.EventShardsForTest()[cfDay20Name]; es.Tier() != TierWarm {
		t.Fatalf("day 20 before the close: %v; want warm", es.Tier())
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts, tok := cfReopenDay23(t, dir)
	if st := ts.OpenStats(); st.Demoted != 1 || st.ColdRecovered != 0 {
		t.Fatalf("OpenStats: demoted %d, recovered %d; want 1 demoted at open, none recovered", st.Demoted, st.ColdRecovered)
	}
	ts.SweepHandlesForTest()
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
	cfWantSettledEntry(t, ts, dir, "after the sweep")
	if n, err := ts.NodeCountByLabel(tok); err != nil || n != cfHistoryRows {
		t.Fatalf("NodeCountByLabel(Logon) from the seal = %d, %v; want %d", n, err, cfHistoryRows)
	}
}

// TestCrashFix_TransientWriteCloseSettles: no restart at all. A by-ID write
// into the cold, settled day 20 opens it writable (which unseals it) and, at
// MaxOpenColdShards 0, closes it at its check-in, on the writer's path: that
// close walks no history, so it leaves the set unknown, and hands the shard to
// the sweep. Before, the set stayed unknown until a later idle close of
// another handle. The sweep's build waits until the writes have stopped for
// IdleTimeout, as the idle close of a handle left open does: a burst of three
// writes is one walk after it, not a walk behind each write.
func TestCrashFix_TransientWriteCloseSettles(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ts, tok, ids := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	p := cfNewGatherProbe(t, ts, dir)
	p.wantSettled(t, tok, "before the writes")
	steps := cfSettleSteps(ts)
	for i := 0; i < 3; i++ {
		v1 := types.NewNode(ids[cfFormerRows+i], tok, nil)
		v1.SetVersion(1)
		if err := ts.PutNodeVersion(v1.ID(), 1, v1); err != nil {
			t.Fatalf("by-ID write %d into day 20: %v", i, err)
		}
		if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil {
			t.Fatal("day 20 open after the by-ID write's check-in; want closed (MaxOpenColdShards 0)")
		}
		ts.SweepHandlesForTest()
		e, _ := ts.CatalogForTest().GetShard(cfDay20Name)
		if !e.CountsSealed || e.FormerLabelsKnown || e.Recover || cfQueued(ts) != 1 || cfSettleSteps(ts) != steps {
			t.Fatalf("after write %d and a sweep: entry %+v, %d queued, %d settle steps; want sealed without the set (the writer's close walked nothing), queued, and no step while the writes go on",
				i, *e, cfQueued(ts), cfSettleSteps(ts)-steps)
		}
	}
	cfAfterIdleTimeout(ts)
	if got := cfSettleSteps(ts) - steps; got != 1 {
		t.Fatalf("settle steps after three writes: %d; want 1 (one walk for the burst)", got)
	}
	p.wantSettled(t, tok, "after the writes and the sweep")
	cfWantSettledEntry(t, ts, dir, "after the writes and the sweep")
}

// TestCrashFix_OlderCatalogSettles: catalogs written before this model. (a)
// Day 20 sealed without a former-label set (no release wrote one): owed, and
// built. (b) A release without the recovery mark killed with day 20's
// writable handle open: no mark names it, its entry is merely unsealed; the
// sweep's open is refused as a metered read's would be and recovers it, so no
// read has to be refused first.
func TestCrashFix_OlderCatalogSettles(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	t.Run("sealed without the set", func(t *testing.T) {
		dir := t.TempDir()
		ts, _, _ := cfHistoryColdDay(t, dir, &tierClock{})
		if err := ts.Close(); err != nil {
			t.Fatal(err)
		}
		cfEditSavedCatalog(t, dir, func(e map[string]any) {
			delete(e, "former_labels_known")
			delete(e, "former_labels")
		})
		if e := cfSavedEntry(t, dir, cfDay20Name); !e.CountsSealed || e.FormerLabelsKnown {
			t.Fatalf("setup: day 20's saved entry %+v; want sealed without the set", e)
		}
		ts, tok := cfReopenDay23(t, dir)
		ts.SweepHandlesForTest()
		cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
		cfWantSettledEntry(t, ts, dir, "after the sweep")
	})
	t.Run("no mark and a WAL", func(t *testing.T) {
		dir := t.TempDir()
		cfRunChild(t, dir, "history")
		cfDropRecoverMarks(t, dir)
		ts, tok := cfReopenDay23(t, dir)
		if got := ts.OpenStats().ColdRecovered; got != 0 {
			t.Fatalf("OpenStats().ColdRecovered = %d; want 0 (no mark)", got)
		}
		ts.SweepHandlesForTest()
		if got, err := cfMeteredGet(t, ts, cfLateIDs(t, dir)[0]); err != nil || got == nil {
			t.Fatalf("first metered read of the late logon = %v, %v; want it, never refused", got, err)
		}
		cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
		cfWantSettledEntry(t, ts, dir, "after the sweep")
	})
}

// TestCrashFix_ClearUnsealsColdShards: Clear empties a closed cold shard
// through a writable handle of its own. Its sealed counts, which every count
// door answers from, must go before that: they named the rows Clear removed.
// Before, NodeCount and NodeCountByLabel kept answering the old numbers, and
// a reopen adopted them.
func TestCrashFix_ClearUnsealsColdShards(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ts, tok, _ := cfHistoryColdDay(t, dir, &tierClock{})
	if n, err := ts.NodeCountByLabel(tok); err != nil || n != cfHistoryRows {
		t.Fatalf("NodeCountByLabel before Clear = %d, %v; want %d", n, err, cfHistoryRows)
	}
	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	counts := func(when string) {
		t.Helper()
		n, err := ts.NodeCount()
		l, lerr := ts.NodeCountByLabel(tok)
		all, aerr := ts.AllNodes(QueryOpts{})
		t.Logf("%s: NodeCount=%d NodeCountByLabel=%d AllNodes=%d (%v %v %v)", when, n, l, len(all), err, lerr, aerr)
		if err != nil || lerr != nil || aerr != nil || n != 0 || l != 0 || len(all) != 0 {
			t.Fatalf("%s: NodeCount %d, NodeCountByLabel %d, AllNodes %d (%v, %v, %v); want 0, 0, 0", when, n, l, len(all), err, lerr, aerr)
		}
	}
	// Clear queued the shard it cleared (no read has touched it yet): the
	// sweep seals it empty, so every fan-out drops it unopened.
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); e.CountsSealed {
		t.Fatalf("day 20's entry after Clear: %+v; want unsealed", *e)
	}
	ts.SweepHandlesForTest()
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); !cfEntrySettled(*e) || !e.NoRows {
		t.Fatalf("day 20's entry after Clear and the sweep: %+v; want settled with no rows", *e)
	}
	counts("after Clear and the sweep")
	// A second Clear finds the shard sealed empty: that seal goes too, in the
	// catalog and in the shard's own no-rows cache, until the sweep seals it
	// again.
	ts.HoldSettleStepsForTest()
	if err := ts.Clear(); err != nil {
		t.Fatalf("second Clear: %v", err)
	}
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); e.CountsSealed || e.NoRows || cfDay20(t, ts).noRows.Load() {
		t.Fatalf("day 20 after a Clear of a shard sealed empty: entry %+v, no-rows cache %v; want unsealed, the cache dropped", *e, cfDay20(t, ts).noRows.Load())
	}
	cfRelease(ts)
	counts("after the second Clear and the sweep")
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts, _ = cfReopenDay23(t, dir)
	counts("after the reopen")
	ts.SweepHandlesForTest()
	counts("after the reopen and the sweep")
}

// TestShardCatalog_RestoreKeepsDerivedState: a catalog
// transaction (a rotation, a window shard, a move, a drop) snapshots the
// entries and puts them back when its save fails. A late write's open of
// another shard, which holds only that shard's lock, unseals and marks it
// meanwhile. The rollback must undo the transaction's own changes and keep
// that unseal and mark: put back, the stale seal and no-rows mark would be
// saved by the next save and adopted after a kill, hiding the new rows.
func TestShardCatalog_RestoreKeepsDerivedState(t *testing.T) {
	sc := NewShardCatalog(filepath.Join(t.TempDir(), "shard_catalog.json"))
	base := julDay(1, 0)
	for i, n := range []string{"a", "b", "gone"} {
		sc.AddShard(ShardEntry{Name: n, Kind: ShardEvent, Tier: TierCold, Path: "events/" + n,
			TimeStart: base.Add(time.Duration(i) * 24 * time.Hour), TimeEnd: base.Add(time.Duration(i+1) * 24 * time.Hour)})
	}
	sc.SealShardCounts("b", 7, 0, map[uint16]int{1: 7}, nil)
	sc.SetShardNoRows("b", false)
	sc.SetShardFormerLabels("b", []uint16{3}, true)
	sc.SealShardCounts("a", 0, 0, nil, nil)
	sc.SetShardNoRows("a", true)
	sc.SealShardCounts("gone", 5, 2, map[uint16]int{1: 5}, map[uint16]int{4: 2})
	sc.AddShard(ShardEntry{Name: "s", Kind: ShardEvent, Tier: TierCold, Path: "events/s", TimeStart: base, TimeEnd: base})
	sc.SealShardCounts("s", 5, 2, map[uint16]int{1: 5}, map[uint16]int{4: 2})
	sc.AddShard(ShardEntry{Name: "c", Kind: ShardEvent, Tier: TierCold, Path: "events/c", TimeStart: base, TimeEnd: base, ApproxNodes: 7, ApproxRels: 3})
	sc.AddShard(ShardEntry{Name: "r", Kind: ShardEvent, Tier: TierCold, Path: "events/r", TimeStart: base, TimeEnd: base})
	sc.SealShardCounts("r", 5, 2, map[uint16]int{1: 5}, map[uint16]int{4: 2})

	snapshot := sc.snapshotShards()
	// The transaction: a new shard, a tier change, a removal.
	sc.AddShard(ShardEntry{Name: "new", Kind: ShardEvent, Tier: TierHot, Path: "events/new", TimeStart: base, TimeEnd: base, Recover: true})
	sc.UpdateShardTier("a", TierWarm)
	sc.RemoveShard("gone")
	// Meanwhile, under their own shard locks: b opens writable (unseal,
	// mark), a's no-rows mark is woken.
	sc.UnsealShardCounts("b")
	sc.SetShardRecover("b", true)
	sc.SetShardNoRows("a", false)
	// The transaction's own statistics, on a sealed shard and on one a close
	// seals meanwhile.
	sc.UpdateShardStats("s", 9, 9)
	sc.UpdateShardStats("c", 9, 9)
	sc.SealShardCounts("c", 1, 0, map[uint16]int{1: 1}, nil)
	// r is opened for a late write and sealed again by its close: one node
	// more, the counts by label and by type as they were (a node that changes
	// none, or one label moved from a node to a new one).
	sc.UnsealShardCounts("r")
	sc.SetShardRecover("r", true)
	sc.SealShardCounts("r", 6, 2, map[uint16]int{1: 5}, map[uint16]int{4: 2})
	sc.SetShardRecover("r", false)

	sc.restoreShards(snapshot)

	if _, ok := sc.GetShard("new"); ok {
		t.Error("the transaction's new shard survived the rollback")
	}
	if _, ok := sc.GetShard("gone"); !ok {
		t.Error("the shard the transaction removed is not back")
	}
	a, _ := sc.GetShard("a")
	if a.Tier != TierCold {
		t.Errorf("a's tier after the rollback: %v; want cold (the transaction's change undone)", a.Tier)
	}
	if a.NoRows {
		t.Error("a's no-rows mark is back after the rollback; a write woke it meanwhile")
	}
	if e, _ := sc.GetShard("s"); !e.CountsSealed || e.ApproxNodes != 5 || e.ApproxRels != 2 {
		t.Errorf("s after the rollback: %+v; want its seal and the totals 5 and 2 (the transaction's statistics undone)", *e)
	}
	if e, _ := sc.GetShard("c"); !e.CountsSealed || e.ApproxNodes != 1 || e.ApproxRels != 0 {
		t.Errorf("c after the rollback: %+v; want the seal a close put on it meanwhile, with that seal's totals 1 and 0", *e)
	}
	if e, _ := sc.GetShard("r"); !e.CountsSealed || e.ApproxNodes != 6 || e.ApproxRels != 2 {
		t.Errorf("r after the rollback: sealed %v with %d nodes and %d rels; want the new seal's 6 and 2 (resealed meanwhile, equal counts by label)", e.CountsSealed, e.ApproxNodes, e.ApproxRels)
	}
	b, _ := sc.GetShard("b")
	if b.CountsSealed || b.FormerLabelsKnown || len(b.NodeCountsByLabel) != 0 || !b.Recover {
		t.Errorf("b after the rollback: sealed %v, former known %v, counts %v, recover %v; want the unseal and the mark kept",
			b.CountsSealed, b.FormerLabelsKnown, b.NodeCountsByLabel, b.Recover)
	}
}

// cfStepper is a sweep step hook that lets the settle steps through one at a
// time, on the test's word.
type cfStepper struct{ allow atomic.Int64 }

func (s *cfStepper) fault(step string) error {
	if step != "settle" {
		return nil
	}
	for {
		n := s.allow.Load()
		if n <= 0 {
			return errSettleStepsHeld
		}
		if s.allow.CompareAndSwap(n, n-1) {
			return nil
		}
	}
}

// step lets one settle step run and returns once its pass has ended (its
// catalog save included): the calling goroutine's pass runs it, or waits for
// the sweep goroutine's pass that took it.
func (s *cfStepper) step(ts *Store) {
	s.allow.Add(1)
	ts.SweepPassForTest()
}

// cfManyDays are the "many" child's seven cold days; with cfManyLate each
// takes a late write and so is recovered, owing its build, at the reopen.
var cfManyDays = []string{"2026-07-14", "2026-07-15", "2026-07-16", "2026-07-17", "2026-07-18", "2026-07-19", "2026-07-20"}

const cfManyLate = cfEnvLate + "=14,15,16,17,18,19,20"

// cfSavedSettled counts the shards of names whose saved catalog entry is
// settled.
func cfSavedSettled(t *testing.T, dir string, names []string) int {
	t.Helper()
	n := 0
	for _, name := range names {
		if cfEntrySettled(cfSavedEntry(t, dir, name)) {
			n++
		}
	}
	return n
}

func cfQueued(ts *Store) int {
	n, _ := ts.owedQueued(nil)
	return n
}

// cfSealedWithoutSet builds, in dir, the catalog of a release that wrote no
// former-label set: day 20, with its history logons, cleanly closed and
// sealed, the set's fields removed from the saved file.
func cfSealedWithoutSet(t *testing.T, dir string) []types.NodeID {
	t.Helper()
	ts, _, ids := cfHistoryColdDay(t, dir, &tierClock{})
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfEditSavedCatalog(t, dir, func(e map[string]any) {
		delete(e, "former_labels_known")
		delete(e, "former_labels")
	})
	if e := cfSavedEntry(t, dir, cfDay20Name); !e.CountsSealed || e.FormerLabelsKnown || e.Recover {
		t.Fatalf("setup: day 20's saved entry %+v; want sealed without the set, unmarked", e)
	}
	return ids
}

// cfReopenDay23Cfg reopens dir on day 23 with cfCfg changed by edit.
func cfReopenDay23Cfg(t *testing.T, dir string, edit func(*Config)) (*Store, uint16) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	cfg := cfCfg(dir, clock)
	edit(&cfg)
	ts, tok := cfOpenCfg(t, cfg)
	t.Cleanup(func() { _ = ts.Close() })
	return ts, tok
}

// TestCrashFix_BeforeTheStepRefusedAndControlExact: what the step is for. With
// the settle steps held after a kill and New's recovery, the tight gather is
// refused on every retry (each opens day 20 and starts the build at its own
// charge; a refusal keeps nothing), while the unlimited control answers
// exactly. Once the step has run, the same tight gather opens nothing.
func TestCrashFix_BeforeTheStepRefusedAndControlExact(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "history")
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, tok := cfOpenHeld(t, dir, clock)
	defer func() { _ = ts.Close() }()
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); !e.CountsSealed || e.Recover || e.FormerLabelsKnown {
		t.Fatalf("day 20 after New's recovery: %+v; want sealed, unmarked, the set unknown", *e)
	}
	if n := cfQueued(ts); n != 1 {
		t.Fatalf("shards queued for the sweep after New: %d; want 1 (day 20)", n)
	}
	p := cfNewGatherProbe(t, ts, dir)
	var refused *storecontract.ReadBudgetError
	for try := 1; try <= 2; try++ {
		_, opens, work, err := p.gather(t, tok+7, cfTight)
		t.Logf("held, try %d: err=%v opens=%d work=%+v", try, err, opens, work)
		if !errors.As(err, &refused) || refused.Limit != "history examined" || opens != 1 {
			t.Fatalf("tight gather before the step, try %d: %v, %d opens; want the history-examined refusal after one open", try, err, opens)
		}
	}
	if steps := cfSettleSteps(ts); steps != 0 {
		t.Fatalf("settle steps while held: %d; want 0", steps)
	}
	cfRelease(ts)
	if steps := cfSettleSteps(ts); steps != 1 {
		t.Fatalf("settle steps for one recovered shard: %d; want 1 (the build)", steps)
	}
	p.wantSettled(t, tok, "after the step")
	cfWantSettledEntry(t, ts, dir, "after the step")
}

// TestCrashFix_ReaderHoldingTheShardAtItsTurn: a reader holds day 20 open
// when the sweep reaches it. The step does not wait for the reader and does
// not forget the shard: the reader's check-in close finds the entry still not
// settled and queues it again.
func TestCrashFix_ReaderHoldingTheShardAtItsTurn(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "history")
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, tok := cfOpenHeld(t, dir, clock)
	defer func() { _ = ts.Close() }()
	es := cfDay20(t, ts)
	_, release, err := es.checkoutStoreReadOnly(ts)
	if err != nil {
		t.Fatal(err)
	}
	cfRelease(ts) // day 20's turn: a handle is open
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != 0 || queued != 0 {
		t.Fatalf("with a reader on day 20: %d settle steps, %d queued; want none run and the shard off the queue", steps, queued)
	}
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); cfEntrySettled(*e) {
		t.Fatal("day 20 settled while a reader held it")
	}
	release() // the transient check-in closes day 20 and queues it
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the reader's check-in; want closed")
	}
	ts.SweepHandlesForTest()
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the reader left")
	cfWantSettledEntry(t, ts, dir, "after the reader left")
}

// TestCrashFix_OneSettleStepAPass: seven recovered shards owe their build. A
// pass runs one step and saves the catalog, so after pass k the saved catalog
// holds k settled shards: a kill then loses nothing that was done, and a
// rotation's swap waits for one shard's walk, not seven.
func TestCrashFix_OneSettleStepAPass(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "many", cfManyLate)
	var stepper cfStepper
	ts, _ := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = stepper.fault })
	if got := ts.OpenStats().ColdRecovered; got != len(cfManyDays) {
		t.Fatalf("OpenStats().ColdRecovered = %d; want %d", got, len(cfManyDays))
	}
	if n := cfQueued(ts); n != len(cfManyDays) {
		t.Fatalf("shards queued for the sweep after New: %d; want %d", n, len(cfManyDays))
	}
	if n := cfSavedSettled(t, dir, cfManyDays); n != 0 {
		t.Fatalf("settled shards saved before any step: %d; want 0", n)
	}
	for k := 1; k <= len(cfManyDays); k++ {
		stepper.step(ts)
		if steps, saved := cfSettleSteps(ts), cfSavedSettled(t, dir, cfManyDays); steps != int64(k) || saved != k {
			t.Fatalf("after pass %d: %d settle steps, %d settled shards in the saved catalog; want %d and %d", k, steps, saved, k, k)
		}
		// The newest window first: a cut reads recent days first.
		if newest := cfManyDays[len(cfManyDays)-k]; !cfEntrySettled(cfSavedEntry(t, dir, newest)) {
			t.Fatalf("after pass %d: %s is not settled; want the newest owed window settled first", k, newest)
		}
	}
	if n := cfQueued(ts); n != 0 {
		t.Fatalf("shards still queued after seven passes: %d", n)
	}
	for _, late := range cfLateIDs(t, dir) {
		if got, err := cfMeteredGet(t, ts, late); err != nil || got == nil {
			t.Fatalf("metered read of a late logon = %v, %v; want it", got, err)
		}
	}
}

// TestCrashFix_KillBetweenStepsKeepsTheFirst: the reopen after the first kill
// is killed again as the sweep starts its second step. The first step's shard
// is settled in the saved catalog; the next New recovers nothing, queues the
// six that are left and settles them.
func TestCrashFix_KillBetweenStepsKeepsTheFirst(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "many", cfManyLate)
	cfRunChild(t, dir, "settlekill")
	if n := cfSavedSettled(t, dir, cfManyDays); n != 1 {
		t.Fatalf("settled shards in the saved catalog after the kill in step 2: %d; want 1", n)
	}
	clock := &tierClock{}
	clock.set(julDay(23, 14))
	ts, _ := cfOpenHeld(t, dir, clock)
	defer func() { _ = ts.Close() }()
	if got := ts.OpenStats().ColdRecovered; got != 0 {
		t.Fatalf("OpenStats().ColdRecovered after the second kill = %d; want 0 (the first reopen recovered them)", got)
	}
	if n := cfQueued(ts); n != len(cfManyDays)-1 {
		t.Fatalf("shards queued after the second kill: %d; want %d", n, len(cfManyDays)-1)
	}
	cfRelease(ts)
	if steps, saved := cfSettleSteps(ts), cfSavedSettled(t, dir, cfManyDays); steps != int64(len(cfManyDays)-1) || saved != len(cfManyDays) {
		t.Fatalf("after the sweep: %d settle steps, %d settled shards saved; want %d and %d", steps, saved, len(cfManyDays)-1, len(cfManyDays))
	}
}

// TestCrashFix_KillInsideAStepOwesItAgain: a second kill inside a step. In
// the build (the read-only handle open, the walk about to start) nothing
// durable has changed: the next New finds the entry owed as it was. In the
// repair (the writable open of a shard the catalog did not mark) the mark was
// saved before the open: the next New recovers the shard before its first
// read. Either way the sweep then settles it.
func TestCrashFix_KillInsideAStepOwesItAgain(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	t.Run("build", func(t *testing.T) {
		dir := t.TempDir()
		cfRunChild(t, dir, "history")
		cfRunChild(t, dir, "buildkill")
		if e := cfSavedEntry(t, dir, cfDay20Name); !e.CountsSealed || e.Recover || e.FormerLabelsKnown {
			t.Fatalf("day 20's saved entry after the kill in the build: %+v; want sealed, unmarked, the set unknown", e)
		}
		ts, tok := cfReopenDay23(t, dir)
		if got := ts.OpenStats().ColdRecovered; got != 0 {
			t.Fatalf("OpenStats().ColdRecovered = %d; want 0 (the first reopen recovered day 20)", got)
		}
		ts.SweepHandlesForTest()
		cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the next run's sweep")
		cfWantSettledEntry(t, ts, dir, "after the next run's sweep")
	})
	t.Run("repair", func(t *testing.T) {
		dir := t.TempDir()
		cfRunChild(t, dir, "history")
		cfDropRecoverMarks(t, dir) // as an earlier release left it: no mark names day 20
		cfRunChild(t, dir, "repairkill")
		if e := cfSavedEntry(t, dir, cfDay20Name); !e.Recover || e.CountsSealed {
			t.Fatalf("day 20's saved entry after the kill in the repair: %+v; want marked and not sealed", e)
		}
		clock := &tierClock{}
		clock.set(julDay(23, 14))
		ts, tok := cfOpenHeld(t, dir, clock)
		defer func() { _ = ts.Close() }()
		if got := ts.OpenStats().ColdRecovered; got != 1 {
			t.Fatalf("OpenStats().ColdRecovered = %d; want 1 (the mark the repair saved)", got)
		}
		if got, err := cfMeteredGet(t, ts, cfLateIDs(t, dir)[0]); err != nil || got == nil {
			t.Fatalf("first metered read of the late logon = %v, %v; want it", got, err)
		}
		cfRelease(ts)
		cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the next run's sweep")
		cfWantSettledEntry(t, ts, dir, "after the next run's sweep")
	})
}

// TestCrashFix_NoHistoryAndNoRows: the two empty states. A recovered shard
// with rows and no history row is owed like any other and its set, empty, is
// recorded. A recovered shard with no row at all is sealed no-rows by New's
// repair: that is settled, so it is never queued and never opened again, and
// every fan-out drops it.
func TestCrashFix_NoHistoryAndNoRows(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	for _, tc := range []struct {
		child string
		empty bool
	}{{"cold", false}, {"emptywarm", true}} {
		t.Run(tc.child, func(t *testing.T) {
			dir := t.TempDir()
			cfRunChild(t, dir, tc.child)
			clock := &tierClock{}
			clock.set(julDay(23, 13))
			ts, tok := cfOpenHeld(t, dir, clock)
			defer func() { _ = ts.Close() }()
			if got := ts.OpenStats().ColdRecovered; got != 1 {
				t.Fatalf("OpenStats().ColdRecovered = %d; want 1 (day 20)", got)
			}
			es := cfDay20(t, ts)
			wantQueued := 1
			if tc.empty {
				wantQueued = 0
			}
			if got, queued := es.noRows.Load(), cfQueued(ts); got != tc.empty || queued != wantQueued {
				t.Fatalf("day 20 after New: NoRows %v, %d queued; want %v, %d", got, queued, tc.empty, wantQueued)
			}
			p := cfNewGatherProbe(t, ts, dir)
			cfRelease(ts)
			if tc.empty {
				if opens, steps := p.opens.Load(), cfSettleSteps(ts); opens != 0 || steps != 0 {
					t.Fatalf("day 20 (no rows): %d opens, %d settle steps after New; want none", opens, steps)
				}
			} else if c := es.cachedCounts(); c == nil || c.formerLabels == nil || len(c.formerLabels) != 0 {
				t.Fatalf("day 20 (no history) after the step: counts %+v; want an empty, known former-label set", c)
			}
			cfWantSettledEntry(t, ts, dir, "after the sweep")
			if n, opens, work, err := p.gather(t, tok+7, cfTight); err != nil || n != 0 || opens != 0 || work != (storecontract.ReadWork{}) {
				t.Fatalf("tight gather = %d members, %d opens, %+v, %v; want 0, 0 opens, no work, nil", n, opens, work, err)
			}
			wantNodes := 4 // the "cold" child's three logons and its late one
			if tc.empty {
				wantNodes = 0
			}
			if all, err := ts.AllNodes(QueryOpts{}); err != nil || len(all) != wantNodes {
				t.Fatalf("AllNodes = %d, %v; want %d (the unlimited control)", len(all), err, wantNodes)
			}
		})
	}
}

// cfBlockAt returns a sweep step hook that blocks at step (it reports each
// arrival on entered) until release is closed.
func cfBlockAt(step string) (hook func(string) error, entered chan struct{}, release chan struct{}) {
	entered, release = make(chan struct{}, 64), make(chan struct{})
	return func(s string) error {
		if s == step {
			entered <- struct{}{}
			<-release
		}
		return nil
	}, entered, release
}

func cfWait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("%s: not within 30 s", what)
	}
}

// TestCrashFix_CloseDuringAStep: Close arrives while a build step walks.
// Close waits for that one step, no further step starts, and the catalog it
// saves is what the steps left: one shard settled, six owed, which the next
// run settles.
func TestCrashFix_CloseDuringAStep(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "many", cfManyLate)
	hook, entered, release := cfBlockAt("build")
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	cfg := cfCfg(dir, clock)
	cfg.sweepFault = hook
	ts, _ := cfOpenCfg(t, cfg)
	cfWait(t, entered, "the first build step")
	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("Close returned (%v) while a step was in flight; want it to wait for the step", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Close did not return after the step ended")
	}
	ts.SweepPassForTest() // a pass after Close has begun runs no step, whether or not the goroutine took its last kick
	if steps, saved := cfSettleSteps(ts), cfSavedSettled(t, dir, cfManyDays); steps != 1 || saved != 1 {
		t.Fatalf("after Close during the first step: %d settle steps, %d settled shards saved; want 1 and 1", steps, saved)
	}
	ts, _ = cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = holdSettleSteps })
	if n := cfQueued(ts); n != len(cfManyDays)-1 {
		t.Fatalf("shards the reopen queued: %d; want %d", n, len(cfManyDays)-1)
	}
	cfRelease(ts)
	if saved := cfSavedSettled(t, dir, cfManyDays); saved != len(cfManyDays) {
		t.Fatalf("settled shards saved after the reopen's sweep: %d; want %d", saved, len(cfManyDays))
	}
}

// TestCrashFix_MissingDirectoryLeftAsItIs: a cold shard whose directory is
// gone (a volume not mounted) cannot be settled, and trying must cost it
// nothing: New's repair of a marked shard and the sweep's steps leave its
// sealed counts in the catalog, create no directory, and stop after
// settleGiveUp steps.
func TestCrashFix_MissingDirectoryLeftAsItIs(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfSealedWithoutSet(t, dir)
	cfSetRecoverMark(t, dir, cfDay20Name)
	day20 := filepath.Join(dir, "events", cfDay20Name)
	if err := os.RemoveAll(day20); err != nil {
		t.Fatal(err)
	}
	ts, _ := cfReopenDay23(t, dir)
	if got := ts.OpenStats().ColdRecovered; got != 0 {
		t.Fatalf("OpenStats().ColdRecovered = %d; want 0 (no directory to recover)", got)
	}
	check := func(when string) {
		t.Helper()
		mem, _ := ts.CatalogForTest().GetShard(cfDay20Name)
		for _, e := range []ShardEntry{*mem, cfSavedEntry(t, dir, cfDay20Name)} {
			if !e.CountsSealed || !e.Recover || e.ApproxNodes != cfHistoryRows {
				t.Fatalf("%s: day 20's entry %+v; want its seal (%d nodes) and its mark, untouched", when, e, cfHistoryRows)
			}
		}
		if _, err := os.Stat(day20); !os.IsNotExist(err) {
			t.Fatalf("%s: day 20's directory: %v; want none created", when, err)
		}
	}
	check("after New")
	saves := ts.CatalogForTest().SavesForTest()
	ts.SweepHandlesForTest()
	ts.SweepHandlesForTest()
	check("after the sweep")
	if got := ts.CatalogForTest().SavesForTest() - saves; got != 0 {
		t.Fatalf("catalog saves by steps that changed nothing: %d; want 0", got)
	}
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != settleGiveUp || queued != 0 {
		t.Fatalf("settle steps on a shard without a directory: %d, %d queued; want %d and the shard off the queue", steps, queued, settleGiveUp)
	}
}

// TestCrashFix_StepHandleIsPrivate: the build step's handle is the sweep's
// own. With a cap of two kept cold handles, the step takes no slot, shows in
// no gauge and counts no cold open (no access opened the shard), during the
// step and after it; the reads that follow get the cap's handles.
func TestCrashFix_StepHandleIsPrivate(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	type seen struct {
		open            bool
		capCount        int64
		coldOpens       uint64
		handles         TierHandles
		nativeReadOnly  int64
		nativeWritables int64
	}
	var during seen
	var ts *Store
	var ro, rw atomic.Int64
	day20 := filepath.Join(dir, "events", cfDay20Name)
	countOpens := func(cfg BadgerStoreConfig) {
		if cfg.Dir != day20 {
			return
		}
		if cfg.ReadOnly {
			ro.Add(1)
		} else {
			rw.Add(1)
		}
	}
	ready := make(chan struct{})
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.MaxOpenColdShards = 2
		c.sweepFault = func(step string) error {
			switch step {
			case "settle":
				<-ready // ts is assigned, its open hook installed
			case "build":
				s, _ := cfDay20(t, ts).StoreStateForTest()
				st := ts.ShardAccessStats()
				during = seen{s != nil, ts.OpenColdShardsForTest(), st.ColdOpens, st.ColdHandles, ro.Load(), rw.Load()}
			}
			return nil
		}
	})
	ts.SetShardOpenHookForTest(countOpens)
	close(ready)
	ts.SweepHandlesForTest()
	ts.SetShardOpenHookForTest(nil)
	if steps := cfSettleSteps(ts); steps != 1 {
		t.Fatalf("settle steps: %d; want 1", steps)
	}
	if during != (seen{nativeReadOnly: 1}) {
		t.Fatalf("during the build step: %+v; want one native read-only open and nothing else: no installed handle, no cap slot, no cold open, no gauge", during)
	}
	if st := ts.ShardAccessStats(); st.ColdOpens != 0 || st.ColdHandles != (TierHandles{}) || ts.OpenColdShardsForTest() != 0 {
		t.Fatalf("after the build step: ColdOpens %d, handles %+v, cap count %d; want 0, none, 0", st.ColdOpens, st.ColdHandles, ts.OpenColdShardsForTest())
	}
	cfWantSettledEntry(t, ts, dir, "after the step")
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("metered read = %v, %v", got, err)
	}
	if st := ts.ShardAccessStats(); st.ColdOpens != 1 || st.ColdHandles.ReadOnly != 1 || ts.OpenColdShardsForTest() != 1 {
		t.Fatalf("after one read under the cap: ColdOpens %d, handles %+v, cap count %d; want the read's kept handle (1, one read-only, 1)", st.ColdOpens, st.ColdHandles, ts.OpenColdShardsForTest())
	}
	// The kept handle's idle close leaves the shard settled: nothing queued.
	cfIdleClose(ts)
	if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil || cfQueued(ts) != 0 {
		t.Fatalf("after the idle close of the kept handle: open %v, %d queued; want closed, none", s != nil, cfQueued(ts))
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the idle close")
}

// TestCrashFix_FailingStepGivesUp: a step that cannot settle its shard (here
// the walk fails) is tried settleGiveUp times and then left: closes of the
// shard do not queue it again, so reads of a shard with an unreadable history
// row do not each start a walk. A write, after which the shard may differ,
// lets the sweep try again.
func TestCrashFix_FailingStepGivesUp(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	var fail atomic.Bool
	fail.Store(true)
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.sweepFault = func(step string) error {
			if step == "build" && fail.Load() {
				return errors.New("history walk failed (the test)")
			}
			return nil
		}
	})
	ts.SweepHandlesForTest()
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != settleGiveUp || queued != 0 {
		t.Fatalf("with a failing walk: %d settle steps, %d queued; want %d and the shard off the queue", steps, queued, settleGiveUp)
	}
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); !e.CountsSealed || e.FormerLabelsKnown || e.Recover {
		t.Fatalf("day 20 after the failed steps: %+v; want its counts sealed, the set unknown, no mark", *e)
	}
	for i := 0; i < 3; i++ { // each read's check-in closes day 20, still not settled
		if got, err := cfMeteredGet(t, ts, ids[i]); err != nil || got == nil {
			t.Fatalf("metered read %d = %v, %v", i, got, err)
		}
	}
	ts.SweepHandlesForTest()
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != settleGiveUp || queued != 0 {
		t.Fatalf("after three reads of the shard: %d settle steps, %d queued; want still %d and none", steps, queued, settleGiveUp)
	}
	fail.Store(false)
	v1 := types.NewNode(ids[cfFormerRows], tok, nil)
	v1.SetVersion(1)
	if err := ts.PutNodeVersion(v1.ID(), 1, v1); err != nil {
		t.Fatalf("by-ID write into day 20: %v", err)
	}
	cfAfterIdleTimeout(ts)
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after a write and a working step")
	cfWantSettledEntry(t, ts, dir, "after a write and a working step")
}

// TestCrashFix_RefusalRevivesAGivenUpShard: a shard the sweep gave up on is
// taken up again when a metered read's open of it is refused for a repair:
// the refusal is new knowledge (here an index definition the reference shard
// gained), and a reader is waiting.
func TestCrashFix_RefusalRevivesAGivenUpShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	var fail atomic.Bool
	fail.Store(true)
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.sweepFault = func(step string) error {
			if step == "build" && fail.Load() {
				return errors.New("history walk failed (the test)")
			}
			return nil
		}
	})
	ts.SweepHandlesForTest()
	if steps := cfSettleSteps(ts); steps != settleGiveUp {
		t.Fatalf("with a failing walk: %d settle steps; want %d", steps, settleGiveUp)
	}
	fail.Store(false)
	if err := ts.refShard.CreateCompositePropertyIndex(tok, []string{"user", "host"}); err != nil {
		t.Fatal(err)
	}
	var refused *storecontract.ReadBudgetError
	if got, err := cfMeteredGet(t, ts, ids[0]); !errors.As(err, &refused) || refused.Limit != "unsupported" {
		t.Fatalf("metered read with a definition to sync = %v, %v; want the unsupported refusal", got, err)
	}
	ts.SweepHandlesForTest()
	if steps := cfSettleSteps(ts); steps != settleGiveUp+2 {
		t.Fatalf("settle steps after the refusal: %d; want %d (the repair, then the build)", steps, settleGiveUp+2)
	}
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("metered read after the sweep = %v, %v; want the logon", got, err)
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the refusal and the sweep")
	cfWantSettledEntry(t, ts, dir, "after the refusal and the sweep")
}

// TestCrashFix_ReadAndWriteDuringABuild: while a build step walks day 20's
// history behind the shard's gate, what only looks at the shard under its
// lock does not wait (the registry setters a graph runs right after New, the
// listing, the idle close); a read and a write of day 20 wait for the step
// and then answer. The write unseals what the step sealed, its close queues
// the shard again and the next step settles it.
func TestCrashFix_ReadAndWriteDuringABuild(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	hook, entered, release := cfBlockAt("build")
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = hook })
	cfWait(t, entered, "the build step")

	peeked := make(chan struct{})
	go func() {
		defer close(peeked)
		ts.SetNameRegistries(registrypkg.NewLabelRegistry(), registrypkg.NewRelTypeRegistry())
		if _, err := ts.ListShards(); err != nil {
			t.Errorf("ListShards: %v", err)
		}
		ts.CloseIdleShardsForTest()
		_ = ts.ShardAccessStats()
	}()
	cfWait(t, peeked, "the registry setters, the listing and the idle close during a build step")

	type result struct {
		n   *types.Node
		err error
	}
	read, wrote := make(chan result, 1), make(chan error, 1)
	go func() {
		n, err := cfMeteredGet(t, ts, ids[0])
		read <- result{n, err}
	}()
	go func() {
		v1 := types.NewNode(ids[cfFormerRows], tok, nil)
		v1.SetVersion(1)
		wrote <- ts.PutNodeVersion(v1.ID(), 1, v1)
	}()
	select {
	case r := <-read:
		t.Fatalf("the read of day 20 returned (%v, %v) during the build step; want it to wait for the gate", r.n, r.err)
	case err := <-wrote:
		t.Fatalf("the write into day 20 returned (%v) during the build step; want it to wait for the gate", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case r := <-read:
		if r.err != nil || r.n == nil || r.n.ID() != ids[0] {
			t.Fatalf("the read after the step = %v, %v; want the logon", r.n, r.err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the read did not return after the step")
	}
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("the write after the step: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the write did not return after the step")
	}
	cfAfterIdleTimeout(ts)
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the write's close and the next step")
	cfWantSettledEntry(t, ts, dir, "after the write's close and the next step")
	if steps := cfSettleSteps(ts); steps != 2 {
		t.Fatalf("settle steps: %d; want 2 (the build, and the build after the write)", steps)
	}
	if n, err := ts.NodeCountByLabel(tok); err != nil || n != cfHistoryRows {
		t.Fatalf("NodeCountByLabel(Logon) from the seal = %d, %v; want %d", n, err, cfHistoryRows)
	}
}

// TestCrashFix_IdleCloseOfLateWriteLeavesItSettled: the neighbour of the
// check-in close. A late logon leaves day 20's writable handle open until the
// idle close, which runs off every caller's path and holds the handle anyway:
// it builds the set as it seals, so the shard is settled when it closes and
// the sweep has nothing to do. (A close on a caller's path leaves the walk to
// the sweep: TestCrashFix_TransientWriteCloseSettles.)
func TestCrashFix_IdleCloseOfLateWriteLeavesItSettled(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ts, tok, _ := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	ts.HoldSettleStepsForTest()
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil {
		t.Fatalf("late PutNode: %v", err)
	}
	es := cfDay20(t, ts)
	if s, ro := es.StoreStateForTest(); s == nil || ro {
		t.Fatalf("day 20 after the late write: open %v, read-only %v; want a writable handle", s != nil, ro)
	}
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); e.CountsSealed || !e.Recover {
		t.Fatalf("day 20's entry while open writable: %+v; want unsealed and marked", *e)
	}
	cfIdleClose(ts)
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the idle close; want closed")
	}
	if e, _ := ts.CatalogForTest().GetShard(cfDay20Name); !cfEntrySettled(*e) || e.ApproxNodes != cfHistoryRows+1 {
		t.Fatalf("day 20's entry after the idle close: %+v; want settled with %d nodes", *e, cfHistoryRows+1)
	}
	if queued, steps := cfQueued(ts), cfSettleSteps(ts); queued != 0 || steps != 0 {
		t.Fatalf("after the idle close: %d queued, %d settle steps; want none (the close settled the shard)", queued, steps)
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the idle close")
	cfWantSettledEntry(t, ts, dir, "after the idle close")
}

// TestCrashFix_KillInsideClearLeavesNoStaleSeal: Clear is killed after it
// cleared the reference shard, which removes its pending marker, and before
// its last catalog save. The next open does not finish the Clear (no marker),
// so the saved catalog must already hold the cleared cold shard unsealed: a
// seal left there would be adopted and count the removed rows.
func TestCrashFix_KillInsideClearLeavesNoStaleSeal(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "clearkill")
	ts, tok := cfReopenDay23(t, dir)
	n, err := ts.NodeCount()
	l, lerr := ts.NodeCountByLabel(tok)
	all, aerr := ts.AllNodes(QueryOpts{})
	if err != nil || lerr != nil || aerr != nil || n != 0 || l != 0 || len(all) != 0 {
		t.Fatalf("after a kill inside Clear: NodeCount %d, NodeCountByLabel %d, AllNodes %d (%v, %v, %v); want 0, 0, 0", n, l, len(all), err, lerr, aerr)
	}
}

// TestCrashFix_FailedDropReopensMarked: a retention drop
// closes its cold shard, which clears the shard's recovery mark, and reopens
// it writable when its catalog save fails. The reopened handle carries the
// mark again, so a kill with it open is recovered by the next New.
func TestCrashFix_FailedDropReopensMarked(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	ts, tok, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // a late write: day 20 open writable
		t.Fatalf("late PutNode: %v", err)
	}
	good := ts.catalog.path
	ts.catalog.path = blockedMetadataPath(t)
	_, err := ts.PurgeNodesByLabelBefore(tok, ms(julDay(21, 0)), 8)
	ts.catalog.path = good
	if err == nil {
		t.Fatal("PurgeNodesByLabelBefore with a blocked catalog save returned nil")
	}
	s, ro := es.StoreStateForTest()
	e, ok := ts.CatalogForTest().GetShard(es.Name())
	if !ok || s == nil || ro || !e.Recover || e.CountsSealed {
		t.Fatalf("after the failed drop: cataloged %v, open %v, read-only %v, entry %+v; want day 20 back, open writable, marked and unsealed", ok, s != nil, ro, e)
	}
	if got, err := ts.GetNode(ids[0]); err != nil || got == nil {
		t.Fatalf("GetNode after the failed drop = %v, %v; want the logon", got, err)
	}
}
