package tiered

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The handle sweep's queue of owed cold shards, and what its goroutine does
// with it unaided: the tests of crashfix_settle_test.go drive the passes
// themselves (SweepHandlesForTest), so these wait for the goroutine instead,
// and look at the order the queue is worked in.

// cfEventually waits, up to a bound no load reaches, for ok. The test runs no
// pass of the sweep meanwhile: what happens is the sweep goroutine's doing.
func cfEventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); !ok(); {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within 30 s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// cfNoIdleTick fails the test unless the store's idle tick (IdleTimeout/2) is
// further away than cfEventually waits: what the sweep does then, it does
// without one.
func cfNoIdleTick(t *testing.T, ts *Store) {
	t.Helper()
	if every := ts.idleTimeoutValue() / 2; every < time.Minute {
		t.Fatalf("idle tick every %v; the test needs it out of reach", every)
	}
}

// cfSevenColdDays builds, in dir, days 14 to 22 with one logon each, closed
// cleanly on day 23 (days 14 to 20 cold and sealed), and removes the
// former-label sets from the saved catalog: the catalog of a release that
// recorded none, so the first New on this one owes every cold day its set.
// Returns each cold day's logon by shard name.
func cfSevenColdDays(t *testing.T, dir string) map[string]types.NodeID {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(14, 12))
	cfg := cfCfg(dir, clock)
	cfg.SyncWrites = false
	ts, tok := cfOpenCfg(t, cfg)
	ids := map[string]types.NodeID{}
	for d := 14; d <= 22; d++ {
		clock.set(julDay(d, 12))
		if err := ts.CheckRotationForTest(); err != nil {
			t.Fatalf("rotation to day %d: %v", d, err)
		}
		id, err := cfPut(t, ts, tok, julDay(d, 1))
		if err != nil {
			t.Fatalf("PutNode day %d: %v", d, err)
		}
		ids[fmt.Sprintf("2026-07-%02d", d)] = id
	}
	clock.set(julDay(23, 12))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	ts.SweepHandlesForTest()
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfEditSavedCatalog(t, dir, func(e map[string]any) {
		delete(e, "former_labels_known")
		delete(e, "former_labels")
	})
	for _, name := range cfManyDays {
		if e := cfSavedEntry(t, dir, name); !e.CountsSealed || e.FormerLabelsKnown || e.Recover {
			t.Fatalf("setup: %s's saved entry %+v; want sealed without the set, unmarked", name, e)
		}
	}
	return ids
}

func cfEntry(ts *Store, name string) ShardEntry {
	e, ok := ts.CatalogForTest().GetShard(name)
	if !ok {
		return ShardEntry{}
	}
	return *e
}

// cfAddIndexDefinition gives the reference shard an index definition no
// closed cold shard has: a metered open of each is refused from now on (only a
// writable open syncs it), and a build step's open finds the repair owed.
func cfAddIndexDefinition(t *testing.T, ts *Store, tok uint16) {
	t.Helper()
	if err := ts.refShard.CreateCompositePropertyIndex(tok, []string{"user", "host"}); err != nil {
		t.Fatal(err)
	}
}

func cfWantRefused(t *testing.T, ts *Store, id types.NodeID) {
	t.Helper()
	var refused *storecontract.ReadBudgetError
	if got, err := cfMeteredGet(t, ts, id); !errors.As(err, &refused) || refused.Limit != "unsupported" {
		t.Fatalf("metered read with a definition to sync = %v, %v; want the unsupported refusal", got, err)
	}
}

// TestCrashFix_GoroutineSettlesAFirstRunQueueUnaided: the first New on this
// release finds seven cold days sealed without their sets. The test runs no
// pass and no idle tick is in reach: New's one kick and the goroutine's kick
// to itself after each step settle all seven, one step and one save each.
// Without that kick the queue would move one shard an idle tick.
func TestCrashFix_GoroutineSettlesAFirstRunQueueUnaided(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfSevenColdDays(t, dir)
	ts, _ := cfReopenDay23(t, dir)
	cfNoIdleTick(t, ts)
	cfEventually(t, "seven owed shards settled by the sweep goroutine", func() bool {
		return cfSavedSettled(t, dir, cfManyDays) == len(cfManyDays)
	})
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != int64(len(cfManyDays)) || queued != 0 {
		t.Fatalf("after the goroutine's work: %d settle steps, %d queued; want %d and none", steps, queued, len(cfManyDays))
	}
	cfEventually(t, "one pass a step at least", func() bool { return ts.sweep.passes.Load() >= int64(len(cfManyDays)) })
}

// TestCrashFix_RefusalIsRepairedUnaided: a metered read's refusal wakes the
// sweep at once: the read that comes back finds the shard repaired, with no
// pass run by the test and no idle tick. The repair does not wait for the
// shard's writes to stop, as its build does: a refused reader's shard that is
// written all the while would otherwise stay refused.
func TestCrashFix_RefusalIsRepairedUnaided(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	for _, written := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "written just before"}[written], func(t *testing.T) {
			dir := t.TempDir()
			ts, tok, ids := cfHistoryColdDay(t, dir, &tierClock{})
			defer func() { _ = ts.Close() }()
			cfNoIdleTick(t, ts)
			steps := cfSettleSteps(ts)
			if written {
				v1 := types.NewNode(ids[cfFormerRows], tok, nil)
				v1.SetVersion(1)
				if err := ts.PutNodeVersion(v1.ID(), 1, v1); err != nil {
					t.Fatalf("by-ID write into day 20: %v", err)
				}
			}
			cfAddIndexDefinition(t, ts, tok)
			cfWantRefused(t, ts, ids[0])
			cfEventually(t, "the refused shard repaired by the sweep goroutine", func() bool {
				return !cfEntry(ts, cfDay20Name).Recover && cfSettleSteps(ts) > steps
			})
			if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
				t.Fatalf("metered read after the repair = %v, %v; want the logon", got, err)
			}
			if !written {
				cfEventually(t, "the repaired shard's set built", func() bool { return cfEntrySettled(cfEntry(ts, cfDay20Name)) })
				return
			}
			// The build still waits for the writes to stop.
			if e := cfEntry(ts, cfDay20Name); !e.CountsSealed || e.FormerLabelsKnown || cfQueued(ts) != 1 || cfSettleSteps(ts)-steps != 1 {
				t.Fatalf("after the repair of a shard written just before: entry %+v, %d queued, %d settle steps; want sealed without the set, queued, one step (the repair)",
					e, cfQueued(ts), cfSettleSteps(ts)-steps)
			}
			cfAfterIdleTimeout(ts)
			cfWantSettledEntry(t, ts, dir, "after the writes stopped")
		})
	}
}

// TestCrashFix_IdleTimeoutZeroBuildsAtOnce: with IdleTimeout 0 nothing closes
// an idle handle and no idle tick runs, so "after IdleTimeout without a write"
// is "at once": a by-ID write's check-in close queues the shard and wakes the
// sweep, which builds the set with no pass run by the test (one walk a write;
// a burst is not gathered into one).
func TestCrashFix_IdleTimeoutZeroBuildsAtOnce(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ts, tok, ids := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	ts.SetIdleTimeoutForTest(0)
	steps := cfSettleSteps(ts)
	for i := 1; i <= 2; i++ {
		v1 := types.NewNode(ids[cfFormerRows+i], tok, nil)
		v1.SetVersion(1)
		if err := ts.PutNodeVersion(v1.ID(), 1, v1); err != nil {
			t.Fatalf("by-ID write %d into day 20: %v", i, err)
		}
		cfEventually(t, "the written shard settled by the sweep goroutine", func() bool {
			return cfEntrySettled(cfEntry(ts, cfDay20Name)) && cfSettleSteps(ts)-steps == int64(i)
		})
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after two writes at IdleTimeout 0")
}

// cfGate is a sweep step hook that lets the settle steps through on the
// test's word (as cfStepper) and can stop one inside its "settle" call, the
// step's shard locked, until the test lets go.
type cfGate struct {
	allow   atomic.Int64
	seen    atomic.Int64 // steps let through
	blockAt atomic.Int64 // the step (1-based) that waits in "settle"; 0: none
	entered chan struct{}
	release chan struct{}
}

func cfNewGate() *cfGate {
	return &cfGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
}

func (g *cfGate) fault(step string) error {
	if step != "settle" {
		return nil
	}
	for {
		n := g.allow.Load()
		if n <= 0 {
			return errSettleStepsHeld
		}
		if g.allow.CompareAndSwap(n, n-1) {
			break
		}
	}
	if g.seen.Add(1) == g.blockAt.Load() {
		g.entered <- struct{}{}
		<-g.release
	}
	return nil
}

func (g *cfGate) step(ts *Store) {
	g.allow.Add(1)
	ts.SweepPassForTest()
}

// TestCrashFix_RefusedShardIsFirstAndChainsKeepTheirTurn: seven days queued,
// newest first, and an index definition none of them has, so each needs the
// whole chain (a build whose open finds the repair owed, the repair, the
// build). Reads of the two last days in the queue are refused: their repairs
// are the next two steps, in the order of the refusals, before either's
// build. A shard owed again keeps its turn ahead of the shards that have not
// started, so each chain ends before the next begins; and a refusal that
// arrives in the middle of another shard's chain goes ahead of that chain.
func TestCrashFix_RefusedShardIsFirstAndChainsKeepTheirTurn(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSevenColdDays(t, dir)
	gate := cfNewGate()
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = gate.fault })
	if n := cfQueued(ts); n != len(cfManyDays) {
		t.Fatalf("queued after New: %d; want %d", n, len(cfManyDays))
	}
	cfAddIndexDefinition(t, ts, tok)
	const first, second, third, newest, next = "2026-07-14", "2026-07-16", "2026-07-15", "2026-07-20", "2026-07-19"
	cfWantRefused(t, ts, ids[first]) // the oldest day: last in the queue until now
	cfWantRefused(t, ts, ids[second])

	gate.step(ts) // 1: the first refused day's repair
	if a, b := cfEntry(ts, first), cfEntry(ts, second); a.Recover || !a.CountsSealed || !b.Recover {
		t.Fatalf("after step 1: %s %+v, %s recover %v; want the first refused read's shard repaired first, ahead of the queue", first, a, second, b.Recover)
	}
	if got, err := cfMeteredGet(t, ts, ids[first]); err != nil || got == nil {
		t.Fatalf("metered read of %s after its repair = %v, %v; want the logon", first, got, err)
	}
	gate.step(ts) // 2: the second refused day's repair, before the first one's build
	if a, b := cfEntry(ts, first), cfEntry(ts, second); b.Recover || cfEntrySettled(a) {
		t.Fatalf("after step 2: %s recover %v, %s settled %v; want the second refused read's shard repaired before the first one's build", second, b.Recover, first, cfEntrySettled(a))
	}
	gate.step(ts) // 3, 4: their builds, ahead of the five that have not started
	gate.step(ts)
	if a, b, c := cfEntry(ts, first), cfEntry(ts, second), cfEntry(ts, newest); !cfEntrySettled(a) || !cfEntrySettled(b) || c.Recover || c.FormerLabelsKnown {
		t.Fatalf("after step 4: %s settled %v, %s settled %v, %s %+v; want both settled before the queue's next shard starts (a shard owed again keeps its turn)",
			first, cfEntrySettled(a), second, cfEntrySettled(b), newest, c)
	}

	// 5: the newest day's build finds the repair owed. While that step runs,
	// a read of another day is refused.
	gate.blockAt.Store(5)
	gate.allow.Add(1)
	passed := make(chan struct{})
	go func() { defer close(passed); ts.SweepPassForTest() }()
	cfWait(t, gate.entered, "step 5")
	cfWantRefused(t, ts, ids[third])
	close(gate.release)
	cfWait(t, passed, "the pass of step 5")
	if e := cfEntry(ts, newest); !e.Recover {
		t.Fatalf("after step 5: %s %+v; want its build's open to have found the repair owed", newest, e)
	}
	gate.step(ts) // 6: the refused day's repair, ahead of the newest day's chain
	if a, b := cfEntry(ts, third), cfEntry(ts, newest); a.Recover || !b.Recover {
		t.Fatalf("after step 6: %s recover %v, %s recover %v; want the refused read's shard repaired before the chain in progress goes on", third, a.Recover, newest, b.Recover)
	}
	gate.step(ts) // 7: the refused day's build (its chain goes on first)
	gate.step(ts) // 8: the newest day's repair
	gate.step(ts) // 9: the newest day's build
	if a, b, c := cfEntry(ts, third), cfEntry(ts, newest), cfEntry(ts, next); !cfEntrySettled(a) || !cfEntrySettled(b) || c.Recover || c.FormerLabelsKnown {
		t.Fatalf("after step 9: %s settled %v, %s settled %v, %s %+v; want both chains finished before the next day starts",
			third, cfEntrySettled(a), newest, cfEntrySettled(b), next, c)
	}
	gate.allow.Store(1 << 20)
	ts.SweepHandlesForTest()
	if saved := cfSavedSettled(t, dir, cfManyDays); saved != len(cfManyDays) {
		t.Fatalf("settled shards saved at the end: %d; want %d", saved, len(cfManyDays))
	}
	if steps := cfSettleSteps(ts); steps != int64(3*len(cfManyDays))-3 {
		t.Fatalf("settle steps: %d; want %d (three a day; the three refused days need no build to find their repair owed)", steps, 3*len(cfManyDays)-3)
	}
}

// cfHoldMove starts a move of the cold shard name to the cold volume and
// holds it behind the shard's gate (the move's "gated" step) until the
// returned release; done receives the move's result.
func cfHoldMove(t *testing.T, ts *Store, name string) (release func(), done <-chan error) {
	t.Helper()
	entered, hold := make(chan struct{}, 1), make(chan struct{})
	ts.SetMoveFaultForTest(func(step string) error {
		if step == "gated" {
			entered <- struct{}{}
			<-hold
		}
		return nil
	})
	result := make(chan error, 1)
	go func() { result <- ts.MoveShard(context.Background(), name, "cold") }()
	cfWait(t, entered, "the move's gate")
	var once atomic.Bool
	release = func() {
		if once.CompareAndSwap(false, true) {
			close(hold)
		}
	}
	t.Cleanup(release)
	return release, result
}

// TestCrashFix_MoveGateDelaysOnlyItsShard: a queued shard behind a move's
// gate is not waited for (the pass would hold up every other shard, and a
// rotation's swap, for the copy): the pass returns, the other six settle, and
// the gate's end wakes the sweep for the seventh, with no pass run by the
// test.
func TestCrashFix_MoveGateDelaysOnlyItsShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfSevenColdDays(t, dir)
	coldDir := t.TempDir()
	ts, _ := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.ColdDir = coldDir
		c.sweepFault = holdSettleSteps
	})
	cfNoIdleTick(t, ts)
	const moved = "2026-07-20" // the first in the queue
	release, done := cfHoldMove(t, ts, moved)
	ts.SetSweepFaultForTest(nil)
	swept := make(chan struct{})
	go func() { defer close(swept); ts.SweepHandlesForTest() }()
	cfWait(t, swept, "the sweep with one queued shard behind a move's gate")
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != int64(len(cfManyDays)-1) || queued != 1 || cfEntrySettled(cfEntry(ts, moved)) {
		t.Fatalf("with %s behind its move's gate: %d settle steps, %d queued, its entry %+v; want the six others settled and it queued",
			moved, steps, queued, cfEntry(ts, moved))
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("MoveShard: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the move did not end")
	}
	cfEventually(t, "the moved shard settled after its gate dropped", func() bool {
		return cfSavedSettled(t, dir, cfManyDays) == len(cfManyDays)
	})
	if e := cfEntry(ts, moved); e.Volume != VolumeCold {
		t.Fatalf("%s after the move: volume %q; want cold", moved, e.Volume)
	}
}

// TestCrashFix_DefinitionAddedWhileClosedIsRepairedBeforeAnyRead: the build
// step's open is the open a metered read makes, index definitions included: a
// shard whose definitions are not the anchor's is marked and repaired by the
// sweep, so the first metered read is not refused. A build that sealed such a
// shard would call it settled while every metered read of it was refused.
func TestCrashFix_DefinitionAddedWhileClosedIsRepairedBeforeAnyRead(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = holdSettleSteps })
	cfAddIndexDefinition(t, ts, tok)
	cfRelease(ts)
	if steps := cfSettleSteps(ts); steps != 3 {
		t.Fatalf("settle steps: %d; want 3 (the build that finds the repair owed, the repair, the build)", steps)
	}
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("first metered read = %v, %v; want the logon, never refused", got, err)
	}
	cfWantSettledEntry(t, ts, dir, "after the sweep")
}

// TestCrashFix_DropLeavesNoStepAnEntry: a retention drop's close leaves its
// shard closed, cold and unsealed, and queues it as every close does. The
// shard's catalog entry leaves under the same lock, so a pass that comes the
// moment that lock is released finds nothing owed and opens nothing on a
// directory about to be removed.
func TestCrashFix_DropLeavesNoStepAnEntry(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	ts, tok, _ := cfColdDay20(t)
	es := cfDay20(t, ts)
	day20 := ts.shardDir(es.path)
	steps := cfSettleSteps(ts)
	var opens atomic.Int64
	ran := false
	ts.dropHook = func(step string) {
		if step != "unlocked" {
			return
		}
		ran = true
		ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
			if cfg.Dir == day20 {
				opens.Add(1)
			}
		})
		ts.SweepHandlesForTest()
	}
	res, err := ts.PurgeNodesByLabelBefore(tok, ms(julDay(21, 0)), 8)
	ts.dropHook = nil
	ts.SweepHandlesForTest()
	ts.SetShardOpenHookForTest(nil)
	if err != nil || res.NodesPurged != 3 || !ran {
		t.Fatalf("purge of day 20: %+v, %v, hook ran %v; want its 3 logons dropped", res, err, ran)
	}
	if got, n := cfSettleSteps(ts)-steps, opens.Load(); got != 0 || n != 0 {
		t.Fatalf("after the drop's close: %d settle steps, %d opens of the dropped directory; want none", got, n)
	}
}

// TestCrashFix_FailedDropWaitsForAStepOnItsShard: a drop whose catalog save
// fails puts its entry back and reopens the shard writable. A build step that
// began on the restored entry meanwhile holds a read-only handle on the
// directory: the reopen waits for it behind the shard's gate (the writable
// open would fail on the directory's lock), then unseals what the step sealed.
func TestCrashFix_FailedDropWaitsForAStepOnItsShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	ts, tok, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // a late write: day 20 open writable
		t.Fatalf("late PutNode: %v", err)
	}
	hook, entered, release := cfBlockAt("build")
	var released atomic.Bool
	letGo := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	t.Cleanup(letGo) // before the store's Close, which waits for the step
	ts.SetSweepFaultForTest(hook)
	good := ts.catalog.path
	ts.catalog.path = blockedMetadataPath(t)
	restored := make(chan struct{})
	ts.dropHook = func(step string) {
		if step != "restored" {
			return
		}
		ts.sweepOwe(es) // the pass its close woke reaches the shard now
		go ts.SweepPassForTest()
		cfWait(t, entered, "a build step on the restored entry")
		close(restored)
	}
	purged := make(chan error, 1)
	go func() {
		_, err := ts.PurgeNodesByLabelBefore(tok, ms(julDay(21, 0)), 8)
		purged <- err
	}()
	cfWait(t, restored, "the failed save's rollback")
	// The rollback goes on from here. While the step holds the shard it
	// must touch nothing: its first act after the wait is to mark the entry.
	var err error
	for until := time.Now().Add(100 * time.Millisecond); time.Now().Before(until); time.Sleep(time.Millisecond) {
		select {
		case err = <-purged:
			t.Fatalf("the failed drop returned (%v) while a build step held its shard; want it to wait for the step", err)
		default:
		}
		if cfEntry(ts, es.Name()).Recover {
			t.Fatal("the failed drop went on to reopen its shard while a build step held it; want it to wait for the step")
		}
	}
	letGo()
	select {
	case err = <-purged:
	case <-time.After(30 * time.Second):
		t.Fatal("the failed drop did not return after the step")
	}
	ts.dropHook = nil
	ts.SweepPassForTest() // joins the hook's pass: its save reads the catalog's path
	ts.catalog.path = good
	if err == nil || strings.Contains(err.Error(), "reopen shard") {
		t.Fatalf("the failed drop = %v; want the catalog save's error, the shard reopened", err)
	}
	ts.SetSweepFaultForTest(nil)
	ts.SweepHandlesForTest()
	s, ro := es.StoreStateForTest()
	if e := cfEntry(ts, es.Name()); s == nil || ro || !e.Recover || e.CountsSealed {
		t.Fatalf("after the failed drop: open %v, read-only %v, entry %+v; want day 20 open writable, marked, and not sealed (the step's seal dropped)", s != nil, ro, e)
	}
	if got, err := ts.GetNode(ids[0]); err != nil || got == nil {
		t.Fatalf("GetNode after the failed drop = %v, %v; want the logon", got, err)
	}
}

// TestCrashFix_CloseWhileThePassLooksIsNotLost: a reader holds day 20 when
// the pass looks at it, and its check-in close comes just as the pass lets go
// of the shard's lock. The pass decides the shard's place in the queue under
// that lock, so the close finds the shard off the queue and queues it. Decided
// after the unlock, the close found it still queued, added nothing, and the
// pass then took it off: owed by nobody for the run.
func TestCrashFix_CloseWhileThePassLooksIsNotLost(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfRunChild(t, dir, "history")
	var held, armed atomic.Bool
	held.Store(true)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.sweepFault = func(step string) error {
			if step == "settle" && held.Load() {
				return errSettleStepsHeld
			}
			if step == "looked" && armed.CompareAndSwap(true, false) {
				entered <- struct{}{}
				<-release
			}
			return nil
		}
	})
	es := cfDay20(t, ts)
	_, letGo, err := es.checkoutStoreReadOnly(ts)
	if err != nil {
		t.Fatal(err)
	}
	armed.Store(true)
	held.Store(false)
	swept := make(chan struct{})
	go func() { defer close(swept); ts.SweepHandlesForTest() }()
	cfWait(t, entered, "the pass's look at day 20") // it saw a handle open and released the lock
	letGo()                                         // the reader's check-in close
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the reader's check-in; want closed")
	}
	close(release)
	cfWait(t, swept, "the sweep")
	ts.SweepHandlesForTest()
	if steps := cfSettleSteps(ts); steps != 1 {
		t.Fatalf("settle steps after the reader left: %d; want 1 (its close queued the shard)", steps)
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the reader left")
	cfWantSettledEntry(t, ts, dir, "after the reader left")
}

// TestCrashFix_MeteredOpenOfACleanMarkedShardClearsTheMark: a closed cold
// shard that carries the recovery mark while its directory is clean (a
// writable open that failed after its mark was saved, a refusal that was
// another worker's) is no close's to hand over and is never refused. A metered
// read's open of it succeeds, which is the proof the mark doubts: the mark
// goes, and that read's check-in close hands the shard to the sweep for its
// seal. Before, it stayed marked and unsealed until the next New, whose
// foreground repair it then cost.
func TestCrashFix_MeteredOpenOfACleanMarkedShardClearsTheMark(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ts, tok, ids := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	es := cfDay20(t, ts)
	ts.HoldSettleStepsForTest()
	// As a writable open that failed after its unseal and mark were saved.
	es.LockShardMuForTest()
	es.dropCachedCounts(ts)
	ts.CatalogForTest().SetShardRecover(es.Name(), true)
	es.UnlockShardMuForTest()
	if n := cfQueued(ts); n != 0 {
		t.Fatalf("queued before the read: %d; want 0", n)
	}
	var writable atomic.Int64
	day20 := ts.shardDir(es.path)
	ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
		if cfg.Dir == day20 && !cfg.ReadOnly {
			writable.Add(1)
		}
	})
	steps := cfSettleSteps(ts)
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("metered read of the marked, clean shard = %v, %v; want the logon", got, err)
	}
	if e := cfEntry(ts, es.Name()); e.Recover || !e.CountsSealed || cfQueued(ts) != 1 {
		t.Fatalf("after the metered read: entry %+v, %d queued; want the mark cleared, the counts sealed by the check-in, the shard queued", e, cfQueued(ts))
	}
	cfRelease(ts)
	ts.SetShardOpenHookForTest(nil)
	if got, rw := cfSettleSteps(ts)-steps, writable.Load(); got != 1 || rw != 0 {
		t.Fatalf("after the sweep: %d settle steps, %d writable opens of day 20; want one build and no writable open", got, rw)
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
	cfWantSettledEntry(t, ts, dir, "after the sweep")
}

// TestCrashFix_ShardDemotedDuringItsSwapSettles: with WarmMemTableSize set the
// sweep swaps a warm shard's hot-size handle (close, reopen at the warm size).
// A rotation that demotes the shard between the two leaves the reopened handle
// uninstalled: its close is a clean close of a cold shard's writable handle, so
// it clears the shard's mark and hands the shard to the sweep like every other
// close. Before, the shard stayed closed, marked and unsealed, owed by nobody:
// the tight gather was refused for the run, and the next New, after a clean
// Close, repaired it in the foreground.
func TestCrashFix_ShardDemotedDuringItsSwapSettles(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(20, 12))
	cfg := cfCfg(dir, clock)
	cfg.SyncWrites = false // the fixture's 800 rows, as cfHistoryStore writes them; the reopen below runs cfCfg
	cfg.WarmMemTableSize = 8 << 20
	var armed atomic.Bool
	var ts *Store
	ready := make(chan struct{})
	cfg.sweepFault = func(step string) error {
		if step == "closed" && armed.CompareAndSwap(true, false) {
			<-ready
			clock.set(julDay(23, 12))
			if err := ts.CheckRotationForTest(); err != nil {
				t.Errorf("rotation inside the swap: %v", err)
			}
		}
		return nil
	}
	var err error
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	close(ready)
	tok, ids := cfWriteHistoryLogons(t, ts, cfHistoryRows)
	clock.set(julDay(21, 12))
	armed.Store(true)
	if err := ts.CheckRotationForTest(); err != nil { // day 20 warm on its hot-size handle: the swap is owed
		t.Fatal(err)
	}
	ts.SweepHandlesForTest()
	if armed.Load() {
		t.Fatal("setup: the swap's closed step never ran")
	}
	es := cfDay20(t, ts)
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the swap it was demoted in; want closed")
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the sweep")
	cfWantSettledEntry(t, ts, dir, "after the sweep")
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("metered read = %v, %v; want the logon", got, err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts2, _ := cfReopenDay23Cfg(t, dir, func(c *Config) {
		c.WarmMemTableSize = 8 << 20
		c.sweepFault = holdSettleSteps
	})
	if st, queued := ts2.OpenStats(), cfQueued(ts2); st.ColdRecovered != 0 || queued != 0 {
		t.Fatalf("reopen after the clean Close: ColdRecovered %d, %d queued; want 0 and 0 (open time follows the handles open at a kill)", st.ColdRecovered, queued)
	}
}

// TestCrashFix_MeteredOpenRevivesAGivenUpShard: the sweep gave up on a marked
// shard whose directory was gone (three repairs refused). The directory comes
// back (a volume mounted again): the first metered read's open succeeds, which
// clears the mark and tells the sweep to try again, so that read's check-in
// close queues the shard and its set is built. Without it the shard stayed
// given up for the process: no close queued it, and a clean directory is
// never refused.
func TestCrashFix_MeteredOpenRevivesAGivenUpShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	cfSetRecoverMark(t, dir, cfDay20Name)
	day20 := filepath.Join(dir, "events", cfDay20Name)
	away := filepath.Join(dir, "unmounted-2026-07-20")
	if err := os.Rename(day20, away); err != nil {
		t.Fatal(err)
	}
	ts, tok := cfReopenDay23(t, dir)
	ts.SweepHandlesForTest()
	if steps, queued := cfSettleSteps(ts), cfQueued(ts); steps != settleGiveUp || queued != 0 {
		t.Fatalf("without the directory: %d settle steps, %d queued; want %d and none (given up)", steps, queued, settleGiveUp)
	}
	if err := os.Rename(away, day20); err != nil {
		t.Fatal(err)
	}
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil {
		t.Fatalf("metered read with the directory back = %v, %v; want the logon", got, err)
	}
	ts.SweepHandlesForTest()
	if steps := cfSettleSteps(ts); steps != settleGiveUp+1 {
		t.Fatalf("settle steps after the read: %d; want %d (one build)", steps, settleGiveUp+1)
	}
	cfNewGatherProbe(t, ts, dir).wantSettled(t, tok, "after the read and the sweep")
	cfWantSettledEntry(t, ts, dir, "after the read and the sweep")
}

// TestCrashFix_ClearRevivesAGivenUpShard: a shard the sweep gave up on (three
// failed steps) is not queued by its closes. Clear empties it and it may be
// settled again: Clear resets the count, so the cleared shard is sealed empty
// and every fan-out drops it unopened.
func TestCrashFix_ClearRevivesAGivenUpShard(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	cfSealedWithoutSet(t, dir)
	var fail atomic.Bool
	fail.Store(true)
	ts, _ := cfReopenDay23Cfg(t, dir, func(c *Config) {
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
	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	ts.SweepHandlesForTest()
	if e := cfEntry(ts, cfDay20Name); !cfEntrySettled(e) || !e.NoRows {
		t.Fatalf("day 20's entry after Clear and the sweep: %+v; want settled with no rows", e)
	}
}

// TestCrashFix_NoStepWhileClearRuns: a cold shard with a handle installed
// when Clear starts is closed through the common close, which queues it and
// wakes the sweep. No step runs while Clear runs: a step then would compare
// the emptied shard's index definitions with an anchor Clear has not emptied
// yet, and repair the shard from definitions about to go. Clear queues its
// shards at its end. Serial: the Clear stage hook is process-wide.
func TestCrashFix_NoStepWhileClearRuns(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	ts, tok, _ := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	cfAddIndexDefinition(t, ts, tok)
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // a late write: day 20 open writable at Clear
		t.Fatalf("late PutNode: %v", err)
	}
	steps := cfSettleSteps(ts)
	during := int64(-1)
	clearStepFaultForTest = func(stage string) error {
		if stage == "archive" { // every event shard cleared, the anchor not yet
			ts.SweepPassForTest()
			during = cfSettleSteps(ts) - steps
		}
		return nil
	}
	err := ts.Clear()
	clearStepFaultForTest = nil
	if err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if during != 0 {
		t.Fatalf("settle steps run while Clear ran: %d; want 0", during)
	}
	ts.SweepHandlesForTest()
	if e := cfEntry(ts, cfDay20Name); !cfEntrySettled(e) || !e.NoRows {
		t.Fatalf("day 20's entry after Clear and the sweep: %+v; want settled with no rows", e)
	}
	if got := cfSettleSteps(ts) - steps; got != 1 {
		t.Fatalf("settle steps after Clear: %d; want 1 (the cleared shard sealed empty, no repair)", got)
	}
}

// TestCrashFix_SettledThenPlainReadRepairIsQueuedAgain: a shard settled by the
// whole chain (a build whose open finds the repair owed, the repair, the
// build: three steps, the give-up count) starts again at none. A plain read
// then repairs it through a writable open (a second index definition to
// sync) and closes it at its check-in, sealed without the set: that close
// queues it, and the sweep settles it again.
func TestCrashFix_SettledThenPlainReadRepairIsQueuedAgain(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	dir := t.TempDir()
	ids := cfSealedWithoutSet(t, dir)
	ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = holdSettleSteps })
	cfAddIndexDefinition(t, ts, tok)
	cfRelease(ts)
	es := cfDay20(t, ts)
	es.LockShardMuForTest()
	tries := es.settleTries
	es.UnlockShardMuForTest()
	if steps := cfSettleSteps(ts); steps != settleGiveUp || !cfEntrySettled(cfEntry(ts, cfDay20Name)) || tries != 0 {
		t.Fatalf("after the chain: %d settle steps, entry %+v, %d steps counted against the shard; want %d steps, settled, and the count back at 0",
			steps, cfEntry(ts, cfDay20Name), tries, settleGiveUp)
	}
	if err := ts.refShard.CreateCompositePropertyIndex(tok, []string{"host", "user"}); err != nil {
		t.Fatal(err)
	}
	if got, err := ts.GetNode(ids[0]); err != nil || got == nil {
		t.Fatalf("plain GetNode = %v, %v; want the logon", got, err)
	}
	if e := cfEntry(ts, cfDay20Name); cfEntrySettled(e) || cfQueued(ts) != 1 {
		t.Fatalf("after the plain read's repair: entry %+v, %d queued; want it sealed without the set and queued", e, cfQueued(ts))
	}
	cfAfterIdleTimeout(ts)
	cfWantSettledEntry(t, ts, dir, "after the plain read's repair and the sweep")
}

// TestCrashFix_PlainReadRepairRevivesAGivenUpShard: the sweep gave up on a
// shard (three failed walks). A plain read then repairs it through a writable
// open (an index definition to sync) and closes it at its check-in, unsealed
// and sealed again without the set. Every writable open of an access may
// change the shard, so it resets the count: the close queues the shard and
// the sweep settles it. Left given up, nothing revived it (a metered read is
// not refused and finds no mark to clear), and the tight gather was refused
// for the run.
func TestCrashFix_PlainReadRepairRevivesAGivenUpShard(t *testing.T) {
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
	cfAddIndexDefinition(t, ts, tok)
	if got, err := ts.GetNode(ids[0]); err != nil || got == nil {
		t.Fatalf("plain GetNode = %v, %v; want the logon", got, err)
	}
	if e := cfEntry(ts, cfDay20Name); cfEntrySettled(e) || e.Recover || cfQueued(ts) != 1 {
		t.Fatalf("after the plain read's repair: entry %+v, %d queued; want it repaired, sealed without the set, and queued", e, cfQueued(ts))
	}
	cfAfterIdleTimeout(ts)
	cfWantSettledEntry(t, ts, dir, "after the plain read's repair and the sweep")
	if n, opens, _, err := cfNewGatherProbe(t, ts, dir).gather(t, tok+7, cfTight); err != nil || n != 0 || opens != 0 {
		t.Fatalf("tight gather of an absent label after the sweep: %d members, %d opens, %v; want none, unopened", n, opens, err)
	}
}

// TestCrashFix_FailedClearWakesTheSweep: a pass that comes while a Clear runs
// puts its shards back and sends no kick. A Clear that fails must send it: no
// end of Clear queues the shards then, and with no idle tick in reach they
// would wait for the next close, refusal or rotation. Serial: the Clear stage
// hook is process-wide.
func TestCrashFix_FailedClearWakesTheSweep(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	ts, tok, _ := cfHistoryColdDay(t, dir, &tierClock{})
	defer func() { _ = ts.Close() }()
	cfNoIdleTick(t, ts)
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // a late write: day 20 open writable at Clear
		t.Fatalf("late PutNode: %v", err)
	}
	steps := cfSettleSteps(ts)
	failed := errors.New("clear failed (the test)")
	clearStepFaultForTest = func(stage string) error {
		if stage != "archive" { // every event shard cleared and closed, the anchor not yet
			return nil
		}
		// The goroutine has taken the kick of day 20's close, and its pass,
		// like this one, put the shard back: nothing is pending.
		cfEventually(t, "the sweep's kick taken", func() bool { return len(ts.sweep.kick) == 0 })
		ts.SweepPassForTest()
		if got, queued := cfSettleSteps(ts)-steps, cfQueued(ts); got != 0 || queued != 1 {
			t.Errorf("while Clear runs: %d settle steps, %d queued; want no step and day 20 queued", got, queued)
		}
		return failed
	}
	err := ts.Clear()
	clearStepFaultForTest = nil
	if !errors.Is(err, failed) {
		t.Fatalf("Clear = %v; want the stage's failure", err)
	}
	cfEventually(t, "day 20 taken up by the sweep goroutine after the failed Clear", func() bool {
		return cfSettleSteps(ts) > steps && cfQueued(ts) == 0
	})
}

// TestCrashFix_RefusedShardKeepsItsPlace: a refused read's shard is the next
// step, behind the shards earlier refusals put there, also when a pass finds
// it behind a gate (a move's copy), which sends any other shard to the end of
// the queue, and when its reader is refused a second time.
func TestCrashFix_RefusedShardKeepsItsPlace(t *testing.T) {
	testtier.Slow(t)
	const first, second, newest = "2026-07-14", "2026-07-16", "2026-07-20"
	t.Run("behind a gate", func(t *testing.T) {
		t.Parallel() // no process-wide hook
		dir := t.TempDir()
		ids := cfSevenColdDays(t, dir)
		gate := cfNewGate()
		coldDir := t.TempDir()
		ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) {
			c.ColdDir = coldDir
			c.sweepFault = gate.fault
		})
		cfAddIndexDefinition(t, ts, tok)
		cfWantRefused(t, ts, ids[first]) // first in the queue now
		release, done := cfHoldMove(t, ts, first)
		gate.step(ts) // the pass finds it behind its move's gate and runs the next shard's step
		if a, b := cfEntry(ts, first), cfEntry(ts, newest); !a.Recover || !b.Recover || cfSettleSteps(ts) != 1 {
			t.Fatalf("with %s behind its gate: its recover %v, %s recover %v, %d settle steps; want it waiting and the newest day's build to have found its repair owed",
				first, a.Recover, newest, b.Recover, cfSettleSteps(ts))
		}
		release()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("MoveShard: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the move did not end")
		}
		if e := cfEntry(ts, first); !e.Recover {
			t.Fatalf("%s after its move: %+v; want it still marked (the move repairs nothing)", first, e)
		}
		gate.step(ts) // the refused read's shard, not the chain that began meanwhile
		if a, b := cfEntry(ts, first), cfEntry(ts, newest); a.Recover || !a.CountsSealed || !b.Recover {
			t.Fatalf("the step after the gate dropped: %s %+v, %s recover %v; want the refused read's shard repaired before the newest day's chain goes on", first, a, newest, b.Recover)
		}
	})
	t.Run("refused twice", func(t *testing.T) {
		t.Parallel() // no process-wide hook
		dir := t.TempDir()
		ids := cfSevenColdDays(t, dir)
		gate := cfNewGate()
		ts, tok := cfReopenDay23Cfg(t, dir, func(c *Config) { c.sweepFault = gate.fault })
		cfAddIndexDefinition(t, ts, tok)
		cfWantRefused(t, ts, ids[first])
		cfWantRefused(t, ts, ids[second])
		cfWantRefused(t, ts, ids[first]) // its reader again: it keeps its place
		ts.sweep.mu.Lock()
		order := []string{ts.sweep.owed[0].name, ts.sweep.owed[1].name}
		ts.sweep.mu.Unlock()
		if order[0] != first || order[1] != second {
			t.Fatalf("the queue after %s was refused a second time begins %v; want %s, %s (the order of the first refusals)", first, order, first, second)
		}
		gate.step(ts)
		if a, b := cfEntry(ts, first), cfEntry(ts, second); a.Recover || !a.CountsSealed || !b.Recover {
			t.Fatalf("after one step: %s %+v, %s recover %v; want the first refused read's shard repaired first, its second refusal moving nothing", first, a, second, b.Recover)
		}
	})
}

// TestCrashFix_FailedDropReopenWakesTheNoRowsMark: the failed drop's reopen
// leaves its shard open writable, so nothing a settle step sealed on the
// restored entry meanwhile may stand: the seal, and with it the no-rows mark
// that keeps a shard out of every fan-out. The step of an empty shard is
// stood in for by its one effect here (the mark, set after the rollback): the
// real window is a few instructions wide.
func TestCrashFix_FailedDropReopenWakesTheNoRowsMark(t *testing.T) {
	testtier.Slow(t)
	t.Parallel() // no process-wide hook
	ts, tok, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	if _, err := cfPut(t, ts, tok, julDay(20, 4)); err != nil { // a late write: day 20 open writable
		t.Fatalf("late PutNode: %v", err)
	}
	good := ts.catalog.path
	ts.catalog.path = blockedMetadataPath(t)
	ts.dropHook = func(step string) {
		if step == "restored" {
			es.noRows.Store(true)
		}
	}
	_, err := ts.PurgeNodesByLabelBefore(tok, ms(julDay(21, 0)), 8)
	ts.dropHook = nil
	ts.catalog.path = good
	if err == nil || strings.Contains(err.Error(), "reopen shard") {
		t.Fatalf("the failed drop = %v; want the catalog save's error, the shard reopened", err)
	}
	if s, ro := es.StoreStateForTest(); s == nil || ro || es.noRows.Load() {
		t.Fatalf("after the failed drop: open %v, read-only %v, no-rows mark %v; want day 20 open writable and in every fan-out", s != nil, ro, es.noRows.Load())
	}
	if got, err := ts.GetNode(ids[0]); err != nil || got == nil {
		t.Fatalf("GetNode after the failed drop = %v, %v; want the logon", got, err)
	}
}

// TestHandleSweepQueue_RefusedShardsLeadInTheirOrder: the queue's rules, on
// the queue alone. The shards of refused reads lead it in the order of their
// first refusals; one refused again stays where it is; a shard in the middle
// of its steps goes behind them and ahead of the rest; a shard put back
// because it cannot have its step now stays at the head if a refused read
// waits on it and goes to the end otherwise; and one sent to the end as still
// written is no refused read's shard any more, so its next refusal puts it
// first again.
func TestHandleSweepQueue_RefusedShardsLeadInTheirOrder(t *testing.T) {
	t.Parallel()
	ts := &Store{}
	ts.sweep.kick = make(chan struct{}, 1)
	a, b, c, d := &EventShard{name: "a"}, &EventShard{name: "b"}, &EventShard{name: "c"}, &EventShard{name: "d"}
	order := func() string {
		ts.sweep.mu.Lock()
		defer ts.sweep.mu.Unlock()
		var names []string
		for _, es := range ts.sweep.owed {
			names = append(names, es.name)
		}
		return strings.Join(names, "")
	}
	want := func(when, names string) {
		t.Helper()
		if got := order(); got != names {
			t.Fatalf("%s: the queue is %q; want %q", when, got, names)
		}
	}
	ts.sweepOwe(a, b, c, d)
	want("queued", "abcd")
	ts.sweepOweFirst(d, true)
	ts.sweepOweFirst(c, true)
	want("d, then c refused", "dcab")
	ts.sweepOweFirst(d, true)
	want("d refused again", "dcab")
	ts.sweepOweFirst(b, false)
	want("b in the middle of its steps", "dcba")
	if !ts.sweepPutBack(d) || ts.firstOwed(1) != c {
		t.Fatalf("d behind a gate: put back to %q, next look %v; want it to stay first and the pass to look at c", order(), ts.firstOwed(1))
	}
	if ts.sweepPutBack(b) {
		t.Fatal("b behind a gate: stays; want it at the end")
	}
	want("b behind a gate", "dcab")
	ts.sweepUnqueue(d, true)
	want("d still written", "cabd")
	ts.sweepOweFirst(d, true)
	want("d refused after it went to the end", "cdab")
	ts.sweepUnqueue(c, false)
	ts.sweepOweFirst(a, false)
	want("c settled, a in the middle of its steps", "dab")
	if n, queued := ts.owedQueued(c); n != 3 || queued {
		t.Fatalf("after c left: %d queued, c queued %v; want 3 and not", n, queued)
	}
}
