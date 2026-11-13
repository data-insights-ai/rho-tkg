package tiered

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// errSweepStop is the error a test returns from a sweep step to end the swap there.
var errSweepStop = errors.New("swap stopped by the test")

// failedSwapRig is a calendar store (WarmMemTableSize 8 MB) whose day-20
// shard went warm on its hot-size handle by a rotation, with the handle sweep
// held at one step of its swap.
type failedSwapRig struct {
	ts    *Store
	clock *tierClock
	day20 *EventShard
	tok   uint16
	ids   []types.NodeID
	put   func(day, hour int) types.NodeID
	// release lets the held swap go on; idempotent.
	release func()
}

// newFailedSwapRig opens the store, writes one day-20 node and rotates to day
// 21, then waits until the sweep goroutine is held at holdStep. After the hold
// each step goes to onStep, whose error ends the swap at that step.
func newFailedSwapRig(t *testing.T, holdStep string, onStep func(step string) error) *failedSwapRig {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.WarmMemTableSize = 8 << 20
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	rig := &failedSwapRig{ts: ts, clock: clock, tok: tok}
	rig.put = func(day, hour int) types.NodeID {
		t.Helper()
		id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(day, hour)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v %v", ok, err)
		}
		if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		rig.ids = append(rig.ids, id)
		return id
	}
	rig.put(20, 3)
	rig.day20 = ts.HotShardForTest()

	reached, release := make(chan struct{}), make(chan struct{})
	rig.release = sync.OnceFunc(func() { close(release) })
	t.Cleanup(rig.release)
	var once sync.Once
	ts.SetSweepFaultForTest(func(step string) error {
		if step == holdStep {
			once.Do(func() { close(reached); <-release })
		}
		if onStep != nil {
			return onStep(step)
		}
		return nil
	})
	clock.set(julDay(21, 8))
	if err := ts.CheckRotationForTest(); err != nil { // day 20 warm on its hot-size handle; kicks the sweep
		t.Fatal(err)
	}
	awaitSweepStep(t, reached, holdStep)
	return rig
}

// promote moves the clock back into day 20 and cuts day 21's window to
// [20 08:00, 20 09:00) in memory and in the catalog, then starts the rotation
// that promotes day 20 (the window holding the clock). It returns once the
// rotation flipped day 20's tier to hot; the rotation then waits for the
// held swap's gate. The returned channel carries the rotation's result.
func (r *failedSwapRig) promote(t *testing.T) <-chan error {
	t.Helper()
	r.clock.set(julDay(20, 9))
	r.ts.MuForTest().Lock()
	day21 := r.ts.HotShardForTest()
	day21.timeStart = julDay(20, 8)
	day21.SetTimeEndForTest(julDay(20, 9))
	cat := r.ts.CatalogForTest()
	cat.mu.Lock()
	for i := range cat.Shards {
		if cat.Shards[i].Name == day21.name {
			cat.Shards[i].TimeStart, cat.Shards[i].TimeEnd = julDay(20, 8), julDay(20, 9)
		}
	}
	cat.mu.Unlock()
	r.ts.MuForTest().Unlock()
	rotated := make(chan error, 1)
	go func() { rotated <- r.ts.CheckRotationForTest() }()
	for deadline := time.Now().Add(30 * time.Second); r.day20.Tier() != TierHot; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the promotion never flipped day 20 to hot")
		}
	}
	return rotated
}

// catalogTier is the tier the saved catalog names for the shard.
func catalogTier(t *testing.T, ts *Store, name string) ShardTier {
	t.Helper()
	e, ok := ts.CatalogForTest().GetShard(name)
	if !ok {
		t.Fatalf("shard %s is not in the catalog", name)
	}
	return e.Tier
}

// wantPromotedShardUsable asserts the shard a rotation promoted while its
// swap failed: still hot (in memory and in the catalog), the store's hot
// shard, on an open writable hot-size handle, and holding every row, with a
// write accepted after.
func wantPromotedShardUsable(t *testing.T, r *failedSwapRig) {
	t.Helper()
	ts, day20 := r.ts, r.day20
	if err := ts.backgroundError(); err != nil {
		t.Fatalf("the failed swap faulted the store: %v", err)
	}
	if ts.HotShardForTest() != day20 || day20.Tier() != TierHot {
		t.Fatalf("day 20 is %s (hot shard %v), want the hot shard", day20.Tier(), ts.HotShardForTest() == day20)
	}
	if got := catalogTier(t, ts, day20.name); got != TierHot {
		t.Fatalf("the catalog names day 20 %s, want hot", got)
	}
	if s, ro := day20.StoreStateForTest(); s == nil || ro || !day20.HotArenaForTest() {
		t.Fatalf("promoted day 20: open %v read-only %v hot-size %v; want a writable hot-size handle", s != nil, ro, day20.HotArenaForTest())
	}
	for _, id := range r.ids {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("GetNode(%d) after the failed swap: %v", id, err)
		}
	}
	r.put(20, 10)
	if ns, err := ts.NodesByLabel(r.tok, QueryOpts{}); err != nil || len(ns) != len(r.ids) {
		t.Fatalf("NodesByLabel: %d rows (err %v), want %d", len(ns), err, len(r.ids))
	}
}

// TestHandleSweep_FailedSwapKeepsPromotedShardHot: a rotation promotes day 20
// to hot while the sweep holds its gate between the close of its hot-size
// handle and the reopen, and the swap then ends without a handle: the held
// step returns an error (it stands for any stop there), or the warm open
// fails. The shard keeps its tier, in memory and in the catalog, and gets a
// hot-size handle at the gate's release: it is the store's hot shard and takes
// its writes. Catches a swap that demotes the shard to cold on a failed
// reopen (the hot shard has no handle and the catalog names it cold).
func TestHandleSweep_FailedSwapKeepsPromotedShardHot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, failAt string }{
		{"stopped after the close", "closed"},
		{"warm open failed", "reopen"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newFailedSwapRig(t, "closed", func(step string) error {
				if step == tc.failAt {
					return errSweepStop
				}
				return nil
			})
			rotated := r.promote(t)
			r.release()
			if err := <-rotated; err != nil {
				t.Fatalf("promotion: %v", err)
			}
			r.ts.SweepHandlesForTest() // waits for the goroutine's swap
			wantPromotedShardUsable(t, r)
		})
	}
}

// TestHandleSweep_FailedSwapThenPromotionGetsHandle: the order the other way
// round. The swap's warm open fails and demotes the shard to cold (in memory
// and in the catalog); before the swap lets go of the gate a rotation promotes
// the shard (its check that the shard is warm ran earlier). The shard ends
// hot on a hot-size handle and the catalog names it hot again. Catches a swap
// that leaves the promoted shard without a handle (nil store on the hot shard)
// and the cold tier in the catalog.
func TestHandleSweep_FailedSwapThenPromotionGetsHandle(t *testing.T) {
	t.Parallel()
	r := newFailedSwapRig(t, "unopened", func(step string) error {
		if step == "reopen" {
			return errSweepStop
		}
		return nil
	})
	// The swap failed: demoted, no handle, held at "unopened" with its gate up.
	if r.day20.Tier() != TierCold || catalogTier(t, r.ts, r.day20.name) != TierCold {
		t.Fatalf("the failed warm open left day 20 %s (catalog %s), want cold in both", r.day20.Tier(), catalogTier(t, r.ts, r.day20.name))
	}
	// A rotation promotes it now: the tier flip is the rotation's own (under
	// the shard's lock), then it waits for the gate.
	r.day20.shardMu.Lock()
	r.day20.setTier(TierHot)
	r.day20.shardMu.Unlock()
	r.release()
	r.ts.SweepHandlesForTest()
	if s, ro := r.day20.StoreStateForTest(); s == nil || ro || !r.day20.HotArenaForTest() {
		t.Fatalf("promoted day 20: open %v read-only %v hot-size %v; want a writable hot-size handle", s != nil, ro, r.day20.HotArenaForTest())
	}
	if got := catalogTier(t, r.ts, r.day20.name); got != TierHot {
		t.Fatalf("the catalog names the promoted shard %s, want hot", got)
	}
	r.put(20, 10)
	for _, id := range r.ids {
		if _, err := r.ts.GetNode(id); err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
	}
}

// TestHandleSweep_FailedSwapDemotesWarmShard: a swap whose warm open fails on
// a shard nobody promoted still demotes it to cold, as before: the catalog
// names it cold so a restart does not need that open, and its next access
// opens it (and reports why when the cause stays). Pins the case the
// promoted-shard rule leaves alone.
func TestHandleSweep_FailedSwapDemotesWarmShard(t *testing.T) {
	t.Parallel()
	r := newFailedSwapRig(t, "reopen", func(step string) error {
		if step == "reopen" {
			return errSweepStop
		}
		return nil
	})
	r.release()
	r.ts.SweepHandlesForTest()
	if r.day20.Tier() != TierCold || catalogTier(t, r.ts, r.day20.name) != TierCold {
		t.Fatalf("day 20 is %s (catalog %s) after a failed warm open, want cold in both", r.day20.Tier(), catalogTier(t, r.ts, r.day20.name))
	}
	if s, _ := r.day20.StoreStateForTest(); s != nil {
		t.Fatal("the shard whose reopen failed has a handle")
	}
	for _, id := range r.ids {
		if _, err := r.ts.GetNode(id); err != nil {
			t.Fatalf("GetNode(%d) opens the demoted shard: %v", id, err)
		}
	}
}

// TestHandleSweep_FailedSwapOfPromotedShardFaultsStore: when even the hot-size
// open of the promoted shard fails (its directory is gone), the shard keeps
// its tier, the failure is recorded as the store's sticky fault and nothing is
// left half-demoted: day 20 is still hot in memory and in the catalog.
func TestHandleSweep_FailedSwapOfPromotedShardFaultsStore(t *testing.T) {
	t.Parallel()
	r := newFailedSwapRig(t, "closed", nil)
	rotated := r.promote(t)
	dir := r.ts.shardDir(r.day20.path)
	if err := os.Rename(dir, dir+".gone"); err != nil {
		t.Fatal(err)
	}
	r.release()
	if err := <-rotated; err != nil {
		t.Fatalf("promotion: %v", err)
	}
	r.ts.SweepHandlesForTest()
	if r.day20.Tier() != TierHot || catalogTier(t, r.ts, r.day20.name) != TierHot {
		t.Fatalf("day 20 is %s (catalog %s), want hot in both", r.day20.Tier(), catalogTier(t, r.ts, r.day20.name))
	}
	if err := r.ts.backgroundError(); err == nil {
		t.Fatal("a promoted shard that could not be reopened left no store fault")
	}
}
