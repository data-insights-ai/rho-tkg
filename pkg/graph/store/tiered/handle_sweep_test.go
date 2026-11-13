package tiered

import (
	"errors"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// handleSweepFixture is a disk store with WarmMemTableSize 8 MB holding one event
// node in the hot shard; rotate turns that shard warm.
func handleSweepFixture(t *testing.T, mutate func(*Config)) (*Store, *types.Node, uint16) {
	t.Helper()
	cfg := Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		FlushInterval: 1<<63 - 1, WarmMemTableSize: 8 << 20}
	if mutate != nil {
		mutate(&cfg)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	return ts, n, tok
}

// lateWrite updates n in its owner shard and reads the value back.
func lateWrite(t *testing.T, ts *Store, n *types.Node, v string) {
	t.Helper()
	_ = n.SetProperty("late", v)
	if err := ts.ReplaceNode(n); err != nil {
		t.Fatalf("late write: %v", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("read after the late write: %v", err)
	}
	if p, _ := got.GetProperty("late"); p != v {
		t.Fatalf("late property %v, want %q", p, v)
	}
}

// TestHandleSweep_WarmReopensAtWarmSize pins R3 T-a: after a rotation the
// sweep replaces the old hot shard's hot-size handle (hotArena false, one
// swap) and the warm shard still takes a late write and serves its rows.
// Catches a sweep that skips the reopen (the warm shard has no handle and the
// write fails) and one that never runs.
func TestHandleSweep_WarmReopensAtWarmSize(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, nil)
	old := ts.HotShardForTest()
	if !old.HotArenaForTest() {
		t.Fatal("the hot shard's handle is not marked hot-size")
	}
	forceRotation(t, ts)
	ts.SweepHandlesForTest()
	if old.Tier() != TierWarm {
		t.Fatalf("rotated-out shard is %s, want warm", old.Tier())
	}
	if old.HotArenaForTest() {
		t.Fatal("the warm shard kept its hot-size handle after the sweep")
	}
	if s, ro := old.StoreStateForTest(); s == nil || ro {
		t.Fatalf("warm handle open %v read-only %v; want open and writable", s != nil, ro)
	}
	if swaps, closes := ts.HandleSweepCountsForTest(); swaps != 1 || closes != 0 {
		t.Fatalf("sweep counts swaps %d closes %d, want 1 and 0", swaps, closes)
	}
	if !ts.HotShardForTest().HotArenaForTest() {
		t.Fatal("the new hot shard's handle is not marked hot-size")
	}
	lateWrite(t, ts, n, "after-swap")
	ts.SweepHandlesForTest() // nothing left to swap
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 {
		t.Fatalf("a second pass swapped again: %d swaps", swaps)
	}
}

// demotedSweepFixture is a calendar store (ColdAfter 48 h, MaxOpenColdShards
// 4) whose day-20 shard went hot, warm and then cold by rotation, still on its
// hot-size handle, holding n.
func demotedSweepFixture(t *testing.T) (*Store, *types.Node, *EventShard) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(20, 8))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.MaxOpenColdShards = 4 // a read keeps its read-only handle, which the late write upgrades
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	id, ok, err := ts.MintNodeIDAt(tok, ms(julDay(20, 3)), true)
	if err != nil || !ok {
		t.Fatalf("MintNodeIDAt: %v %v", ok, err)
	}
	n := types.NewNode(id, tok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	day20 := ts.HotShardForTest()
	clock.set(julDay(21, 8))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatal(err)
	}
	clock.set(julDay(23, 8))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatal(err)
	}
	if day20.Tier() != TierCold {
		t.Fatalf("day 20 is %s, want cold", day20.Tier())
	}
	return ts, n, day20
}

// TestHandleSweep_LateWriteHandleStaysOpen: a cold shard a late write opened
// writable (at the warm size, hotArena false) keeps its handle across a sweep;
// IdleTimeout closes it, as before R3, so a shard with steady late writes is
// not reopened on every kick. Catches a sweep that closes every writable cold
// handle (the hotArena check dropped).
func TestHandleSweep_LateWriteHandleStaysOpen(t *testing.T) {
	t.Parallel()
	ts, n, day20 := demotedSweepFixture(t)
	ts.SweepHandlesForTest() // closes the demoted hot-size handle
	lateWrite(t, ts, n, "late")
	opened, ro := day20.StoreStateForTest()
	if opened == nil || ro || day20.HotArenaForTest() {
		t.Fatalf("after the late write: open %v read-only %v hot-size %v; want a writable warm-size handle", opened != nil, ro, day20.HotArenaForTest())
	}
	ts.SweepHandlesForTest()
	if s, _ := day20.StoreStateForTest(); s != opened {
		t.Fatal("the sweep closed the handle a late write opened")
	}
	if _, closes := ts.HandleSweepCountsForTest(); closes != 1 {
		t.Fatalf("sweep closed %d handles, want 1 (the demoted one only)", closes)
	}
}

// TestHandleSweep_DemotedHandleCloses pins R3 T-b: a shard demoted to cold by
// a rotation has its handle closed by the sweep with its counts sealed; a read
// opens it read-only (no writable handle), a late write upgrades it. Catches
// a sweep that leaves demoted shards open.
func TestHandleSweep_DemotedHandleCloses(t *testing.T) {
	t.Parallel()
	ts, n, day20 := demotedSweepFixture(t)
	id := n.ID()
	ts.SweepHandlesForTest()
	if s, _ := day20.StoreStateForTest(); s != nil {
		t.Fatal("the demoted shard's handle is still open after the sweep")
	}
	if day20.cachedCounts() == nil {
		t.Fatal("the demoted shard closed without sealed counts")
	}
	if _, closes := ts.HandleSweepCountsForTest(); closes != 1 {
		t.Fatalf("sweep closed %d demoted handles, want 1", closes)
	}
	if _, err := ts.GetNode(id); err != nil {
		t.Fatalf("read of the demoted shard: %v", err)
	}
	if s, ro := day20.StoreStateForTest(); s == nil || !ro {
		t.Fatalf("after a read the demoted shard is open %v read-only %v; want a read-only handle", s != nil, ro)
	}
	lateWrite(t, ts, n, "late-cold")
	if s, ro := day20.StoreStateForTest(); s == nil || ro {
		t.Fatalf("after a late write the demoted shard is open %v read-only %v; want writable", s != nil, ro)
	}
}

// TestHandleSweep_PinDefersSwap pins R3 T-c: a checkout held on the shard
// keeps the swap waiting (gate up, no swap counted) while the pinned old
// handle still answers; after the checkin the swap completes, once. Catches a
// swap that closes a pinned handle.
func TestHandleSweep_PinDefersSwap(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, nil)
	es := ts.HotShardForTest()
	gated := make(chan struct{}, 4)
	ts.SetSweepFaultForTest(func(step string) error {
		if step == "gated" {
			gated <- struct{}{}
		}
		return nil
	})
	pinned, err := es.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatal(err)
	}
	forceRotation(t, ts) // kicks the sweep goroutine, which waits for the pin
	awaitSweepStep(t, gated, "gated")
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 0 {
		t.Fatalf("swapped %d times under a pin", swaps)
	}
	if s, _ := es.StoreStateForTest(); s != pinned {
		t.Fatal("the pinned handle was replaced under its pin")
	}
	if _, err := pinned.GetNode(n.ID()); err != nil {
		t.Fatalf("the pinned old handle does not answer: %v", err)
	}
	es.CheckinStoreForTest()
	ts.SweepHandlesForTest() // waits for the goroutine's swap
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 {
		t.Fatalf("swaps after the checkin %d, want 1", swaps)
	}
	if s, _ := es.StoreStateForTest(); s == pinned || s == nil {
		t.Fatal("the shard did not get a new handle after the checkin")
	}
	lateWrite(t, ts, n, "after-pin")
}

// TestHandleSweep_HotWriteDuringSwap pins R3 T-d: a write to the hot shard
// completes while a swap of the warm shard is held between its close and its
// reopen. Catches a swap that holds ts.mu or a store-wide lock.
func TestHandleSweep_HotWriteDuringSwap(t *testing.T) {
	t.Parallel()
	ts, n, tok := handleSweepFixture(t, nil)
	old := ts.HotShardForTest()
	reached, release := make(chan struct{}), make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	var once sync.Once
	ts.SetSweepFaultForTest(func(step string) error {
		if step == "closed" {
			once.Do(func() { close(reached) })
			<-release
		}
		return nil
	})
	forceRotation(t, ts)
	awaitSweepStep(t, reached, "closed")
	if s, _ := old.StoreStateForTest(); s != nil {
		t.Fatal("the warm shard is not closed at the hold")
	}
	hot := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
	if err := ts.PutNode(hot); err != nil {
		t.Fatalf("hot write during the swap: %v", err)
	}
	if _, err := ts.GetNode(hot.ID()); err != nil {
		t.Fatalf("hot read during the swap: %v", err)
	}
	unhold()
	ts.SweepHandlesForTest()
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 {
		t.Fatalf("swaps %d, want 1", swaps)
	}
	lateWrite(t, ts, n, "after-hold")
}

// TestHandleSweep_InMemoryNoOp pins R3 T-g: an in-memory store keeps its
// warm handle (a close would drop the rows). Catches a sweep that closes or
// reopens an in-memory shard.
func TestHandleSweep_InMemoryNoOp(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, func(c *Config) { c.InMemory, c.DataDir = true, "" })
	old := ts.HotShardForTest()
	before, _ := old.StoreStateForTest()
	forceRotation(t, ts)
	ts.SweepHandlesForTest()
	if s, _ := old.StoreStateForTest(); s != before {
		t.Fatal("the sweep replaced an in-memory shard's handle")
	}
	if swaps, closes := ts.HandleSweepCountsForTest(); swaps != 0 || closes != 0 {
		t.Fatalf("sweep counts swaps %d closes %d on an in-memory store, want 0", swaps, closes)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("read: %v", err)
	}
}

// TestHandleSweep_ScopeDefers pins R3 T-h: while a change-log scope is open
// the sweep leaves the warm shard's handle alone; after the scope ends the
// next pass swaps it. Catches a swap mid-scope (the scope's buffered records
// live on the handle).
func TestHandleSweep_ScopeDefers(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, func(c *Config) { c.ChangeLog = true })
	old := ts.HotShardForTest()
	if err := ts.BeginLogScope(); err != nil {
		t.Fatal(err)
	}
	forceRotation(t, ts)
	ts.SweepHandlesForTest()
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 0 || !old.HotArenaForTest() {
		t.Fatalf("swapped %d times inside a scope (hot-size %v)", swaps, old.HotArenaForTest())
	}
	if _, err := ts.CommitLogScope(); err != nil {
		t.Fatal(err)
	}
	ts.SweepHandlesForTest()
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 || old.HotArenaForTest() {
		t.Fatalf("after the scope: swaps %d (hot-size %v), want 1", swaps, old.HotArenaForTest())
	}
	lateWrite(t, ts, n, "after-scope")
}

// holdHandleSweep ends every swap at its gate, before it closes a handle: a
// test whose premise is a demoted shard keeping its handle, or a warm shard
// on its hot-size handle, sets it before the rotations.
func holdHandleSweep(ts *Store) {
	ts.SetSweepFaultForTest(func(string) error { return errors.New("handle sweep held by the test") })
}

// awaitSweepStep waits for the sweep goroutine to reach step; the deadline
// only turns a sweep that never runs into a failure instead of a hang.
func awaitSweepStep[T any](t *testing.T, ch <-chan T, step string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second):
		t.Fatalf("the sweep never reached %q", step)
	}
}

// TestHandleSweep_ScopeOpenedMidSwapReachesReopenedHandle: a change-log scope
// opened while a warm swap is held between its close and its reopen (the
// scope's fold skips the closed shard) is handed to the reopened handle, so a
// diverted write to that shard is buffered and a DiscardLogScope drops it.
// Catches an install that skips the scope hand-off: the write self-commits a
// change-log record no rollback can reach.
func TestHandleSweep_ScopeOpenedMidSwapReachesReopenedHandle(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, func(c *Config) { c.ChangeLog = true })
	old := ts.HotShardForTest()
	reached, release := make(chan struct{}), make(chan struct{})
	unhold := sync.OnceFunc(func() { close(release) })
	defer unhold()
	var once sync.Once
	ts.SetSweepFaultForTest(func(step string) error {
		if step == "closed" {
			once.Do(func() { close(reached) })
			<-release
		}
		return nil
	})
	forceRotation(t, ts)
	awaitSweepStep(t, reached, "closed")
	if err := ts.BeginLogScope(); err != nil {
		t.Fatal(err)
	}
	ts.SetLogDivert(true)
	unhold()
	ts.SweepHandlesForTest() // waits for the held swap's reopen
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 || old.HotArenaForTest() {
		t.Fatalf("swaps %d (hot-size %v), want 1", swaps, old.HotArenaForTest())
	}
	before, err := ts.ChangeFeed(0, 0) // the gated shard's feed waits for the swap
	if err != nil {
		t.Fatal(err)
	}
	_ = n.SetProperty("late", "rolled-back")
	if err := ts.ReplaceNode(n); err != nil {
		t.Fatalf("diverted write to the reopened shard: %v", err)
	}
	ts.SetLogDivert(false)
	if err := ts.DiscardLogScope(); err != nil {
		t.Fatal(err)
	}
	after, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Fatalf("change feed %d records after a discarded scope, want %d: the reopened handle missed the scope", len(after), len(before))
	}
}
