package tiered

import (
	"fmt"
	"testing"
	"time"
)

// addRetainedColdShards adds n closed cold event shards to ts's shard map, the
// shape of a long-running store's history: each has no handle, so a sweep owes
// it nothing. They sit before the hot shard's window and are removed again
// when the test ends (Close walks the map).
func addRetainedColdShards(t *testing.T, ts *Store, n int) {
	t.Helper()
	base := time.Now().Add(-1000 * time.Hour)
	names := make([]string, 0, n)
	ts.MuForTest().Lock()
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("retained-%04d", i)
		es := &EventShard{name: name, tier: TierCold, readOnly: true, path: "events/" + name,
			timeStart: base.Add(time.Duration(i) * time.Hour), timeEnd: base.Add(time.Duration(i+1) * time.Hour)}
		es.initTier(TierCold)
		ts.EventShardsForTest()[name] = es
		names = append(names, name)
	}
	ts.MuForTest().Unlock()
	t.Cleanup(func() {
		ts.MuForTest().Lock()
		for _, name := range names {
			delete(ts.EventShardsForTest(), name)
		}
		ts.MuForTest().Unlock()
	})
}

// awaitSweepSwaps waits for the sweep goroutine to have swapped want warm
// handles and finished a pass after the one counted by passesBefore; the
// deadline only turns a sweep that never runs into a failure.
func awaitSweepSwaps(t *testing.T, ts *Store, want, passesBefore int64) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(time.Millisecond) {
		swaps, _ := ts.HandleSweepCountsForTest()
		passes, _ := ts.SweepPassCountsForTest()
		if swaps >= want && passes > passesBefore {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sweep did not reach %d swaps and a finished pass (swaps %d, passes %d, before %d)", want, swaps, passes, passesBefore)
		}
	}
}

// TestHandleSweep_VisitsBoundedByShardsNeedingWork: what a pass costs follows
// the shards that need work, not the shards the store retains. One rotation
// leaves one warm shard on its hot-size handle among 10, 100 and 1000 closed
// cold shards; the passes that follow look at that one shard (visited 1, a
// count the retained shards do not move), and a pass with nothing owed looks
// at none. Catches a pass that walks every event shard (visited retained + 1).
func TestHandleSweep_VisitsBoundedByShardsNeedingWork(t *testing.T) {
	t.Parallel()
	for _, retained := range []int{10, 100, 1000} {
		t.Run(fmt.Sprintf("%d retained", retained), func(t *testing.T) {
			t.Parallel()
			ts, _, _ := handleSweepFixture(t, nil)
			old := ts.HotShardForTest()
			addRetainedColdShards(t, ts, retained)
			passes0, visited0 := ts.SweepPassCountsForTest()
			forceRotation(t, ts) // one shard owes a swap; kicks the sweep goroutine
			awaitSweepSwaps(t, ts, 1, passes0)
			_, visited1 := ts.SweepPassCountsForTest()
			if got := visited1 - visited0; got > 1 {
				t.Fatalf("the passes after one rotation looked at %d shards among %d retained, want at most the 1 that needed work", got, retained)
			}
			ts.SweepHandlesForTest() // nothing is owed now
			if _, visited2 := ts.SweepPassCountsForTest(); visited2 != visited1 {
				t.Fatalf("a pass with nothing owed looked at %d shards, want 0", visited2-visited1)
			}
			if s, ro := old.StoreStateForTest(); s == nil || ro || old.HotArenaForTest() {
				t.Fatalf("the shard that needed work: open %v read-only %v hot-size %v; want a writable warm-size handle", s != nil, ro, old.HotArenaForTest())
			}
		})
	}
}

// TestHandleSweep_PinnedShardDoesNotBlockOthers: shards whose pins are held
// do not delay the shards behind them in the same pass. Three warm shards are
// pinned and three are not, all on hot-size handles; a pass with a drain
// deadline the test never waits out swaps the three free shards while the pins
// are held, then the pinned ones once they are released. Catches a pass that
// waits for a pinned shard before it goes on (the order of the shards is the
// map's, so a pinned shard before a free one blocks it).
func TestHandleSweep_PinnedShardDoesNotBlockOthers(t *testing.T) {
	t.Parallel()
	const free, pinned = 3, 3
	ts, _, _ := handleSweepFixture(t, nil)
	holdHandleSweep(ts) // the rotations leave their shards on the hot-size handle
	var shards []*EventShard
	for i := 0; i < free+pinned; i++ {
		shards = append(shards, ts.HotShardForTest())
		forceRotation(t, ts)
	}
	var held []*EventShard
	for _, es := range shards[:pinned] {
		if _, err := es.CheckoutStoreForTest(ts); err != nil {
			t.Fatal(err)
		}
		held = append(held, es)
	}
	release := func() {
		for _, es := range held {
			es.CheckinStoreForTest()
		}
		held = nil
	}
	defer func() { release() }()
	ts.SetSweepDrainTimeoutForTest(time.Minute)
	ts.SetSweepFaultForTest(nil)
	done := make(chan struct{})
	go func() {
		ts.SweepHandlesForTest()
		close(done)
	}()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(time.Millisecond) {
		if swaps, _ := ts.HandleSweepCountsForTest(); swaps >= free {
			break
		}
		if time.Now().After(deadline) {
			swaps, _ := ts.HandleSweepCountsForTest()
			t.Fatalf("%d of the %d free shards swapped while %d others are pinned: a pinned shard holds up the pass", swaps, free, pinned)
		}
	}
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != free {
		t.Fatalf("swaps %d with %d shards pinned, want %d", swaps, pinned, free)
	}
	release()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the pass did not finish after the pins were released")
	}
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != free+pinned {
		t.Fatalf("swaps %d after the release, want %d", swaps, free+pinned)
	}
}
