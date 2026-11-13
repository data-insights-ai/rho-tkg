package tiered

import (
	"sync"
	"testing"
)

// TestHandleSweep_SwapInstallingMidBeginJoinsScopeOnce: a swap installs its
// reopened handle after BeginLogScope set the store's scope flag and before
// its fold reached that shard. The install hands the handle to the open scope
// (handNewShardToLogScope), and the fold then finds the scope open on it: the
// begin succeeds and the shard is in the scope once, so a diverted write to it
// is buffered until the commit emits it exactly once, and the next scope opens
// cleanly. Catches a hand-off that is not idempotent (BeginLogScope fails with
// "change-log scope already open" on a good store).
func TestHandleSweep_SwapInstallingMidBeginJoinsScopeOnce(t *testing.T) {
	t.Parallel()
	ts, n, _ := handleSweepFixture(t, func(c *Config) { c.ChangeLog = true })
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
	forceRotation(t, ts) // the old hot shard is warm; its swap is held between the close and the reopen
	awaitSweepStep(t, reached, "closed")

	// The swap reopens and installs after the flag and before the fold: the
	// hook lets it go and waits until it has handed the handle to the scope.
	ts.SetScopeFlagHookForTest(func() {
		unhold()
		ts.SweepHandlesForTest()
	})
	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("BeginLogScope with a swap installing between its flag and its fold: %v", err)
	}
	ts.SetScopeFlagHookForTest(nil)
	if swaps, _ := ts.HandleSweepCountsForTest(); swaps != 1 {
		t.Fatalf("swaps %d, want 1: the swap did not install inside the begin", swaps)
	}
	before, err := ts.ChangeFeed(0, 0) // the gated shard's feed waits for a swap, so only now
	if err != nil {
		t.Fatal(err)
	}

	ts.SetLogDivert(true)
	_ = n.SetProperty("late", "in-scope")
	if err := ts.ReplaceNode(n); err != nil {
		t.Fatalf("diverted write to the reopened shard: %v", err)
	}
	ts.SetLogDivert(false)
	if mid, err := ts.ChangeFeed(0, 0); err != nil || len(mid) != len(before) {
		t.Fatalf("change feed %d records (err %v) before the commit, want %d: the write is buffered in the scope", len(mid), err, len(before))
	}
	if _, err := ts.CommitLogScope(); err != nil {
		t.Fatal(err)
	}
	if after, err := ts.ChangeFeed(0, 0); err != nil || len(after) != len(before)+1 {
		t.Fatalf("change feed %d records (err %v) after the commit, want %d: one record for the one write", len(after), err, len(before)+1)
	}

	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("the next scope: %v", err)
	}
	if err := ts.DiscardLogScope(); err != nil {
		t.Fatal(err)
	}
}

// TestHandleSweep_BeginLogScopeTwiceStillFails: making the hand-off idempotent
// does not let a caller open a second scope on top of the first: the store's
// own BeginLogScope still refuses, and the open scope survives (it commits).
// Catches a begin fold that skips a handle with the scope open and so hides a
// caller's double begin.
func TestHandleSweep_BeginLogScopeTwiceStillFails(t *testing.T) {
	t.Parallel()
	ts, _, _ := handleSweepFixture(t, func(c *Config) { c.ChangeLog = true })
	if err := ts.BeginLogScope(); err != nil {
		t.Fatal(err)
	}
	if err := ts.BeginLogScope(); err == nil {
		t.Fatal("a second BeginLogScope inside an open scope succeeded")
	}
	if _, err := ts.CommitLogScope(); err != nil {
		t.Fatalf("the first scope did not survive the refused begin: %v", err)
	}
	if err := ts.BeginLogScope(); err != nil {
		t.Fatalf("a scope after the commit: %v", err)
	}
	if err := ts.DiscardLogScope(); err != nil {
		t.Fatal(err)
	}
}
