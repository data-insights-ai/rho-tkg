package tiered

import (
	"errors"
	"testing"
	"time"
)

// BACKLOG 19n: Close()'s three active-request drains (event shards, ref
// archive, reference shard) used to spin-wait unconditionally on
// activeReqs/refActiveReqs/archiveActiveReqs reaching zero — unlike the
// purge protocol's coldShardDrainSpinLimit-bounded drain in the same
// package. A leaked checkin anywhere (a bug this doesn't cause but must not
// compound) would hang Close() forever instead of surfacing the problem.
// These tests simulate a leaked checkin via the *ForTest atomic-counter
// accessors and confirm Close() still terminates, reporting a wrapped
// ErrDrainTimeout instead of hanging. The drain is lowered to drainTestSpins
// through drainSpinsForTest, so each test waits ~50ms, not the real ~5s;
// TestDrainSpinLimitConstant pins the real bound once.

const drainTestSpins = 50

// TestDrainSpinLimitConstant pins the production drain bound (~5s at 1ms/spin)
// the drain tests lower, and that a store with no override uses it.
func TestDrainSpinLimitConstant(t *testing.T) {
	t.Parallel()
	if coldShardDrainSpinLimit != 5000 {
		t.Fatalf("coldShardDrainSpinLimit = %d, want 5000", coldShardDrainSpinLimit)
	}
	ts := newDrainTestStore(t)
	defer ts.Close()
	if got := ts.drainSpinLimit(); got != coldShardDrainSpinLimit {
		t.Fatalf("drainSpinLimit without override = %d, want %d", got, coldShardDrainSpinLimit)
	}
	ts.drainSpinsForTest.Store(drainTestSpins)
	if got := ts.drainSpinLimit(); got != drainTestSpins {
		t.Fatalf("drainSpinLimit with override = %d, want %d", got, drainTestSpins)
	}
}

func newDrainTestStore(t *testing.T) *Store {
	t.Helper()
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ts
}

// closeWithDeadline runs ts.Close() in a goroutine and fails the test if it
// does not return within deadline — the direct proof Close() is no longer
// capable of hanging forever on a leaked checkin.
func closeWithDeadline(t *testing.T, ts *Store, deadline time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- ts.Close() }()
	select {
	case err := <-done:
		return err
	case <-time.After(deadline):
		t.Fatalf("Close() did not return within %s — the bounded drain regressed to an unbounded spin", deadline)
		return nil
	}
}

func TestTieredClose_LeakedEventShardCheckinTimesOutInsteadOfHanging(t *testing.T) {
	t.Parallel()
	ts := newDrainTestStore(t)
	ts.drainSpinsForTest.Store(drainTestSpins)

	ts.MuForTest().RLock()
	var es *EventShard
	for _, shard := range ts.EventShardsForTest() {
		es = shard
		break
	}
	ts.MuForTest().RUnlock()
	if es == nil {
		t.Fatal("expected at least one event shard (the hot shard) on a fresh store")
	}
	es.ActiveReqsForTest().Add(1) // simulate a leaked checkin — never decremented

	err := closeWithDeadline(t, ts, 5*time.Second)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close() err = %v, want a wrapped ErrDrainTimeout", err)
	}
}

func TestTieredClose_LeakedRefShardCheckinTimesOutInsteadOfHanging(t *testing.T) {
	t.Parallel()
	ts := newDrainTestStore(t)
	ts.drainSpinsForTest.Store(drainTestSpins)
	ts.RefActiveReqsForTest().Add(1) // simulate a leaked checkin

	err := closeWithDeadline(t, ts, 5*time.Second)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close() err = %v, want a wrapped ErrDrainTimeout", err)
	}
}
