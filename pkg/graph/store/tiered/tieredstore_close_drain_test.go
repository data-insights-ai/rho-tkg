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
// ErrDrainTimeout instead of hanging.

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
	if testing.Short() {
		t.Skip("waits out the full ~5s bounded drain")
	}
	ts := newDrainTestStore(t)

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

	err := closeWithDeadline(t, ts, 8*time.Second)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close() err = %v, want a wrapped ErrDrainTimeout", err)
	}
}

func TestTieredClose_LeakedRefShardCheckinTimesOutInsteadOfHanging(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the full ~5s bounded drain")
	}
	ts := newDrainTestStore(t)
	ts.RefActiveReqsForTest().Add(1) // simulate a leaked checkin

	err := closeWithDeadline(t, ts, 8*time.Second)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("Close() err = %v, want a wrapped ErrDrainTimeout", err)
	}
}
