package tiered

import (
	"testing"
	"time"
)

// BACKLOG 19k: MigrateFromBadger had no enforced single-writer discipline —
// two concurrent calls against the SAME destination Store could interleave
// dst.ontology.SetLabelRegistry (shared, previously-unlocked state on the
// ontology object) and the insertedNodes/insertedRels rollback bookkeeping,
// corrupting routing or rollback state. Fixed by taking dst.migrateMu for
// the whole call.
//
// This test reproduces the guard deterministically (no timing flakiness) by
// taking dst.migrateMu directly — a package-internal test, so no synthetic
// goroutine race is needed — to simulate an in-flight migration, and
// confirms a second MigrateFromBadger call blocks until it releases.
func TestMigrateFromBadger_SerializesConcurrentCalls(t *testing.T) {
	dst := newTestTieredStore(t)

	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// Simulate an in-flight migration holding dst.migrateMu for its duration.
	dst.migrateMu.Lock()

	done := make(chan error, 1)
	go func() {
		done <- MigrateFromBadger(src, dst)
	}()

	select {
	case err := <-done:
		dst.migrateMu.Unlock()
		t.Fatalf("MigrateFromBadger completed (err=%v) while a simulated in-flight migration held dst.migrateMu — BACKLOG 19k regression", err)
	case <-time.After(150 * time.Millisecond):
		// Still blocked on migrateMu, as required.
	}

	dst.migrateMu.Unlock()

	if err := <-done; err != nil {
		t.Fatalf("MigrateFromBadger (after the simulated in-flight migration released) = %v, want nil (empty source, empty destination)", err)
	}
}
