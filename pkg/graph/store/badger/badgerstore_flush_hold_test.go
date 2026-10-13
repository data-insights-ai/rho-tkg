package badger

import (
	"testing"
	"time"
)

// Direct tests of store.MutationFlushHoldCapability (backlog 52). The graph
// path (DurableCommit + SyncWrites, crash children) is
// pkg/graph/durable_commit_syncwrites_test.go.
//
// Catches: a hold that still flushes per mutation, one that leaks past
// Release (the next mutation must flush everything held), a hold without the
// pending-write bound, a hold that changes the async mode, and
// FlushesEveryMutation answering true without SyncWrites (the graph would then
// take the exclusive lock for nothing).
func TestMutationFlushHold(t *testing.T) {
	cases := []struct {
		name       string
		cfg        Config
		every      bool
		heldSyncs  func(int64) bool // syncs of 6 held puts
		afterSyncs int64            // syncs of one put after Release
		afterPend  bool             // pending after that put
	}{
		{"sync-writes", Config{SyncWrites: true}, true, func(n int64) bool { return n == 0 }, 1, false},
		{"sync-writes-bound", Config{SyncWrites: true, MaxPendingWrites: 5}, true, func(n int64) bool { return n >= 1 && n < 6 }, 1, false},
		{"async-unchanged", Config{FlushInterval: time.Hour}, false, func(n int64) bool { return n == 0 }, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.cfg.Dir = t.TempDir()
			bs, err := New(c.cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			if got := bs.FlushesEveryMutation(); got != c.every {
				t.Fatalf("FlushesEveryMutation = %v, want %v", got, c.every)
			}
			bs.HoldMutationFlushes()
			before := bs.SyncCountForTest()
			for id := int64(1); id <= 6; id++ {
				putTestNode(t, bs, id, 10, nil)
			}
			if n := bs.SyncCountForTest() - before; !c.heldSyncs(n) {
				t.Fatalf("6 held puts: %d syncs", n)
			}
			bs.ReleaseMutationFlushes()
			before = bs.SyncCountForTest()
			putTestNode(t, bs, 7, 10, nil)
			if n := bs.SyncCountForTest() - before; n != c.afterSyncs {
				t.Fatalf("put after Release: %d syncs, want %d", n, c.afterSyncs)
			}
			if got := bs.PendingWriteCount() != 0; got != c.afterPend {
				t.Fatalf("put after Release: pending %d, want pending=%v", bs.PendingWriteCount(), c.afterPend)
			}
		})
	}
	t.Run("nil-store", func(t *testing.T) {
		var bs *Store
		if bs.FlushesEveryMutation() {
			t.Fatal("nil store: FlushesEveryMutation = true")
		}
	})
}
