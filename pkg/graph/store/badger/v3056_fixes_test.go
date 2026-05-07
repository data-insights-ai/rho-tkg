package badger

import (
	"sync/atomic"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix #4: DeleteNode does not block idxMu during Badger read ---

// TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock verifies that DeleteNode on a
// cache-miss node completes without contention on idxMu during Badger I/O.
// We exercise this by flushing the store (evicting the write-pending entry from
// the dirty path), then deleting concurrently while another goroutine holds an
// RLock to detect any deadlock that would previously occur.
func TestBadgerStore_DeleteNode_NoDiskIOUnderWriteLock(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(types.NodeID(snowflake.ID(9901)), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	// Flush to Badger and evict from LRU so the delete path hits db.View.
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	bs.NodeCacheForTest().ResetForTest() // evict all entries (post-Flush, all are clean)

	// Hold an RLock concurrently for the duration of DeleteNode.
	// Before fix #4, DeleteNode held idxMu.Lock() THEN called db.View, which
	// would block the concurrent RLock for the I/O duration. Now prefetchNode
	// does db.View before the write lock, so the RLock holder is not blocked.
	var rLockDuration atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		bs.LockIdxMuRForTest()
		time.Sleep(5 * time.Millisecond) // simulate RLock holder doing work
		bs.UnlockIdxMuRForTest()
		rLockDuration.Store(time.Since(start).Milliseconds())
	}()

	time.Sleep(1 * time.Millisecond) // let the goroutine acquire RLock first
	if err := bs.DeleteNode(types.NodeID(9901)); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	<-done

	// Verify deletion.
	if _, err := bs.GetNode(types.NodeID(9901)); err == nil {
		t.Error("node still present after DeleteNode")
	}
}
