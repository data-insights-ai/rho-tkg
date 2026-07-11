package tiered

import (
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// newDiskChangeLogStore builds a disk-backed change-log tiered store (a cold
// shard's log segment must survive a badger handle close, which requires disk).
func newDiskChangeLogStore(t *testing.T) *Store {
	t.Helper()
	ts, err := New(diskChangeLogConfig(t.TempDir()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts
}

// closeEventShardStore closes a named event shard's badger handle and nils it so
// the next access lazily reopens it (read-only for the feed).
func closeEventShardStore(t *testing.T, ts *Store, name string) {
	t.Helper()
	es, ok := ts.EventShardsForTest()[name]
	if !ok {
		t.Fatalf("event shard %q not found", name)
	}
	es.LockShardMuForTest()
	defer es.UnlockShardMuForTest()
	if es.Store() != nil {
		if err := es.Store().Close(); err != nil {
			t.Fatalf("close event shard %q: %v", name, err)
		}
		es.SetStoreForTest(nil)
	}
}

// TestTieredChangeFeedAcrossRotation proves a rotated (warm) shard still owns its
// committed log records and the feed reads them: the store-global allocator keeps
// climbing across the rotation boundary, and the k-way merge iterates EVERY
// catalog shard (ADR-0005 §2.3). Records from the retired shard and the new hot
// shard emerge in one ascending LSN stream.
func TestTieredChangeFeedAcrossRotation(t *testing.T) {
	ts, _, _, signalTok := newChangeLogTieredStore(t)
	nodeGen := tieredNodeGen(t)

	// Records on the first hot shard.
	for i := 0; i < 3; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("PutNode pre-rotation: %v", err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	forceRotation(t, ts)
	// Records on the NEW hot shard — LSNs keep climbing across the boundary.
	for i := 0; i < 2; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("PutNode post-rotation: %v", err)
		}
	}

	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if got := lsns(feed); !equalLSNs(got, []uint64{1, 2, 3, 4, 5}) {
		t.Fatalf("cross-rotation feed = %v, want [1 2 3 4 5] (retired shard's records included)", got)
	}
}

// TestTieredChangeFeedColdShardReadOnly proves the feed reads a COLD shard's log
// segment read-only (ADR-0005 §2.3): after a shard is demoted to cold and its
// badger handle closed, the merge lazily reopens it read-only to page its
// immutable log — no flush on a closed shard, no record lost.
func TestTieredChangeFeedColdShardReadOnly(t *testing.T) {
	ts := newDiskChangeLogStore(t)
	nodeGen := tieredNodeGen(t)
	_, signalTok := wireRegistry(t, ts)

	for i := 0; i < 3; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	oldName := ts.HotShardForTest().name
	forceRotation(t, ts)
	// Add a record to the new hot shard so the feed spans cold + hot.
	if err := ts.PutNode(types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)); err != nil {
		t.Fatalf("PutNode post-rotation: %v", err)
	}
	if err := ts.Flush(); err != nil {
		t.Fatalf("Flush post-rotation: %v", err)
	}

	// Demote the retired shard to cold and close its badger handle (fully cold).
	demoteToCold(ts, oldName)
	closeEventShardStore(t, ts, oldName)

	feed, err := ts.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed over cold shard: %v", err)
	}
	if got := lsns(feed); !equalLSNs(got, []uint64{1, 2, 3, 4}) {
		t.Fatalf("cold-shard feed = %v, want [1 2 3 4] (cold log segment read read-only)", got)
	}
	// Sanity: every record is a NodePut (no corruption paging a closed shard).
	for i, rec := range feed {
		if rec.Tag != storecontract.ChangeNodePut {
			t.Errorf("record[%d] tag = %v, want NodePut", i, rec.Tag)
		}
	}
}
