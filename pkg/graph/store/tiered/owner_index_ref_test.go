package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestOwnerIndexSyncedBeforeDurableReferenceRows (UNIQUELOOK review F1): a
// claim of a reference-label value is synced before the reference shard's
// durable flush makes its row durable. The owner index runs without
// SyncWrites, so without the hook on the reference shard a power loss could
// keep the row and lose its claim: the ready index then misses the holder and
// GetOrCreateByKey makes a duplicate. Caught by the hook installed on event
// shards only.
func TestOwnerIndexSyncedBeforeDurableReferenceRows(t *testing.T) {
	t.Parallel()
	for _, syncWrites := range []bool{false, true} {
		ts, err := New(Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour,
			FlushInterval: 1<<63 - 1, SyncWrites: syncWrites})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
		if err := ts.DeclareUniqueOwnerIndex(caseTok, "key"); err != nil {
			t.Fatal(err)
		}
		gen := tieredNodeGen(t)
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		if err := n.SetProperty("key", "a"); err != nil {
			t.Fatal(err)
		}
		if err := ts.PutUniqueOwner(caseTok, "key", "s:a", n.ID()); err != nil {
			t.Fatal(err)
		}
		before := ts.ownerIdxSyncs.Load()
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		if err := ts.DurableFlush(); err != nil {
			t.Fatal(err)
		}
		if got := ts.ownerIdxSyncs.Load() - before; got != 1 {
			t.Errorf("SyncWrites=%v: %d owner-index syncs before the reference row became durable, want 1", syncWrites, got)
		}
		if ts.ownerIdx.dirty.Load() {
			t.Errorf("SyncWrites=%v: the claim is unsynced after its reference row's durable flush", syncWrites)
		}
		_ = ts.Close()
	}
}

// TestOwnerIndexDeclarationRefusedByOlderRelease (UNIQUELOOK review F2): a
// tiered store with a declared owner index carries store format marker 16 on
// its reference shard, so a release that reads markers up to 15 (and would
// write reference-label rows without claims) refuses it. Not parallel: the
// readable cap is the process's.
func TestOwnerIndexDeclarationRefusedByOlderRelease(t *testing.T) {
	dir := t.TempDir()
	ts, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
	if err := ts.DeclareUniqueOwnerIndex(caseTok, "key"); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	restore := badger.SetMaxReadableStoreFormatVersionForTest(storeutil.CompositeDiskStoreFormatVersion)
	old, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour})
	restore()
	if err == nil {
		_ = old.Close()
		t.Fatal("a release reading markers up to 15 opened a tiered store with an owner index")
	}
	again, err := New(Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("this release reopening: %v", err)
	}
	_ = again.Close()
}
