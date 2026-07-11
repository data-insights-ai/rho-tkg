package tiered

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ─── DeleteRelationshipsBatch ─────────────────────────────────────────────────
//
// These tests cover the v3.1.0 partitioning optimisation: same-shard rels
// collapse into per-shard BadgerStore.DeleteRelationshipsBatch calls; cross-shard
// rels continue down the per-ID DeleteRelationship path.

// setupBatchDelete creates a Store with Case+User as reference labels and
// returns the store plus the resolved tokens for Case (ref) and Signal (event).
func setupBatchDelete(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	return ts, caseTok, signalTok
}

func TestTieredStoreHighFrequencyIndex_InheritsOnRotate(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if !ts.hotShard.store.HasHFIndexForTest(signalTok) {
		t.Fatal("initial hot shard missing high-frequency index")
	}

	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("RotateHotShard: %v", err)
	}
	if !ts.hotShard.store.HasHFIndexForTest(signalTok) {
		t.Fatal("rotated hot shard did not inherit high-frequency index")
	}
}

func TestTieredStoreTrackedTemporalIndexes_InheritOnLazyArchiveOpen(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(userTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if ts.refArchive.Load() != nil {
		t.Fatal("test setup unexpectedly opened refArchive before lazy-open assertion")
	}

	if err := ts.EnsureRefArchiveForTest(); err != nil {
		t.Fatalf("EnsureRefArchiveForTest: %v", err)
	}
	archive := ts.refArchive.Load()
	if archive == nil {
		t.Fatal("EnsureRefArchiveForTest left refArchive nil")
	}
	if !archive.HasTemporalIndexForTest(caseTok) {
		t.Fatal("lazy-opened refArchive did not inherit temporal index")
	}
	if !archive.HasHFIndexForTest(userTok) {
		t.Fatal("lazy-opened refArchive did not inherit high-frequency index")
	}
}

func TestTieredStoreSequentialStoreWideOperationBlocksClose(t *testing.T) {
	ts := newTestTieredStore(t)

	release, err := ts.beginSequentialStoreWideOperation()
	if err != nil {
		t.Fatalf("beginSequentialStoreWideOperation: %v", err)
	}
	released := false
	defer func() {
		if !released {
			release()
		}
	}()

	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()

	select {
	case err := <-closed:
		t.Fatalf("Close returned before store-wide operation release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if ts.ClosedForTest().Load() {
		t.Fatal("Close set closed flag while store-wide operation was active")
	}

	release()
	released = true
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after store-wide operation release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after store-wide operation release")
	}
}

func TestTieredStoreTemporalIndexOpsCoverClosedColdShards(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)

	coldNames := make([]string, 0, 2)
	for range 2 {
		coldName := ts.HotShardForTest().Name()
		if err := ts.ForceRotate(); err != nil {
			t.Fatalf("ForceRotate: %v", err)
		}
		demoteToCold(ts, coldName)
		cold := ts.EventShardsForTest()[coldName]
		cold.LockShardMuForTest()
		if cold.Store() != nil {
			if err := cold.Store().Close(); err != nil {
				cold.UnlockShardMuForTest()
				t.Fatalf("close cold store %q: %v", coldName, err)
			}
			cold.SetStoreForTest(nil)
		}
		cold.UnlockShardMuForTest()
		coldNames = append(coldNames, coldName)
	}

	if err := ts.CreateTemporalIndex(signalTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	for _, name := range coldNames {
		requireColdShardTemporalIndexState(t, ts, name, signalTok, true, false)
	}
	if err := ts.DropTemporalIndex(signalTok); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	for _, name := range coldNames {
		requireColdShardTemporalIndexState(t, ts, name, signalTok, false, false)
	}

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	for _, name := range coldNames {
		requireColdShardTemporalIndexState(t, ts, name, signalTok, false, true)
	}
	if err := ts.DropHighFrequencyIndex(signalTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	for _, name := range coldNames {
		requireColdShardTemporalIndexState(t, ts, name, signalTok, false, false)
	}
}

func requireColdShardTemporalIndexState(t *testing.T, ts *Store, name string, labelToken uint16, wantTemporal, wantHFI bool) {
	t.Helper()
	cold := ts.EventShardsForTest()[name]
	if cold == nil {
		t.Fatalf("cold shard %q missing", name)
	}
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		cold.UnlockShardMuForTest()
		t.Fatalf("cold shard %q was left open before verification", name)
	}
	cold.UnlockShardMuForTest()

	store, release, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("checkout cold shard %q: %v", name, err)
	}
	if got := store.HasTemporalIndexForTest(labelToken); got != wantTemporal {
		release()
		t.Fatalf("cold shard %q temporal index = %v, want %v", name, got, wantTemporal)
	}
	if got := store.HasHFIndexForTest(labelToken); got != wantHFI {
		release()
		t.Fatalf("cold shard %q high-frequency index = %v, want %v", name, got, wantHFI)
	}
	release()

	cold.LockShardMuForTest()
	defer cold.UnlockShardMuForTest()
	if cold.Store() != nil {
		t.Fatalf("cold shard %q was left open after verification", name)
	}
}

func TestTieredStoreHighFrequencyIndex_ConflictsWithTrackedTemporalIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateHighFrequencyIndex err = %v, want ErrTemporalIndexExists", err)
	}
}

func TestTieredStoreHighFrequencyIndex_ConflictsWithUntrackedShardTemporalIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.RefShardForTest().CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex on ref shard: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateHighFrequencyIndex err = %v, want ErrTemporalIndexExists", err)
	}
	if len(ts.HFBucketsForTest()) != 0 {
		t.Fatalf("hfIdxBuckets = %#v, want empty after cross-kind conflict", ts.HFBucketsForTest())
	}
	if ts.HotShardForTest().Store().HasHFIndexForTest(caseTok) {
		t.Fatal("hot shard got high-frequency index despite ref shard temporal-index conflict")
	}
}

func TestTieredStoreTemporalIndex_ConflictsWithUntrackedShardHighFrequencyIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.RefShardForTest().CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on ref shard: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateTemporalIndex err = %v, want ErrTemporalIndexExists", err)
	}
	if len(ts.TempIdxLabelsForTest()) != 0 {
		t.Fatalf("tempIdxLabels = %#v, want empty after cross-kind conflict", ts.TempIdxLabelsForTest())
	}
	if ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard got temporal index despite ref shard high-frequency-index conflict")
	}
}

func TestTieredStoreTemporalIndex_RetriesSameKindPartialCreate(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.RefShardForTest().CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex on ref shard: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex retry: %v", err)
	}
	if !ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard missing temporal index after same-kind retry")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 1 || got[0] != caseTok {
		t.Fatalf("tempIdxLabels = %#v, want [%d]", got, caseTok)
	}
}

func TestTieredStoreTemporalIndex_CreateIdempotentAndDropMissing(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex duplicate: %v", err)
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 1 || got[0] != caseTok {
		t.Fatalf("tempIdxLabels after duplicate create = %#v, want [%d]", got, caseTok)
	}
	if !ts.RefShardForTest().HasTemporalIndexForTest(caseTok) {
		t.Fatal("reference shard missing temporal index after create")
	}
	if !ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard missing temporal index after create")
	}

	if err := ts.DropTemporalIndex(caseTok); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 0 {
		t.Fatalf("tempIdxLabels after drop = %#v, want empty", got)
	}
	if err := ts.DropTemporalIndex(caseTok); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("DropTemporalIndex missing err = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestTieredStoreTemporalIndex_DropMissingIgnoresBrokenSnapshotFile(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	ts.temporalIdxFile = t.TempDir()

	if err := ts.DropTemporalIndex(caseTok); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("DropTemporalIndex missing with broken snapshot file = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestTieredStoreTemporalIndex_CreatePersistFailureRollsBack(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupDiskBatchDelete(t)
	ts.temporalIdxFile = filepath.Join(t.TempDir(), "missing-parent", "temporal_indexes.msgpack")

	if err := ts.CreateTemporalIndex(caseTok); err == nil {
		t.Fatal("CreateTemporalIndex returned nil for persist failure")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 0 {
		t.Fatalf("tempIdxLabels after failed create = %#v, want empty", got)
	}
	if ts.RefShardForTest().HasTemporalIndexForTest(caseTok) {
		t.Fatal("reference shard kept temporal index after failed create")
	}
	if ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard kept temporal index after failed create")
	}
}

func TestTieredStoreTemporalIndex_CreateSnapshotFailureRollsBack(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	ts.temporalIdxFile = t.TempDir()

	if err := ts.CreateTemporalIndex(caseTok); err == nil {
		t.Fatal("CreateTemporalIndex returned nil for snapshot failure")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 0 {
		t.Fatalf("tempIdxLabels after failed create = %#v, want empty", got)
	}
	if ts.RefShardForTest().HasTemporalIndexForTest(caseTok) {
		t.Fatal("reference shard kept temporal index after failed create")
	}
	if ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard kept temporal index after failed create")
	}
}

func TestTieredStoreTemporalIndex_DropPersistFailureRestoresIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupDiskBatchDelete(t)
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex case: %v", err)
	}
	if err := ts.CreateTemporalIndex(signalTok); err != nil {
		t.Fatalf("CreateTemporalIndex signal: %v", err)
	}

	ts.temporalIdxFile = filepath.Join(t.TempDir(), "missing-parent", "temporal_indexes.msgpack")
	if err := ts.DropTemporalIndex(caseTok); err == nil {
		t.Fatal("DropTemporalIndex returned nil for persist failure")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 2 || got[0] != signalTok || got[1] != caseTok {
		t.Fatalf("tempIdxLabels after failed drop = %#v, want [%d %d]", got, signalTok, caseTok)
	}
	if !ts.RefShardForTest().HasTemporalIndexForTest(caseTok) {
		t.Fatal("reference shard missing temporal index after failed drop rollback")
	}
	if !ts.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("hot shard missing temporal index after failed drop rollback")
	}
}

func TestTieredStoreHighFrequencyIndex_CreateIdempotentAndDropMissing(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex duplicate: %v", err)
	}
	if got := ts.HFBucketsForTest(); len(got) != 1 || got[signalTok] != time.Hour {
		t.Fatalf("hfIdxBuckets after duplicate create = %#v, want map[%d:%s]", got, signalTok, time.Hour)
	}
	if !ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("reference shard missing high-frequency index after create")
	}
	if !ts.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("hot shard missing high-frequency index after create")
	}

	if err := ts.DropHighFrequencyIndex(signalTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if got := ts.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("hfIdxBuckets after drop = %#v, want empty", got)
	}
	if err := ts.DropHighFrequencyIndex(signalTok); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("DropHighFrequencyIndex missing err = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestTieredStoreHighFrequencyIndex_DropMissingIgnoresBrokenSnapshotFile(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	ts.temporalIdxFile = t.TempDir()

	if err := ts.DropHighFrequencyIndex(signalTok); !errors.Is(err, ErrTemporalIndexNotFound) {
		t.Fatalf("DropHighFrequencyIndex missing with broken snapshot file = %v, want ErrTemporalIndexNotFound", err)
	}
}

func TestTieredStoreHighFrequencyIndex_CreatePersistFailureRollsBack(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupDiskBatchDelete(t)
	ts.temporalIdxFile = filepath.Join(t.TempDir(), "missing-parent", "temporal_indexes.msgpack")

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err == nil {
		t.Fatal("CreateHighFrequencyIndex returned nil for persist failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("hfIdxBuckets after failed create = %#v, want empty", got)
	}
	if ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("reference shard kept high-frequency index after failed create")
	}
	if ts.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("hot shard kept high-frequency index after failed create")
	}
}

func TestTieredStoreHighFrequencyIndex_CreateSnapshotFailureRollsBack(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	ts.temporalIdxFile = t.TempDir()

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err == nil {
		t.Fatal("CreateHighFrequencyIndex returned nil for snapshot failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("hfIdxBuckets after failed create = %#v, want empty", got)
	}
	if ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("reference shard kept high-frequency index after failed create")
	}
	if ts.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("hot shard kept high-frequency index after failed create")
	}
}

func TestTieredStoreHighFrequencyIndex_DropPersistFailureRestoresIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupDiskBatchDelete(t)
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex case: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(signalTok, 2*time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex signal: %v", err)
	}

	ts.temporalIdxFile = filepath.Join(t.TempDir(), "missing-parent", "temporal_indexes.msgpack")
	if err := ts.DropHighFrequencyIndex(caseTok); err == nil {
		t.Fatal("DropHighFrequencyIndex returned nil for persist failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 2 || got[caseTok] != time.Hour || got[signalTok] != 2*time.Hour {
		t.Fatalf("hfIdxBuckets after failed drop = %#v, want restored case and signal buckets", got)
	}
	if !ts.RefShardForTest().HasHFIndexForTest(caseTok) {
		t.Fatal("reference shard missing high-frequency index after failed drop rollback")
	}
	if !ts.HotShardForTest().Store().HasHFIndexForTest(caseTok) {
		t.Fatal("hot shard missing high-frequency index after failed drop rollback")
	}
}

func TestTieredStoreHighFrequencyIndex_DropSnapshotFailureLeavesIndex(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	ts.temporalIdxFile = t.TempDir()
	if err := ts.DropHighFrequencyIndex(signalTok); err == nil {
		t.Fatal("DropHighFrequencyIndex returned nil for snapshot failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 1 || got[signalTok] != time.Hour {
		t.Fatalf("hfIdxBuckets after failed drop = %#v, want signal bucket", got)
	}
	if !ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("reference shard missing high-frequency index after failed drop")
	}
	if !ts.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("hot shard missing high-frequency index after failed drop")
	}
}

func TestTieredStoreTemporalIndex_RollsBackEarlierShardOnBackfillFailure(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hot := ts.HotShardForTest().Store()
	if err := hot.Flush(); err != nil {
		t.Fatalf("Flush hot shard: %v", err)
	}
	hot.NodeCacheForTest().ResetForTest()
	if err := hot.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt-node-wire"))
	}); err != nil {
		t.Fatalf("corrupt hot shard node row: %v", err)
	}

	err := ts.CreateTemporalIndex(signalTok)
	if err == nil {
		t.Fatal("CreateTemporalIndex returned nil for corrupted later shard")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("CreateTemporalIndex error = %v, want corruption/operational error", err)
	}
	if ts.RefShardForTest().HasTemporalIndexForTest(signalTok) {
		t.Fatal("ref shard kept temporal index after later shard backfill failure")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 0 {
		t.Fatalf("tempIdxLabels = %#v, want empty after failed create", got)
	}
}

func TestTieredStoreHighFrequencyIndex_RollsBackEarlierShardOnBackfillFailure(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	hot := ts.HotShardForTest().Store()
	if err := hot.Flush(); err != nil {
		t.Fatalf("Flush hot shard: %v", err)
	}
	hot.NodeCacheForTest().ResetForTest()
	if err := hot.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(n.ID().SnowflakeID()), []byte("corrupt-node-wire"))
	}); err != nil {
		t.Fatalf("corrupt hot shard node row: %v", err)
	}

	err := ts.CreateHighFrequencyIndex(signalTok, time.Hour)
	if err == nil {
		t.Fatal("CreateHighFrequencyIndex returned nil for corrupted later shard")
	}
	if errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("CreateHighFrequencyIndex error = %v, want corruption/operational error", err)
	}
	if ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("ref shard kept high-frequency index after later shard backfill failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("hfIdxBuckets = %#v, want empty after failed create", got)
	}
}

func TestTieredStoreTemporalIndexDrop_RollsBackEarlierShardOnLaterFailure(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.CreateTemporalIndex(signalTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts.HotShardForTest().Store().Close(); err != nil {
		t.Fatalf("close hot shard: %v", err)
	}

	err := ts.DropTemporalIndex(signalTok)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DropTemporalIndex error = %v, want ErrStoreClosed", err)
	}
	if !ts.RefShardForTest().HasTemporalIndexForTest(signalTok) {
		t.Fatal("ref shard temporal index was not restored after later shard drop failure")
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 1 || got[0] != signalTok {
		t.Fatalf("tempIdxLabels = %#v, want [%d] after failed drop", got, signalTok)
	}
}

func TestTieredStoreHighFrequencyIndex_RetriesSameKindPartialCreate(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.RefShardForTest().CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on ref shard: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex retry: %v", err)
	}
	if !ts.HotShardForTest().Store().HasHFIndexForTest(caseTok) {
		t.Fatal("hot shard missing high-frequency index after same-kind retry")
	}
	if got := ts.HFBucketsForTest(); len(got) != 1 || got[caseTok] != time.Hour {
		t.Fatalf("hfIdxBuckets = %#v, want %d:%v", got, caseTok, time.Hour)
	}
}

func TestTieredStoreHighFrequencyIndexRejectsPartialRetryWithDifferentBucket(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.RefShardForTest().CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on ref shard: %v", err)
	}
	err := ts.CreateHighFrequencyIndex(caseTok, time.Minute)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateHighFrequencyIndex retry with different bucket = %v, want ErrTemporalIndexExists", err)
	}
	if ts.HotShardForTest().Store().HasHFIndexForTest(caseTok) {
		t.Fatal("hot shard got high-frequency index despite different-bucket retry")
	}
	if got := ts.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("hfIdxBuckets = %#v, want empty after rejected different-bucket retry", got)
	}
	_, hasHFI, bucketSize, err := ts.RefShardForTest().TemporalIndexState(caseTok)
	if err != nil {
		t.Fatalf("TemporalIndexState ref shard: %v", err)
	}
	if !hasHFI || bucketSize != time.Hour {
		t.Fatalf("ref shard HFI state = has:%v bucket:%v, want has:true bucket:%v", hasHFI, bucketSize, time.Hour)
	}
}

func TestTieredStoreHighFrequencyIndexRejectsTrackedDifferentBucket(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	err := ts.CreateHighFrequencyIndex(caseTok, time.Minute)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("CreateHighFrequencyIndex duplicate with different bucket = %v, want ErrTemporalIndexExists", err)
	}
	if got := ts.HFBucketsForTest(); len(got) != 1 || got[caseTok] != time.Hour {
		t.Fatalf("hfIdxBuckets = %#v, want %d:%v", got, caseTok, time.Hour)
	}
	_, hasHFI, bucketSize, err := ts.HotShardForTest().Store().TemporalIndexState(caseTok)
	if err != nil {
		t.Fatalf("TemporalIndexState hot shard: %v", err)
	}
	if !hasHFI || bucketSize != time.Hour {
		t.Fatalf("hot shard HFI state = has:%v bucket:%v, want has:true bucket:%v", hasHFI, bucketSize, time.Hour)
	}
}

func TestTieredStoreApplyTrackedIndexesRejectsCrossKindShardState(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	target := ts.HotShardForTest().Store()

	if err := target.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on target shard: %v", err)
	}
	ts.tempIdxMu.Lock()
	ts.tempIdxLabels = append(ts.tempIdxLabels, caseTok)
	ts.tempIdxMu.Unlock()

	err := ts.applyTrackedTemporalIndexes(target)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("applyTrackedTemporalIndexes cross-kind state = %v, want ErrTemporalIndexExists", err)
	}
}

func TestTieredStoreApplyTrackedIndexesRejectsDifferentHFIBucket(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	target := ts.HotShardForTest().Store()

	if err := target.CreateHighFrequencyIndex(caseTok, time.Minute); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on target shard: %v", err)
	}
	ts.tempIdxMu.Lock()
	ts.hfIdxBuckets[caseTok] = time.Hour
	ts.tempIdxMu.Unlock()

	err := ts.applyTrackedTemporalIndexes(target)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("applyTrackedTemporalIndexes different HFI bucket = %v, want ErrTemporalIndexExists", err)
	}
	_, hasHFI, bucketSize, err := target.TemporalIndexState(caseTok)
	if err != nil {
		t.Fatalf("TemporalIndexState target shard: %v", err)
	}
	if !hasHFI || bucketSize != time.Minute {
		t.Fatalf("target shard HFI state = has:%v bucket:%v, want has:true bucket:%v", hasHFI, bucketSize, time.Minute)
	}
}

func TestTieredStoreApplyTrackedIndexesRollsBackEarlierCreatesOnLaterFailure(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	target := ts.HotShardForTest().Store()

	if err := target.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on target shard: %v", err)
	}
	ts.tempIdxMu.Lock()
	ts.tempIdxLabels = append(ts.tempIdxLabels, caseTok, signalTok)
	ts.tempIdxMu.Unlock()

	err := ts.applyTrackedTemporalIndexes(target)
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("applyTrackedTemporalIndexes = %v, want ErrTemporalIndexExists", err)
	}
	if target.HasTemporalIndexForTest(caseTok) {
		t.Fatal("applyTrackedTemporalIndexes left earlier-created temporal index after later failure")
	}
	if !target.HasHFIndexForTest(signalTok) {
		t.Fatal("applyTrackedTemporalIndexes removed pre-existing conflicting HFI")
	}
}

func TestRollbackAppliedTrackedTemporalIndexesDropsTemporalAndHighFrequencyCreates(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	target := ts.HotShardForTest().Store()

	if err := target.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := target.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}

	if err := rollbackAppliedTrackedTemporalIndexes(target, []uint16{caseTok}, []uint16{signalTok}); err != nil {
		t.Fatalf("rollbackAppliedTrackedTemporalIndexes: %v", err)
	}
	if target.HasTemporalIndexForTest(caseTok) {
		t.Fatal("rollbackAppliedTrackedTemporalIndexes left temporal index behind")
	}
	if target.HasHFIndexForTest(signalTok) {
		t.Fatal("rollbackAppliedTrackedTemporalIndexes left high-frequency index behind")
	}

	if err := rollbackAppliedTrackedTemporalIndexes(target, []uint16{caseTok}, []uint16{signalTok}); err != nil {
		t.Fatalf("rollbackAppliedTrackedTemporalIndexes missing indexes: %v", err)
	}
}

func TestRollbackAppliedTrackedTemporalIndexesReportsDropFailures(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	target := ts.HotShardForTest().Store()

	if err := target.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := target.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("Close target shard: %v", err)
	}

	err := rollbackAppliedTrackedTemporalIndexes(target, []uint16{caseTok}, []uint16{signalTok})
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("rollbackAppliedTrackedTemporalIndexes closed shard = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreLoadTemporalIndexDefsRollsBackEarlierShardCreatesOnLaterFailure(t *testing.T) {
	t.Parallel()

	refStore, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore ref: %v", err)
	}
	t.Cleanup(func() { _ = refStore.Close() })
	eventStore, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore event: %v", err)
	}
	t.Cleanup(func() { _ = eventStore.Close() })

	caseTok := uint16(1)
	signalTok := uint16(2)
	if err := eventStore.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex on event shard: %v", err)
	}

	temporalIdxFile := filepath.Join(t.TempDir(), "temporal_indexes.msgpack")
	if err := saveTemporalIndexFile(temporalIdxFile, temporalIndexFileData{
		TemporalLabels: []uint16{caseTok, signalTok},
	}); err != nil {
		t.Fatalf("saveTemporalIndexFile: %v", err)
	}

	ts := &Store{
		refShard:        refStore,
		eventShards:     map[string]*EventShard{"event": {store: eventStore}},
		temporalIdxFile: temporalIdxFile,
		hfIdxBuckets:    make(map[uint16]time.Duration),
	}

	err = ts.loadTemporalIndexDefs()
	if !errors.Is(err, ErrTemporalIndexExists) {
		t.Fatalf("loadTemporalIndexDefs = %v, want ErrTemporalIndexExists", err)
	}
	if refStore.HasTemporalIndexForTest(caseTok) {
		t.Fatal("loadTemporalIndexDefs left temporal index on earlier ref shard after later shard failure")
	}
	if refStore.HasTemporalIndexForTest(signalTok) {
		t.Fatal("loadTemporalIndexDefs left second temporal index on earlier ref shard after later shard failure")
	}
	if eventStore.HasTemporalIndexForTest(caseTok) {
		t.Fatal("loadTemporalIndexDefs left same-shard temporal index after later conflict")
	}
	if !eventStore.HasHFIndexForTest(signalTok) {
		t.Fatal("loadTemporalIndexDefs removed pre-existing conflicting event HFI")
	}
	if len(ts.TempIdxLabelsForTest()) != 0 {
		t.Fatalf("tempIdxLabels after failed load = %#v, want empty", ts.TempIdxLabelsForTest())
	}
	if len(ts.HFBucketsForTest()) != 0 {
		t.Fatalf("hfIdxBuckets after failed load = %#v, want empty", ts.HFBucketsForTest())
	}
}

func TestTieredStoreHighFrequencyIndexDrop_RollsBackEarlierShardOnLaterFailure(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts.HotShardForTest().Store().Close(); err != nil {
		t.Fatalf("close hot shard: %v", err)
	}

	err := ts.DropHighFrequencyIndex(signalTok)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DropHighFrequencyIndex error = %v, want ErrStoreClosed", err)
	}
	if !ts.RefShardForTest().HasHFIndexForTest(signalTok) {
		t.Fatal("ref shard HFI was not restored after later shard drop failure")
	}
	if got := ts.HFBucketsForTest(); len(got) != 1 || got[signalTok] != time.Hour {
		t.Fatalf("hfIdxBuckets = %#v, want %d:%v after failed drop", got, signalTok, time.Hour)
	}
}

func TestTieredStoreHighFrequencyIndexRejectsInvalidBucket(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	if err := ts.CreateHighFrequencyIndex(caseTok, 0); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(0) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, -time.Second); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(-1s) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Nanosecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1ns) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, 1500*time.Microsecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1.5ms) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex valid bucket after rejects: %v", err)
	}
}

func TestTieredStoreTrackedTemporalIndexes_SurviveRestart(t *testing.T) {
	dir := t.TempDir()
	caseTok := uint16(1)
	signalTok := uint16(3)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	if err := ts1.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts1.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer func() { _ = ts2.Close() }()

	if got := ts2.TempIdxLabelsForTest(); len(got) != 1 || got[0] != caseTok {
		t.Fatalf("TempIdxLabels after restart = %#v, want [%d]", got, caseTok)
	}
	if got := ts2.HFBucketsForTest(); got[signalTok] != time.Hour {
		t.Fatalf("HFBuckets after restart = %#v, want %d:%v", got, signalTok, time.Hour)
	}

	forceRotation(t, ts2)
	if !ts2.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("rotated hot shard after restart did not inherit temporal index")
	}
	if !ts2.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("rotated hot shard after restart did not inherit high-frequency index")
	}
}

func TestTieredStoreTrackedHighFrequencyDrop_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	signalTok := uint16(3)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	if err := ts1.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts1.DropHighFrequencyIndex(signalTok); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer func() { _ = ts2.Close() }()
	if got := ts2.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("HFBuckets after dropped index restart = %#v, want empty", got)
	}
	forceRotation(t, ts2)
	if ts2.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("rotated hot shard inherited dropped high-frequency index")
	}
}

func TestTieredStoreTrackedTemporalIndexes_ClearSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	caseTok := uint16(1)
	signalTok := uint16(3)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	if err := ts1.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ts1.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ts1.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer func() { _ = ts2.Close() }()
	if got := ts2.TempIdxLabelsForTest(); len(got) != 0 {
		t.Fatalf("TempIdxLabels after Clear+restart = %#v, want empty", got)
	}
	if got := ts2.HFBucketsForTest(); len(got) != 0 {
		t.Fatalf("HFBuckets after Clear+restart = %#v, want empty", got)
	}
	forceRotation(t, ts2)
	if ts2.HotShardForTest().Store().HasTemporalIndexForTest(caseTok) {
		t.Fatal("rotated hot shard inherited cleared temporal index")
	}
	if ts2.HotShardForTest().Store().HasHFIndexForTest(signalTok) {
		t.Fatal("rotated hot shard inherited cleared high-frequency index")
	}
}

func TestTieredStoreIndexAPIsRejectZeroLabelToken(t *testing.T) {
	t.Parallel()
	ts, _, _ := setupBatchDelete(t)

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return ts.CreatePropertyIndex(0, "name") }},
		{name: "drop property", run: func() error { return ts.DropPropertyIndex(0, "name") }},
		{name: "create temporal", run: func() error { return ts.CreateTemporalIndex(0) }},
		{name: "drop temporal", run: func() error { return ts.DropTemporalIndex(0) }},
		{name: "create high frequency", run: func() error { return ts.CreateHighFrequencyIndex(0, time.Hour) }},
		{name: "drop high frequency", run: func() error { return ts.DropHighFrequencyIndex(0) }},
		{name: "create vector", run: func() error { return ts.CreateVectorIndex(0, "vec", 2, DistanceCosine) }},
		{name: "drop vector", run: func() error { return ts.DropVectorIndex(0, "vec") }},
		{name: "search vector", run: func() error {
			_, err := ts.SearchNearestNodes(0, "vec", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := ts.SearchNearestFiltered(0, "vec", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrInvalidStoreMutation) {
				t.Fatalf("%s err = %v, want ErrInvalidStoreMutation", tc.name, err)
			}
		})
	}
}

func TestTieredStoreIndexAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(ts *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "CreatePropertyIndex", run: func() error { return ts.CreatePropertyIndex(0, "") }},
			{name: "DropPropertyIndex", run: func() error { return ts.DropPropertyIndex(0, "") }},
			{name: "CreateTemporalIndex", run: func() error { return ts.CreateTemporalIndex(0) }},
			{name: "DropTemporalIndex", run: func() error { return ts.DropTemporalIndex(0) }},
			{name: "CreateHighFrequencyIndex", run: func() error { return ts.CreateHighFrequencyIndex(0, 0) }},
			{name: "DropHighFrequencyIndex", run: func() error { return ts.DropHighFrequencyIndex(0) }},
			{name: "CreateVectorIndex", run: func() error { return ts.CreateVectorIndex(0, "", 0, DistanceMetric(99)) }},
			{name: "DropVectorIndex", run: func() error { return ts.DropVectorIndex(0, "") }},
			{name: "SearchNearestNodes", run: func() error {
				_, err := ts.SearchNearestNodes(0, "", nil, -1, QueryOpts{Limit: -1})
				return err
			}},
			{name: "SearchNearestFiltered", run: func() error {
				_, err := ts.SearchNearestFiltered(0, "", nil, -1, nil)
				return err
			}},
			{name: "NodesByLabelAndProperty", run: func() error {
				_, err := ts.NodesByLabelAndProperty(0, "", nil, QueryOpts{Limit: -1})
				return err
			}},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	ts, _, _ := setupBatchDelete(t)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(ts) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestTieredStoreWriteAPIsCheckLifecycleBeforeValidation(t *testing.T) {
	t.Parallel()

	checks := func(ts *Store) []struct {
		name string
		run  func() error
	} {
		return []struct {
			name string
			run  func() error
		}{
			{name: "PutNode", run: func() error { return ts.PutNode(nil) }},
			{name: "PutNodeGeneratedID", run: func() error { return ts.PutNodeGeneratedID(nil, generatedcreate.FreshGraphID) }},
			{name: "ReplaceNode", run: func() error { return ts.ReplaceNode(nil) }},
			{name: "DeleteNode", run: func() error { return ts.DeleteNode(0) }},
			{name: "RemoveNodeLabelToken", run: func() error { return ts.RemoveNodeLabelToken(0, 0, nil) }},
			{name: "RemoveNodeLabelTokenWithHistory", run: func() error {
				return ts.RemoveNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "AddNodeLabelToken", run: func() error { return ts.AddNodeLabelToken(0, 0, nil) }},
			{name: "AddNodeLabelTokenWithHistory", run: func() error {
				return ts.AddNodeLabelTokenWithHistory(0, 0, nil, 0, nil)
			}},
			{name: "ReplaceNodeWithHistory", run: func() error { return ts.ReplaceNodeWithHistory(nil, 0, nil) }},
			{name: "PutNodeVersion", run: func() error { return ts.PutNodeVersion(0, 0, nil) }},
			{name: "DeleteNodeCascade", run: func() error { return ts.DeleteNodeCascade(0) }},
			{name: "DeleteNodeWithHistory", run: func() error { return ts.DeleteNodeWithHistory(0, 0, nil, nil) }},
			{name: "PutRelationship", run: func() error { return ts.PutRelationship(nil) }},
			{name: "PutRelationshipGeneratedID", run: func() error {
				return ts.PutRelationshipGeneratedID(nil, generatedcreate.FreshGraphID)
			}},
			{name: "PutRelationshipGeneratedIDWithEndpointHashes", run: func() error {
				_, _, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(nil, generatedcreate.FreshGraphID)
				return err
			}},
			{name: "ReplaceRelationship", run: func() error { return ts.ReplaceRelationship(nil) }},
			{name: "DeleteRelationship", run: func() error { return ts.DeleteRelationship(0) }},
			{name: "ReplaceRelWithHistory", run: func() error { return ts.ReplaceRelWithHistory(nil, 0, nil) }},
			{name: "PutRelVersion", run: func() error { return ts.PutRelVersion(0, 0, nil) }},
			{name: "DeleteRelWithHistory", run: func() error { return ts.DeleteRelWithHistory(0, 0, nil) }},
		}
	}

	var nilStore *Store
	for _, check := range checks(nilStore) {
		if err := check.run(); !errors.Is(err, ErrNilStore) {
			t.Fatalf("nil %s error = %v, want ErrNilStore", check.name, err)
		}
	}

	ts, _, _ := setupBatchDelete(t)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, check := range checks(ts) {
		if err := check.run(); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("closed %s error = %v, want ErrStoreClosed", check.name, err)
		}
	}
}

func TestTieredStoreIndexAPIsRejectReservedPropertyKey(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return ts.CreatePropertyIndex(caseTok, "tkg_hash") }},
		{name: "drop property", run: func() error { return ts.DropPropertyIndex(caseTok, "tkg_hash") }},
		{name: "query property", run: func() error {
			_, err := ts.NodesByLabelAndProperty(caseTok, "tkg_hash", "x", QueryOpts{})
			return err
		}},
		{name: "create vector", run: func() error { return ts.CreateVectorIndex(caseTok, "tkg_hash", 2, DistanceCosine) }},
		{name: "drop vector", run: func() error { return ts.DropVectorIndex(caseTok, "tkg_hash") }},
		{name: "search vector", run: func() error {
			_, err := ts.SearchNearestNodes(caseTok, "tkg_hash", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := ts.SearchNearestFiltered(caseTok, "tkg_hash", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, types.ErrReservedPrefix) {
				t.Fatalf("%s err = %v, want ErrReservedPrefix", tc.name, err)
			}
		})
	}
}

func TestTieredStoreIndexAPIsReturnStoreClosedAfterClose(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
		t.Fatalf("CreatePropertyIndex setup: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex setup: %v", err)
	}
	if err := ts.CreateHighFrequencyIndex(signalTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex setup: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex setup: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "create property", run: func() error { return ts.CreatePropertyIndex(caseTok, "status") }},
		{name: "drop property", run: func() error { return ts.DropPropertyIndex(caseTok, "status") }},
		{name: "create temporal", run: func() error { return ts.CreateTemporalIndex(caseTok) }},
		{name: "drop temporal", run: func() error { return ts.DropTemporalIndex(caseTok) }},
		{name: "create high frequency", run: func() error { return ts.CreateHighFrequencyIndex(signalTok, time.Hour) }},
		{name: "drop high frequency", run: func() error { return ts.DropHighFrequencyIndex(signalTok) }},
		{name: "create vector", run: func() error { return ts.CreateVectorIndex(caseTok, "vec", 2, DistanceCosine) }},
		{name: "drop vector", run: func() error { return ts.DropVectorIndex(caseTok, "vec") }},
		{name: "search vector", run: func() error {
			_, err := ts.SearchNearestNodes(caseTok, "vec", []float32{1, 0}, 1, QueryOpts{})
			return err
		}},
		{name: "search filtered vector", run: func() error {
			_, err := ts.SearchNearestFiltered(caseTok, "vec", []float32{1, 0}, 1, nil)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("%s err = %v, want ErrStoreClosed", tc.name, err)
			}
		})
	}
}

func TestTieredStoreVectorIndexCreateSkipsIneligibleNodesAndSentinels(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	key := "vec"

	indexed := types.NewNode(types.NodeID(snowflake.ID(201)), caseTok, nil)
	if err := indexed.SetProperty(key, []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty indexed: %v", err)
	}
	missingProperty := types.NewNode(types.NodeID(snowflake.ID(202)), caseTok, nil)
	nonVector := types.NewNode(types.NodeID(snowflake.ID(203)), caseTok, nil)
	if err := nonVector.SetProperty(key, "not-a-vector"); err != nil {
		t.Fatalf("SetProperty nonVector: %v", err)
	}
	wrongLabel := types.NewNode(types.NodeID(snowflake.ID(204)), signalTok, nil)
	if err := wrongLabel.SetProperty(key, []float32{0, 1}); err != nil {
		t.Fatalf("SetProperty wrongLabel: %v", err)
	}
	for _, n := range []*types.Node{indexed, missingProperty, nonVector, wrongLabel} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); !errors.Is(err, ErrVectorIndexExists) {
		t.Fatalf("CreateVectorIndex duplicate = %v, want ErrVectorIndexExists", err)
	}
	got, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0}, 10, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != indexed.ID() {
		t.Fatalf("SearchNearestNodes IDs = %v, want only %d", tieredNodeIDsForTest(got), indexed.ID())
	}

	if err := ts.DropVectorIndex(caseTok, key); err != nil {
		t.Fatalf("DropVectorIndex: %v", err)
	}
	if _, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0}, 1, QueryOpts{}); !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("SearchNearestNodes after DropVectorIndex = %v, want ErrVectorIndexNotFound", err)
	}
	if err := ts.DropVectorIndex(caseTok, key); !errors.Is(err, ErrVectorIndexNotFound) {
		t.Fatalf("DropVectorIndex missing = %v, want ErrVectorIndexNotFound", err)
	}
}

func TestTieredStore_VectorIndex_CreateWithOptionsAppliesTuning(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"

	n1 := types.NewNode(types.NodeID(snowflake.ID(201)), caseTok, nil)
	if err := n1.SetProperty(key, []float32{1, 0, 0}); err != nil {
		t.Fatalf("SetProperty n1: %v", err)
	}
	if err := ts.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	n2 := types.NewNode(types.NodeID(snowflake.ID(202)), caseTok, nil)
	if err := n2.SetProperty(key, []float32{0, 1, 0}); err != nil {
		t.Fatalf("SetProperty n2: %v", err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	opts := storepkg.VectorIndexOptions{UseBruteForce: true, M: 8, EfConstruction: 50, EfSearch: 10}
	if err := ts.CreateVectorIndexWithOptions(caseTok, key, 3, DistanceCosine, opts); err != nil {
		t.Fatalf("CreateVectorIndexWithOptions: %v", err)
	}

	got, ok := ts.VectorIndexOptionsForTest(caseTok, key)
	if !ok {
		t.Fatal("VectorIndexOptionsForTest: index not found")
	}
	if got != opts {
		t.Fatalf("VectorIndexOptionsForTest = %+v, want %+v", got, opts)
	}

	results, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(results) != 1 || results[0].ID() != n1.ID() {
		t.Fatalf("SearchNearestNodes (brute force tuning applied) = %#v, want n1", results)
	}
}

func TestTieredStore_VectorIndex_CreateWithOptionsZeroValueMatchesPlainCreate(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"
	if err := ts.CreateVectorIndex(caseTok, key, 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	got, ok := ts.VectorIndexOptionsForTest(caseTok, key)
	if !ok {
		t.Fatal("VectorIndexOptionsForTest: index not found")
	}
	if got != (storepkg.VectorIndexOptions{}) {
		t.Fatalf("VectorIndexOptionsForTest after plain CreateVectorIndex = %+v, want zero value", got)
	}
}

func TestTieredStoreDropVectorIndexSnapshotErrorLeavesIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(205)), caseTok, nil)
	if err := n.SetProperty(key, []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ts.vectorIdxFile = t.TempDir()
	if err := ts.DropVectorIndex(caseTok, key); err == nil {
		t.Fatal("DropVectorIndex returned nil for unreadable snapshot path")
	}
	got, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after failed DropVectorIndex: %v", err)
	}
	if len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("SearchNearestNodes IDs after failed DropVectorIndex = %v, want %d", tieredNodeIDsForTest(got), n.ID())
	}
}

func TestTieredStoreDropVectorIndexPersistFailureRestoresIndex(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupDiskBatchDelete(t)
	key := "vec"

	n := types.NewNode(types.NodeID(snowflake.ID(206)), caseTok, nil)
	if err := n.SetProperty(key, []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex primary: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "other_vec", 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex secondary: %v", err)
	}

	ts.vectorIdxFile = filepath.Join(t.TempDir(), "missing-parent", "vector_indexes.msgpack")
	if err := ts.DropVectorIndex(caseTok, key); err == nil {
		t.Fatal("DropVectorIndex returned nil for persist failure")
	}
	got, err := ts.SearchNearestNodes(caseTok, key, []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes after failed DropVectorIndex: %v", err)
	}
	if len(got) != 1 || got[0].ID() != n.ID() {
		t.Fatalf("SearchNearestNodes IDs after failed DropVectorIndex = %v, want %d", tieredNodeIDsForTest(got), n.ID())
	}
}

func TestTieredStoreSearchNearestNodesAppliesCursorPagination(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"
	nodes := []*types.Node{
		types.NewNode(types.NodeID(snowflake.ID(101)), caseTok, nil),
		types.NewNode(types.NodeID(snowflake.ID(102)), caseTok, nil),
		types.NewNode(types.NodeID(snowflake.ID(103)), caseTok, nil),
	}
	vectors := [][]float32{{0, 0}, {1, 0}, {2, 0}}
	for i, n := range nodes {
		if err := n.SetProperty(key, vectors[i]); err != nil {
			t.Fatalf("SetProperty[%d]: %v", i, err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	limited, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0}, 3, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("SearchNearestNodes limit: %v", err)
	}
	if len(limited) != 2 || limited[0].ID() != nodes[0].ID() || limited[1].ID() != nodes[1].ID() {
		t.Fatalf("limited result IDs = %v, want first two distance-ordered nodes", tieredNodeIDsForTest(limited))
	}

	afterFirst, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(nodes[0].ID().SnowflakeID()),
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes after+limit: %v", err)
	}
	if len(afterFirst) != 1 || afterFirst[0].ID() != nodes[1].ID() {
		t.Fatalf("after+limit result IDs = %v, want second node", tieredNodeIDsForTest(afterFirst))
	}

	missingCursor, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0}, 3, QueryOpts{
		After: types.EntityID(snowflake.ID(999)),
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes missing cursor: %v", err)
	}
	if len(missingCursor) != 0 {
		t.Fatalf("missing cursor result IDs = %v, want none", tieredNodeIDsForTest(missingCursor))
	}
}

func TestTieredStoreSearchNearestNodesAppliesTemporalFilterBeforeHeap(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"
	tooNew := types.NewNode(types.NodeID(snowflake.ID(101)), caseTok, nil)
	tooNew.SetTemporal(&types.TemporalMetadata{ValidFrom: 100})
	eligible := types.NewNode(types.NodeID(snowflake.ID(102)), caseTok, nil)
	eligible.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})

	for i, n := range []*types.Node{tooNew, eligible} {
		vec := []float32{float32(i * 10), 0}
		if err := n.SetProperty(key, vec); err != nil {
			t.Fatalf("SetProperty[%d]: %v", i, err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	got, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0}, 1, QueryOpts{ValidAt: 50})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != eligible.ID() {
		t.Fatalf("temporal result IDs = %v, want eligible farther node %d", tieredNodeIDsForTest(got), eligible.ID())
	}
}

func TestTieredStoreSearchNearestNodesDepthPrecedesTemporalRowReads(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)
	key := "vec"

	archived := types.NewNode(types.NodeID(snowflake.ID(1301)), caseTok, nil)
	archived.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := archived.SetProperty(key, []float32{0, 0}); err != nil {
		t.Fatalf("SetProperty archived: %v", err)
	}
	live := types.NewNode(types.NodeID(snowflake.ID(1302)), caseTok, nil)
	live.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := live.SetProperty(key, []float32{10, 0}); err != nil {
		t.Fatalf("SetProperty live: %v", err)
	}
	for _, n := range []*types.Node{archived, live} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, key, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("archive shard is nil after ArchiveNode")
	}
	if err := archive.Flush(); err != nil {
		t.Fatalf("archive Flush: %v", err)
	}
	archive.NodeCacheForTest().ResetForTest()
	if err := archive.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(storeutil.NodeKey(archived.ID().SnowflakeID()), []byte("corrupt-archived-node-wire"))
	}); err != nil {
		t.Fatalf("corrupt archive node row: %v", err)
	}

	got, err := ts.SearchNearestNodes(caseTok, key, []float32{0, 0}, 1, QueryOpts{
		Depth:   DepthHot,
		ValidAt: 5,
	})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != live.ID() {
		t.Fatalf("SearchNearestNodes IDs = %v, want live node %d", tieredNodeIDsForTest(got), live.ID())
	}
}

func TestTieredStoreSearchNearestNodesTemporalFilterSkipsStaleCurrentRowBeforeHeap(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	keyName := "vec"
	stale := types.NewNode(types.NodeID(snowflake.ID(1311)), signalTok, nil)
	stale.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := stale.SetProperty(keyName, []float32{0, 0}); err != nil {
		t.Fatalf("SetProperty stale: %v", err)
	}
	live := types.NewNode(types.NodeID(snowflake.ID(1312)), caseTok, nil)
	live.SetTemporal(&types.TemporalMetadata{ValidFrom: 1})
	if err := live.SetProperty(keyName, []float32{10, 0}); err != nil {
		t.Fatalf("SetProperty live: %v", err)
	}
	for _, n := range []*types.Node{stale, live} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.CreateVectorIndex(caseTok, keyName, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: caseTok, PropertyKey: keyName}
	ts.vectorIdxMu.Lock()
	if err := ts.vectorIndexes[key].Add(stale.ID().SnowflakeID(), []float32{0, 0}); err != nil {
		ts.vectorIdxMu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ts.vectorIdxMu.Unlock()

	got, err := ts.SearchNearestNodes(caseTok, keyName, []float32{0, 0}, 1, QueryOpts{ValidAt: 5})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 1 || got[0].ID() != live.ID() {
		t.Fatalf("SearchNearestNodes IDs = %v, want live node %d", tieredNodeIDsForTest(got), live.ID())
	}
}

func TestTieredStoreSearchNearestFilteredSkipsStaleCurrentRowBeforeHeap(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	keyName := "vec"
	stale := types.NewNode(types.NodeID(snowflake.ID(1321)), signalTok, nil)
	if err := stale.SetProperty(keyName, []float32{0, 0}); err != nil {
		t.Fatalf("SetProperty stale: %v", err)
	}
	live := types.NewNode(types.NodeID(snowflake.ID(1322)), caseTok, nil)
	if err := live.SetProperty(keyName, []float32{10, 0}); err != nil {
		t.Fatalf("SetProperty live: %v", err)
	}
	for _, n := range []*types.Node{stale, live} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	if err := ts.CreateVectorIndex(caseTok, keyName, 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: caseTok, PropertyKey: keyName}
	ts.vectorIdxMu.Lock()
	if err := ts.vectorIndexes[key].Add(stale.ID().SnowflakeID(), []float32{0, 0}); err != nil {
		ts.vectorIdxMu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ts.vectorIdxMu.Unlock()

	filterCalls := make(map[snowflake.ID]int)
	ids, err := ts.SearchNearestFiltered(caseTok, keyName, []float32{0, 0}, 1, func(id snowflake.ID) bool {
		filterCalls[id]++
		return true
	})
	if err != nil {
		t.Fatalf("SearchNearestFiltered: %v", err)
	}
	if len(ids) != 1 || ids[0] != live.ID().SnowflakeID() {
		t.Fatalf("SearchNearestFiltered IDs = %v, want live node %d", ids, live.ID())
	}
	if filterCalls[stale.ID().SnowflakeID()] != 0 {
		t.Fatalf("SearchNearestFiltered called external filter for stale ID %d", stale.ID())
	}
	if filterCalls[live.ID().SnowflakeID()] != 1 {
		t.Fatalf("SearchNearestFiltered live filter calls = %d, want 1", filterCalls[live.ID().SnowflakeID()])
	}
}

func TestTieredStoreSearchNearestNodesSkipsStaleCurrentRowShape(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	keyName := "vec"
	node := types.NewNode(types.NodeID(snowflake.ID(104)), caseTok, nil)
	if err := node.SetProperty(keyName, []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty node: %v", err)
	}
	if err := ts.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(signalTok, keyName, 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	key := indexpkg.VectorIndexKey{LabelToken: signalTok, PropertyKey: keyName}
	ts.vectorIdxMu.Lock()
	if err := ts.vectorIndexes[key].Add(node.ID().SnowflakeID(), []float32{1, 0}); err != nil {
		ts.vectorIdxMu.Unlock()
		t.Fatalf("seed stale vector entry: %v", err)
	}
	ts.vectorIdxMu.Unlock()

	got, err := ts.SearchNearestNodes(signalTok, keyName, []float32{1, 0}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("SearchNearestNodes returned stale current-row shape IDs = %v, want none", tieredNodeIDsForTest(got))
	}
}

func tieredNodeIDsForTest(nodes []*types.Node) []types.NodeID {
	ids := make([]types.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID()
	}
	return ids
}

func TestTieredStoreSearchNearestFilteredReturnsClosedWhenFilterClosesStore(t *testing.T) {
	t.Parallel()
	ts, caseTok, _ := setupBatchDelete(t)

	n := types.NewNode(types.NodeID(1), caseTok, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty vec: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ids, err := ts.SearchNearestFiltered(caseTok, "vec", []float32{1, 0}, 1, func(snowflake.ID) bool {
		if err := ts.Close(); err != nil {
			t.Fatalf("Close from filter: %v", err)
		}
		return true
	})
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("SearchNearestFiltered error = %v, want ErrStoreClosed", err)
	}
	if ids != nil {
		t.Fatalf("SearchNearestFiltered ids = %v, want nil", ids)
	}
}

func TestTieredStoreDropPropertyIndexRejectsEventLabel(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.DropPropertyIndex(signalTok, "status"); !errors.Is(err, ErrEventPropertyIndex) {
		t.Fatalf("DropPropertyIndex(event label) = %v, want ErrEventPropertyIndex", err)
	}
}

func TestTieredStorePutNodeWithBackdatedEventIDRemainsReadable(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	id := types.NodeID(gen.Generate())
	forceRotation(t, ts)

	n := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode with backdated event ID: %v", err)
	}

	got, err := ts.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after successful PutNode: %v", err)
	}
	if got.ID() != id {
		t.Fatalf("GetNode ID = %d, want %d", got.ID(), id)
	}
}

func TestTieredStorePutNodesBatchWithBackdatedEventIDRemainsReadable(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	id := types.NodeID(gen.Generate())
	forceRotation(t, ts)

	n := types.NewNode(id, signalTok, nil)
	if err := ts.PutNodesBatch([]*types.Node{n}); err != nil {
		t.Fatalf("PutNodesBatch with backdated event ID: %v", err)
	}

	if _, err := ts.GetNode(id); err != nil {
		t.Fatalf("GetNode after successful PutNodesBatch: %v", err)
	}
}

func TestTieredStorePutNodeRejectsDuplicateIDInClosedColdShard(t *testing.T) {
	ts, caseTok, signalTok := setupDiskBatchDelete(t)
	gen := tieredNodeGen(t)

	id := types.NodeID(gen.Generate())
	original := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(original); err != nil {
		t.Fatalf("PutNode original: %v", err)
	}
	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	closeColdEventShardForCreateDuplicateTest(t, ts, coldName)

	dup := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(dup); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNode duplicate ID from closed cold shard = %v, want ErrNodeExists", err)
	}
}

func TestTieredStorePutNodeGeneratedIDFreshProofStoresNode(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNodeGeneratedID(n, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("PutNodeGeneratedID: %v", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode after PutNodeGeneratedID: %v", err)
	}
	if got.ID() != n.ID() || got.PrimaryLabelToken() != n.PrimaryLabelToken() {
		t.Fatalf("stored node = id %d label %d, want id %d label %d", got.ID(), got.PrimaryLabelToken(), n.ID(), n.PrimaryLabelToken())
	}
}

func TestTieredStoreGeneratedNodeRequiresFreshProof(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	id := types.NodeID(gen.Generate())
	original := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(original); err != nil {
		t.Fatalf("PutNode original: %v", err)
	}
	dup := types.NewNode(id, signalTok, nil)
	if err := ts.PutNodeGeneratedID(dup, generatedcreate.Proof{}); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNodeGeneratedID without proof = %v, want ErrNodeExists", err)
	}
}

func TestTieredStorePutNodeGeneratedIDRejectsInvalidAndClosedStore(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.PutNodeGeneratedID(nil, generatedcreate.FreshGraphID); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodeGeneratedID(nil) = %v, want ErrInvalidStoreMutation", err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ts.PutNodeGeneratedID(n, generatedcreate.FreshGraphID); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("PutNodeGeneratedID after Close = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStorePutNodesBatchGeneratedIDFreshProofStoresMixedNodes(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	event := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNodesBatchGeneratedID([]*types.Node{ref, event}, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("PutNodesBatchGeneratedID: %v", err)
	}
	for _, want := range []*types.Node{ref, event} {
		got, err := ts.GetNode(want.ID())
		if err != nil {
			t.Fatalf("GetNode(%d) after PutNodesBatchGeneratedID: %v", want.ID(), err)
		}
		if got.PrimaryLabelToken() != want.PrimaryLabelToken() {
			t.Fatalf("GetNode(%d) label = %d, want %d", want.ID(), got.PrimaryLabelToken(), want.PrimaryLabelToken())
		}
	}
}

func TestTieredStoreGeneratedNodeBatchRequiresFreshProof(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	gen := tieredNodeGen(t)

	id := types.NodeID(gen.Generate())
	original := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(original); err != nil {
		t.Fatalf("PutNode original: %v", err)
	}
	dup := types.NewNode(id, signalTok, nil)
	if err := ts.PutNodesBatchGeneratedID([]*types.Node{dup}, generatedcreate.Proof{}); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNodesBatchGeneratedID without proof = %v, want ErrNodeExists", err)
	}
}

func TestTieredStorePutNodesBatchGeneratedIDRejectsInvalidAndClosedStore(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)

	if err := ts.PutNodesBatchGeneratedID([]*types.Node{nil}, generatedcreate.FreshGraphID); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodesBatchGeneratedID(nil node) = %v, want ErrInvalidStoreMutation", err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ts.PutNodesBatchGeneratedID([]*types.Node{n}, generatedcreate.FreshGraphID); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("PutNodesBatchGeneratedID after Close = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStorePutRelationshipRejectsDuplicateIDInClosedColdShard(t *testing.T) {
	ts, _, signalTok := setupDiskBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	oldStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	oldEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{oldStart, oldEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode old endpoint %d: %v", n.ID(), err)
		}
	}
	relID := types.RelID(relGen.Generate())
	original := types.NewRelationship(relID, 1, oldStart.ID(), oldEnd.ID())
	if err := ts.PutRelationship(original); err != nil {
		t.Fatalf("PutRelationship original: %v", err)
	}
	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)
	closeColdEventShardForCreateDuplicateTest(t, ts, coldName)

	newStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	newEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{newStart, newEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode new endpoint %d: %v", n.ID(), err)
		}
	}
	dup := types.NewRelationship(relID, 1, newStart.ID(), newEnd.ID())
	if err := ts.PutRelationship(dup); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationship duplicate ID from closed cold shard = %v, want ErrRelExists", err)
	}
}

func TestTieredStorePutRelationshipRejectsCurrentIDDuplicateInClosedColdStartShard(t *testing.T) {
	ts, _, signalTok := setupDiskBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	oldStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(oldStart); err != nil {
		t.Fatalf("PutNode old start: %v", err)
	}
	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)

	hotEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(hotEnd); err != nil {
		t.Fatalf("PutNode hot end: %v", err)
	}
	relID := types.RelID(relGen.Generate())
	original := types.NewRelationship(relID, 1, oldStart.ID(), hotEnd.ID())
	if err := ts.PutRelationship(original); err != nil {
		t.Fatalf("PutRelationship original current-ID cold-start rel: %v", err)
	}
	closeColdEventShardForCreateDuplicateTest(t, ts, coldName)

	newStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	newEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{newStart, newEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode new endpoint %d: %v", n.ID(), err)
		}
	}
	dup := types.NewRelationship(relID, 1, newStart.ID(), newEnd.ID())
	if err := ts.PutRelationship(dup); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationship duplicate current ID from closed cold start shard = %v, want ErrRelExists", err)
	}
}

func TestTieredStoreGeneratedRelationshipRequiresFreshProof(t *testing.T) {
	ts, _, signalTok := setupDiskBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	oldStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(oldStart); err != nil {
		t.Fatalf("PutNode old start: %v", err)
	}
	coldName := ts.HotShardForTest().Name()
	forceRotation(t, ts)

	hotEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(hotEnd); err != nil {
		t.Fatalf("PutNode hot end: %v", err)
	}
	relID := types.RelID(relGen.Generate())
	original := types.NewRelationship(relID, 1, oldStart.ID(), hotEnd.ID())
	if err := ts.PutRelationship(original); err != nil {
		t.Fatalf("PutRelationship original current-ID cold-start rel: %v", err)
	}
	closeColdEventShardForCreateDuplicateTest(t, ts, coldName)

	newStart := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	newEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{newStart, newEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode new endpoint %d: %v", n.ID(), err)
		}
	}
	dup := types.NewRelationship(relID, 1, newStart.ID(), newEnd.ID())
	if err := ts.PutRelationshipGeneratedID(dup, generatedcreate.Proof{}); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationshipGeneratedID without proof = %v, want ErrRelExists", err)
	}
}

func TestTieredStorePutRelationshipGeneratedIDFreshProofStoresCrossShardRel(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	signal := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(signal); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	cas := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := ts.PutNode(cas); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}

	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, signal.ID(), cas.ID())
	if err := ts.PutRelationshipGeneratedID(rel, generatedcreate.FreshGraphID); err != nil {
		t.Fatalf("PutRelationshipGeneratedID: %v", err)
	}
	stored, err := ts.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if stored.StartNodeID() != signal.ID() || stored.EndNodeID() != cas.ID() {
		t.Fatalf("stored endpoints = %d -> %d, want %d -> %d", stored.StartNodeID(), stored.EndNodeID(), signal.ID(), cas.ID())
	}
	outgoing, err := ts.OutgoingRelationships(signal.ID(), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(outgoing) != 1 || outgoing[0].ID() != rel.ID() {
		t.Fatalf("OutgoingRelationships = %v, want only %d", relIDsOf(outgoing), rel.ID())
	}
	incoming, err := ts.IncomingRelationships(cas.ID(), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(incoming) != 1 || incoming[0].ID() != rel.ID() {
		t.Fatalf("IncomingRelationships = %v, want only %d", relIDsOf(incoming), rel.ID())
	}
}

func TestTieredStorePutRelationshipGeneratedIDRejectsInvalidAndClosedStore(t *testing.T) {
	ts, _, _ := setupBatchDelete(t)

	if err := ts.PutRelationshipGeneratedID(nil, generatedcreate.FreshGraphID); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipGeneratedID(nil) = %v, want ErrInvalidStoreMutation", err)
	}

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)
	rel := types.NewRelationship(
		types.RelID(relGen.Generate()),
		1,
		types.NodeID(nodeGen.Generate()),
		types.NodeID(nodeGen.Generate()),
	)
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := ts.PutRelationshipGeneratedID(rel, generatedcreate.FreshGraphID); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("PutRelationshipGeneratedID after Close = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreGeneratedRelationshipWithEndpointHashes(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	signal := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	signal.SetIntegrity(&types.NodeIntegrity{Hash: "signal-hash"})
	if err := ts.PutNode(signal); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	cas := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	cas.SetIntegrity(&types.NodeIntegrity{Hash: "case-hash"})
	if err := ts.PutNode(cas); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}

	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, signal.ID(), cas.ID())
	rel.SetIntegrity(&types.RelIntegrity{Hash: "rel-hash"})
	fromHash, toHash, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(rel, generatedcreate.FreshGraphID)
	if err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes: %v", err)
	}
	if fromHash != "signal-hash" || toHash != "case-hash" {
		t.Fatalf("returned endpoint hashes = %q, %q; want signal-hash, case-hash", fromHash, toHash)
	}
	if ig := rel.Integrity(); ig == nil || ig.FromNodeHash != "signal-hash" || ig.ToNodeHash != "case-hash" {
		t.Fatalf("caller relationship integrity = %+v; want endpoint hashes", ig)
	}
	stored, err := ts.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := stored.Integrity(); ig == nil || ig.FromNodeHash != "signal-hash" || ig.ToNodeHash != "case-hash" {
		t.Fatalf("stored relationship integrity = %+v; want endpoint hashes", ig)
	}
}

func setupDiskBatchDelete(t *testing.T) (*Store, uint16, uint16) {
	t.Helper()
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	return ts, caseTok, signalTok
}

func closeColdEventShardForCreateDuplicateTest(t *testing.T, ts *Store, shardName string) {
	t.Helper()
	demoteToCold(ts, shardName)

	ts.MuForTest().RLock()
	cold := ts.EventShardsForTest()[shardName]
	ts.MuForTest().RUnlock()
	if cold == nil {
		t.Fatalf("missing event shard %q", shardName)
	}

	ts.SetIdleTimeoutForTest(time.Nanosecond)
	cold.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()

	cold.LockShardMuForTest()
	store := cold.Store()
	cold.UnlockShardMuForTest()
	if store != nil {
		t.Fatalf("event shard %q remained open; duplicate test requires a closed cold shard", shardName)
	}
}

type unmarshalableProperty struct {
	Ch chan int
}

func (u unmarshalableProperty) HashBytes() []byte { return []byte("unmarshalable") }
func (u unmarshalableProperty) DeepCopyValue() any {
	return unmarshalableProperty{Ch: u.Ch}
}

func TestTieredStoreDeleteNodeWithHistory_RollsBackRelDeletesWhenNodeTombstoneFails(t *testing.T) {
	t.Parallel()
	if err := types.RegisterPropertyStructType(unmarshalableProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	ts, _, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)
	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode(a): %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode(b): %v", err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	beforeHistory, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory before: %v", err)
	}

	relTombstone := r.DeepCopy()
	nodeTombstone := a.DeepCopy()
	if err := nodeTombstone.SetProperties(types.PropertySlice{{Key: "bad", Value: unmarshalableProperty{Ch: make(chan int)}}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	err = ts.DeleteNodeWithHistory(a.ID(), a.Version(), nodeTombstone, []storepkg.RelTombstone{{
		ID:          r.ID(),
		PrevVersion: r.Version(),
		Tombstone:   relTombstone,
	}})
	if err == nil {
		t.Fatal("DeleteNodeWithHistory returned nil for unmarshalable node tombstone")
	}

	if _, err := ts.GetNode(a.ID()); err != nil {
		t.Fatalf("source node missing after failed DeleteNodeWithHistory: %v", err)
	}
	if _, err := ts.GetRelationship(r.ID()); err != nil {
		t.Fatalf("relationship was not restored after failed DeleteNodeWithHistory: %v", err)
	}
	afterHistory, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatalf("GetRelHistory after: %v", err)
	}
	if len(afterHistory) != len(beforeHistory) {
		t.Fatalf("relationship history length after rollback = %d, want %d", len(afterHistory), len(beforeHistory))
	}
}

func TestTieredStoreDeleteNodeWithHistoryRejectsMissingRelTombstone(t *testing.T) {
	t.Parallel()
	ts, a, _, _, r, _ := newTieredDeleteNodeWithHistoryFixture(t)

	err := ts.DeleteNodeWithHistory(a.ID(), a.Version(), a.DeepCopy(), nil)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory missing rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	assertTieredDeleteNodeWithHistoryRejected(t, ts, a.ID(), r.ID())
}

func TestTieredStoreDeleteNodeWithHistoryRejectsDuplicateRelTombstone(t *testing.T) {
	t.Parallel()
	ts, a, _, _, r, _ := newTieredDeleteNodeWithHistoryFixture(t)

	relTombstone := r.DeepCopy()
	err := ts.DeleteNodeWithHistory(a.ID(), a.Version(), a.DeepCopy(), []storepkg.RelTombstone{
		{ID: r.ID(), PrevVersion: r.Version(), Tombstone: relTombstone},
		{ID: r.ID(), PrevVersion: r.Version(), Tombstone: relTombstone.DeepCopy()},
	})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory duplicate rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	assertTieredDeleteNodeWithHistoryRejected(t, ts, a.ID(), r.ID())
}

func TestTieredStoreDeleteNodeWithHistoryRejectsUnrelatedRelTombstone(t *testing.T) {
	t.Parallel()
	ts, a, _, _, r, unrelated := newTieredDeleteNodeWithHistoryFixture(t)

	err := ts.DeleteNodeWithHistory(a.ID(), a.Version(), a.DeepCopy(), []storepkg.RelTombstone{
		{ID: r.ID(), PrevVersion: r.Version(), Tombstone: r.DeepCopy()},
		{ID: unrelated.ID(), PrevVersion: unrelated.Version(), Tombstone: unrelated.DeepCopy()},
	})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory unrelated rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	assertTieredDeleteNodeWithHistoryRejected(t, ts, a.ID(), r.ID())
	if _, err := ts.GetRelationship(unrelated.ID()); err != nil {
		t.Fatalf("unrelated relationship changed after rejected tombstone: %v", err)
	}
}

func TestTieredStoreDeleteNodeWithHistoryRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	n := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	tombstone := n.DeepCopy()

	if err := ts.DeleteNodeWithHistory(n.ID(), n.Version(), nil, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory nil node tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNodeWithHistory(0, n.Version(), tombstone, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory zero node id = %v, want ErrInvalidStoreMutation", err)
	}
	badRelID := types.RelID(relGen.Generate())
	if err := ts.DeleteNodeWithHistory(n.ID(), n.Version(), tombstone, []storepkg.RelTombstone{{
		ID:          badRelID,
		PrevVersion: 0,
		Tombstone:   nil,
	}}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeWithHistory nil rel tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("node changed after rejected DeleteNodeWithHistory: %v", err)
	}
}

func TestTieredStoreDeleteNodeWithHistoryPurgesOrphanIncomingAdjacency(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	orphanRelID := relGen.Generate()
	if err := ts.hotShard.store.PutRelIncoming(end.ID().SnowflakeID(), start.ID().SnowflakeID(), 1, orphanRelID); err != nil {
		t.Fatalf("PutRelIncoming orphan: %v", err)
	}
	if got := ts.hotShard.store.IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 1 {
		t.Fatalf("orphan setup incoming IDs = %v, want one entry", got)
	}

	if err := ts.DeleteNodeWithHistory(end.ID(), end.Version(), end.DeepCopy(), nil); err != nil {
		t.Fatalf("DeleteNodeWithHistory with orphan incoming adjacency: %v", err)
	}
	if _, err := ts.GetNode(end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode deleted endpoint = %v, want ErrNodeNotFound", err)
	}
	if got := ts.hotShard.store.IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("orphan incoming adjacency survived DeleteNodeWithHistory: %v", got)
	}
}

func TestTieredStoreDeleteNodeCascadePurgesOrphanIncomingAdjacency(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	orphanRelID := relGen.Generate()
	if err := ts.hotShard.store.PutRelIncoming(end.ID().SnowflakeID(), start.ID().SnowflakeID(), 1, orphanRelID); err != nil {
		t.Fatalf("PutRelIncoming orphan: %v", err)
	}
	if got := ts.hotShard.store.IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 1 {
		t.Fatalf("orphan setup incoming IDs = %v, want one entry", got)
	}

	if err := ts.DeleteNodeCascade(end.ID()); err != nil {
		t.Fatalf("DeleteNodeCascade with orphan incoming adjacency: %v", err)
	}
	if _, err := ts.GetNode(end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode deleted endpoint = %v, want ErrNodeNotFound", err)
	}
	if got := ts.hotShard.store.IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("orphan incoming adjacency survived DeleteNodeCascade: %v", got)
	}
}

func TestTieredStoreDeleteNodeWithHistorySameShardSuccess(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	nodeTombstone := start.DeepCopy()
	relTombstone := rel.DeepCopy()
	if err := ts.DeleteNodeWithHistory(start.ID(), start.Version(), nodeTombstone, []storepkg.RelTombstone{{
		ID:          rel.ID(),
		PrevVersion: rel.Version(),
		Tombstone:   relTombstone,
	}}); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	if _, err := ts.GetNode(start.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after DeleteNodeWithHistory = %v, want ErrNodeNotFound", err)
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteNodeWithHistory = %v, want ErrRelNotFound", err)
	}
	nodeHistory, err := ts.GetNodeHistory(start.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(nodeHistory) != 1 || nodeHistory[0].ID() != start.ID() {
		t.Fatalf("GetNodeHistory = %#v, want one tombstone for %d", nodeHistory, start.ID())
	}
	relHistory, err := ts.GetRelHistory(rel.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(relHistory) != 1 || relHistory[0].ID() != rel.ID() {
		t.Fatalf("GetRelHistory = %#v, want one tombstone for %d", relHistory, rel.ID())
	}
	if got := ts.HotShardForTest().Store().OutgoingRelIDs(start.ID().SnowflakeID()); len(got) != 0 {
		t.Fatalf("outgoing index after DeleteNodeWithHistory = %v, want empty", got)
	}
}

func TestTieredStoreDeleteNodeWithHistoryCrossShardIncomingSuccess(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	nodeTombstone := end.DeepCopy()
	relTombstone := rel.DeepCopy()
	if err := ts.DeleteNodeWithHistory(end.ID(), end.Version(), nodeTombstone, []storepkg.RelTombstone{{
		ID:          rel.ID(),
		PrevVersion: rel.Version(),
		Tombstone:   relTombstone,
	}}); err != nil {
		t.Fatalf("DeleteNodeWithHistory: %v", err)
	}

	if _, err := ts.GetNode(end.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after DeleteNodeWithHistory = %v, want ErrNodeNotFound", err)
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteNodeWithHistory = %v, want ErrRelNotFound", err)
	}
	nodeHistory, err := ts.GetNodeHistory(end.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(nodeHistory) != 1 || nodeHistory[0].ID() != end.ID() {
		t.Fatalf("GetNodeHistory = %#v, want one tombstone for %d", nodeHistory, end.ID())
	}
	relHistory, err := ts.GetRelHistory(rel.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(relHistory) != 1 || relHistory[0].ID() != rel.ID() {
		t.Fatalf("GetRelHistory = %#v, want one tombstone for %d", relHistory, rel.ID())
	}
	if got := ts.RefShardForTest().IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("incoming index after DeleteNodeWithHistory = %v, want empty", got)
	}
}

func TestTieredStoreDeleteRelWithHistorySameShard(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	tombstone := rel.DeepCopy()
	if err := ts.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteRelWithHistory = %v, want ErrRelNotFound", err)
	}
	history, err := ts.GetRelHistory(rel.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 1 || history[0].ID() != rel.ID() {
		t.Fatalf("GetRelHistory = %#v, want one tombstone for %d", history, rel.ID())
	}
	if got := ts.HotShardForTest().Store().IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("incoming index after DeleteRelWithHistory = %v, want empty", got)
	}
}

func TestTieredStoreDeleteRelWithHistoryCrossShard(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	tombstone := rel.DeepCopy()
	if err := ts.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone); err != nil {
		t.Fatalf("DeleteRelWithHistory: %v", err)
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after DeleteRelWithHistory = %v, want ErrRelNotFound", err)
	}
	history, err := ts.GetRelHistory(rel.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 1 || history[0].ID() != rel.ID() {
		t.Fatalf("GetRelHistory = %#v, want one tombstone for %d", history, rel.ID())
	}
	if got := ts.RefShardForTest().IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("reference incoming index after DeleteRelWithHistory = %v, want empty", got)
	}
}

func TestTieredStoreDeleteRelWithHistoryRestoresIncomingOnEntityFailure(t *testing.T) {
	if err := types.RegisterPropertyStructType(unmarshalableProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	tombstone := rel.DeepCopy()
	if err := tombstone.SetProperties(types.PropertySlice{{Key: "bad", Value: unmarshalableProperty{Ch: make(chan int)}}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}
	err := ts.DeleteRelWithHistory(rel.ID(), rel.Version(), tombstone)
	if err == nil {
		t.Fatal("DeleteRelWithHistory returned nil for unmarshalable tombstone")
	}
	if _, getErr := ts.GetRelationship(rel.ID()); getErr != nil {
		t.Fatalf("relationship missing after failed DeleteRelWithHistory: %v", getErr)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 1 || got[0] != rel.ID().SnowflakeID() {
		t.Fatalf("incoming index after failed DeleteRelWithHistory = %v, want [%d]", got, rel.ID().SnowflakeID())
	}
	if history, histErr := ts.GetRelHistory(rel.ID()); histErr != nil || len(history) != 0 {
		t.Fatalf("relationship history after failed DeleteRelWithHistory = len %d err %v, want empty nil", len(history), histErr)
	}
}

func TestTieredStoreDeleteRelWithHistoryRejectsInvalidInputs(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	if err := ts.DeleteRelWithHistory(rel.ID(), rel.Version(), nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelWithHistory nil tombstone = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteRelWithHistory(0, rel.Version(), rel.DeepCopy()); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelWithHistory zero id = %v, want ErrInvalidStoreMutation", err)
	}
	missing := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.DeleteRelWithHistory(missing.ID(), missing.Version(), missing); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("DeleteRelWithHistory missing rel = %v, want ErrRelNotFound", err)
	}
	if _, err := ts.GetRelationship(rel.ID()); err != nil {
		t.Fatalf("relationship changed after rejected DeleteRelWithHistory: %v", err)
	}
}

func TestTieredStoreRestoreRelHistorySnapshotReplacesExistingHistory(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	oldV1 := rel.DeepCopy()
	oldV1.SetVersion(1)
	oldV2 := rel.DeepCopy()
	oldV2.SetVersion(2)
	replacement := rel.DeepCopy()
	replacement.SetVersion(7)

	if err := ts.PutRelVersion(rel.ID(), oldV1.Version(), oldV1); err != nil {
		t.Fatalf("PutRelVersion oldV1: %v", err)
	}
	if err := ts.PutRelVersion(rel.ID(), oldV2.Version(), oldV2); err != nil {
		t.Fatalf("PutRelVersion oldV2: %v", err)
	}
	if err := ts.restoreRelHistorySnapshot(rel.ID(), []*types.Relationship{replacement}); err != nil {
		t.Fatalf("restoreRelHistorySnapshot: %v", err)
	}

	history, err := ts.GetRelHistory(rel.ID())
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(history) != 1 || history[0].Version() != replacement.Version() {
		t.Fatalf("GetRelHistory after restore = versions %v, want [%d]", tieredRelHistoryVersions(history), replacement.Version())
	}
}

func tieredRelHistoryVersions(history []*types.Relationship) []uint32 {
	versions := make([]uint32, len(history))
	for i, r := range history {
		versions[i] = r.Version()
	}
	return versions
}

func newTieredDeleteNodeWithHistoryFixture(t *testing.T) (*Store, *types.Node, *types.Node, *types.Node, *types.Relationship, *types.Relationship) {
	t.Helper()
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	c := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{a, b, c} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	unrelated := types.NewRelationship(types.RelID(relGen.Generate()), 1, b.ID(), c.ID())
	for _, rel := range []*types.Relationship{r, unrelated} {
		if err := ts.PutRelationship(rel); err != nil {
			t.Fatalf("PutRelationship(%d): %v", rel.ID(), err)
		}
	}
	return ts, a, b, c, r, unrelated
}

func assertTieredDeleteNodeWithHistoryRejected(t *testing.T, ts *Store, nid types.NodeID, rid types.RelID) {
	t.Helper()
	if _, err := ts.GetNode(nid); err != nil {
		t.Fatalf("node deleted after rejected DeleteNodeWithHistory: %v", err)
	}
	if _, err := ts.GetRelationship(rid); err != nil {
		t.Fatalf("relationship deleted after rejected DeleteNodeWithHistory: %v", err)
	}
	if hist, err := ts.GetNodeHistory(nid); err != nil || len(hist) != 0 {
		t.Fatalf("node history after rejected DeleteNodeWithHistory = len %d err %v, want empty nil", len(hist), err)
	}
	if hist, err := ts.GetRelHistory(rid); err != nil || len(hist) != 0 {
		t.Fatalf("relationship history after rejected DeleteNodeWithHistory = len %d err %v, want empty nil", len(hist), err)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_EmptyInput verifies the no-op contract.
// nil and empty slice MUST return nil and perform no work.
func TestTieredStore_DeleteRelationshipsBatch_EmptyInput(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	if err := ts.DeleteRelationshipsBatch(nil); err != nil {
		t.Errorf("DeleteRelationshipsBatch(nil): %v", err)
	}
	if err := ts.DeleteRelationshipsBatch([]types.RelID{}); err != nil {
		t.Errorf("DeleteRelationshipsBatch([]): %v", err)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_SameShardBucketed verifies that
// many same-shard rels delete correctly via the new partitioning path. Both
// endpoints are event nodes so all rels live entirely on the hot shard.
func TestTieredStore_DeleteRelationshipsBatch_SameShardBucketed(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	const N = 200
	// Two event nodes are enough endpoints to share among many rels.
	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}

	ids := make([]types.RelID, 0, N)
	for i := 0; i < N; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
		ids = append(ids, r.ID())
	}

	// All rels live on the hot event shard.
	if got, _ := ts.RelationshipCount(); got != N {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, N)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch: %v", err)
	}

	// All rels gone.
	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}
	// Adjacency cleaned: outgoing from a / incoming into b are empty.
	out, err := ts.OutgoingRelationships(a.ID(), 0)
	if err != nil {
		t.Fatalf("OutgoingRelationships: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("OutgoingRelationships(a) = %d, want 0", len(out))
	}
	in, err := ts.IncomingRelationships(b.ID(), 0)
	if err != nil {
		t.Fatalf("IncomingRelationships: %v", err)
	}
	if len(in) != 0 {
		t.Errorf("IncomingRelationships(b) = %d, want 0", len(in))
	}
	// Each ID surfaces ErrRelNotFound on lookup.
	for _, id := range ids[:5] { // spot check — full N iteration is noisy
		if _, err := ts.GetRelationship(id); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Errorf("GetRelationship(%d): err = %v, want ErrRelNotFound", id, err)
		}
	}
}

// TestTieredStore_DeleteRelationshipsBatch_CrossShardOnly verifies that an
// all-cross-shard input still deletes correctly. Cross-shard rels skip the
// per-shard batch and go through the existing per-ID DeleteRelationship path.
// Endpoints: Case (ref shard) ←→ Signal (event shard).
func TestTieredStore_DeleteRelationshipsBatch_CrossShardOnly(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	const N = 20
	caseNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	signalNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(signalNode); err != nil {
		t.Fatal(err)
	}

	ids := make([]types.RelID, 0, N)
	for i := 0; i < N; i++ {
		// Case (ref) -> Signal (event) = cross-shard rel.
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, caseNode.ID(), signalNode.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship %d: %v", i, err)
		}
		ids = append(ids, r.ID())
	}

	if got, _ := ts.RelationshipCount(); got != N {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, N)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch (cross-shard): %v", err)
	}

	// All rels gone — entity+out/ on ref shard, in/ on event shard.
	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}
	// in/ on the end-node shard (event) must be cleared. Use the test helper
	// to inspect the actual end-shard adjacency so we catch a regression where
	// the cross-shard rollback ordering breaks.
	endShard, endCheckin, err := ts.shardForNodeIDChecked(signalNode.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer endCheckin()
	if leftover := endShard.IncomingRelIDs(signalNode.ID().SnowflakeID(), 0); len(leftover) != 0 {
		t.Errorf("end-shard in/ leftover after cross-shard batch delete: %d entries", len(leftover))
	}

	// RunRepair must not detect any orphaned in/ entries — proves the
	// cross-shard split-delete completed both legs for every ID.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Errorf("RunRepair: %d orphaned in/ entries after batch delete", res.OrphanedInEntries)
	}
}

func TestTieredStore_DeleteRelationshipsBatchDeduplicatesInput(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)
	caseNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	signalNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(signalNode); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, caseNode.ID(), signalNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	if err := ts.DeleteRelationshipsBatch([]types.RelID{r.ID(), r.ID()}); err != nil {
		t.Fatalf("DeleteRelationshipsBatch duplicate ID: %v", err)
	}
	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("RelationshipCount after duplicate batch delete = %d, want 0", count)
	}
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Fatalf("RunRepair: %d orphaned in/ entries after duplicate batch delete", res.OrphanedInEntries)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_Mixed verifies that an arbitrary
// interleaving of same-shard and cross-shard rels deletes correctly. This is
// the primary "behavioural parity" test from the design plan.
func TestTieredStore_DeleteRelationshipsBatch_Mixed(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Endpoints: two ref nodes (Case) and two event nodes (Signal).
	c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{c1, c2, s1, s2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	const sameShardCount = 100
	const crossShardCount = 30

	ids := make([]types.RelID, 0, sameShardCount+crossShardCount)
	sameShardIDs := make([]types.RelID, 0, sameShardCount)
	crossShardIDs := make([]types.RelID, 0, crossShardCount)

	// Same-shard rels: Signal -> Signal (both endpoints on hot event shard).
	for i := 0; i < sameShardCount; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, s1.ID(), s2.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship same-shard %d: %v", i, err)
		}
		sameShardIDs = append(sameShardIDs, r.ID())
	}
	// Cross-shard rels: Case (ref) -> Signal (event).
	for i := 0; i < crossShardCount; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, c1.ID(), s1.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship cross-shard %d: %v", i, err)
		}
		crossShardIDs = append(crossShardIDs, r.ID())
	}

	// Interleave the two slices so neither bucket appears in any obvious order.
	maxLen := len(sameShardIDs)
	if len(crossShardIDs) > maxLen {
		maxLen = len(crossShardIDs)
	}
	for i := 0; i < maxLen; i++ {
		if i < len(sameShardIDs) {
			ids = append(ids, sameShardIDs[i])
		}
		if i < len(crossShardIDs) {
			ids = append(ids, crossShardIDs[i])
		}
	}

	totalBefore := sameShardCount + crossShardCount
	if got, _ := ts.RelationshipCount(); got != totalBefore {
		t.Fatalf("pre-delete RelationshipCount = %d, want %d", got, totalBefore)
	}

	if err := ts.DeleteRelationshipsBatch(ids); err != nil {
		t.Fatalf("DeleteRelationshipsBatch (mixed): %v", err)
	}

	if got, _ := ts.RelationshipCount(); got != 0 {
		t.Errorf("post-delete RelationshipCount = %d, want 0", got)
	}

	// Sanity: each ID is gone.
	gone := append(append([]types.RelID(nil), sameShardIDs...), crossShardIDs...)
	sort.Slice(gone, func(i, j int) bool { return gone[i] < gone[j] })
	for _, id := range []types.RelID{gone[0], gone[len(gone)/2], gone[len(gone)-1]} {
		if _, err := ts.GetRelationship(id); !errors.Is(err, storepkg.ErrRelNotFound) {
			t.Errorf("GetRelationship(%d) after batch delete: err = %v, want ErrRelNotFound", id, err)
		}
	}

	// Cross-shard repair invariant: no orphaned in/ entries on the end shard.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 {
		t.Errorf("RunRepair: %d orphaned in/ entries after mixed batch delete", res.OrphanedInEntries)
	}
}

// TestTieredStore_DeleteRelationshipsBatch_MissingID verifies that a missing
// rel ID surfaces ErrRelNotFound, matching the per-ID DeleteRelationship
// contract. The previous loop body would also return ErrRelNotFound on the
// first miss; the partitioned path must do the same.
func TestTieredStore_DeleteRelationshipsBatch_MissingID(t *testing.T) {
	t.Parallel()
	ts, _, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	a := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	b := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// One real ID, one phantom ID generated from the same generator (so the
	// timestamp routes to the hot shard) but never persisted.
	phantom := types.RelID(relGen.Generate())
	err := ts.DeleteRelationshipsBatch([]types.RelID{r.ID(), phantom})
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("DeleteRelationshipsBatch with missing ID: err = %v, want ErrRelNotFound", err)
	}
}

func TestTieredStore_DeleteRelationshipsBatchRollsBackPriorShardOnCrossShardFailure(t *testing.T) {
	t.Parallel()
	ts, caseTok, signalTok := setupBatchDelete(t)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	refStart := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	refEnd := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	eventEnd := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{refStart, refEnd, eventEnd} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	sameShard := types.NewRelationship(types.RelID(relGen.Generate()), 1, refStart.ID(), refEnd.ID())
	crossShard := types.NewRelationship(types.RelID(relGen.Generate()), 1, refStart.ID(), eventEnd.ID())
	for _, r := range []*types.Relationship{sameShard, crossShard} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", r.ID(), err)
		}
	}

	hot := ts.HotShardForTest().Store()
	hot.SetDBClosedForTest(true)
	err := ts.DeleteRelationshipsBatch([]types.RelID{sameShard.ID(), crossShard.ID()})
	hot.SetDBClosedForTest(false)
	if !errors.Is(err, storepkg.ErrStoreClosed) {
		t.Fatalf("DeleteRelationshipsBatch error = %v, want ErrStoreClosed", err)
	}

	for _, r := range []*types.Relationship{sameShard, crossShard} {
		if _, err := ts.GetRelationship(r.ID()); err != nil {
			t.Fatalf("relationship %d missing after rollback: %v", r.ID(), err)
		}
	}
	if got, err := ts.RelationshipCount(); err != nil || got != 2 {
		t.Fatalf("RelationshipCount after rollback = %d err %v, want 2 nil", got, err)
	}
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 || res.MissingInEntries != 0 {
		t.Fatalf("RunRepair after rollback: orphaned in=%d missing in=%d, want 0/0", res.OrphanedInEntries, res.MissingInEntries)
	}
}
