package tiered

import (
	"errors"
	"path/filepath"
	"sort"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/generatedcreate"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
