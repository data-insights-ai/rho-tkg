package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- F3: Store.Clear must wipe store-level indexes ---

// TestTieredStoreClear_ClearsVectorIndexes verifies that the Store-level
// vector index map is reset on Clear so CreateVectorIndex does not return
// ErrVectorIndexExists on a freshly cleared store.
func TestTieredStoreClear_ClearsVectorIndexes(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ts.CreateVectorIndex(caseTok, "v", 3, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex after Clear: %v", err)
	}
}

func TestTieredStoreClear_ClearsHFITracking(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateHighFrequencyIndex(caseTok, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if beforeLen := len(ts.HFBucketsForTest()); beforeLen == 0 {
		t.Fatalf("hfIdxBuckets not populated after CreateHighFrequencyIndex")
	}

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if afterLen := len(ts.HFBucketsForTest()); afterLen != 0 {
		t.Fatalf("hfIdxBuckets after Clear = %d, want 0 (rotation would re-install stale HFI)", afterLen)
	}
}

// TestTieredStoreClear_ClearsTempIdxLabels verifies that the tracked temporal
// index labels list is wiped on Clear, so a subsequent shard rotation does
// not re-install temporal indexes for stale labels.
func TestTieredStoreClear_ClearsTempIdxLabels(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	beforeLen := len(ts.TempIdxLabelsForTest())
	if beforeLen == 0 {
		t.Fatalf("tempIdxLabels not populated after CreateTemporalIndex")
	}

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}

	afterLen := len(ts.TempIdxLabelsForTest())
	if afterLen != 0 {
		t.Fatalf("tempIdxLabels after Clear = %d, want 0 (rotation would re-install stale labels)", afterLen)
	}
}

func TestTieredStoreClear_ClearsClosedColdShard(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	hotName := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, hotName)

	cold := ts.EventShardsForTest()[hotName]
	cold.LockShardMuForTest()
	if cold.Store() != nil {
		if err := cold.Store().Close(); err != nil {
			cold.UnlockShardMuForTest()
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	store, err := cold.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatalf("checkout cold store after Clear: %v", err)
	}
	_, err = store.GetNode(n.ID())
	cold.CheckinStoreForTest()
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after Clear = %v, want ErrNodeNotFound", err)
	}
}

func TestTieredStoreClear_DoesNotOpenClosedInMemoryColdShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	_, _ = reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := makeEvtNode(t, gen, ts)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

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
			t.Fatalf("close cold store: %v", err)
		}
		cold.SetStoreForTest(nil)
	}
	cold.UnlockShardMuForTest()

	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	cold.LockShardMuForTest()
	defer cold.UnlockShardMuForTest()
	if cold.Store() != nil {
		t.Fatal("Clear lazy-opened a closed in-memory cold shard")
	}
}

func TestTieredStoreClear_ClearsRestartedWarmShard(t *testing.T) {
	dir := t.TempDir()
	reg := registrypkg.NewLabelRegistry()
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New ts1: %v", err)
	}
	ts1.SetLabelRegistry(reg)
	if err := ts1.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	warmName := ts1.HotShardForTest().Name()
	if err := ts1.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	if err := ts1.Close(); err != nil {
		t.Fatalf("Close ts1: %v", err)
	}

	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New ts2: %v", err)
	}
	t.Cleanup(func() { _ = ts2.Close() })
	ts2.SetLabelRegistry(reg)

	warm := ts2.EventShardsForTest()[warmName]
	if warm == nil || warm.Store() == nil || warm.Store().ReadOnlyForTest() {
		t.Fatalf("restarted warm shard not open writable")
	}
	if err := ts2.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	_, err = warm.Store().GetNode(n.ID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNode after Clear = %v, want ErrNodeNotFound", err)
	}
	if warm.Store().ReadOnlyForTest() {
		t.Fatalf("warm shard reopened read-only after Clear, want writable")
	}
}

func TestTieredStoreClear_ResetsCatalogVerificationCache(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	name := ts.HotShardForTest().Name()
	if !ts.CatalogForTest().UpdateShardStats(name, 7, 3) {
		t.Fatalf("UpdateShardStats returned false")
	}
	if !ts.CatalogForTest().UpdateShardVerified(name, true) {
		t.Fatalf("UpdateShardVerified returned false")
	}

	if err := ts.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}

	entry, ok := ts.CatalogForTest().GetShard(name)
	if !ok {
		t.Fatalf("GetShard(%q) returned false", name)
	}
	if entry.Verified {
		t.Fatalf("Verified after Clear = true, want false")
	}
	if entry.ApproxNodes != 0 || entry.ApproxRels != 0 {
		t.Fatalf("catalog stats after Clear = nodes %d rels %d, want 0/0", entry.ApproxNodes, entry.ApproxRels)
	}
}
