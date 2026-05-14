package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- F3: Store.Clear must wipe store-level indexes ---

func blockedMetadataPath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "metadata-dir")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatalf("Mkdir metadata dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "child"), []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile metadata child: %v", err)
	}
	return dir
}

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

func TestTieredStoreClear_VectorMetadataFailureDoesNotClearEntities(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	ts.vectorIdxFile = blockedMetadataPath(t)
	if err := ts.Clear(); err == nil {
		t.Fatal("Clear returned nil for blocked vector metadata deletion")
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after failed Clear = %v, want original node", err)
	}
	if _, err := ts.SearchNearestNodes(caseTok, "vec", []float32{1, 0}, 1, QueryOpts{}); err != nil {
		t.Fatalf("SearchNearestNodes after failed Clear = %v, want original index", err)
	}
}

func TestTieredStoreClear_TemporalMetadataFailureDoesNotClearEntities(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	ts.temporalIdxFile = blockedMetadataPath(t)
	if err := ts.Clear(); err == nil {
		t.Fatal("Clear returned nil for blocked temporal metadata deletion")
	}
	if _, err := ts.GetNode(n.ID()); err != nil {
		t.Fatalf("GetNode after failed Clear = %v, want original node", err)
	}
	if got := ts.TempIdxLabelsForTest(); len(got) != 1 || got[0] != caseTok {
		t.Fatalf("TempIdxLabels after failed Clear = %#v, want [%d]", got, caseTok)
	}
}

func TestTieredStoreClear_CheckoutFailureRestoresIndexMetadata(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := n.SetProperty("vec", []float32{1, 0}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "vec", 2, DistanceCosine); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}
	if err := ts.CreateTemporalIndex(caseTok); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}

	injected := errors.New("synthetic checkout failure")
	ts.recordBackgroundError(injected)
	if err := ts.Clear(); !errors.Is(err, injected) {
		t.Fatalf("Clear error = %v, want injected checkout failure", err)
	}

	vectorDefs, err := loadVectorIndexFile(ts.vectorIdxFile)
	if err != nil {
		t.Fatalf("loadVectorIndexFile after failed Clear: %v", err)
	}
	if len(vectorDefs) != 1 || vectorDefs[0].LabelToken != caseTok || vectorDefs[0].PropertyKey != "vec" {
		t.Fatalf("vector index defs after failed Clear = %#v, want original vec definition", vectorDefs)
	}
	temporalDefs, err := loadTemporalIndexFile(ts.temporalIdxFile)
	if err != nil {
		t.Fatalf("loadTemporalIndexFile after failed Clear: %v", err)
	}
	if len(temporalDefs.TemporalLabels) != 1 || temporalDefs.TemporalLabels[0] != caseTok {
		t.Fatalf("temporal index defs after failed Clear = %#v, want original label %d", temporalDefs, caseTok)
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

func TestTieredStoreClearEventShard_PropagatesCheckoutError(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	injected := errors.New("synthetic lifecycle failure")
	ts.recordBackgroundError(injected)

	err := ts.clearEventShard(ts.HotShardForTest())
	if !errors.Is(err, injected) {
		t.Fatalf("clearEventShard error = %v, want injected lifecycle failure", err)
	}
}

func TestTieredStoreClearEventShard_ClosedFailsClosed(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	ts.ClosedForTest().Store(true)

	err := ts.clearEventShard(ts.HotShardForTest())
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("clearEventShard on closed store = %v, want ErrStoreClosed", err)
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

func TestTieredStoreClear_CatalogSaveFailureKeepsLiveCatalogCleared(t *testing.T) {
	ts := newDiskTestTieredStore(t)
	name := ts.HotShardForTest().Name()
	if !ts.CatalogForTest().UpdateShardStats(name, 7, 3) {
		t.Fatalf("UpdateShardStats returned false")
	}
	if !ts.CatalogForTest().UpdateShardVerified(name, true) {
		t.Fatalf("UpdateShardVerified returned false")
	}

	ts.catalog.path = blockedMetadataPath(t)
	if err := ts.Clear(); err == nil {
		t.Fatal("Clear returned nil for blocked catalog save")
	}

	entry, ok := ts.CatalogForTest().GetShard(name)
	if !ok {
		t.Fatalf("GetShard(%q) returned false", name)
	}
	if entry.Verified {
		t.Fatalf("Verified after failed catalog save = true, want false")
	}
	if entry.ApproxNodes != 0 || entry.ApproxRels != 0 {
		t.Fatalf("catalog stats after failed catalog save = nodes %d rels %d, want 0/0", entry.ApproxNodes, entry.ApproxRels)
	}
}
