package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
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

// TestTieredStoreClear_FailedCloseDetachesAndFences (CLOSEFAULT): Clear's
// close of a warm or demoted shard's handle fails. Close may have closed the
// DB whatever it returned, so the handle is detached and the store fenced, as
// the idle close does; Clear stops there with the shard not cleared, a warm
// one cold in memory, and its row reads after RecoverBackgroundError. The
// durable pending marker stays, so the reopen finishes the Clear (CLEARSHARDS,
// finishInterruptedClear) and the row is gone. Catches the closed handle left
// installed with no fence: a second Clear passes the close (it returns nil)
// and clears a shard whose final flush failed with no fault recorded.
func TestTieredStoreClear_FailedCloseDetachesAndFences(t *testing.T) {
	t.Parallel()
	for _, warm := range []bool{false, true} {
		name := "cold-open"
		if warm {
			name = "warm"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{DataDir: t.TempDir(), RefLabels: []string{"Case", "User"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}
			ts, err := New(cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)
			signalTok, _ := reg.GetOrCreate("Signal")
			n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), signalTok, nil)
			if err := ts.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}
			old := ts.HotShardForTest().Name()
			if err := ts.ForceRotate(); err != nil {
				t.Fatalf("ForceRotate: %v", err)
			}
			ts.SweepHandlesForTest() // the warm-size swap settles first
			es := ts.EventShardsForTest()[old]
			if !warm {
				demoteToCold(ts, old) // the handle stays installed
			}
			handle, _ := es.StoreStateForTest()
			if handle == nil {
				t.Fatal("no handle installed on the rotated shard")
			}
			fault := errors.New("injected close fault")
			handle.FailNextCloseForTest(fault)

			err = ts.Clear()
			if warm {
				wantColdInMemory(t, ts, es)
			}
			wantCloseFaultFenced(t, ts, es, err, fault, n.ID())

			_ = ts.Close()
			re, err := New(cfg)
			if err != nil {
				t.Fatalf("New after the failed Clear: %v", err)
			}
			t.Cleanup(func() { _ = re.Close() })
			if _, err := re.GetNode(n.ID()); !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("GetNode after the reopen = %v, want ErrNodeNotFound (the open finishes the Clear)", err)
			}
		})
	}
}

// manifestFault breaks a shard open through the shard open hook: when arm
// says so for an open's config, the hook overwrites the shard's MANIFEST with
// garbage before badger reads it (badger refuses it, errBadMagic, and the
// recovery probe returns that as it is: not a truncation). It keeps the
// original; restore writes it back.
type manifestFault struct {
	mu       sync.Mutex
	path     string
	original []byte
}

func (m *manifestFault) breakAt(t *testing.T, dir string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.path != "" {
		return
	}
	path := filepath.Join(dir, "MANIFEST")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Errorf("read MANIFEST: %v", err)
		return
	}
	if err := os.WriteFile(path, []byte("not a badger manifest, broken by the test"), 0o600); err != nil {
		t.Errorf("break MANIFEST: %v", err)
		return
	}
	m.path, m.original = path, original
}

func (m *manifestFault) broken() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.path != ""
}

func (m *manifestFault) restore(t *testing.T) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.path == "" {
		t.Fatal("the MANIFEST was never broken: the fault did not fire")
	}
	if err := os.WriteFile(m.path, m.original, 0o600); err != nil {
		t.Fatal(err)
	}
	m.path = ""
}

// clearExitFix is a disk store with two warm shards rotated out (w1, w2),
// one row in each (a1, b1) and an unwritten ID in each window (a2, b2); the
// hot shard holds no row.
type clearExitFix struct {
	ts             *Store
	cfg            Config
	tok            uint16
	w1, w2         string
	a1, b1         *types.Node
	a2, b2         types.NodeID
	clearOpenOrder []string // the shards Clear opened, in order (the hook's record)
}

func newClearExitFix(t *testing.T) *clearExitFix {
	t.Helper()
	f := &clearExitFix{cfg: Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1}}
	ts, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ts = ts
	t.Cleanup(func() { _ = f.ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	f.tok, _ = reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	f.a1, f.a2 = types.NewNode(types.NodeID(gen.Generate()), f.tok, nil), types.NodeID(gen.Generate())
	if err := ts.PutNode(f.a1); err != nil {
		t.Fatal(err)
	}
	f.w1 = ts.HotShardForTest().Name()
	forceRotation(t, ts)
	f.b1, f.b2 = types.NewNode(types.NodeID(gen.Generate()), f.tok, nil), types.NodeID(gen.Generate())
	if err := ts.PutNode(f.b1); err != nil {
		t.Fatal(err)
	}
	f.w2 = ts.HotShardForTest().Name()
	forceRotation(t, ts)
	ts.SweepHandlesForTest()
	for _, name := range []string{f.w1, f.w2} {
		if es := ts.EventShardsForTest()[name]; es.Tier() != TierWarm {
			t.Fatalf("fixture: shard %s is %s, want warm", name, es.Tier())
		}
	}
	return f
}

// clearBreaking runs Clear with the second warm shard Clear opens broken at
// its first open that pick selects; it returns Clear's error and the broken
// shard's name.
func (f *clearExitFix) clearBreaking(t *testing.T, m *manifestFault, pick func(BadgerStoreConfig) bool) (string, error) {
	t.Helper()
	var mu sync.Mutex
	var broken string
	f.ts.SetShardOpenHookForTest(func(cfg BadgerStoreConfig) {
		name := filepath.Base(cfg.Dir)
		if name != f.w1 && name != f.w2 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if !slices.Contains(f.clearOpenOrder, name) {
			f.clearOpenOrder = append(f.clearOpenOrder, name)
		}
		if broken == "" && len(f.clearOpenOrder) == 2 && name == f.clearOpenOrder[1] && pick(cfg) {
			broken = name
			m.breakAt(t, cfg.Dir)
		}
	})
	err := f.ts.Clear()
	f.ts.SetShardOpenHookForTest(nil)
	return broken, err
}

func (f *clearExitFix) marker(t *testing.T, ts *Store) bool {
	t.Helper()
	v, err := ts.refShard.MetaGet(clearPendingKey)
	if err != nil {
		t.Fatal(err)
	}
	return v != nil
}

// TestTieredStoreClear_TemporaryOpenFailure (CLOSEFAULT, L1): Clear closes a
// warm shard and its temporary open of that shard fails (clearEventShard's
// exit after the close). Clear errors and records no background error; the
// shard is cold in memory without a handle, its catalog entry warm, its row
// kept; the shard cleared before it reads not-found; the durable pending
// marker stays. With the MANIFEST restored a row written now reads back,
// and after Close and New the open finishes the Clear: every row reads
// not-found, the new one too, and the marker is gone. Catches the exit
// leaving a warm shard without a handle (its next access a nil store).
func TestTieredStoreClear_TemporaryOpenFailure(t *testing.T) {
	t.Parallel()
	testClearExit(t, func(cfg BadgerStoreConfig) bool { return !cfg.ReadOnly }, false)
}

// TestTieredStoreClear_ReopenFailure (CLOSEFAULT, L1): as
// TestTieredStoreClear_TemporaryOpenFailure, but the shard's clear succeeds
// and the reopen of the cleared warm shard fails (its recovery probe): the
// shard is cold in memory without a handle, its catalog entry warm, its row
// cleared.
func TestTieredStoreClear_ReopenFailure(t *testing.T) {
	t.Parallel()
	testClearExit(t, func(cfg BadgerStoreConfig) bool { return cfg.ReadOnly }, true)
}

func testClearExit(t *testing.T, pick func(BadgerStoreConfig) bool, faultedCleared bool) {
	f := newClearExitFix(t)
	var m manifestFault
	broken, err := f.clearBreaking(t, &m, pick)
	t.Logf("Clear with %s broken: %v (opens %v)", broken, err, f.clearOpenOrder)
	if err == nil || broken == "" {
		t.Fatalf("Clear = %v with %q broken, want an error from the broken shard", err, broken)
	}
	if bg := f.ts.backgroundError(); bg != nil {
		t.Fatalf("background error %v, want none (no close failed)", bg)
	}
	es := f.ts.EventShardsForTest()[broken]
	wantColdInMemory(t, f.ts, es)
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("the broken shard has a handle")
	}
	if !f.marker(t, f.ts) {
		t.Fatal("the pending marker is gone after the failed Clear")
	}
	before, faulted, faultedLater := f.a1, f.b1, f.b2
	if broken == f.w1 {
		before, faulted, faultedLater = f.b1, f.a1, f.a2
	}
	if _, err := f.ts.GetNode(before.ID()); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("the row of the shard cleared before: %v, want ErrNodeNotFound", err)
	}
	m.restore(t)
	if _, err := f.ts.GetNode(faulted.ID()); faultedCleared != errors.Is(err, ErrNodeNotFound) || (!faultedCleared && err != nil) {
		t.Fatalf("the broken shard's row: %v, want cleared %v", err, faultedCleared)
	}
	late := types.NewNode(faultedLater, f.tok, nil)
	if err := f.ts.PutNode(late); err != nil {
		t.Fatalf("a write into the broken shard after the restore: %v", err)
	}
	if _, err := f.ts.GetNode(late.ID()); err != nil {
		t.Fatalf("the row written after the failed Clear: %v", err)
	}
	if err := f.ts.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := New(f.cfg)
	if err != nil {
		t.Fatalf("New after the failed Clear: %v", err)
	}
	f.ts = re
	for _, id := range []types.NodeID{f.a1.ID(), f.b1.ID(), late.ID()} {
		if _, err := re.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("GetNode %d after the reopen = %v, want ErrNodeNotFound (the open finishes the Clear)", id, err)
		}
	}
	if f.marker(t, re) {
		t.Fatal("the pending marker is still set after the reopen")
	}
}

// TestTieredStoreClear_SucceededLeavesNoMarker is the control of the two
// Clear exit tests: an unfaulted Clear leaves no marker, and a row written
// after it survives the reopen.
func TestTieredStoreClear_SucceededLeavesNoMarker(t *testing.T) {
	t.Parallel()
	f := newClearExitFix(t)
	if err := f.ts.Clear(); err != nil {
		t.Fatal(err)
	}
	if f.marker(t, f.ts) {
		t.Fatal("the pending marker is set after a Clear that succeeded")
	}
	for _, id := range []types.NodeID{f.a1.ID(), f.b1.ID()} {
		if _, err := f.ts.GetNode(id); !errors.Is(err, ErrNodeNotFound) {
			t.Fatalf("GetNode %d after Clear = %v, want ErrNodeNotFound", id, err)
		}
	}
	late := types.NewNode(f.b2, f.tok, nil)
	if err := f.ts.PutNode(late); err != nil {
		t.Fatal(err)
	}
	_ = f.ts.Close()
	re, err := New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.ts = re
	if _, err := re.GetNode(late.ID()); err != nil {
		t.Fatalf("the row written after Clear, after the reopen: %v", err)
	}
}

func TestTieredStoreClear_ClearsRestartedWarmShard(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
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
	t.Parallel()
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
