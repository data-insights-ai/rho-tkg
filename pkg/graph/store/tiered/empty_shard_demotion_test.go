package tiered

import (
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Demotion must release an unpinned empty disk shard without waiting for an idle sweep.
func TestEmptyShardDemotion_ClosesImmediately(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	cfg := tierClockCfg(t.TempDir(), clock)
	ts := openMintStore(t, cfg)
	old := ts.hotShard
	clock.set(julDay(26, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	old.shardMu.Lock()
	open := old.store != nil
	old.shardMu.Unlock()
	if open {
		t.Fatal("demoted empty shard remains open before idle sweep")
	}
	if old.currentTier() != TierCold || !old.noRows.Load() || !ts.catalogEntryNoRows(t, cfg, old.name) {
		t.Fatal("closed empty shard lacks cold tier or durable no-rows seal")
	}
	if got := ts.openColdShards.Load(); got != 0 {
		t.Fatalf("open cold handles = %d, want 0", got)
	}
}

func TestEmptyShardDemotion_WarmThenCold(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts := openMintStore(t, tierClockCfg(t.TempDir(), clock))
	old := ts.hotShard
	clock.set(julDay(23, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	if old.currentTier() != TierWarm || old.store == nil {
		t.Fatal("empty warm shard was closed")
	}
	clock.set(julDay(27, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	if old.currentTier() != TierCold || old.store != nil || !old.noRows.Load() {
		t.Fatal("empty warm-to-cold shard was not closed and sealed")
	}
	if got := ts.openColdShards.Load(); got != 0 {
		t.Fatalf("open cold handles = %d, want 0", got)
	}
}

func TestEmptyShardDemotion_RetainsProtectedHandles(t *testing.T) {
	for _, mode := range []string{"write-pin", "read-pin", "open-read-pin", "scope", "memory"} {
		t.Run(mode, func(t *testing.T) {
			clock := &tierClock{}
			clock.set(julDay(22, 12))
			cfg := tierClockCfg(t.TempDir(), clock)
			cfg.ChangeLog = mode == "scope"
			if mode == "memory" {
				cfg.DataDir, cfg.InMemory = "", true
			}
			ts := openMintStore(t, cfg)
			old, held := ts.hotShard, ts.hotShard.store
			release := func() {}
			var err error
			switch mode {
			case "write-pin":
				held, err = old.checkoutStore(ts)
				release = old.checkinStore
			case "read-pin":
				held, release, err = old.pinForRead(ts, true)
			case "open-read-pin":
				var ok bool
				held, release, ok, err = old.checkoutOpenStoreForRead(ts)
				if !ok {
					t.Fatal("open hot handle was not checked out")
				}
			case "scope":
				err = ts.BeginLogScope()
			}
			if err != nil {
				t.Fatal(err)
			}
			defer func() { release() }()
			clock.set(julDay(26, 12))
			if err := ts.checkRotation(); err != nil {
				t.Fatal(err)
			}
			if old.store != held {
				t.Fatal("protected handle changed during demotion")
			}
			if empty, err := held.HoldsNoRows(); err != nil || !empty {
				t.Fatalf("protected empty handle unusable: empty=%v, err=%v", empty, err)
			}
			if got := ts.openColdShards.Load(); got != 1 {
				t.Fatalf("retained cold handles = %d, want 1", got)
			}
			if mode == "scope" {
				if _, err := ts.CommitLogScope(); err != nil {
					t.Fatal(err)
				}
			}
			release()
			release = func() {}
			if mode != "memory" {
				ts.closeIdleShards()
				if old.store != nil {
					t.Fatal("released empty handle escaped existing idle cleanup")
				}
			}
		})
	}
}

func TestEmptyShardDemotion_HistoryAndLateWriteSurviveCopy(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	cfg := tierClockCfg(t.TempDir(), clock)
	ts := openMintStore(t, cfg)
	history := ts.hotShard
	id := types.NodeID(vtID(vtCounter(julDay(22, 12))))
	node := types.NewNode(id, mintSignalTok, nil)
	wantProperties := map[string]any{"sequence": int64(1<<54 + 7), "present": true, "evidence": []byte{0, 255, 7, 0}}
	for key, value := range wantProperties {
		if err := node.SetProperty(key, value); err != nil {
			t.Fatal(err)
		}
	}
	wantTemporal := types.TemporalMetadata{
		ValidFrom: types.Instant(julDay(22, 12).UnixMilli()), ValidTo: types.Instant(julDay(22, 13).UnixMilli()),
		TxFrom: 101, TxTo: 202, CreatedAt: 99, UpdatedAt: 101, CreatedBy: "masked-source", UpdatedBy: "masked-revision",
	}
	node.SetTemporal(&wantTemporal)
	node.SetVersion(7)
	if err := ts.PutNode(node); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(id, 7, node); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(id); err != nil {
		t.Fatal(err)
	}
	if count, err := history.store.NodeCount(); err != nil || count != 0 {
		t.Fatalf("history fixture current nodes = %d, %v", count, err)
	}
	clock.set(julDay(23, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	empty := ts.hotShard
	clock.set(julDay(28, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	if history.store == nil || history.noRows.Load() {
		t.Fatal("history-only shard was treated as empty")
	}
	if empty.store != nil || !empty.noRows.Load() {
		t.Fatal("empty shard was not closed")
	}
	late := types.NodeID(vtID(vtCounter(julDay(23, 12))))
	putMintNode(t, ts, late, mintSignalTok)
	if empty.noRows.Load() || ts.catalogEntryNoRows(t, cfg, empty.name) {
		t.Fatal("late write did not persistently clear no-rows seal")
	}
	assertRows := func(s *Store) {
		t.Helper()
		nodes, err := s.AllNodes(QueryOpts{Depth: DepthAll})
		if err != nil || len(nodes) != 1 || nodes[0].ID() != late {
			t.Fatalf("current set = %v, %v; want only late row", nodes, err)
		}
		h, err := s.GetNodeHistory(id)
		if err != nil || len(h) != 1 {
			t.Fatalf("history = %d rows, %v; want original version", len(h), err)
		}
		version, err := s.GetNodeVersion(id, 7)
		if err != nil {
			t.Fatal(err)
		}
		for _, got := range []*types.Node{h[0], version} {
			if got.ID() != id || got.Version() != 7 || got.PrimaryLabelToken().Value() != mintSignalTok || got.LabelTokenCount() != 1 {
				t.Fatalf("historical identity/version/labels changed: %v", got)
			}
			if !reflect.DeepEqual(got.PropertiesMap(), wantProperties) {
				t.Fatalf("historical typed payload = %#v, want %#v", got.PropertiesMap(), wantProperties)
			}
			if temporal, ok := got.TemporalByValue(); !ok || temporal != wantTemporal {
				t.Fatalf("historical temporal metadata = %+v (present=%v), want %+v", temporal, ok, wantTemporal)
			}
		}
	}
	assertRows(ts)
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	copyDir := t.TempDir()
	copyTree(t, cfg.DataDir, copyDir)
	cfg.DataDir = copyDir
	reopened := openMintStore(t, cfg)
	assertRows(reopened)
}

func TestEmptyShardDemotion_FailedCatalogKeepsOldHandle(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts := openMintStore(t, tierClockCfg(t.TempDir(), clock))
	old, handle := ts.hotShard, ts.hotShard.store
	saved := ts.catalog.path
	ts.catalog.path = filepath.Join(t.TempDir(), "missing", "catalog.json")
	clock.set(julDay(26, 12))
	err := ts.checkRotation()
	ts.catalog.path = saved
	if err == nil || ts.hotShard != old || old.store != handle || old.currentTier() != TierHot {
		t.Fatalf("failed rotation changed old hot handle: %v", err)
	}
	if empty, err := handle.HoldsNoRows(); err != nil || !empty {
		t.Fatalf("old handle after failure = %v, %v", empty, err)
	}
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	if old.store != nil || !old.noRows.Load() {
		t.Fatal("same-clock retry did not close empty demoted handle")
	}
}

func TestEmptyShardDemotion_CloseFailureDetachesAndFences(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts := openMintStore(t, tierClockCfg(t.TempDir(), clock))
	old, handle := ts.hotShard, ts.hotShard.store
	clock.set(julDay(23, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	// A failed metadata-only flush leaves pending work while HoldsNoRows stays
	// true; without pending work, Badger's final flush returns before its guard.
	seedFailure := errors.New("seed pending metadata")
	handle.FailNextFlushForTest(seedFailure)
	if err := handle.MetaSetDurableRaising("empty-close-fixture", []byte{1}, 0); !errors.Is(err, seedFailure) {
		t.Fatalf("metadata fixture flush = %v, want injected failure", err)
	}
	if empty, err := handle.HoldsNoRows(); err != nil || !empty {
		t.Fatalf("metadata fixture is not row-empty: %v, %v", empty, err)
	}
	select {
	case <-handle.GCDoneForTest():
		t.Fatal("fixture requires the existing disk GC lifecycle channel to be open")
	default:
	}
	// Hold the final flush after the emptiness probe, then use the existing
	// idle-close fixture's DB-closed flag to fail Close rather than the probe.
	handle.LockFlushMuForTest()
	locked := true
	defer func() {
		if locked {
			handle.UnlockFlushMuForTest()
		}
	}()
	clock.set(julDay(27, 12))
	rotated := make(chan error, 1)
	go func() { rotated <- ts.checkRotation() }()
	select {
	case <-handle.GCDoneForTest(): // Close signalled stop; its final flush is blocked.
	case <-time.After(5 * time.Second):
		t.Fatal("rotation did not reach Close")
	}
	handle.SetDBClosedForTest(true)
	handle.UnlockFlushMuForTest()
	locked = false
	select {
	case err := <-rotated:
		// The rotation is committed: the write that triggered it does not
		// report the cleanup's fault; the fence below does, as after a failed
		// idle close.
		if err != nil {
			t.Fatalf("rotation error = %v, want nil (the fault goes to the fence)", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("faulted rotation did not finish")
	}
	if err := ts.backgroundError(); !errors.Is(err, badgerv4.ErrDBClosed) || !strings.Contains(err.Error(), "close empty demoted shard") {
		t.Fatalf("background error = %v, want the demotion Close failure", err)
	}
	if ts.hotShard == old || ts.hotShard.name != "2026-07-27" || old.currentTier() != TierCold || old.store != nil {
		t.Fatal("Close failure rolled back topology or retained the closed handle")
	}
	if entry, ok := ts.catalog.GetShard(old.name); !ok || entry.Tier != TierCold {
		t.Fatalf("committed catalog lost old cold shard: %+v", entry)
	}
	if got := ts.openColdShards.Load(); got != 0 {
		t.Fatalf("cold handles after failed Close = %d, want 0", got)
	}
	for range 2 {
		if _, err := old.checkoutStore(ts); !errors.Is(err, badgerv4.ErrDBClosed) {
			if err == nil {
				old.checkinStore()
			}
			t.Fatalf("old checkout escaped sticky Close fault: %v", err)
		}
		if _, err := ts.hotShard.checkoutStore(ts); !errors.Is(err, badgerv4.ErrDBClosed) {
			if err == nil {
				ts.hotShard.checkinStore()
			}
			t.Fatalf("new hot checkout escaped sticky Close fault: %v", err)
		}
	}
	if err := ts.Close(); !errors.Is(err, badgerv4.ErrDBClosed) {
		t.Fatalf("owner Close lost the sticky fault: %v", err)
	}
}

func TestEmptyShardDemotion_ProbeFailureRetainsHandle(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts := openMintStore(t, tierClockCfg(t.TempDir(), clock))
	old, handle := ts.hotShard, ts.hotShard.store
	clock.set(julDay(23, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	handle.SetDBClosedForTest(true)
	clock.set(julDay(27, 12))
	err := ts.checkRotation()
	handle.SetDBClosedForTest(false)
	if err != nil {
		t.Fatalf("rotation error = %v, want nil: a failed probe keeps the handle and fails no write", err)
	}
	putMintNode(t, ts, types.NodeID(vtID(vtCounter(julDay(27, 13)))), mintSignalTok)
	if ts.hotShard.name != "2026-07-27" || old.currentTier() != TierCold || old.store != handle || old.noRows.Load() {
		t.Fatal("probe failure changed the retained handle or claimed emptiness")
	}
	if got := ts.openColdShards.Load(); got != 1 {
		t.Fatalf("cold handles after failed probe = %d, want 1", got)
	}
	if err := ts.backgroundError(); err != nil {
		t.Fatalf("probe failure invented a Close fault: %v", err)
	}
	if empty, err := handle.HoldsNoRows(); err != nil || !empty {
		t.Fatalf("retained handle unusable after clearing injected probe fault: %v, %v", empty, err)
	}
}

// A shard whose only keys are uniqueness claims holds no row: a claim no row
// backs is ignored by every reader, so demotion closes and seals it, and the
// claim stays on disk for a later write to the shard.
func TestEmptyShardDemotion_ClaimsOnlyShardIsEmpty(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	cfg := tierClockCfg(t.TempDir(), clock)
	ts := openMintStore(t, cfg)
	old := ts.hotShard
	claim := storeutil.UniqueOwnerClaimKey(mintSignalTok, "host", "h1")
	if err := old.store.DBForTest().Update(func(txn *badgerv4.Txn) error {
		return txn.Set(claim, make([]byte, 8))
	}); err != nil {
		t.Fatal(err)
	}
	if empty, err := old.store.HoldsNoRows(); err != nil || !empty {
		t.Fatalf("claims-only shard HoldsNoRows = %v, %v; want empty", empty, err)
	}
	clock.set(julDay(26, 12))
	if err := ts.checkRotation(); err != nil {
		t.Fatal(err)
	}
	if old.store != nil || !old.noRows.Load() {
		t.Fatal("claims-only shard was not closed and sealed")
	}
	putMintNode(t, ts, types.NodeID(vtID(vtCounter(julDay(22, 13)))), mintSignalTok)
	if old.noRows.Load() || old.store == nil {
		t.Fatal("late write did not reopen the claims-only shard")
	}
	if err := old.store.DBForTest().View(func(txn *badgerv4.Txn) error {
		_, err := txn.Get(claim)
		return err
	}); err != nil {
		t.Fatalf("claim lost across the empty close: %v", err)
	}
}
