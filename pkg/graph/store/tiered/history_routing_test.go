package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestEventShard_CheckoutStoreIfOpen_DoesNotLazyOpenClosedCold(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	hotName := hot.Name()
	ts.MuForTest().RUnlock()

	store, ok, err := hot.checkoutStoreIfOpen(ts)
	if err != nil {
		t.Fatalf("checkoutStoreIfOpen hot: %v", err)
	}
	if !ok || store == nil {
		t.Fatal("checkoutStoreIfOpen hot returned no store")
	}
	hot.checkinStore()

	demoteToCold(ts, hotName)
	store, ok, err = hot.checkoutStoreIfOpen(ts)
	if err != nil {
		t.Fatalf("checkoutStoreIfOpen open cold: %v", err)
	}
	if !ok || store == nil {
		t.Fatal("checkoutStoreIfOpen open cold returned no store")
	}
	if got := hot.ActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("open cold activeReqs = %d, want 1", got)
	}
	hot.checkinStore()

	hot.LockShardMuForTest()
	if err := hot.Store().Close(); err != nil {
		hot.UnlockShardMuForTest()
		t.Fatalf("close cold store: %v", err)
	}
	hot.SetStoreForTest(nil)
	hot.UnlockShardMuForTest()

	store, ok, err = hot.checkoutStoreIfOpen(ts)
	if err != nil {
		t.Fatalf("checkoutStoreIfOpen closed cold: %v", err)
	}
	if ok || store != nil {
		t.Fatalf("checkoutStoreIfOpen closed cold = (%v, %v), want no store", store, ok)
	}
	if got := hot.ActiveReqsForTest().Load(); got != 0 {
		t.Fatalf("closed cold activeReqs = %d, want 0", got)
	}
	hot.LockShardMuForTest()
	stillClosed := hot.Store() == nil
	hot.UnlockShardMuForTest()
	if !stillClosed {
		t.Fatal("checkoutStoreIfOpen lazy-opened a closed cold shard")
	}

	closedTS := newTestTieredStore(t)
	closedTS.MuForTest().RLock()
	closedHot := closedTS.HotShardForTest()
	closedTS.MuForTest().RUnlock()
	closedTS.ClosedForTest().Store(true)
	if _, _, err := closedHot.checkoutStoreIfOpen(closedTS); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("checkoutStoreIfOpen after close = %v, want ErrStoreClosed", err)
	}
	closedTS.ClosedForTest().Store(false)
}

func TestTieredStore_CheckedRoutersReturnStableArchivePlacement(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(rel.ID(), 0, rel); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	archive := ts.RefArchiveForTest().Load()
	if archive == nil {
		t.Fatal("ArchiveNode left refArchive nil")
	}

	nodeShard, nodeCheckin, nodeIsArchive, err := ts.shardForNodeIDCheckedWithArchive(n.ID())
	if err != nil {
		t.Fatalf("shardForNodeIDCheckedWithArchive: %v", err)
	}
	relShard, relCheckin, relIsArchive, err := ts.shardForRelIDCheckedWithArchive(rel.ID())
	if err != nil {
		nodeCheckin()
		t.Fatalf("shardForRelIDCheckedWithArchive: %v", err)
	}

	ts.refArchive.Store(nil)
	defer func() {
		ts.refArchive.Store(archive)
		relCheckin()
		nodeCheckin()
	}()

	if nodeShard != archive || !nodeIsArchive {
		t.Fatalf("node archive placement = (%v, %v), want archive + true", nodeShard == archive, nodeIsArchive)
	}
	if relShard != archive || !relIsArchive {
		t.Fatalf("rel archive placement = (%v, %v), want archive + true", relShard == archive, relIsArchive)
	}
	if ts.RefArchiveForTest().Load() != nil {
		t.Fatal("test did not clear live refArchive pointer")
	}
	if nodeShard.HasNodeID(n.ID().SnowflakeID()) && !nodeIsArchive {
		t.Fatal("node history routing would misclassify archive shard")
	}
	if relShard.HasRelID(rel.ID().SnowflakeID()) && !relIsArchive {
		t.Fatal("rel history routing would misclassify archive shard")
	}

	stores := []namedStore{
		{name: "nil"},
		{name: "archive", store: archive},
	}
	got, err := ts.findNodeInAnyShardStore(n.ID().SnowflakeID(), stores)
	if err != nil {
		t.Fatalf("findNodeInAnyShardStore archive node: %v", err)
	}
	if got != archive {
		t.Fatal("findNodeInAnyShardStore did not find archive node in pinned snapshot")
	}
	got, err = ts.findNodeInAnyShardStore(types.NodeID(0).SnowflakeID(), stores)
	if err != nil {
		t.Fatalf("findNodeInAnyShardStore missing node: %v", err)
	}
	if got != nil {
		t.Fatal("findNodeInAnyShardStore found a missing node")
	}
}

// allShardStoresWithLazyOpen previously used an unpinned lazy-open helper,
// which opened a cold shard but did not bump activeReqs. closeIdleShards could then race
// the caller and close the BadgerStore mid-iteration. Verify the new
// checkout-based contract: while the caller is iterating the returned
// stores, closeIdleShards must skip every cold shard (activeReqs > 0).
// After the release callback is invoked, idle close is unblocked.
func TestTieredStore_AllShardStoresWithLazyOpen_PinsColdShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	if err := ts.RotateHotShard(); err != nil {
		t.Fatal(err)
	}
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	if coldES == nil || coldES.Tier() != TierCold {
		t.Fatalf("expected cold shard for %q", hotName)
	}

	stores, release, err := ts.AllShardStoresWithLazyOpenForTest()
	if err != nil {
		t.Fatalf("allShardStoresWithLazyOpen: %v", err)
	}
	if len(stores) < 2 {
		t.Fatalf("expected at least reference + 1 event shard, got %d", len(stores))
	}
	if coldES.ActiveReqsForTest().Load() == 0 {
		t.Fatalf("expected cold shard to be pinned (activeReqs > 0) after lazy-open")
	}

	// Force idle-close attempt: must be skipped while pinned.
	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	coldES.LockShardMuForTest()
	storeWhilePinned := coldES.Store()
	coldES.UnlockShardMuForTest()
	if storeWhilePinned == nil {
		t.Fatal("closeIdleShards closed cold shard while RunRepair-style caller was still iterating")
	}

	release()
	if coldES.ActiveReqsForTest().Load() != 0 {
		t.Fatalf("activeReqs = %d after release, want 0", coldES.ActiveReqsForTest().Load())
	}

	// After release, closeIdleShards must succeed.
	coldES.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	coldES.LockShardMuForTest()
	storeAfterRelease := coldES.Store()
	coldES.UnlockShardMuForTest()
	if storeAfterRelease != nil {
		t.Fatal("closeIdleShards did not close cold shard after release")
	}
}

// resolveShardStore must pin every closeable handle it returns to mirror the
// allShardStoresWithLazyOpen contract used by VerifyShard. Reference and
// archive shards are not idle-closed, but Close still closes them.
func TestTieredStore_ResolveShardStore_PinsColdEventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	time.Sleep(2 * time.Millisecond)
	_ = ts.RotateHotShard()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	store, release, err := ts.ResolveShardStoreForTest(hotName)
	if err != nil {
		t.Fatalf("resolveShardStore(%q): %v", hotName, err)
	}
	if store == nil {
		t.Fatal("resolveShardStore returned nil store")
	}
	if release == nil {
		t.Fatal("resolveShardStore returned nil release")
	}
	if coldES.ActiveReqsForTest().Load() != 1 {
		t.Fatalf("activeReqs = %d after resolve, want 1", coldES.ActiveReqsForTest().Load())
	}

	ts.SetIdleTimeoutForTest(time.Millisecond)
	coldES.SetLastAccessForTest(0)
	ts.CloseIdleShardsForTest()
	coldES.LockShardMuForTest()
	storeWhilePinned := coldES.Store()
	coldES.UnlockShardMuForTest()
	if storeWhilePinned == nil {
		t.Fatal("closeIdleShards closed cold shard while VerifyShard-style caller held it")
	}

	release()
	if coldES.ActiveReqsForTest().Load() != 0 {
		t.Fatalf("activeReqs = %d after release, want 0", coldES.ActiveReqsForTest().Load())
	}

	// Reference shard must also be pinned because Close closes it.
	_, refRelease, err := ts.ResolveShardStoreForTest("reference")
	if err != nil {
		t.Fatalf("resolveShardStore(reference): %v", err)
	}
	if refRelease == nil {
		t.Fatal("resolveShardStore(reference) returned nil release")
	}
	if got := ts.RefActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("refActiveReqs after reference resolve = %d, want 1", got)
	}
	refRelease()
	if got := ts.RefActiveReqsForTest().Load(); got != 0 {
		t.Fatalf("refActiveReqs after reference release = %d, want 0", got)
	}
}

// Close() must wait for in-flight checkouts to drain before closing event
// shard stores. Badger v4 WriteBatch.Flush blocks forever on a closed DB;
// closing while a long-running RunRepair / VerifyShard still holds a
// checkout would deadlock that caller.
func TestTieredStore_Close_WaitsForActiveCheckouts(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	// Take a checkout on the hot shard and hold it across a Close call
	// running in a goroutine. Close must block until checkin is observed.
	ts.MuForTest().RLock()
	hotES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	store, err := hotES.CheckoutStoreForTest(ts)
	if err != nil {
		t.Fatalf("checkoutStore: %v", err)
	}
	_ = store

	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()

	// Close should still be waiting after a brief delay.
	select {
	case err := <-closed:
		t.Fatalf("Close returned before checkin (err=%v) — would race WriteBatch.Flush against db.Close()", err)
	case <-time.After(20 * time.Millisecond):
	}

	hotES.CheckinStoreForTest()

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after checkin: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s after checkin")
	}
}

func TestTieredStore_Close_WaitsForReferenceCheckout(t *testing.T) {
	ts := newTestTieredStore(t)
	_, release, err := ts.ResolveShardStoreForTest("reference")
	if err != nil {
		t.Fatalf("resolve reference: %v", err)
	}
	if got := ts.RefActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("refActiveReqs after resolve = %d, want 1", got)
	}

	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()

	select {
	case err := <-closed:
		t.Fatalf("Close returned before reference release (err=%v)", err)
	case <-time.After(20 * time.Millisecond):
	}

	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close after reference release: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s after reference release")
	}
}

func TestTieredStore_CheckoutArchiveLifecycle(t *testing.T) {
	t.Run("nil store", func(t *testing.T) {
		var ts *Store
		archive, release, err := ts.checkoutArchive()
		if !errors.Is(err, ErrNilStore) {
			t.Fatalf("checkoutArchive nil error = %v, want ErrNilStore", err)
		}
		if archive != nil {
			t.Fatal("checkoutArchive nil returned a store")
		}
		release()
	})

	t.Run("zero value store", func(t *testing.T) {
		var ts Store
		archive, release, err := ts.checkoutArchive()
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("checkoutArchive zero value error = %v, want ErrStoreClosed", err)
		}
		if archive != nil {
			t.Fatal("checkoutArchive zero value returned a store")
		}
		release()
	})

	t.Run("absent archive", func(t *testing.T) {
		ts := newTestTieredStore(t)
		archive, release, err := ts.checkoutArchive()
		if err != nil {
			t.Fatalf("checkoutArchive absent archive: %v", err)
		}
		if archive != nil {
			t.Fatal("checkoutArchive absent archive returned a store")
		}
		release()
		if got := ts.ArchiveActiveReqsForTest().Load(); got != 0 {
			t.Fatalf("archiveActiveReqs after absent release = %d, want 0", got)
		}
	})

	t.Run("background error", func(t *testing.T) {
		ts := newTestTieredStore(t)
		injected := errors.New("injected archive checkout background error")
		ts.recordBackgroundError(injected)
		archive, release, err := ts.checkoutArchive()
		if !errors.Is(err, injected) {
			t.Fatalf("checkoutArchive background error = %v, want injected error", err)
		}
		if archive != nil {
			t.Fatal("checkoutArchive background error returned a store")
		}
		release()
		if got := ts.ArchiveActiveReqsForTest().Load(); got != 0 {
			t.Fatalf("archiveActiveReqs after background error = %d, want 0", got)
		}
	})

	t.Run("closed store", func(t *testing.T) {
		ts := newTestTieredStore(t)
		if err := ts.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		archive, release, err := ts.checkoutArchive()
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("checkoutArchive closed error = %v, want ErrStoreClosed", err)
		}
		if archive != nil {
			t.Fatal("checkoutArchive closed returned a store")
		}
		release()
		if got := ts.ArchiveActiveReqsForTest().Load(); got != 0 {
			t.Fatalf("archiveActiveReqs after closed checkout = %d, want 0", got)
		}
	})
}

// After Close marks ts.ClosedForTest(), fresh checkoutStore calls must return
// ErrStoreClosed on every shard tier. Without this, a goroutine racing
// Close past the activeReqs spin-wait could obtain a checkout on a
// shard whose store is about to be closed — Badger v4 WriteBatch.Flush
// then blocks forever (CLAUDE.md). The pre-fix shape only had the spin-
// wait, which is necessary but not sufficient.
func TestTieredStore_CheckoutStore_RefusesAfterClose(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	hotES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	// Hold a checkout so Close blocks at the spin-wait; this lets us
	// observe the closed flag while Close is mid-flight.
	if _, err := hotES.CheckoutStoreForTest(ts); err != nil {
		t.Fatalf("checkout: %v", err)
	}

	closed := make(chan error, 1)
	go func() { closed <- ts.Close() }()

	// Wait for ts.ClosedForTest() to be observable. Close sets it before the
	// spin-wait begins, so polling here cannot deadlock against the
	// spin-wait.
	deadline := time.Now().Add(2 * time.Second)
	for !ts.ClosedForTest().Load() {
		if time.Now().After(deadline) {
			t.Fatal("ts.ClosedForTest() never set within 2s")
		}
		time.Sleep(time.Millisecond)
	}

	if _, err := hotES.CheckoutStoreForTest(ts); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("hot checkoutStore after closed=true: got %v, want ErrStoreClosed", err)
	}

	hotES.CheckinStoreForTest() // release the original checkout so Close finishes
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after checkin")
	}

	if _, err := hotES.CheckoutStoreForTest(ts); !errors.Is(err, ErrStoreClosed) {
		t.Errorf("hot checkoutStore post-Close: got %v, want ErrStoreClosed", err)
	}
}

// mustClosingTieredStore returns a Store whose closed flag is set to
// true and whose hot event shard's BadgerStore is marked dbClosed — simulating
// the intermediate state where Close has set the flag but not yet called
// db.Close(). Uses a standalone store (not newTestTieredGraph) so there is no
// graph-owned t.Cleanup that would restore closed=false before the test ends.
// Callers must defer the returned cleanup.
func mustClosingTieredStore(t *testing.T) (*Store, *EventShard, func()) {
	t.Helper()
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts.MuForTest().RLock()
	hot := ts.HotShardForTest()
	ts.MuForTest().RUnlock()

	// Mark the hot shard's BadgerStore as dbClosed. This makes any real DB
	// call (e.g. DropAll in Clear) return ErrDBClosed immediately rather than
	// hanging — which is the safe-failure behaviour we are guarding against.
	// Without the checkoutStore guard the caller would dereference this store
	// while Close is concurrently calling db.Close(), risking a hang on
	// WriteBatch.Flush or concurrent DropAll+Close undefined behaviour.
	hot.Store().SetDBClosedForTest(true)
	ts.ClosedForTest().Store(true)

	cleanup := func() {
		// Restore flags so ts.Close() can run its own shutdown cleanly.
		hot.Store().SetDBClosedForTest(false)
		ts.ClosedForTest().Store(false)
		_ = ts.Close()
	}
	return ts, hot, cleanup
}

// TestTieredStore_Clear_DoesNotTouchClosingEventShard verifies that when the
// Store is in the mid-Close state (closed=true, hot shard's BadgerStore
// marked dbClosed), Clear fails closed before calling db.DropAll.
// Pre-fix: Clear called es.Store().Clear() directly — DropAll on a BadgerStore
// whose db.Close() races concurrently is undefined behaviour and can hang.
// Post-fix: Clear sees ts.ClosedForTest()=true and returns ErrStoreClosed
// without touching the store.
//
// The test runs Clear in a goroutine and fails after 3 s to detect hangs that
// would not be caught by an err != nil assertion.
func TestTieredStore_Clear_DoesNotTouchClosingEventShard(t *testing.T) {
	ts, _, cleanup := mustClosingTieredStore(t)
	defer cleanup()

	done := make(chan error, 1)
	go func() { done <- ts.Clear() }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("Clear returned %v; expected ErrStoreClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Clear blocked: checkoutStore guard did not skip the closing event shard — would hang on concurrent db.DropAll + db.Close")
	}
}

// TestTieredStore_ListShards_ReportsEventShardNotOpen_WhenClosing verifies that
// when the Store enters the mid-Close state, ListShards fails closed instead of
// returning Open=true with stale counts read from a closing BadgerStore.
// Pre-fix: ListShards called es.Store().NodeCount() directly — no pin, no
// check of ts.ClosedForTest(); the shard was counted as open even after Close started.
func TestTieredStore_ListShards_ReportsEventShardNotOpen_WhenClosing(t *testing.T) {
	ts, hot, cleanup := mustClosingTieredStore(t)
	defer cleanup()

	// Pre-load a count so that without the guard the shard would be reported
	// as Open=true with Nodes=1 (stale). With the guard it must be Open=false.
	hot.Store().SetNodeCountForTest(1)

	if _, err := ts.ListShards(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("ListShards returned %v; expected ErrStoreClosed", err)
	}
}

// TestTieredStore_RebuildCatalog_DoesNotTouchClosingEventShard verifies that
// when ts.ClosedForTest()=true, RebuildCatalog returns ErrStoreClosed rather
// than calling NodeCount/RelCount on a BadgerStore that Close is concurrently
// tearing down. Runs in a goroutine with a timeout to catch hangs.
func TestTieredStore_RebuildCatalog_DoesNotTouchClosingEventShard(t *testing.T) {
	ts, _, cleanup := mustClosingTieredStore(t)
	defer cleanup()

	done := make(chan error, 1)
	go func() { done <- ts.RebuildCatalog() }()

	select {
	case err := <-done:
		if !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("RebuildCatalog returned %v; expected ErrStoreClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RebuildCatalog blocked: event shard was not skipped when closing")
	}
}
