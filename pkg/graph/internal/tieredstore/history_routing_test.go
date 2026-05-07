package tieredstore

import (
	"errors"
	"testing"
	"time"

	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/index"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// allShardStoresWithLazyOpen previously called es.GetStoreForTest(ts) which opens a
// cold shard but does not bump activeReqs. closeIdleShards could then race
// the caller and close the BadgerStore mid-iteration. Verify the new
// checkout-based contract: while the caller is iterating the returned
// stores, closeIdleShards must skip every cold shard (activeReqs > 0).
// After the release callback is invoked, idle close is unblocked.
func TestTieredStore_AllShardStoresWithLazyOpen_PinsColdShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
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
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()
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

// resolveShardStore must pin the cold event shard it returns to mirror the
// allShardStoresWithLazyOpen contract used by VerifyShard. Reference and
// archive lookups stay no-op (those stores are never closed by idle-close)
// but must still return a non-nil release for caller-side defer parity.
func TestTieredStore_ResolveShardStore_PinsColdEventShard(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
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
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()
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

	// Reference shard never gets pinned (always live), but release must
	// still be a non-nil callable so admin callers can defer it
	// unconditionally.
	_, refRelease, err := ts.ResolveShardStoreForTest("reference")
	if err != nil {
		t.Fatalf("resolveShardStore(reference): %v", err)
	}
	if refRelease == nil {
		t.Fatal("resolveShardStore(reference) returned nil release")
	}
	refRelease()
}

// Close() must wait for in-flight checkouts to drain before closing event
// shard stores. Badger v4 WriteBatch.Flush blocks forever on a closed DB;
// closing while a long-running RunRepair / VerifyShard still holds a
// checkout would deadlock that caller.
func TestTieredStore_Close_WaitsForActiveCheckouts(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
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

// After Close marks ts.ClosedForTest(), fresh checkoutStore calls must return
// ErrStoreClosed on every shard tier. Without this, a goroutine racing
// Close past the activeReqs spin-wait could obtain a checkout on a
// shard whose store is about to be closed — Badger v4 WriteBatch.Flush
// then blocks forever (CLAUDE.md). The pre-fix shape only had the spin-
// wait, which is necessary but not sufficient.
func TestTieredStore_CheckoutStore_RefusesAfterClose(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := indexpkg.NewLabelRegistry()
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

// mustClosingTieredStore returns a TieredStore whose closed flag is set to
// true and whose hot event shard's BadgerStore is marked dbClosed — simulating
// the intermediate state where Close has set the flag but not yet called
// db.Close(). Uses a standalone store (not newTestTieredGraph) so there is no
// graph-owned t.Cleanup that would restore closed=false before the test ends.
// Callers must defer the returned cleanup.
func mustClosingTieredStore(t *testing.T) (*TieredStore, *EventShard, func()) {
	t.Helper()
	ts, err := NewTieredStore(TieredStoreConfig{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewTieredStore: %v", err)
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
// TieredStore is in the mid-Close state (closed=true, hot shard's BadgerStore
// marked dbClosed), Clear skips the event shard without calling db.DropAll.
// Pre-fix: Clear called es.Store().Clear() directly — DropAll on a BadgerStore
// whose db.Close() races concurrently is undefined behaviour and can hang.
// Post-fix: checkoutStore sees ts.ClosedForTest()=true, returns ErrStoreClosed, Clear
// continues without touching the store.
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
		// Any error indicates the guard fired (ErrStoreClosed propagated) or
		// the refShard.Clear() failed — both are unexpected here.
		if err != nil {
			t.Fatalf("Clear returned %v; expected nil (event shard should be skipped)", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Clear blocked: checkoutStore guard did not skip the closing event shard — would hang on concurrent db.DropAll + db.Close")
	}
}

// TestTieredStore_ListShards_ReportsEventShardNotOpen_WhenClosing verifies that
// when the TieredStore enters the mid-Close state between the RLock snapshot
// and the per-shard checkoutStore call, the hot event shard is reported as
// Open=false with zero counts — not Open=true with stale counts read from a
// closing BadgerStore.
// Pre-fix: ListShards called es.Store().NodeCount() directly — no pin, no
// check of ts.ClosedForTest(); the shard was counted as open even after Close started.
func TestTieredStore_ListShards_ReportsEventShardNotOpen_WhenClosing(t *testing.T) {
	ts, hot, cleanup := mustClosingTieredStore(t)
	defer cleanup()

	// Pre-load a count so that without the guard the shard would be reported
	// as Open=true with Nodes=1 (stale). With the guard it must be Open=false.
	hot.Store().SetNodeCountForTest(1)

	infos, err := ts.ListShards()
	if err != nil {
		t.Fatalf("ListShards returned error: %v", err)
	}

	for _, si := range infos {
		if si.Kind != ShardEvent {
			continue
		}
		if si.Open {
			t.Fatalf("event shard %q reported Open=true during simulated close; "+
				"pre-fix: checkoutStore was not called, so the stale nodeCount=1 "+
				"was read from a closing BadgerStore", si.Name)
		}
		if si.Nodes != 0 || si.Rels != 0 {
			t.Fatalf("event shard %q reported Nodes=%d Rels=%d during simulated close; "+
				"expected zero (counts must not be read from a closing store)", si.Name, si.Nodes, si.Rels)
		}
	}
}

// TestTieredStore_RebuildCatalog_DoesNotTouchClosingEventShard verifies that
// when ts.ClosedForTest()=true, RebuildCatalog's checkoutStore returns ErrStoreClosed
// and the catalog stat update for the event shard is skipped — rather than
// calling NodeCount/RelCount on a BadgerStore that Close is concurrently
// tearing down. Runs in a goroutine with a timeout to catch hangs.
func TestTieredStore_RebuildCatalog_DoesNotTouchClosingEventShard(t *testing.T) {
	ts, _, cleanup := mustClosingTieredStore(t)
	defer cleanup()

	done := make(chan error, 1)
	go func() { done <- ts.RebuildCatalog() }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RebuildCatalog returned %v; expected nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RebuildCatalog blocked: event shard was not skipped when closing")
	}
}
