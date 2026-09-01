package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldShardsWithOpenStore counts cold shards currently holding a Badger handle.
func coldShardsWithOpenStore(ts *Store) int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	n := 0
	for _, es := range ts.eventShards {
		if es.currentTier() != TierCold {
			continue
		}
		es.shardMu.Lock()
		if es.store != nil {
			n++
		}
		es.shardMu.Unlock()
	}
	return n
}

// coldCapStore builds a store with `shards` past windows, all demoted.
func coldCapStore(t *testing.T, shards, cap int) (*Store, uint16) {
	t.Helper()
	dir := t.TempDir()
	cfg := openParallelCfg(dir)
	cfg.ColdAfter = time.Millisecond
	cfg.MaxOpenColdShards = cap
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	for i := 0; i < shards; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	return ts, signalTok
}

// TestColdShardStaysOpenForTheNextRead is the regression this cap exists to
// allow fixing. A cold shard used to be closed the moment its read finished, so
// reading the same old data twice paid the open twice: measured through a
// caller as 16.9s to open a case whose signals live in demoted shards, then
// 7.5s again on the very next read. Retaining the handle is the whole point.
func TestColdShardStaysOpenForTheNextRead(t *testing.T) {
	ts, signalTok := coldCapStore(t, 4, 8)
	closeColdStores(t, ts)

	// A DATA read, not a count. Counts are answered from the sealed catalog
	// and never open a cold shard, which is a different (and also desirable)
	// property — see TestSealedCountsSurviveARestart.
	if _, err := ts.NodesByLabel(signalTok, QueryOpts{}); err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if n := coldShardsWithOpenStore(ts); n == 0 {
		t.Fatal("every cold shard was closed again as soon as the read finished; the next read pays the open again")
	}
}

// TestColdShardCapBoundsOpenHandles is the other half: retaining handles must
// not mean retaining ALL of them. A scan across a years-old store would
// otherwise hold one Badger per historical shard until the idle timer fired.
//
// The bound is EVENTUAL, not immediate. The cap runs on the idle sweeper rather
// than inside the checkout, because the checkout is reached from callers that
// already hold ts.mu — RebuildCatalog among them — and the cap needs ts.mu to
// see the shard list. Enforcing it there deadlocked the whole package. So the
// test drives the sweeper's work directly.
func TestColdShardCapBoundsOpenHandles(t *testing.T) {
	const cap = 2
	ts, signalTok := coldCapStore(t, 6, cap)
	closeColdStores(t, ts)

	// A data read that touches every shard, retaining a handle for each.
	if _, err := ts.NodesByLabel(signalTok, QueryOpts{}); err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if n := coldShardsWithOpenStore(ts); n <= cap {
		t.Skipf("only %d shards were retained; the cap has nothing to trim", n)
	}

	// What the sweeper does on its tick.
	ts.enforceColdShardCap()

	if n := coldShardsWithOpenStore(ts); n > cap {
		t.Fatalf("%d cold shards left open with a cap of %d", n, cap)
	}

	// And the answer is still right with most shards closed.
	got, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount second: %v", err)
	}
	if got == 0 {
		t.Fatal("NodeCount returned 0 after the cap closed shards")
	}
}

// TestColdCapNeverClosesAShardBeingRead: the cap sorts by last access, and a
// shard with a reader in flight can look old. Closing it under the reader is
// the use-after-close the whole lifecycle is built to prevent.
func TestColdCapNeverClosesAShardBeingRead(t *testing.T) {
	ts, _ := coldCapStore(t, 5, 1)
	closeColdStores(t, ts)

	ts.mu.RLock()
	var victim *EventShard
	for _, es := range ts.eventShards {
		if es.currentTier() == TierCold {
			victim = es
			break
		}
	}
	ts.mu.RUnlock()
	if victim == nil {
		t.Skip("no cold shard")
	}

	store, release, err := victim.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer release()
	if store == nil {
		t.Fatal("no store from checkout")
	}

	// Force the cap repeatedly while the reader is pinned.
	for i := 0; i < 3; i++ {
		ts.enforceColdShardCap()
	}

	victim.shardMu.Lock()
	stillOpen := victim.store != nil
	victim.shardMu.Unlock()
	if !stillOpen {
		t.Fatal("the cap closed a shard with a reader in flight")
	}
	if _, err := store.NodeCount(); err != nil {
		t.Fatalf("pinned store became unusable: %v", err)
	}
}
