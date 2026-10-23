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
	cfg.IdleTimeout = time.Hour // the idle sweeper never ticks during a test; tests drive the cap
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
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
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
// A read checkout cannot push the count over the cap: its check-in closes the
// shard that finds no room. The handles the sweeper's cap trims are the ones
// opened another way — a warm shard demoted to cold keeps the store it had
// (rotation never closes it), and so does a cold shard a write opened. So the
// premise is built by demotion: six rotations leave five cold shards with
// their stores open. The sweeper itself never ticks here (coldCapStore sets
// IdleTimeout to an hour), so the test drives its work directly.
func TestColdShardCapBoundsOpenHandles(t *testing.T) {
	t.Parallel()
	const cap = 2
	const shards = 6
	ts, signalTok := coldCapStore(t, shards, cap)

	// Each rotation demotes the warm shards older than ColdAfter; the shard it
	// just rotated out stays warm. Seven shards: one hot, one warm, five cold.
	if n := coldShardsWithOpenStore(ts); n != shards-1 {
		t.Fatalf("setup: %d cold shards hold a store after demotion, want %d", n, shards-1)
	}

	// What the sweeper does on its tick.
	ts.enforceColdShardCap()

	if n := coldShardsWithOpenStore(ts); n != cap {
		t.Fatalf("%d cold shards left open after the cap, want exactly %d", n, cap)
	}

	// The answer is still right with most shards closed: one node per rotated
	// shard, none on the current hot shard.
	got, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if got != shards {
		t.Fatalf("NodeCount after the cap closed shards = %d, want %d", got, shards)
	}
	nodes, err := ts.NodesByLabel(signalTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(nodes) != shards {
		t.Fatalf("NodesByLabel after the cap closed shards = %d nodes, want %d", len(nodes), shards)
	}

	// That data read reopened the closed shards through read checkouts; their
	// check-ins must hold the cap without waiting for the sweeper.
	if n := coldShardsWithOpenStore(ts); n != cap {
		t.Fatalf("%d cold shards open after a data read, want exactly the cap %d: the check-in bound did not hold", n, cap)
	}
}

// TestColdCapNeverClosesAShardBeingRead: the cap sorts by last access, and a
// shard with a reader in flight can look old. Closing it under the reader is
// the use-after-close the whole lifecycle is built to prevent.
func TestColdCapNeverClosesAShardBeingRead(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
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
