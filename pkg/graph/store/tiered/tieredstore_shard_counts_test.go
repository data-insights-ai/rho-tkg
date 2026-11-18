package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func closeColdStores(t *testing.T, ts *Store) int {
	t.Helper()
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		if es.currentTier() == TierCold {
			shards = append(shards, es)
		}
	}
	ts.mu.RUnlock()

	closed := 0
	sealed := false
	for _, es := range shards {
		es.shardMu.Lock()
		if es.store != nil {
			sealed = es.snapshotCountsLocked(ts) || sealed
			if err := es.store.Close(); err != nil {
				es.shardMu.Unlock()
				t.Fatalf("close cold shard %s: %v", es.name, err)
			}
			es.clearStoreLocked(ts)
			closed++
		}
		es.shardMu.Unlock()
	}
	// Mirror what the real idle-close does: persist the seal. Without this the
	// catalog keeps the counts only in memory and a restart loses them.
	if sealed {
		ts.saveCatalogBestEffort("test close")
	}
	return closed
}

// closeAllColdStores closes every cold shard still open (the store's own sweep
// may have closed some already) and fails unless at least one cold shard
// exists and none is left open. Returns the number of cold shards.
func closeAllColdStores(t *testing.T, ts *Store) int {
	t.Helper()
	closeColdStores(t, ts)
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	cold := 0
	for _, es := range ts.eventShards {
		if es.currentTier() != TierCold {
			continue
		}
		cold++
		es.shardMu.Lock()
		open := es.store != nil
		es.shardMu.Unlock()
		if open {
			t.Fatalf("cold shard %s still open after closeColdStores", es.name)
		}
	}
	if cold == 0 {
		t.Fatal("fixture: no cold shard after four rotations with ColdAfter 1ms")
	}
	return cold
}

// coldTieredStore builds a store whose past event shards are cold and hold
// real nodes and relationships, which is the shape the count folds used to
// reopen: four shards, each with two Signal nodes and one relationship of type
// coldRelType between them.
const coldRelType = 1

func coldTieredStore(t *testing.T) (*Store, uint16) {
	t.Helper()
	dir := t.TempDir()

	cfg := openParallelCfg(dir)
	cfg.ColdAfter = time.Millisecond // every past shard demotes on the next rotate
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Two nodes and a relationship in each shard, then rotate so that shard
	// becomes a past one.
	for i := 0; i < 4; i++ {
		a := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		b := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		for _, n := range []*types.Node{a, b} {
			if err := ts.PutNode(n); err != nil {
				t.Fatalf("put node %d: %v", i, err)
			}
		}
		if err := ts.PutRelationship(types.NewRelationship(types.RelID(relGen.Generate()), coldRelType, a.ID(), b.ID())); err != nil {
			t.Fatalf("put rel %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	return ts, signalTok
}

// TestCountFoldsDoNotReopenClosedShards is the cost this cache exists to
// remove. Counting is the one question about a shard that needs no data from
// it, yet every fold reopened every closed cold shard to ask — and
// AllLabelCounts asks once per label, so the reopen was paid |labels| times.
// Measured on a real 26-shard store with 19 cold: 22.4s against 0.000s warm.
//
// The proof is that the answer survives the shard's FILES BEING MOVED AWAY.
// Counting open handles afterwards proves nothing, because a cold shard opened
// for a read is transiently closed again on check-in — the reopen leaves no
// trace to observe. A shard that cannot be opened at all can only be counted
// from what was recorded before it closed.
func TestCountFoldsDoNotReopenClosedShards(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ts, _ := coldTieredStore(t)

	before, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if before == 0 {
		t.Fatal("fixture produced no nodes; the test would prove nothing")
	}

	closeAllColdStores(t, ts)

	// Move every cold shard's directory out of reach. Nothing that has to open
	// them can succeed from here on.
	ts.mu.RLock()
	var moved []string
	for _, es := range ts.eventShards {
		if es.currentTier() == TierCold {
			moved = append(moved, es.path)
		}
	}
	dataDir := ts.dataDir
	ts.mu.RUnlock()
	for _, rel := range moved {
		full := filepath.Join(dataDir, rel)
		if err := os.Rename(full, full+".moved"); err != nil {
			t.Fatalf("move %s out of reach: %v", rel, err)
		}
	}

	after, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount had to reach the shard files, which are gone: %v", err)
	}
	if after != before {
		t.Fatalf("NodeCount = %d with %d cold shards unreadable, want %d from their recorded counts",
			after, len(moved), before)
	}

	// Put them back so Close() finds what it expects.
	for _, rel := range moved {
		full := filepath.Join(dataDir, rel)
		if err := os.Rename(full+".moved", full); err != nil {
			t.Fatalf("restore %s: %v", rel, err)
		}
	}
}

// TestCountFoldsAgreeWithTheShardsTheyReplace: the cached answer must equal the
// one an open store gives, per label and per relationship type, or the cache is
// just a faster wrong number.
func TestCountFoldsAgreeWithTheShardsTheyReplace(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ts, signalTok := coldTieredStore(t)

	counts := func() [4]int { // nodes, by label, rels, by rel type
		t.Helper()
		var c [4]int
		var err error
		if c[0], err = ts.NodeCount(); err != nil {
			t.Fatalf("NodeCount: %v", err)
		}
		if c[1], err = ts.NodeCountByLabel(signalTok); err != nil {
			t.Fatalf("NodeCountByLabel: %v", err)
		}
		if c[2], err = ts.RelationshipCount(); err != nil {
			t.Fatalf("RelationshipCount: %v", err)
		}
		if c[3], err = ts.RelCountByType(coldRelType); err != nil {
			t.Fatalf("RelCountByType: %v", err)
		}
		return c
	}

	open := counts()
	if want := [4]int{8, 8, 4, 4}; open != want {
		t.Fatalf("open counts (nodes, by label, rels, by type) = %v, want %v", open, want)
	}
	closeAllColdStores(t, ts)
	if cached := counts(); cached != open {
		t.Fatalf("counts from closed shards (nodes, by label, rels, by type) = %v, open %v", cached, open)
	}
}

// TestReopenedShardDropsItsSnapshot: once a shard is open again the store is
// the answer, and a stale copy must not outlive it.
func TestReopenedShardDropsItsSnapshot(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	t.Parallel()
	ts, _ := coldTieredStore(t)
	closeAllColdStores(t, ts)

	ts.mu.RLock()
	var cold *EventShard
	for _, es := range ts.eventShards {
		if es.currentTier() == TierCold {
			cold = es
			break
		}
	}
	ts.mu.RUnlock()
	if cold == nil {
		t.Fatal("fixture: no cold shard after four rotations with ColdAfter 1ms")
	}
	if cold.cachedCounts() == nil {
		t.Fatal("a closed cold shard kept no snapshot")
	}

	store, release, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer release()
	if store == nil {
		t.Fatal("checkout returned no store")
	}
	if cold.cachedCounts() != nil {
		t.Fatal("a reopened shard kept its snapshot; later counts could be answered from a stale copy")
	}
}
