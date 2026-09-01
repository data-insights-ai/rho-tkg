package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// coldStoresOpen reports how many cold shards currently hold an open Badger.
func coldStoresOpen(ts *Store) int {
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
	for _, es := range shards {
		es.shardMu.Lock()
		if es.store != nil {
			es.snapshotCountsLocked()
			if err := es.store.Close(); err != nil {
				es.shardMu.Unlock()
				t.Fatalf("close cold shard %s: %v", es.name, err)
			}
			es.store = nil
			es.readTransientOpen = false
			closed++
		}
		es.shardMu.Unlock()
	}
	return closed
}

// coldTieredStore builds a store whose past event shards are cold and hold
// real nodes, which is the shape the count folds used to reopen.
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

	// A node in each shard, then rotate so that shard becomes a past one.
	for i := 0; i < 4; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put node %d: %v", i, err)
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
	ts, _ := coldTieredStore(t)

	before, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if before == 0 {
		t.Fatal("fixture produced no nodes; the test would prove nothing")
	}

	if n := closeColdStores(t, ts); n == 0 {
		t.Skip("no cold shards were open; nothing to demonstrate")
	}

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
	if len(moved) == 0 {
		t.Skip("no cold shards to move")
	}
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
	ts, signalTok := coldTieredStore(t)

	token := signalTok

	openNodes, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	openByLabel, err := ts.NodeCountByLabel(token)
	if err != nil {
		t.Fatalf("NodeCountByLabel: %v", err)
	}
	openRels, err := ts.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount: %v", err)
	}

	closeColdStores(t, ts)

	cachedNodes, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount cached: %v", err)
	}
	cachedByLabel, err := ts.NodeCountByLabel(token)
	if err != nil {
		t.Fatalf("NodeCountByLabel cached: %v", err)
	}
	cachedRels, err := ts.RelationshipCount()
	if err != nil {
		t.Fatalf("RelationshipCount cached: %v", err)
	}

	if cachedNodes != openNodes {
		t.Fatalf("NodeCount: cached %d, open %d", cachedNodes, openNodes)
	}
	if cachedByLabel != openByLabel {
		t.Fatalf("NodeCountByLabel: cached %d, open %d", cachedByLabel, openByLabel)
	}
	if cachedRels != openRels {
		t.Fatalf("RelationshipCount: cached %d, open %d", cachedRels, openRels)
	}
}

// TestReopenedShardDropsItsSnapshot: once a shard is open again the store is
// the answer, and a stale copy must not outlive it.
func TestReopenedShardDropsItsSnapshot(t *testing.T) {
	ts, _ := coldTieredStore(t)
	closeColdStores(t, ts)

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
		t.Skip("no cold shard to exercise")
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
