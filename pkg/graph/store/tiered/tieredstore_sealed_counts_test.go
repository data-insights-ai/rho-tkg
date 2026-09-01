package tiered

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestSealedCountsSurviveARestart is the whole point of sealing. A shard whose
// window has closed cannot change — every write path opens it first — so its
// counts are settled for good. Recomputing them at each start means opening
// every cold shard to rediscover numbers that were fixed, sometimes months ago.
//
// Proven the same way as the in-memory cache: the counts must survive the
// shard's FILES BEING MOVED AWAY. A count that still answers when the shard
// cannot be opened can only have come from the catalog.
func TestSealedCountsSurviveARestart(t *testing.T) {
	dir := t.TempDir()

	cfg := openParallelCfg(dir)
	cfg.ColdAfter = time.Millisecond
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	for i := 0; i < 4; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	want, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	// Close the cold shards, which seals their counts into the catalog.
	if n := closeColdStores(t, ts); n == 0 {
		t.Skip("no cold shards were open; nothing to seal")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A NEW process. Its in-memory snapshots start empty; only the catalog
	// carries anything forward.
	cfg2 := openParallelCfg(dir)
	cfg2.ColdAfter = time.Millisecond
	ts2, err := New(cfg2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	// Only the shards whose counts were SEALED. A shard demoted for the first
	// time at this open has never been closed with a snapshot, so it has no
	// counts to adopt and must legitimately be opened once — after which it
	// seals and is free forever. The claim under test is narrower than "no
	// cold shard is ever opened": it is that a SEALED one need not be.
	sealed := map[string]bool{}
	for _, e := range ts2.catalog.EventShards() {
		if e.CountsSealed {
			sealed[e.Name] = true
		}
	}
	var moved []string
	ts2.mu.RLock()
	for _, es := range ts2.eventShards {
		if es.currentTier() == TierCold && sealed[es.name] {
			moved = append(moved, es.path)
		}
	}
	dataDir := ts2.dataDir
	ts2.mu.RUnlock()
	if len(moved) == 0 {
		t.Fatal("no shard carried sealed counts across the restart; the seal did not survive")
	}
	for _, rel := range moved {
		full := filepath.Join(dataDir, rel)
		if err := os.Rename(full, full+".moved"); err != nil {
			t.Fatalf("move %s out of reach: %v", rel, err)
		}
	}

	got, err := ts2.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount had to open the shard files, which are gone: %v", err)
	}
	if got != want {
		t.Fatalf("NodeCount after restart = %d with %d SEALED shards unreadable, want %d from the catalog",
			got, len(moved), want)
	}

	for _, rel := range moved {
		full := filepath.Join(dataDir, rel)
		if err := os.Rename(full+".moved", full); err != nil {
			t.Fatalf("restore %s: %v", rel, err)
		}
	}
}

// TestOpeningAShardUnsealsIt: a shard that is open can be written to, so a
// sealed count must not outlive the close it described. Otherwise a crash
// between a write and the next close would leave the following start adopting
// counts from before the write.
func TestOpeningAShardUnsealsIt(t *testing.T) {
	dir := t.TempDir()
	cfg := openParallelCfg(dir)
	cfg.ColdAfter = time.Millisecond
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer ts.Close()

	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	for i := 0; i < 3; i++ {
		if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	closeColdStores(t, ts)

	var cold *EventShard
	ts.mu.RLock()
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

	sealedBefore := false
	for _, e := range ts.catalog.EventShards() {
		if e.Name == cold.name {
			sealedBefore = e.CountsSealed
		}
	}
	if !sealedBefore {
		t.Fatal("closing a cold shard did not seal its counts")
	}

	// The general checkout is the one write paths use.
	if _, err := cold.checkoutStore(ts); err != nil {
		t.Fatalf("checkout: %v", err)
	}
	defer cold.checkinStore()

	for _, e := range ts.catalog.EventShards() {
		if e.Name == cold.name && e.CountsSealed {
			t.Fatal("a shard opened for writing kept its sealed counts; a crash before the next close would leave them adopted as exact")
		}
	}
}
