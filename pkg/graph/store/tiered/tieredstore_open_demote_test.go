package tiered

import (
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdAfterAppliesAtOpen: demotion used to happen only as a side effect of
// the hot shard rotating, so setting ColdAfter on an existing store changed
// nothing until the next rotation — a week on a weekly window. An operator who
// turns it on must not have to wait for that, and the shards it demotes must
// never be opened in the meantime.
func TestColdAfterAppliesAtOpen(t *testing.T) {
	dir := t.TempDir()

	// Build a store with several past shards, all warm.
	cfg := openParallelCfg(dir)
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
	nodesBefore, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	warmBefore := 0
	shards, _ := ts.ListShards()
	for _, sh := range shards {
		if sh.Tier == TierWarm {
			warmBefore++
		}
	}
	if warmBefore == 0 {
		t.Fatal("fixture produced no warm shards; the test would prove nothing")
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen with ColdAfter set. Every past shard is older than the cutoff.
	cfg2 := openParallelCfg(dir)
	cfg2.ColdAfter = time.Nanosecond
	ts2, err := New(cfg2)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	cold, warm := 0, 0
	shards2, _ := ts2.ListShards()
	for _, sh := range shards2 {
		switch sh.Tier {
		case TierCold:
			cold++
		case TierWarm:
			warm++
		}
	}
	if cold == 0 {
		t.Fatalf("no shard was demoted at open; ColdAfter still waits for a rotation (warm=%d)", warm)
	}

	// A demoted shard must not have been opened on the way in — that is the
	// whole point — and the data must still be reachable.
	for _, es := range ts2.eventShards {
		if es.currentTier() != TierCold {
			continue
		}
		es.shardMu.Lock()
		open := es.store != nil
		es.shardMu.Unlock()
		if open {
			t.Fatalf("cold shard %s was opened at boot; demoting at open saves nothing if it is still mounted", es.name)
		}
	}
	if got, err := ts2.NodeCount(); err != nil || got != nodesBefore {
		t.Fatalf("NodeCount after demotion = %d (err %v), want %d", got, err, nodesBefore)
	}
}

// TestDemotionAtOpenIsPersisted: the tier decision must survive, or every start
// re-derives it and a store that has been running for months keeps rediscovering
// the same answer.
func TestDemotionAtOpenIsPersisted(t *testing.T) {
	dir := t.TempDir()

	cfg := openParallelCfg(dir)
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
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
	_ = ts.Close()

	cfg2 := openParallelCfg(dir)
	cfg2.ColdAfter = time.Nanosecond
	ts2, err := New(cfg2)
	if err != nil {
		t.Fatalf("reopen with ColdAfter: %v", err)
	}
	cold1 := 0
	s1, _ := ts2.ListShards()
	for _, sh := range s1 {
		if sh.Tier == TierCold {
			cold1++
		}
	}
	_ = ts2.Close()
	if cold1 == 0 {
		t.Fatal("nothing was demoted; nothing to persist")
	}

	// Reopen with a cutoff so distant that NOTHING would be demoted afresh. Any
	// shard still cold therefore came from the catalog, not from re-deriving
	// the decision — which is what "persisted" has to mean.
	//
	// Not reopened with ColdAfter unset: that now promotes cold shards back
	// deliberately, so it would test the reverse (see TestColdAfterIsReversible).
	cfg3 := openParallelCfg(dir)
	cfg3.ColdAfter = 24 * 365 * time.Hour
	ts3, err := New(cfg3)
	if err != nil {
		t.Fatalf("reopen with a distant cutoff: %v", err)
	}
	defer ts3.Close()
	cold2 := 0
	s2, _ := ts3.ListShards()
	for _, sh := range s2 {
		if sh.Tier == TierCold {
			cold2++
		}
	}
	if cold2 != cold1 {
		t.Fatalf("cold shards after restart = %d, want %d; the demotion was not persisted (nothing would have been demoted afresh at this cutoff)", cold2, cold1)
	}
}

// TestColdAfterIsReversible: demotion has to have a way back. Nothing else in
// the store ever promotes a shard, so without PromoteColdShardsAtOpen a store
// demoted once would stay demoted for good, and ColdAfter would be a one-way
// door rather than a setting an operator can try and undo.
func TestColdAfterIsReversible(t *testing.T) {
	dir := t.TempDir()

	cfg := openParallelCfg(dir)
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
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
	nodesBefore, err := ts.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	_ = ts.Close()

	// On.
	onCfg := openParallelCfg(dir)
	onCfg.ColdAfter = time.Nanosecond
	tsOn, err := New(onCfg)
	if err != nil {
		t.Fatalf("open with ColdAfter: %v", err)
	}
	cold := 0
	s1, _ := tsOn.ListShards()
	for _, sh := range s1 {
		if sh.Tier == TierCold {
			cold++
		}
	}
	_ = tsOn.Close()
	if cold == 0 {
		t.Fatal("nothing was demoted; there is nothing to reverse")
	}

	// Back again, explicitly.
	offCfg := openParallelCfg(dir)
	offCfg.PromoteColdShardsAtOpen = true
	tsOff, err := New(offCfg)
	if err != nil {
		t.Fatalf("reopen with PromoteColdShardsAtOpen: %v", err)
	}
	defer tsOff.Close()

	stillCold := 0
	s2, _ := tsOff.ListShards()
	for _, sh := range s2 {
		if sh.Tier == TierCold {
			stillCold++
		}
	}
	if stillCold != 0 {
		t.Fatalf("%d shards are still cold after PromoteColdShardsAtOpen; ColdAfter would be a one-way door", stillCold)
	}
	if got, err := tsOff.NodeCount(); err != nil || got != nodesBefore {
		t.Fatalf("NodeCount after promotion = %d (err %v), want %d", got, err, nodesBefore)
	}
}
