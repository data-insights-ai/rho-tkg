package tiered

// The hot + warm bound for the relationship temporal index on tiered
// (backlog 10): a cold shard keeps no relationship temporal index. A
// demotion frees it, the DDL never opens a cold shard to build it, a lazy
// cold open never rebuilds it, and a promotion back to warm rebuilds it from
// the anchoring reference shard.

import (
	"slices"
	"testing"
	"time"
)

type relTemporalBuildCounter interface{ RelTemporalIndexBuildsForTest() int64 }

func relTemporalBuilds(t *testing.T, s *BadgerStore) int64 {
	t.Helper()
	c, ok := any(s).(relTemporalBuildCounter)
	if !ok {
		t.Fatal("badger.Store has no RelTemporalIndexBuildsForTest hook")
	}
	return c.RelTemporalIndexBuildsForTest()
}

func eventShardNamed(t *testing.T, ts *Store, name string) *EventShard {
	t.Helper()
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	es := ts.eventShards[name]
	if es == nil {
		t.Fatalf("event shard %q not found", name)
	}
	return es
}

// A rotation that demotes a warm shard to cold frees that shard's
// relationship temporal index (the shard stays open; its entries and its
// definition go), while the reference, warm and hot shards keep theirs. A
// create or drop after the demotion does not build on the cold shard.
func TestTieredRelTemporalIndex_ColdDemotionFreesIndex(t *testing.T) {
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	const typ, later uint16 = 4, 5
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts) // first -> warm
	time.Sleep(5 * time.Millisecond)
	forceRotation(t, ts) // first -> cold (its window ended more than ColdAfter ago)
	cold := eventShardNamed(t, ts, first)
	if cold.currentTier() != TierCold || cold.store == nil {
		t.Fatalf("shard %s tier %s open=%v, want an open cold shard", first, cold.currentTier(), cold.store != nil)
	}
	if got, err := cold.store.RelTemporalIndexTypes(); err != nil || len(got) != 0 {
		t.Errorf("cold shard still lists %v (err %v), want its index freed", got, err)
	}
	builds := relTemporalBuilds(t, cold.store)
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(later); err != nil {
		t.Fatalf("create after demotion: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		got, err := s.RelTemporalIndexTypes()
		if err != nil {
			t.Fatal(err)
		}
		want := []uint16{typ, later}
		if name == first {
			want = []uint16{}
		}
		if !slices.Equal(got, want) {
			t.Errorf("shard %s lists %v, want %v", name, got, want)
		}
	}
	if got, _ := ts.RelTemporalIndexTypes(); !slices.Equal(got, []uint16{typ, later}) {
		t.Errorf("anchor lists %v, want [%d %d]", got, typ, later)
	}
	if err := relTemporalDDL(t, ts).DropRelTemporalIndex(later); err != nil {
		t.Fatalf("drop with a cold shard: %v", err)
	}
	if got := relTemporalBuilds(t, cold.store); got != builds {
		t.Errorf("cold shard built %d indexes during create/drop, want 0", got-builds)
	}
}

// Reopening a store whose shard turned cold never builds that shard's
// relationship temporal index: not at open (the shard is not opened), not on
// a create (the DDL does not open it), not when a read lazily opens it. A
// promotion back to warm rebuilds it from the anchor.
func TestTieredRelTemporalIndex_ColdShardsNeverBuildUntilPromoted(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	const typ, later uint16 = 4, 5
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts)
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	coldCfg := cfg
	coldCfg.ColdAfter = time.Millisecond
	coldCfg.MaxOpenColdShards = 4 // a cold shard anything opens stays open, so an open is visible
	ts, err = New(coldCfg)
	if err != nil {
		t.Fatalf("reopen with ColdAfter: %v", err)
	}
	cold := eventShardNamed(t, ts, first)
	if cold.currentTier() != TierCold || cold.store != nil {
		t.Fatalf("shard %s tier %s open=%v, want a closed cold shard", first, cold.currentTier(), cold.store != nil)
	}
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(later); err != nil {
		t.Fatalf("create with a closed cold shard: %v", err)
	}
	if cold.store != nil {
		t.Error("the create opened the cold shard")
	}
	s, release, err := cold.checkoutStoreForRead(ts)
	if err != nil {
		t.Fatalf("lazy open: %v", err)
	}
	if got := relTemporalBuilds(t, s); got != 0 {
		t.Errorf("lazy cold open built %d relationship temporal indexes, want 0", got)
	}
	if got, err := s.RelTemporalIndexTypes(); err != nil || len(got) != 0 {
		t.Errorf("lazily opened cold shard lists %v (err %v), want none", got, err)
	}
	release()
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	promoteCfg := cfg
	promoteCfg.PromoteColdShardsAtOpen = true
	ts, err = New(promoteCfg)
	if err != nil {
		t.Fatalf("reopen with promotion: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	warm := eventShardNamed(t, ts, first)
	if warm.currentTier() != TierWarm || warm.store == nil {
		t.Fatalf("shard %s tier %s open=%v, want an open warm shard", first, warm.currentTier(), warm.store != nil)
	}
	if got, err := warm.store.RelTemporalIndexTypes(); err != nil || !slices.Equal(got, []uint16{typ, later}) {
		t.Errorf("promoted shard lists %v (err %v), want [%d %d]", got, err, typ, later)
	}
}
