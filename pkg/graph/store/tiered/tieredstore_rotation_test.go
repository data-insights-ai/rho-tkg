package tiered

import (
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStore_Rotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write event node before rotation.
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	oldHotName := ts.HotShardForTest().Name()
	forceRotation(t, ts)

	// Verify new hot shard created.
	if ts.HotShardForTest().Name() == oldHotName {
		t.Error("hot shard name should change after rotation")
	}
	if ts.HotShardForTest().Tier() != TierHot {
		t.Errorf("new hot shard tier = %q, want %q", ts.HotShardForTest().Tier(), TierHot)
	}
	if len(ts.EventShardsForTest()) != 2 {
		t.Errorf("eventShards count = %d, want 2", len(ts.EventShardsForTest()))
	}

	// Old shard should be warm.
	oldShard, ok := ts.EventShardsForTest()[oldHotName]
	if !ok {
		t.Fatal("old shard should still be in eventShards map")
	}
	if oldShard.Tier() != TierWarm {
		t.Errorf("old shard tier = %q, want %q", oldShard.Tier(), TierWarm)
	}
	if !oldShard.ReadOnlyForTest() {
		t.Error("old shard should be readOnly")
	}
}

func TestTieredStore_RotationIdempotent(t *testing.T) {
	ts := newTestTieredStore(t)

	// Expire hot shard.
	ts.MuForTest().Lock()
	ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts.MuForTest().Unlock()

	// Concurrent checkRotation calls should not double-rotate.
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = ts.CheckRotationForTest()
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: checkRotation error: %v", i, err)
		}
	}

	// Should have exactly 2 shards: 1 warm + 1 hot.
	if len(ts.EventShardsForTest()) != 2 {
		t.Errorf("eventShards = %d, want 2 (single rotation)", len(ts.EventShardsForTest()))
	}
}

func TestTieredStore_WarmShardStillReadable(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write event node before rotation.
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	forceRotation(t, ts)

	// Entity from warm shard should still be readable.
	got, err := ts.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode from warm shard: %v", err)
	}
	if got.ID() != n1.ID() {
		t.Error("node ID mismatch from warm shard")
	}
}

func TestTieredStore_WriteAfterRotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write before rotation.
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n1)

	forceRotation(t, ts)
	newHotStore := ts.HotShardForTest().Store()

	// Write after rotation.
	n2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// n2 should be in new hot shard, not the warm shard.
	if !newHotStore.HasNodeID(n2.ID().SnowflakeID()) {
		t.Error("post-rotation node should be in new hot shard")
	}

	// Both nodes should be readable.
	all, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Errorf("AllNodes = %d, want 2", len(all))
	}
}

func TestTieredStore_RotationPreservesEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)

	oldHotName := ts.HotShardForTest().Name()
	forceRotation(t, ts)

	// Warm shard must stay in the eventShards map for snowflake ID → shard resolution.
	if _, ok := ts.EventShardsForTest()[oldHotName]; !ok {
		t.Error("warm shard must stay in eventShards map for snowflake ID resolution")
	}
}

func TestTieredStore_ForceRotate(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	oldHotName := ts.HotShardForTest().Name()

	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	newHotName := ts.HotShardForTest().Name()
	if oldHotName == newHotName {
		t.Error("hot shard name didn't change after rotation")
	}

	// Old hot should now be warm.
	if es, ok := ts.EventShardsForTest()[oldHotName]; !ok || es.Tier() != TierWarm {
		t.Error("old hot shard should be warm")
	}
}

func TestTieredStore_RotateHotShardPublicCallSerializesConcurrentRotations(t *testing.T) {
	ts := newTestTieredStore(t)

	const rotations = 4
	errs := make(chan error, rotations)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range rotations {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- ts.RotateHotShard()
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("RotateHotShard: %v", err)
		}
	}

	ts.MuForTest().RLock()
	defer ts.MuForTest().RUnlock()
	var hotCount int
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierHot {
			hotCount++
		}
	}
	if hotCount != 1 {
		t.Fatalf("hot shard count = %d, want 1", hotCount)
	}
	if got := len(ts.EventShardsForTest()); got != rotations+1 {
		t.Fatalf("event shard count = %d, want %d", got, rotations+1)
	}
}
