package tiered

import (
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
)

func TestShardCatalog_UpdateShardTier(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "shard-1", Tier: TierHot})

	if !sc.UpdateShardTier("shard-1", TierWarm) {
		t.Error("UpdateShardTier should return true for existing shard")
	}

	entry, ok := sc.GetShard("shard-1")
	if !ok {
		t.Fatal("shard-1 should exist")
	}
	if entry.Tier != TierWarm {
		t.Errorf("tier = %q, want %q", entry.Tier, TierWarm)
	}
}

func TestShardCatalog_UpdateShardTierNotFound(t *testing.T) {
	sc := NewShardCatalog("")
	if sc.UpdateShardTier("nonexistent", TierWarm) {
		t.Error("UpdateShardTier should return false for unknown shard")
	}
}

func TestShardCatalog_ColdEventShards(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "hot", Kind: ShardEvent, Tier: TierHot})
	sc.AddShard(ShardEntry{Name: "warm", Kind: ShardEvent, Tier: TierWarm})
	sc.AddShard(ShardEntry{Name: "cold1", Kind: ShardEvent, Tier: TierCold})
	sc.AddShard(ShardEntry{Name: "cold2", Kind: ShardEvent, Tier: TierCold})
	sc.AddShard(ShardEntry{Name: "ref", Kind: ShardReference, Tier: TierHot})

	cold := sc.ColdEventShards()
	if len(cold) != 2 {
		t.Errorf("ColdEventShards = %d, want 2", len(cold))
	}
}

func TestShardCatalog_UpdateVerified(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "shard1", Kind: ShardEvent, Tier: TierWarm})

	if !sc.UpdateShardVerified("shard1", true) {
		t.Error("UpdateShardVerified returned false for existing shard")
	}
	e, ok := sc.GetShard("shard1")
	if !ok || !e.Verified {
		t.Error("Verified not set")
	}

	// Set back to false.
	sc.UpdateShardVerified("shard1", false)
	e, _ = sc.GetShard("shard1")
	if e.Verified {
		t.Error("Verified should be false")
	}

	// Non-existent shard.
	if sc.UpdateShardVerified("nope", true) {
		t.Error("should return false for non-existent shard")
	}
}

func TestShardCatalog_UpdateStats(t *testing.T) {
	sc := NewShardCatalog("")
	sc.AddShard(ShardEntry{Name: "s1", Kind: ShardEvent, Tier: TierHot})

	if !sc.UpdateShardStats("s1", 100, 50) {
		t.Error("UpdateShardStats returned false for existing shard")
	}
	e, ok := sc.GetShard("s1")
	if !ok || e.ApproxNodes != 100 || e.ApproxRels != 50 {
		t.Errorf("stats = (%d, %d), want (100, 50)", e.ApproxNodes, e.ApproxRels)
	}

	if sc.UpdateShardStats("nope", 1, 1) {
		t.Error("should return false for non-existent shard")
	}
}

func TestTieredStore_ListShards_Initial(t *testing.T) {
	ts := newTestTieredStore(t)

	infos, lsErr := ts.ListShards()
	if lsErr != nil {
		t.Fatalf("ListShards: %v", lsErr)
	}
	if len(infos) < 2 {
		t.Fatalf("ListShards = %d, want at least 2 (ref + hot)", len(infos))
	}

	// Find reference shard.
	var foundRef, foundEvent bool
	for _, si := range infos {
		if si.Kind == ShardReference {
			foundRef = true
			if !si.Open {
				t.Error("reference shard should be open")
			}
		}
		if si.Kind == ShardEvent {
			foundEvent = true
		}
	}
	if !foundRef {
		t.Error("no reference shard in ListShards")
	}
	if !foundEvent {
		t.Error("no event shard in ListShards")
	}
}

func TestTieredStore_ListShards_AfterRotation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	infos, lsErr := ts.ListShards()
	if lsErr != nil {
		t.Fatalf("ListShards: %v", lsErr)
	}
	eventCount := 0
	for _, si := range infos {
		if si.Kind == ShardEvent {
			eventCount++
		}
	}
	if eventCount < 2 {
		t.Errorf("expected at least 2 event shards after rotation, got %d", eventCount)
	}
}

func TestTieredStore_ListShards_WithCold(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	// Rotate and demote to cold.
	oldHot := ts.HotShardForTest().Name()
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	demoteToCold(ts, oldHot)

	infos, lsErr := ts.ListShards()
	if lsErr != nil {
		t.Fatalf("ListShards: %v", lsErr)
	}
	var foundCold bool
	for _, si := range infos {
		if si.Name == oldHot && si.Tier == TierCold {
			foundCold = true
		}
	}
	if !foundCold {
		t.Error("expected cold shard in ListShards")
	}
}

func TestTieredStore_ListShards_LiveStats(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := makeRefNode(t, gen, ts)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	infos, lsErr := ts.ListShards()
	if lsErr != nil {
		t.Fatalf("ListShards: %v", lsErr)
	}
	for _, si := range infos {
		if si.Kind == ShardReference {
			if si.Nodes != 1 {
				t.Errorf("reference shard nodes = %d, want 1", si.Nodes)
			}
		}
	}
}

func TestTieredStore_RebuildCatalog(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	_, _ = reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add a ref node and an event node.
	refNode := makeRefNode(t, gen, ts)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := makeEvtNode(t, gen, ts)
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	if err := ts.RebuildCatalog(); err != nil {
		t.Fatalf("RebuildCatalog: %v", err)
	}

	// Check catalog got updated with counts.
	entry, ok := ts.CatalogForTest().GetShard("reference")
	if !ok {
		t.Fatal("reference shard not in catalog")
	}
	if entry.ApproxNodes != 1 {
		t.Errorf("reference ApproxNodes = %d, want 1", entry.ApproxNodes)
	}

	hotEntry, ok := ts.CatalogForTest().GetShard(ts.HotShardForTest().Name())
	if !ok {
		t.Fatal("hot shard not in catalog")
	}
	if hotEntry.ApproxNodes != 1 {
		t.Errorf("hot ApproxNodes = %d, want 1", hotEntry.ApproxNodes)
	}
}
