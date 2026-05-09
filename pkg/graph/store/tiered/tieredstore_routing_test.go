package tiered

import (
	"errors"
	"testing"
	"time"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestTieredStore_OntologyRouting_RefNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.OntologyForTest().ClassifyByToken(caseTok) != ClassReference {
		t.Error("Case should be ClassReference")
	}
	if ts.OntologyForTest().ClassifyByToken(signalTok) != ClassEvent {
		t.Error("Signal should be ClassEvent")
	}
}

func TestTieredStore_OntologyRouting_ShardForNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	if ts.ShardForNodeForTest(caseTok) != ts.RefShardForTest() {
		t.Error("Case node should go to refShard")
	}
	if ts.ShardForNodeForTest(signalTok) != ts.HotShardForTest().Store() {
		t.Error("Signal node should go to hotShard")
	}
}

func TestTieredStore_OntologyRouting_UnknownDefaultsToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	unknownTok, _ := reg.GetOrCreate("SomeNewLabel")
	if ts.ShardForNodeForTest(unknownTok) != ts.HotShardForTest().Store() {
		t.Error("unknown label should default to event shard")
	}
}

func TestTieredStore_DepthHot(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Write to warm shard.
	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	// Write to hot shard.
	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthHot: only hot shard entities.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Errorf("AllNodes(DepthHot) = %d, want 1 (hot only)", len(nodes))
	}
	if nodes[0].ID() != hotN.ID() {
		t.Error("DepthHot should return the hot node")
	}
}

func TestTieredStore_DepthWarm(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthWarm: hot + warm.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthWarm) = %d, want 2 (hot + warm)", len(nodes))
	}
}

func TestTieredStore_DepthAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// DepthAll: all tiers.
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(DepthAll) = %d, want 2", len(nodes))
	}
}

func TestTieredStore_DepthZeroIsAll(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// Zero Depth should be backward-compatible (all tiers).
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Errorf("AllNodes(Depth=0) = %d, want 2 (backward-compatible)", len(nodes))
	}
}

func TestTieredStore_DepthCounters(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// 1 ref node, 1 warm event, 1 hot event.
	refN := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(refN)
	warmN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmN)

	forceRotation(t, ts)

	hotN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotN)

	// NodeCount always returns total (DepthAll).
	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("NodeCount = %d, want 3", count)
	}

	// AllNodeIDs with DepthHot: ref node (always included) + 1 hot event.
	hotIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 2 { // ref + hot
		t.Errorf("AllNodeIDs(DepthHot) = %d, want 2 (ref + hot)", len(hotIDs))
	}

	// AllNodeIDs with DepthWarm: ref + warm + hot.
	warmIDs, err := ts.AllNodeIDs(QueryOpts{Depth: DepthWarm})
	if err != nil {
		t.Fatal(err)
	}
	if len(warmIDs) != 3 {
		t.Errorf("AllNodeIDs(DepthWarm) = %d, want 3 (ref + warm + hot)", len(warmIDs))
	}
}

func TestTieredStore_DepthRelationshipsByType(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)
	var relType uint16 = 1

	// Create rel in warm shard.
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType,
		s1.ID(), s2.ID()))

	forceRotation(t, ts)

	// Create rel in hot shard.
	s3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s4 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType,
		s3.ID(), s4.ID()))

	// DepthHot: only hot shard rels.
	hotRels, err := ts.RelationshipsByType(relType, QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotRels) != 1 {
		t.Errorf("RelationshipsByType(DepthHot) = %d, want 1", len(hotRels))
	}

	// DepthAll: both.
	allRels, err := ts.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allRels) != 2 {
		t.Errorf("RelationshipsByType(DepthAll) = %d, want 2", len(allRels))
	}
}

func TestTieredStore_DepthAllRelIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	rGen := tieredRelGen(t)

	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1,
		s1.ID(), s2.ID()))

	forceRotation(t, ts)

	s3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s4 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(s3)
	_ = ts.PutNode(s4)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1,
		s3.ID(), s4.ID()))

	hotIDs, err := ts.AllRelIDs(QueryOpts{Depth: DepthHot})
	if err != nil {
		t.Fatal(err)
	}
	if len(hotIDs) != 1 {
		t.Errorf("AllRelIDs(DepthHot) = %d, want 1", len(hotIDs))
	}

	allIDs, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(allIDs) != 2 {
		t.Errorf("AllRelIDs(DepthAll) = %d, want 2", len(allIDs))
	}
}

func TestTieredStore_ColdShard_TimestampResolution(t *testing.T) {
	// Verify snowflake ID timestamp correctly resolves to cold shard.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)

	// Remember shard name, rotate, then manually demote to cold.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	demoteToCold(ts, hotName)

	// Resolve shard via shardForNodeID — should find the cold shard.
	shard, err := ts.ShardForNodeIDForTest(n.ID())
	if err != nil {
		t.Fatalf("shardForNodeID: %v", err)
	}
	if !shard.HasNodeID(n.ID().SnowflakeID()) {
		t.Error("shard should have the node")
	}
}

func TestTieredStore_ShardForNodeID_Error(t *testing.T) {
	// Verify shardForNodeID propagates errors. With in-memory stores,
	// the only error path is through getStore on cold shards.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// Normal case: no error for non-existent node (falls back to hot shard).
	shard, err := ts.ShardForNodeIDForTest(types.NodeID(id))
	if err != nil {
		t.Fatalf("shardForNodeID should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_ShardForRelID_Error(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredRelGen(t)
	id := gen.Generate()

	shard, err := ts.ShardForRelIDForTest(types.RelID(id))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest should not error: %v", err)
	}
	if shard == nil {
		t.Error("shard should not be nil")
	}
}

func TestTieredStore_RoutingErrorInWrite(t *testing.T) {
	// Verify that write operations propagate routing errors.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	gen := tieredNodeGen(t)
	id := gen.Generate()

	// DeleteNode for non-existent node should hit shardForNodeID then store.
	err := ts.DeleteNode(types.NodeID(id))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestTieredStore_ShardForRelID_FindsInWarmShard(t *testing.T) {
	// Cross-shard relationship in warm shard should be found without probing cold.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("HAS_SIGNAL")

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create ref node and event node in hot shard.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(evtNode); err != nil {
		t.Fatal(err)
	}

	// Create cross-shard relationship (ref→event).
	relID := relGen.Generate()
	r := types.NewRelationship(types.RelID(relID), relTypeTok, refNode.ID(), evtNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Rotate the event shard to warm.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	// Verify the relationship can still be found via ShardForRelIDForTest.
	shard, err := ts.ShardForRelIDForTest(types.RelID(relID))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest: %v", err)
	}
	if !shard.HasRelID(relID) {
		t.Error("expected shard to have the rel")
	}

	// Now demote the old shard to cold and close it.
	demoteToCold(ts, hotName)
	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	coldES.LockShardMuForTest()
	if coldES.Store() != nil {
		_ = coldES.Store().Close()
		coldES.SetStoreForTest(nil)
	}
	coldES.UnlockShardMuForTest()

	// Entity lives in ref shard (for ref-node rels). It should still be found.
	// The ref shard fast path should resolve it.
	shard, err = ts.ShardForRelIDForTest(types.RelID(relID))
	if err != nil {
		t.Fatalf("ShardForRelIDForTest after cold: %v", err)
	}
	if !shard.HasRelID(relID) {
		t.Error("expected shard to have the rel after cold demotion")
	}
}
