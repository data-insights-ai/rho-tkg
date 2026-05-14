package tiered

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── Store ForEach ──────────────────────────────────────────────────────

func TestTieredStore_ForEachNilCallbackReturnsInvalidMutation(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "ForEachNodeID", run: func() error { return ts.ForEachNodeID(nil) }},
		{name: "ForEachRelID", run: func() error { return ts.ForEachRelID(nil) }},
		{name: "ForEachNodeHistoryID", run: func() error { return ts.ForEachNodeHistoryID(nil) }},
		{name: "ForEachNodeHistoryIDByDepth", run: func() error { return ts.ForEachNodeHistoryIDByDepth(DepthWarm, nil) }},
		{name: "ForEachRelHistoryID", run: func() error { return ts.ForEachRelHistoryID(nil) }},
		{name: "ForEachRelHistoryIDByDepth", run: func() error { return ts.ForEachRelHistoryIDByDepth(DepthWarm, nil) }},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s(nil) = %v, want ErrInvalidStoreMutation", check.name, err)
		}
	}
}

func TestTieredStore_ForEachHistoryByDepthEmptyDoesNotCallCallback(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	for _, depth := range []ShardDepth{DepthAll, DepthHot, DepthWarm} {
		if err := ts.ForEachNodeHistoryIDByDepth(depth, func(types.NodeID) bool {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v) callback ran on empty history", depth)
			return false
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v): %v", depth, err)
		}
		if err := ts.ForEachRelHistoryIDByDepth(depth, func(types.RelID) bool {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v) callback ran on empty history", depth)
			return false
		}); err != nil {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v): %v", depth, err)
		}
	}
}

func TestTieredStore_ForEachNodeID_AllShards(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Ref node (directly on refShard since no label registry set).
	refNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}

	// Event node (via hot shard — all unrecognized tokens route to event).
	evNode := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(evNode); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2 (ref + event)", len(seen))
	}
	if _, ok := seen[refNode.ID().SnowflakeID()]; !ok {
		t.Error("missing ref node")
	}
	if _, ok := seen[evNode.ID().SnowflakeID()]; !ok {
		t.Error("missing event node")
	}
}

func TestTieredStore_ForEachNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Add 3 event nodes + 2 ref nodes.
	for i := 0; i < 3; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), 3, nil) // event
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
		if err := ts.RefShardForTest().PutNode(n); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
	}

	count := 0
	err := ts.ForEachNodeID(func(id types.NodeID) bool {
		count++
		return count < 2 // stop after 2
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if count != 2 {
		t.Fatalf("got %d callbacks, want 2 (early stop)", count)
	}
}

func TestTieredStore_ForEachNodeID_WithRotation(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Add event node to hot shard.
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	// Rotate to create warm shard.
	if err := ts.RotateHotShard(); err != nil {
		t.Fatalf("RotateHotShard: %v", err)
	}

	// Add event node to new hot shard.
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2 (warm + hot)", len(seen))
	}
}

func TestTieredStore_ForEachRelID_AllShards(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Create event nodes (both in hot shard for same-shard rel).
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create rel.
	relGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs, want 1", len(seen))
	}
}

func TestTieredStore_ForEachRelID_RefArchiveAndEarlyStop(t *testing.T) {
	ts := newTestTieredStore(t)
	installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	refNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	refRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, refNode.ID(), refNode.ID())
	if err := ts.PutRelationship(refRel); err != nil {
		t.Fatalf("PutRelationship ref: %v", err)
	}

	archivedNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(archivedNode); err != nil {
		t.Fatalf("PutNode archived: %v", err)
	}
	archivedRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, archivedNode.ID(), archivedNode.ID())
	if err := ts.PutRelationship(archivedRel); err != nil {
		t.Fatalf("PutRelationship archived: %v", err)
	}
	if err := ts.ArchiveNode(archivedNode.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	archivedCallbacks := 0
	err := ts.ForEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		if id == archivedRel.ID() {
			archivedCallbacks++
			return false
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if _, ok := seen[refRel.ID().SnowflakeID()]; !ok {
		t.Fatal("missing ref relationship")
	}
	if archivedCallbacks != 1 {
		t.Fatalf("archived callbacks = %d, want 1", archivedCallbacks)
	}
}

func TestTieredStore_ForEachNodeHistoryID(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Ref node with history (directly on refShard).
	n1 := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(n1); err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID().SnowflakeID()
	if err := ts.PutNodeVersion(types.NodeID(id1), 0, n1); err != nil {
		t.Fatal(err)
	}

	// Event node with history (via PutNode routing).
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	id2 := n2.ID().SnowflakeID()
	if err := ts.PutNodeVersion(types.NodeID(id2), 0, n2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2 (ref + event history)", len(seen))
	}
}

// TestTieredStore_ForEachDeletedNodeID pins the DeletedIterationCapability
// contract across shard types. Live ref/event nodes with history must be
// excluded; history-only nodes must be included regardless of shard.
func TestTieredStore_ForEachDeletedNodeID(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	gen := tieredNodeGen(t)

	// Ref node live + history.
	live := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(live); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(live.ID(), 0, live); err != nil {
		t.Fatal(err)
	}
	// Ref node history only (deleted).
	deletedRef := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.PutNodeVersion(deletedRef.ID(), 0, deletedRef); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := ts.ForEachDeletedNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}
	if _, ok := seen[deletedRef.ID().SnowflakeID()]; !ok {
		t.Errorf("deleted ref node %d should appear, got %v", deletedRef.ID(), seen)
	}
	if _, ok := seen[live.ID().SnowflakeID()]; ok {
		t.Errorf("live ref node %d must NOT appear", live.ID())
	}
}

// TestTieredStore_ForEachDeletedRelID is the rel counterpart.
func TestTieredStore_ForEachDeletedRelID(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	a := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(a); err != nil {
		t.Fatal(err)
	}
	b := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(b); err != nil {
		t.Fatal(err)
	}

	// Rel live + history.
	rLive := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelationship(rLive); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(rLive.ID(), 0, rLive); err != nil {
		t.Fatal(err)
	}
	// Rel deleted (history only).
	rDel := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := ts.PutRelVersion(rDel.ID(), 0, rDel); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	if err := ts.ForEachDeletedRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	}); err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}
	if _, ok := seen[rDel.ID().SnowflakeID()]; !ok {
		t.Errorf("deleted rel %d should appear, got %v", rDel.ID(), seen)
	}
	if _, ok := seen[rLive.ID().SnowflakeID()]; ok {
		t.Errorf("live rel %d must NOT appear", rLive.ID())
	}
}

func TestTieredStore_ForEachHistoryCallbacksDoNotExtendIterator(t *testing.T) {
	ts := newTestTieredStore(t)
	nodeGen := tieredNodeGen(t)

	initialNode := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(initialNode); err != nil {
		t.Fatalf("PutNode initial: %v", err)
	}
	if err := ts.PutNodeVersion(initialNode.ID(), 0, initialNode); err != nil {
		t.Fatalf("PutNodeVersion initial: %v", err)
	}

	nodeCallbacks := 0
	if err := ts.ForEachNodeHistoryID(func(id types.NodeID) bool {
		nodeCallbacks++
		if id != initialNode.ID() {
			t.Errorf("ForEachNodeHistoryID visited callback-created node %d", id)
			return false
		}
		created := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		if err := ts.PutNode(created); err != nil {
			t.Errorf("PutNode in callback: %v", err)
			return false
		}
		if err := ts.PutNodeVersion(created.ID(), 0, created); err != nil {
			t.Errorf("PutNodeVersion in callback: %v", err)
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if nodeCallbacks != 1 {
		t.Fatalf("node callbacks = %d, want 1", nodeCallbacks)
	}

	start := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	if err := ts.RefShardForTest().PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}
	relGen := tieredRelGen(t)
	initialRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(initialRel); err != nil {
		t.Fatalf("PutRelationship initial: %v", err)
	}
	if err := ts.PutRelVersion(initialRel.ID(), 0, initialRel); err != nil {
		t.Fatalf("PutRelVersion initial: %v", err)
	}

	relCallbacks := 0
	if err := ts.ForEachRelHistoryID(func(id types.RelID) bool {
		relCallbacks++
		if id != initialRel.ID() {
			t.Errorf("ForEachRelHistoryID visited callback-created relationship %d", id)
			return false
		}
		createdStart := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		createdEnd := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		if err := ts.PutNode(createdStart); err != nil {
			t.Errorf("PutNode createdStart in callback: %v", err)
			return false
		}
		if err := ts.PutNode(createdEnd); err != nil {
			t.Errorf("PutNode createdEnd in callback: %v", err)
			return false
		}
		created := types.NewRelationship(types.RelID(relGen.Generate()), 1, createdStart.ID(), createdEnd.ID())
		if err := ts.PutRelationship(created); err != nil {
			t.Errorf("PutRelationship in callback: %v", err)
			return false
		}
		if err := ts.PutRelVersion(created.ID(), 0, created); err != nil {
			t.Errorf("PutRelVersion in callback: %v", err)
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if relCallbacks != 1 {
		t.Fatalf("rel callbacks = %d, want 1", relCallbacks)
	}
}

func TestTieredStore_ForEachHistoryByDepthCallbacksDoNotExtendIterator(t *testing.T) {
	ts := newTestTieredStore(t)
	nodeGen := tieredNodeGen(t)

	initialNode := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(initialNode); err != nil {
		t.Fatalf("PutNode initial: %v", err)
	}
	if err := ts.PutNodeVersion(initialNode.ID(), 0, initialNode); err != nil {
		t.Fatalf("PutNodeVersion initial: %v", err)
	}

	nodeCallbacks := 0
	if err := ts.ForEachNodeHistoryIDByDepth(DepthHot, func(id types.NodeID) bool {
		nodeCallbacks++
		if id != initialNode.ID() {
			t.Errorf("ForEachNodeHistoryIDByDepth visited callback-created node %d", id)
			return false
		}
		created := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		if err := ts.PutNode(created); err != nil {
			t.Errorf("PutNode in callback: %v", err)
			return false
		}
		if err := ts.PutNodeVersion(created.ID(), 0, created); err != nil {
			t.Errorf("PutNodeVersion in callback: %v", err)
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryIDByDepth: %v", err)
	}
	if nodeCallbacks != 1 {
		t.Fatalf("node callbacks = %d, want 1", nodeCallbacks)
	}

	start := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}
	if err := ts.RefShardForTest().PutNode(end); err != nil {
		t.Fatalf("PutNode end: %v", err)
	}
	relGen := tieredRelGen(t)
	initialRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	if err := ts.PutRelationship(initialRel); err != nil {
		t.Fatalf("PutRelationship initial: %v", err)
	}
	if err := ts.PutRelVersion(initialRel.ID(), 0, initialRel); err != nil {
		t.Fatalf("PutRelVersion initial: %v", err)
	}

	relCallbacks := 0
	if err := ts.ForEachRelHistoryIDByDepth(DepthHot, func(id types.RelID) bool {
		relCallbacks++
		if id != initialRel.ID() {
			t.Errorf("ForEachRelHistoryIDByDepth visited callback-created relationship %d", id)
			return false
		}
		createdStart := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		createdEnd := types.NewNode(types.NodeID(nodeGen.Generate()), 3, nil)
		if err := ts.PutNode(createdStart); err != nil {
			t.Errorf("PutNode createdStart in callback: %v", err)
			return false
		}
		if err := ts.PutNode(createdEnd); err != nil {
			t.Errorf("PutNode createdEnd in callback: %v", err)
			return false
		}
		created := types.NewRelationship(types.RelID(relGen.Generate()), 1, createdStart.ID(), createdEnd.ID())
		if err := ts.PutRelationship(created); err != nil {
			t.Errorf("PutRelationship in callback: %v", err)
			return false
		}
		if err := ts.PutRelVersion(created.ID(), 0, created); err != nil {
			t.Errorf("PutRelVersion in callback: %v", err)
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryIDByDepth: %v", err)
	}
	if relCallbacks != 1 {
		t.Fatalf("rel callbacks = %d, want 1", relCallbacks)
	}
}

func TestTieredStore_ForEachNodeHistoryIDByDepth_GatesArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)

	live := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(live); err != nil {
		t.Fatalf("PutNode live: %v", err)
	}
	if err := ts.PutNodeVersion(live.ID(), 0, live); err != nil {
		t.Fatalf("PutNodeVersion live: %v", err)
	}

	event := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(event); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}
	if err := ts.PutNodeVersion(event.ID(), 0, event); err != nil {
		t.Fatalf("PutNodeVersion event: %v", err)
	}

	archived := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(archived); err != nil {
		t.Fatalf("PutNode archived: %v", err)
	}
	if err := ts.PutNodeVersion(archived.ID(), 0, archived); err != nil {
		t.Fatalf("PutNodeVersion archived pre-archive: %v", err)
	}
	if err := ts.ArchiveNode(archived.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v) before archive delete: %v", depth, err)
		}
		if _, ok := seen[archived.ID().SnowflakeID()]; ok {
			t.Fatalf("%v included currently archived node history", depth)
		}
	}

	updated := archived.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(updated, archived.Version(), archived); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archived: %v", err)
	}
	if err := ts.DeleteNode(archived.ID()); err != nil {
		t.Fatalf("DeleteNode archived: %v", err)
	}

	all := make(map[snowflake.ID]int)
	if err := ts.ForEachNodeHistoryIDByDepth(DepthAll, func(id types.NodeID) bool {
		all[id.SnowflakeID()]++
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryIDByDepth(DepthAll): %v", err)
	}
	if got := all[live.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll live node callbacks = %d, want 1", got)
	}
	if got := all[event.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll event node callbacks = %d, want 1", got)
	}
	if got := all[archived.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll archived node callbacks = %d, want 1", got)
	}

	callbacks := 0
	if err := ts.ForEachNodeHistoryIDByDepth(DepthAll, func(id types.NodeID) bool {
		callbacks++
		return false
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryIDByDepth early stop: %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("early stop callbacks = %d, want 1", callbacks)
	}

	archivedCallbacks := 0
	if err := ts.ForEachNodeHistoryIDByDepth(DepthAll, func(id types.NodeID) bool {
		if id == archived.ID() {
			archivedCallbacks++
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryIDByDepth archive stop: %v", err)
	}
	if archivedCallbacks != 1 {
		t.Fatalf("archive stop saw archived node %d times, want 1", archivedCallbacks)
	}

	eventStopped := false
	if err := ts.ForEachNodeHistoryIDByDepth(DepthHot, func(id types.NodeID) bool {
		if id == event.ID() {
			eventStopped = true
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachNodeHistoryIDByDepth event stop: %v", err)
	}
	if !eventStopped {
		t.Fatal("event stop did not reach event node history")
	}

	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v): %v", depth, err)
		}
		if _, ok := seen[live.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing live node history", depth)
		}
		if _, ok := seen[event.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing event node history", depth)
		}
		if _, ok := seen[archived.ID().SnowflakeID()]; ok {
			t.Fatalf("%v included archived node history", depth)
		}
	}
}

func TestTieredStore_ForEachNodeHistoryIDByDepth_IncludesRestoredRefWithArchiveHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)

	n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(n); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion pre-archive: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	updated := n.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(updated, n.Version(), n); err != nil {
		t.Fatalf("ReplaceNodeWithHistory archived: %v", err)
	}
	if err := ts.RestoreNode(n.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachNodeHistoryIDByDepth(depth, func(id types.NodeID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachNodeHistoryIDByDepth(%v): %v", depth, err)
		}
		if _, ok := seen[n.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing restored ref node history", depth)
		}
	}
}

func TestTieredStore_ForEachRelHistoryID(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create event nodes + rel + history.
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	relID := r.ID().SnowflakeID()
	if err := ts.PutRelVersion(types.RelID(relID), 0, r); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachRelHistoryID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs, want 1", len(seen))
	}
	if _, ok := seen[relID]; !ok {
		t.Error("expected to find rel in history")
	}
}

func TestTieredStore_ForEachCallbacksCanMutateStore(t *testing.T) {
	ts := newTestTieredStore(t)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNodeVersion(n1.ID(), 0, n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelVersion(rel.ID(), 0, rel); err != nil {
		t.Fatal(err)
	}

	runWithTimeout := func(name string, fn func() error) {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- fn() }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s deadlocked while callback mutated store", name)
		}
	}

	runWithTimeout("ForEachNodeID", func() error {
		var cbErr error
		err := ts.ForEachNodeID(func(types.NodeID) bool {
			cbErr = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), 3, nil))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelID", func() error {
		var cbErr error
		err := ts.ForEachRelID(func(types.RelID) bool {
			cbErr = ts.PutRelationship(types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID()))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachNodeHistoryID", func() error {
		var cbErr error
		err := ts.ForEachNodeHistoryID(func(types.NodeID) bool {
			snap := n1.DeepCopy()
			snap.SetVersion(1)
			cbErr = ts.PutNodeVersion(n1.ID(), 1, snap)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelHistoryID", func() error {
		var cbErr error
		err := ts.ForEachRelHistoryID(func(types.RelID) bool {
			snap := rel.DeepCopy()
			snap.SetVersion(1)
			cbErr = ts.PutRelVersion(rel.ID(), 1, snap)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
}

func TestTieredStore_ForEachRelHistoryIDByDepth_GatesArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	liveNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(liveNode); err != nil {
		t.Fatalf("PutNode live: %v", err)
	}
	liveRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, liveNode.ID(), liveNode.ID())
	if err := ts.PutRelationship(liveRel); err != nil {
		t.Fatalf("PutRelationship live: %v", err)
	}
	if err := ts.PutRelVersion(liveRel.ID(), 0, liveRel); err != nil {
		t.Fatalf("PutRelVersion live: %v", err)
	}

	eventStart := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	eventEnd := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(eventStart); err != nil {
		t.Fatalf("PutNode event start: %v", err)
	}
	if err := ts.PutNode(eventEnd); err != nil {
		t.Fatalf("PutNode event end: %v", err)
	}
	eventRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, eventStart.ID(), eventEnd.ID())
	if err := ts.PutRelationship(eventRel); err != nil {
		t.Fatalf("PutRelationship event: %v", err)
	}
	if err := ts.PutRelVersion(eventRel.ID(), 0, eventRel); err != nil {
		t.Fatalf("PutRelVersion event: %v", err)
	}

	archivedNode := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(archivedNode); err != nil {
		t.Fatalf("PutNode archived: %v", err)
	}
	archivedRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, archivedNode.ID(), archivedNode.ID())
	if err := ts.PutRelationship(archivedRel); err != nil {
		t.Fatalf("PutRelationship archived: %v", err)
	}
	if err := ts.PutRelVersion(archivedRel.ID(), 0, archivedRel); err != nil {
		t.Fatalf("PutRelVersion archived pre-archive: %v", err)
	}
	if err := ts.ArchiveNode(archivedNode.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}
	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v) before archive delete: %v", depth, err)
		}
		if _, ok := seen[archivedRel.ID().SnowflakeID()]; ok {
			t.Fatalf("%v included currently archived relationship history", depth)
		}
	}

	updatedRel := archivedRel.DeepCopy()
	updatedRel.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(updatedRel, archivedRel.Version(), archivedRel); err != nil {
		t.Fatalf("ReplaceRelWithHistory archived: %v", err)
	}
	if err := ts.DeleteRelationship(archivedRel.ID()); err != nil {
		t.Fatalf("DeleteRelationship archived: %v", err)
	}

	all := make(map[snowflake.ID]int)
	if err := ts.ForEachRelHistoryIDByDepth(DepthAll, func(id types.RelID) bool {
		all[id.SnowflakeID()]++
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryIDByDepth(DepthAll): %v", err)
	}
	if got := all[liveRel.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll live relationship callbacks = %d, want 1", got)
	}
	if got := all[eventRel.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll event relationship callbacks = %d, want 1", got)
	}
	if got := all[archivedRel.ID().SnowflakeID()]; got != 1 {
		t.Fatalf("DepthAll archived relationship callbacks = %d, want 1", got)
	}

	callbacks := 0
	if err := ts.ForEachRelHistoryIDByDepth(DepthAll, func(id types.RelID) bool {
		callbacks++
		return false
	}); err != nil {
		t.Fatalf("ForEachRelHistoryIDByDepth early stop: %v", err)
	}
	if callbacks != 1 {
		t.Fatalf("early stop callbacks = %d, want 1", callbacks)
	}

	archivedCallbacks := 0
	if err := ts.ForEachRelHistoryIDByDepth(DepthAll, func(id types.RelID) bool {
		if id == archivedRel.ID() {
			archivedCallbacks++
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryIDByDepth archive stop: %v", err)
	}
	if archivedCallbacks != 1 {
		t.Fatalf("archive stop saw archived relationship %d times, want 1", archivedCallbacks)
	}

	eventStopped := false
	if err := ts.ForEachRelHistoryIDByDepth(DepthHot, func(id types.RelID) bool {
		if id == eventRel.ID() {
			eventStopped = true
			return false
		}
		return true
	}); err != nil {
		t.Fatalf("ForEachRelHistoryIDByDepth event stop: %v", err)
	}
	if !eventStopped {
		t.Fatal("event stop did not reach event relationship history")
	}

	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v): %v", depth, err)
		}
		if _, ok := seen[liveRel.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing live relationship history", depth)
		}
		if _, ok := seen[eventRel.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing event relationship history", depth)
		}
		if _, ok := seen[archivedRel.ID().SnowflakeID()]; ok {
			t.Fatalf("%v included archived relationship history", depth)
		}
	}
}

func TestTieredStore_ForEachRelHistoryIDByDepth_IncludesRestoredRefWithArchiveHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	installDefaultTestLabelRegistry(t, ts)
	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := ts.RefShardForTest().PutNode(n); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, n.ID(), n.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.PutRelVersion(rel.ID(), 0, rel); err != nil {
		t.Fatalf("PutRelVersion pre-archive: %v", err)
	}
	if err := ts.ArchiveNode(n.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	updated := rel.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(updated, rel.Version(), rel); err != nil {
		t.Fatalf("ReplaceRelWithHistory archived: %v", err)
	}
	if err := ts.RestoreNode(n.ID()); err != nil {
		t.Fatalf("RestoreNode: %v", err)
	}

	for _, depth := range []ShardDepth{DepthHot, DepthWarm} {
		seen := make(map[snowflake.ID]struct{})
		if err := ts.ForEachRelHistoryIDByDepth(depth, func(id types.RelID) bool {
			seen[id.SnowflakeID()] = struct{}{}
			return true
		}); err != nil {
			t.Fatalf("ForEachRelHistoryIDByDepth(%v): %v", depth, err)
		}
		if _, ok := seen[rel.ID().SnowflakeID()]; !ok {
			t.Fatalf("%v missing restored ref relationship history", depth)
		}
	}
}

func TestTieredStore_ForEachRelID_EarlyStop(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Create event nodes.
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create 3 rels.
	for i := 0; i < 3; i++ {
		r := types.NewRelationship(types.RelID(relGen.Generate()), 1, n1.ID(), n2.ID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	err := ts.ForEachRelID(func(id types.RelID) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d callbacks, want 1 (early stop)", count)
	}
}
