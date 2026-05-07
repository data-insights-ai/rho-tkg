package tieredstore

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── TieredStore ForEach ──────────────────────────────────────────────────────

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
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatalf("RotateHotShard: %v", err)
	}
	ts.MuForTest().Unlock()

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
