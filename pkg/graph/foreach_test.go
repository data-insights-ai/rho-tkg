package graph

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// ─── MemoryStore ForEach ──────────────────────────────────────────────────────

func TestMemoryStore_ForEachNodeID(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	ids := []snowflake.ID{10, 20, 30}
	for _, id := range ids {
		if err := ms.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachNodeID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("got %d IDs, want 3", len(seen))
	}
	for _, id := range ids {
		if _, ok := seen[id]; !ok {
			t.Errorf("missing ID %d", id)
		}
	}
}

func TestMemoryStore_ForEachNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	for _, id := range []snowflake.ID{10, 20, 30} {
		if err := ms.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	count := 0
	err := ms.ForEachNodeID(func(id snowflake.ID) bool {
		count++
		return false // stop after first
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if count != 1 {
		t.Fatalf("got %d callbacks, want 1 (early stop)", count)
	}
}

func TestMemoryStore_ForEachNodeID_Empty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	count := 0
	err := ms.ForEachNodeID(func(id snowflake.ID) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if count != 0 {
		t.Fatalf("got %d callbacks on empty store, want 0", count)
	}
}

func TestMemoryStore_ForEachRelID(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Create endpoints.
	if err := ms.PutNode(types.NewNode(1, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(2, 1, nil)); err != nil {
		t.Fatal(err)
	}

	relIDs := []snowflake.ID{100, 200, 300}
	for _, id := range relIDs {
		r := types.NewRelationship(id, 1, 1, 2)
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachRelID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelID: %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("got %d IDs, want 3", len(seen))
	}
	for _, id := range relIDs {
		if _, ok := seen[id]; !ok {
			t.Errorf("missing ID %d", id)
		}
	}
}

func TestMemoryStore_ForEachNodeHistoryID(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Create two nodes with history.
	for _, id := range []snowflake.ID{10, 20} {
		if err := ms.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNodeVersion(id, 0, types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	// Third node without history.
	if err := ms.PutNode(types.NewNode(30, 1, nil)); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachNodeHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeHistoryID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2 (only nodes with history)", len(seen))
	}
	if _, ok := seen[30]; ok {
		t.Error("node 30 should not appear (no history)")
	}
}

func TestMemoryStore_ForEachRelHistoryID(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	// Create endpoints.
	if err := ms.PutNode(types.NewNode(1, 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(2, 1, nil)); err != nil {
		t.Fatal(err)
	}

	// Create rel with history.
	r := types.NewRelationship(100, 1, 1, 2)
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(100, 0, r); err != nil {
		t.Fatal(err)
	}

	// Create rel without history.
	r2 := types.NewRelationship(200, 1, 1, 2)
	if err := ms.PutRelationship(r2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachRelHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachRelHistoryID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs, want 1 (only rel with history)", len(seen))
	}
	if _, ok := seen[100]; !ok {
		t.Error("rel 100 should appear (has history)")
	}
}

// ─── TieredStore ForEach ──────────────────────────────────────────────────────

func TestTieredStore_ForEachNodeID_AllShards(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Ref node (directly on refShard since no label registry set).
	refNode := types.NewNode(gen.Generate(), 1, nil)
	if err := ts.refShard.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}

	// Event node (via hot shard — all unrecognized tokens route to event).
	evNode := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(evNode); err != nil {
		t.Fatalf("PutNode event: %v", err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeID: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("got %d IDs, want 2 (ref + event)", len(seen))
	}
	if _, ok := seen[refNode.InternalID().SnowflakeID()]; !ok {
		t.Error("missing ref node")
	}
	if _, ok := seen[evNode.InternalID().SnowflakeID()]; !ok {
		t.Error("missing event node")
	}
}

func TestTieredStore_ForEachNodeID_EarlyStop(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)

	gen := tieredNodeGen(t)

	// Add 3 event nodes + 2 ref nodes.
	for i := 0; i < 3; i++ {
		n := types.NewNode(gen.Generate(), 3, nil) // event
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := 0; i < 2; i++ {
		n := types.NewNode(gen.Generate(), 1, nil)
		if err := ts.refShard.PutNode(n); err != nil {
			t.Fatalf("PutNode ref: %v", err)
		}
	}

	count := 0
	err := ts.ForEachNodeID(func(id snowflake.ID) bool {
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
	n1 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}

	// Rotate to create warm shard.
	ts.mu.Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.mu.Unlock()
		t.Fatalf("RotateHotShard: %v", err)
	}
	ts.mu.Unlock()

	// Add event node to new hot shard.
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
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
	n1 := types.NewNode(gen.Generate(), 3, nil)
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create rel.
	relGen := tieredRelGen(t)
	r := types.NewRelationship(relGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachRelID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
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
	n1 := types.NewNode(gen.Generate(), 1, nil)
	if err := ts.refShard.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	id1 := n1.InternalID().SnowflakeID()
	if err := ts.PutNodeVersion(id1, 0, n1); err != nil {
		t.Fatal(err)
	}

	// Event node with history (via PutNode routing).
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	id2 := n2.InternalID().SnowflakeID()
	if err := ts.PutNodeVersion(id2, 0, n2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachNodeHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
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
	n1 := types.NewNode(gen.Generate(), 3, nil)
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(relGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	relID := r.InternalID().SnowflakeID()
	if err := ts.PutRelVersion(relID, 0, r); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ts.ForEachRelHistoryID(func(id snowflake.ID) bool {
		seen[id] = struct{}{}
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
	n1 := types.NewNode(gen.Generate(), 3, nil)
	n2 := types.NewNode(gen.Generate(), 3, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	// Create 3 rels.
	for i := 0; i < 3; i++ {
		r := types.NewRelationship(relGen.Generate(), 1, n1.InternalID().SnowflakeID(), n2.InternalID().SnowflakeID())
		if err := ts.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}

	count := 0
	err := ts.ForEachRelID(func(id snowflake.ID) bool {
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

// ─── Graph-level: temporal queries still work ─────────────────────────────────

func TestGraph_ForEachBasedTemporalQueries(t *testing.T) {
	t.Parallel()

	g, err := New(Config{
		SnowflakeNodeID: 0,
		Store:           NewMemoryStore(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Add a node.
	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	nodeID := n.InternalID().SnowflakeID()
	time.Sleep(2 * time.Millisecond)
	now := types.Instant(time.Now().UnixMilli())

	// GetNodesValidAt should find Alice.
	nodes, err := g.GetNodesValidAt(now)
	if err != nil {
		t.Fatalf("GetNodesValidAt: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0].InternalID().SnowflakeID() != nodeID {
		t.Error("wrong node ID")
	}

	// Add a relationship.
	n2, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	_, err = g.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	time.Sleep(2 * time.Millisecond)
	now2 := types.Instant(time.Now().UnixMilli())

	// GetRelationshipsValidAt should find the rel.
	rels, err := g.GetRelationshipsValidAt(now2)
	if err != nil {
		t.Fatalf("GetRelationshipsValidAt: %v", err)
	}
	if len(rels) != 1 {
		t.Fatalf("got %d rels, want 1", len(rels))
	}

	// GetNodesValidDuring should find both nodes.
	nodesDuring, err := g.GetNodesValidDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetNodesValidDuring: %v", err)
	}
	if len(nodesDuring) != 2 {
		t.Fatalf("got %d nodes during, want 2", len(nodesDuring))
	}

	// GetRelationshipsValidDuring should find the rel.
	relsDuring, err := g.GetRelationshipsValidDuring(now, now2+1000)
	if err != nil {
		t.Fatalf("GetRelationshipsValidDuring: %v", err)
	}
	if len(relsDuring) != 1 {
		t.Fatalf("got %d rels during, want 1", len(relsDuring))
	}

	// Snapshot should work.
	snap, err := g.Snapshot(now2)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.NodeCount != 2 {
		t.Fatalf("snapshot has %d nodes, want 2", snap.NodeCount)
	}
	if snap.RelCount != 1 {
		t.Fatalf("snapshot has %d rels, want 1", snap.RelCount)
	}
}
