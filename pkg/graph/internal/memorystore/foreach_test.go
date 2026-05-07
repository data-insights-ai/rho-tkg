package memorystore

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── MemoryStore ForEach ──────────────────────────────────────────────────────

func TestMemoryStore_ForEachNodeID(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	ids := []snowflake.ID{10, 20, 30}
	for _, id := range ids {
		if err := ms.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
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
		if err := ms.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}

	count := 0
	err := ms.ForEachNodeID(func(id types.NodeID) bool {
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
	err := ms.ForEachNodeID(func(id types.NodeID) bool {
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
	if err := ms.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}

	relIDs := []snowflake.ID{100, 200, 300}
	for _, id := range relIDs {
		r := types.NewRelationship(types.RelID(id), 1, types.NodeID(1), types.NodeID(2))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", id, err)
		}
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
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
		if err := ms.PutNode(types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNodeVersion(types.NodeID(id), 0, types.NewNode(types.NodeID(id), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	// Third node without history.
	if err := ms.PutNode(types.NewNode(types.NodeID(30), 1, nil)); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachNodeHistoryID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
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
	if err := ms.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}

	// Create rel with history.
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(100, 0, r); err != nil {
		t.Fatal(err)
	}

	// Create rel without history.
	r2 := types.NewRelationship(types.RelID(200), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(r2); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachRelHistoryID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
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
