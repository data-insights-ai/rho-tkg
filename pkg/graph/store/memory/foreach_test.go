package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ─── Store ForEach ──────────────────────────────────────────────────────

func TestMemoryStore_ForEachNilCallbackReturnsInvalidMutation(t *testing.T) {
	t.Parallel()
	ms := New()

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "ForEachNodeID", run: func() error { return ms.ForEachNodeID(nil) }},
		{name: "ForEachRelID", run: func() error { return ms.ForEachRelID(nil) }},
		{name: "ForEachNodeHistoryID", run: func() error { return ms.ForEachNodeHistoryID(nil) }},
		{name: "ForEachRelHistoryID", run: func() error { return ms.ForEachRelHistoryID(nil) }},
	}
	for _, check := range checks {
		if err := check.run(); !errors.Is(err, ErrInvalidStoreMutation) {
			t.Fatalf("%s(nil) = %v, want ErrInvalidStoreMutation", check.name, err)
		}
	}
}

func TestMemoryStore_ForEachNodeID(t *testing.T) {
	t.Parallel()
	ms := New()

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
	ms := New()

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
	ms := New()

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
	ms := New()

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
	ms := New()

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
	ms := New()

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

// TestMemoryStore_ForEachDeletedNodeID pins the v4 DeletedIterationCapability
// contract: visits only IDs with history rows whose current row is absent.
// Live nodes with history MUST NOT appear; live nodes without history MUST
// NOT appear; deleted-but-historic nodes MUST appear.
func TestMemoryStore_ForEachDeletedNodeID(t *testing.T) {
	t.Parallel()
	ms := New()

	// Node 10: live + history (a live entity with a prior version).
	if err := ms.PutNode(types.NewNode(types.NodeID(10), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(types.NodeID(10), 0, types.NewNode(types.NodeID(10), 1, nil)); err != nil {
		t.Fatal(err)
	}
	// Node 20: live, no history.
	if err := ms.PutNode(types.NewNode(types.NodeID(20), 1, nil)); err != nil {
		t.Fatal(err)
	}
	// Node 30: deleted — history exists, current row absent.
	if err := ms.PutNodeVersion(types.NodeID(30), 0, types.NewNode(types.NodeID(30), 1, nil)); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachDeletedNodeID(func(id types.NodeID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDeletedNodeID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs (%v), want 1 (only node 30)", len(seen), seen)
	}
	if _, ok := seen[30]; !ok {
		t.Errorf("node 30 (deleted) should appear")
	}
	if _, ok := seen[10]; ok {
		t.Errorf("node 10 (live with history) must NOT appear")
	}
	if _, ok := seen[20]; ok {
		t.Errorf("node 20 (live without history) must NOT appear")
	}
}

func TestMemoryStore_ForEachDeletedRelID(t *testing.T) {
	t.Parallel()
	ms := New()
	if err := ms.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	// Rel 100: live + history.
	r := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(100, 0, r); err != nil {
		t.Fatal(err)
	}
	// Rel 200: live, no history.
	r2 := types.NewRelationship(types.RelID(200), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(r2); err != nil {
		t.Fatal(err)
	}
	// Rel 300: deleted (history only).
	r3 := types.NewRelationship(types.RelID(300), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelVersion(300, 0, r3); err != nil {
		t.Fatal(err)
	}

	seen := make(map[snowflake.ID]struct{})
	err := ms.ForEachDeletedRelID(func(id types.RelID) bool {
		seen[id.SnowflakeID()] = struct{}{}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDeletedRelID: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("got %d IDs (%v), want 1 (only rel 300)", len(seen), seen)
	}
	if _, ok := seen[300]; !ok {
		t.Errorf("rel 300 (deleted) should appear")
	}
	if _, ok := seen[100]; ok {
		t.Errorf("rel 100 (live with history) must NOT appear")
	}
	if _, ok := seen[200]; ok {
		t.Errorf("rel 200 (live without history) must NOT appear")
	}
}

func TestMemoryStore_ForEachCallbacksCanMutateStore(t *testing.T) {
	ms := New()
	if err := ms.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNode(types.NewNode(types.NodeID(2), 1, nil)); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(types.RelID(100), 1, types.NodeID(1), types.NodeID(2))
	if err := ms.PutRelationship(rel); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(types.NodeID(1), 0, types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelVersion(types.RelID(100), 0, rel); err != nil {
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
		err := ms.ForEachNodeID(func(types.NodeID) bool {
			cbErr = ms.PutNode(types.NewNode(types.NodeID(3), 1, nil))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelID", func() error {
		var cbErr error
		err := ms.ForEachRelID(func(types.RelID) bool {
			cbErr = ms.PutRelationship(types.NewRelationship(types.RelID(101), 1, types.NodeID(1), types.NodeID(2)))
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachNodeHistoryID", func() error {
		var cbErr error
		err := ms.ForEachNodeHistoryID(func(types.NodeID) bool {
			n := types.NewNode(types.NodeID(1), 1, nil)
			n.SetVersion(1)
			cbErr = ms.PutNodeVersion(types.NodeID(1), 1, n)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
	runWithTimeout("ForEachRelHistoryID", func() error {
		var cbErr error
		err := ms.ForEachRelHistoryID(func(types.RelID) bool {
			snap := rel.DeepCopy()
			snap.SetVersion(1)
			cbErr = ms.PutRelVersion(types.RelID(100), 1, snap)
			return false
		})
		if err != nil {
			return err
		}
		return cbErr
	})
}
