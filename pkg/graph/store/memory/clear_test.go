package memory

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// --- Store.Clear tests ---

func TestMemoryStoreClear_Empty(t *testing.T) {
	t.Parallel()
	ms := New()
	if err := ms.Clear(); err != nil {
		t.Fatalf("Clear on empty store: %v", err)
	}
	count, _ := ms.NodeCount()
	if count != 0 {
		t.Fatalf("node count after clear: %d", count)
	}
}

func TestMemoryStoreClear_ClearsEntities(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 1, nil)
	if err := ms.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 1, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	if err := ms.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	nc, _ := ms.NodeCount()
	rc, _ := ms.RelationshipCount()
	if nc != 0 || rc != 0 {
		t.Fatalf("after clear: nodes=%d, rels=%d", nc, rc)
	}

	_, err := ms.GetNode(types.NodeID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	_, err = ms.GetRelationship(types.RelID(100))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}
}

func TestMemoryStoreClear_ClearsHistory(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(types.NodeID(1), 0, n); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	history, err := ms.GetNodeHistory(types.NodeID(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history after clear: got %d entries", len(history))
	}
}

func TestMemoryStoreClear_ClearsPropertyIndexes(t *testing.T) {
	t.Parallel()
	ms := New()

	n := types.NewNode(types.NodeID(snowflake.ID(1)), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	// Index should be gone — CreatePropertyIndex should succeed again.
	// But first the store is empty, so there's nothing to index.
	err := ms.CreatePropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex after clear: %v", err)
	}
}

// TestMemoryStoreClear_ClearsTemporalIndexes mirrors the BadgerStore F2
// test for the Store backend.
func TestMemoryStoreClear_ClearsTemporalIndexes(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex: %v", err)
	}
	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateTemporalIndex(1); err != nil {
		t.Fatalf("CreateTemporalIndex after Clear: %v", err)
	}
}

// TestMemoryStoreClear_ClearsHFIndexes covers the Store symmetric of
// F2 — flagged in the MR-A agent report as a same-shape gap missed by
// the original brief and folded in for parity with BadgerStore.
func TestMemoryStoreClear_ClearsHFIndexes(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex: %v", err)
	}
	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex after Clear: %v", err)
	}
}

func TestMemoryStoreCreateHighFrequencyIndexRejectsInvalidBucket(t *testing.T) {
	t.Parallel()
	ms := New()

	if err := ms.CreateHighFrequencyIndex(1, 0); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(0) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, -time.Second); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(-1s) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Nanosecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1ns) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, 1500*time.Microsecond); !errors.Is(err, ErrInvalidTemporalIndexConfig) {
		t.Fatalf("CreateHighFrequencyIndex(1.5ms) err = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := ms.CreateHighFrequencyIndex(1, time.Hour); err != nil {
		t.Fatalf("CreateHighFrequencyIndex valid bucket after rejects: %v", err)
	}
}
