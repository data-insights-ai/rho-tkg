package graph

import (
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

// --- MemoryStore.Clear tests ---

func TestMemoryStoreClear_Empty(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
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
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(1), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}

	n2 := types.NewNode(snowflake.ID(2), 1, nil)
	if err := ms.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	r := types.NewRelationship(snowflake.ID(100), 1, snowflake.ID(1), snowflake.ID(2))
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

	_, err := ms.GetNode(snowflake.ID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	_, err = ms.GetRelationship(snowflake.ID(100))
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}
}

func TestMemoryStoreClear_ClearsHistory(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(1), 1, nil)
	if err := ms.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutNodeVersion(snowflake.ID(1), 0, n); err != nil {
		t.Fatal(err)
	}

	if err := ms.Clear(); err != nil {
		t.Fatal(err)
	}

	history, err := ms.GetNodeHistory(snowflake.ID(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history after clear: got %d entries", len(history))
	}
}

func TestMemoryStoreClear_ClearsPropertyIndexes(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	n := types.NewNode(snowflake.ID(1), 1, nil)
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

// --- BadgerStore.Clear tests ---

func TestBadgerStoreClear_Empty(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear on empty store: %v", err)
	}
	count, _ := bs.NodeCount()
	if count != 0 {
		t.Fatalf("node count after clear: %d", count)
	}
}

func TestBadgerStoreClear_ClearsAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(1), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	n2 := types.NewNode(snowflake.ID(2), 1, nil)
	if err := bs.PutNode(n2); err != nil {
		t.Fatal(err)
	}
	r := types.NewRelationship(snowflake.ID(100), 1, snowflake.ID(1), snowflake.ID(2))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	nc, _ := bs.NodeCount()
	rc, _ := bs.RelationshipCount()
	if nc != 0 || rc != 0 {
		t.Fatalf("after clear: nodes=%d, rels=%d", nc, rc)
	}

	_, err := bs.GetNode(snowflake.ID(1))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestBadgerStoreClear_ClearsHistory(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(1), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodeVersion(snowflake.ID(1), 0, n); err != nil {
		t.Fatal(err)
	}
	// Flush to persist history to Badger.
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	history, err := bs.GetNodeHistory(snowflake.ID(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Fatalf("history after clear: got %d entries", len(history))
	}
}

func TestBadgerStoreClear_ClearsPropertyIndexes(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)

	n := types.NewNode(snowflake.ID(1), 1, nil)
	n.SetProperties(mustPropertySlice(t, map[string]any{"name": "Alice"}))
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(1, "name"); err != nil {
		t.Fatal(err)
	}

	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}

	// Index should be gone after clear.
	err := bs.CreatePropertyIndex(1, "name")
	if err != nil {
		t.Fatalf("CreatePropertyIndex after clear: %v", err)
	}
}
