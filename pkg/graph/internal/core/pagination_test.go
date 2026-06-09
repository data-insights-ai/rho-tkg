package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ─── memory.Store pagination integration tests ────────────────────────────────

func seedMemoryStore(t *testing.T, ms *memory.Store, label uint16, count int) []snowflake.ID {
	t.Helper()
	ids := make([]snowflake.ID, count)
	for i := range count {
		id := snowflake.ID(1000 + i)
		ids[i] = id
		n := types.NewNode(types.NodeID(id), label, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	return ids
}

func TestMemoryStoreNodesByLabel_Paginated(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	seedMemoryStore(t, ms, 10, 10)

	got, err := ms.NodesByLabel(10, storepkg.QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	// Verify sorted.
	for i := 1; i < len(got); i++ {
		if got[i].ID() <= got[i-1].ID() {
			t.Fatal("results not sorted")
		}
	}
}

func TestMemoryStoreNodesByLabel_MultiPageWalk(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	seedMemoryStore(t, ms, 10, 10)

	var all []*types.Node
	var cursor snowflake.ID
	for {
		page, err := ms.NodesByLabel(10, storepkg.QueryOpts{Limit: 3, After: types.EntityID(cursor)})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		cursor = page[len(page)-1].ID().SnowflakeID()
		if len(page) < 3 {
			break
		}
	}
	if len(all) != 10 {
		t.Fatalf("multi-page walk: expected 10, got %d", len(all))
	}
	// Verify no duplicates.
	seen := make(map[snowflake.ID]struct{})
	for _, n := range all {
		id := n.ID()
		if _, dup := seen[id.SnowflakeID()]; dup {
			t.Fatalf("duplicate ID %d in multi-page walk", id)
		}
		seen[id.SnowflakeID()] = struct{}{}
	}
}

func TestMemoryStoreNodesByLabel_ZeroOptsReturnsAll(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	seedMemoryStore(t, ms, 10, 5)

	got, err := ms.NodesByLabel(10, storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5, got %d", len(got))
	}
}

func TestMemoryStoreRelationshipsByType_Paginated(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	// Create 2 nodes and 5 rels.
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}

	got, err := ms.RelationshipsByType(5, storepkg.QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestMemoryStoreAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	seedMemoryStore(t, ms, 10, 7)

	got, err := ms.AllNodes(storepkg.QueryOpts{Limit: 4})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}
}

func TestMemoryStoreAllRelationships_Paginated(t *testing.T) {
	t.Parallel()
	ms := memory.New()
	n1 := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	n2 := types.NewNode(types.NodeID(snowflake.ID(2)), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
		_ = ms.PutRelationship(r)
	}

	got, err := ms.AllRelationships(storepkg.QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestMemoryStoreNodesByLabelAndProperty_PaginatedIndexed(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(types.NodeID(id), 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ms.CreatePropertyIndex(10, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	got, err := ms.NodesByLabelAndProperty(10, "name", "Alice", storepkg.QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestMemoryStoreNodesByLabelAndProperty_PaginatedFallback(t *testing.T) {
	t.Parallel()
	ms := memory.New()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(types.NodeID(id), 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	// No index — fallback path.

	got, err := ms.NodesByLabelAndProperty(10, "name", "Alice", storepkg.QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

// ─── Graph-layer pagination tests ────────────────────────────────────────────

func TestGraphNodesByLabel_Paginated(t *testing.T) {
	t.Parallel()
	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 10 {
		_, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.Nodes.ByLabel("Person", storepkg.QueryOpts{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestGraphAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 5 {
		_, err := g.Nodes.Add(context.Background(), []string{fmt.Sprintf("Type%d", i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.Nodes.All(storepkg.QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestGraphNodesByLabelAndProperty_Paginated(t *testing.T) {
	t.Parallel()
	g, err := New(Config{SnowflakeNodeID: 0, Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 5 {
		_, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice", "i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", storepkg.QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}
