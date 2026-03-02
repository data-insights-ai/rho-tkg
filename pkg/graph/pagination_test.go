package graph

import (
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
)

func TestPaginateIDs_EmptyInput(t *testing.T) {
	result := paginateIDs(nil, 0, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_NoLimitNoCursor(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := paginateIDs(ids, 0, 0)
	if len(result) != 5 {
		t.Fatalf("expected 5, got %d", len(result))
	}
	for i, id := range ids {
		if result[i] != id {
			t.Fatalf("index %d: expected %d, got %d", i, id, result[i])
		}
	}
}

func TestPaginateIDs_LimitOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := paginateIDs(ids, 0, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 10 || result[1] != 20 || result[2] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := paginateIDs(ids, 20, 0)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 || result[2] != 50 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorAndLimit(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := paginateIDs(ids, 20, 2)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorBeyondAll(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := paginateIDs(ids, 100, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_CursorAtExactID(t *testing.T) {
	// Cursor at exactly the last ID should return nil.
	ids := []snowflake.ID{10, 20, 30}
	result := paginateIDs(ids, 30, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_LimitLargerThanRemaining(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := paginateIDs(ids, 10, 100)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 20 || result[1] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

// ─── MemoryStore pagination integration tests ────────────────────────────────

func seedMemoryStore(t *testing.T, ms *MemoryStore, label uint16, count int) []snowflake.ID {
	t.Helper()
	ids := make([]snowflake.ID, count)
	for i := range count {
		id := snowflake.ID(1000 + i)
		ids[i] = id
		n := types.NewNode(id, label, nil)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	return ids
}

func TestMemoryStoreNodesByLabel_Paginated(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	seedMemoryStore(t, ms, 10, 10)

	got, err := ms.NodesByLabel(10, QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	// Verify sorted.
	for i := 1; i < len(got); i++ {
		if got[i].InternalID().SnowflakeID() <= got[i-1].InternalID().SnowflakeID() {
			t.Fatal("results not sorted")
		}
	}
}

func TestMemoryStoreNodesByLabel_MultiPageWalk(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	seedMemoryStore(t, ms, 10, 10)

	var all []*types.Node
	var cursor snowflake.ID
	for {
		page, err := ms.NodesByLabel(10, QueryOpts{Limit: 3, After: cursor})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		cursor = page[len(page)-1].InternalID().SnowflakeID()
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
		id := n.InternalID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %d in multi-page walk", id)
		}
		seen[id] = struct{}{}
	}
}

func TestMemoryStoreNodesByLabel_ZeroOptsReturnsAll(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	seedMemoryStore(t, ms, 10, 5)

	got, err := ms.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5, got %d", len(got))
	}
}

func TestMemoryStoreRelationshipsByType_Paginated(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	// Create 2 nodes and 5 rels.
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(snowflake.ID(100+i), 5, snowflake.ID(1), snowflake.ID(2))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}

	got, err := ms.RelationshipsByType(5, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestMemoryStoreAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	seedMemoryStore(t, ms, 10, 7)

	got, err := ms.AllNodes(QueryOpts{Limit: 4})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}
}

func TestMemoryStoreAllRelationships_Paginated(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = ms.PutNode(n1)
	_ = ms.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(snowflake.ID(100+i), 5, snowflake.ID(1), snowflake.ID(2))
		_ = ms.PutRelationship(r)
	}

	got, err := ms.AllRelationships(QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestMemoryStoreNodesByLabelAndProperty_PaginatedIndexed(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(id, 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := ms.CreatePropertyIndex(10, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	got, err := ms.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestMemoryStoreNodesByLabelAndProperty_PaginatedFallback(t *testing.T) {
	t.Parallel()
	ms := NewMemoryStore()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(id, 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	// No index — fallback path.

	got, err := ms.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

// ─── BadgerStore pagination integration tests ────────────────────────────────

func seedBadgerStore(t *testing.T, bs *BadgerStore, label uint16, count int) []snowflake.ID {
	t.Helper()
	ids := make([]snowflake.ID, count)
	for i := range count {
		id := snowflake.ID(1000 + i)
		ids[i] = id
		n := types.NewNode(id, label, nil)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", id, err)
		}
	}
	return ids
}

func TestBadgerStoreNodesByLabel_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 10)

	got, err := bs.NodesByLabel(10, QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].InternalID().SnowflakeID() <= got[i-1].InternalID().SnowflakeID() {
			t.Fatal("results not sorted")
		}
	}
}

func TestBadgerStoreNodesByLabel_MultiPageWalk(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 10)

	var all []*types.Node
	var cursor snowflake.ID
	for {
		page, err := bs.NodesByLabel(10, QueryOpts{Limit: 3, After: cursor})
		if err != nil {
			t.Fatalf("NodesByLabel: %v", err)
		}
		if len(page) == 0 {
			break
		}
		all = append(all, page...)
		cursor = page[len(page)-1].InternalID().SnowflakeID()
		if len(page) < 3 {
			break
		}
	}
	if len(all) != 10 {
		t.Fatalf("multi-page walk: expected 10, got %d", len(all))
	}
	seen := make(map[snowflake.ID]struct{})
	for _, n := range all {
		id := n.InternalID().SnowflakeID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate ID %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestBadgerStoreNodesByLabel_ZeroOptsReturnsAll(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 5)

	got, err := bs.NodesByLabel(10, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5, got %d", len(got))
	}
}

func TestBadgerStoreRelationshipsByType_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = bs.PutNode(n1)
	_ = bs.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(snowflake.ID(100+i), 5, snowflake.ID(1), snowflake.ID(2))
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}

	got, err := bs.RelationshipsByType(5, QueryOpts{Limit: 2})
	if err != nil {
		t.Fatalf("RelationshipsByType: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestBadgerStoreAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	seedBadgerStore(t, bs, 10, 7)

	got, err := bs.AllNodes(QueryOpts{Limit: 4})
	if err != nil {
		t.Fatalf("AllNodes: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("expected 4, got %d", len(got))
	}
}

func TestBadgerStoreAllRelationships_Paginated(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()
	n1 := types.NewNode(snowflake.ID(1), 10, nil)
	n2 := types.NewNode(snowflake.ID(2), 10, nil)
	_ = bs.PutNode(n1)
	_ = bs.PutNode(n2)
	for i := range 5 {
		r := types.NewRelationship(snowflake.ID(100+i), 5, snowflake.ID(1), snowflake.ID(2))
		_ = bs.PutRelationship(r)
	}

	got, err := bs.AllRelationships(QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("AllRelationships: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_PaginatedIndexed(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(id, 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.CreatePropertyIndex(10, "name"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	got, err := bs.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestBadgerStoreNodesByLabelAndProperty_PaginatedFallback(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t)
	defer func() { _ = bs.Close() }()

	for i := range 6 {
		id := snowflake.ID(1000 + i)
		n := types.NewNode(id, 10, nil)
		ps, _ := types.NewPropertySlice(map[string]any{"name": "Alice"})
		n.SetProperties(ps)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	// No index — fallback path.

	got, err := bs.NodesByLabelAndProperty(10, "name", "Alice", QueryOpts{Limit: 3})
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
	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 10 {
		_, err := g.AddNode([]string{"Person"}, map[string]any{"i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.NodesByLabel("Person", QueryOpts{Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
}

func TestGraphAllNodes_Paginated(t *testing.T) {
	t.Parallel()
	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 5 {
		_, err := g.AddNode([]string{fmt.Sprintf("Type%d", i)}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.AllNodes(QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestGraphNodesByLabelAndProperty_Paginated(t *testing.T) {
	t.Parallel()
	g, err := New(Config{SnowflakeNodeID: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()

	for i := range 5 {
		_, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice", "i": i})
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := g.NodesByLabelAndProperty("Person", "name", "Alice", QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}
