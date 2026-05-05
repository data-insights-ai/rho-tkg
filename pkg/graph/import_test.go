package graph

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestImportNodeWithID_Basic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(12345)
	n, err := g.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}
	if n.ID() != types.NodeID(id) {
		t.Errorf("ID = %d, want %d", n.ID(), id)
	}

	// Verify retrievable by the same ID.
	got, err := g.GetNode(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	name, ok := got.GetProperty("name")
	if !ok || name != "Alice" {
		t.Errorf("name = %v (ok=%v), want Alice", name, ok)
	}
}

func TestImportNodeWithID_Collision(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(12345)
	_, err := g.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}

	// Second import with same ID should fail.
	_, err = g.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"B"}, nil)
	if !errors.Is(err, ErrNodeExists) {
		t.Errorf("err = %v, want ErrNodeExists", err)
	}
}

func TestImportNodeWithID_ZeroID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, err := g.ImportNodeWithID(context.Background(), 0, []string{"A"}, nil)
	if !errors.Is(err, ErrZeroID) {
		t.Errorf("err = %v, want ErrZeroID", err)
	}
}

func TestImportNodeWithID_Validation(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// No labels.
	_, err := g.ImportNodeWithID(context.Background(), types.NodeID(100), nil, nil)
	if !errors.Is(err, ErrNoLabels) {
		t.Errorf("no labels: err = %v, want ErrNoLabels", err)
	}
}

func TestImportRelationshipWithID_Basic(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.AddNode([]string{"A"}, nil)
	n2, _ := g.AddNode([]string{"B"}, nil)

	relID := snowflake.ID(99999)
	r, err := g.ImportRelationshipWithID(context.Background(), types.RelID(relID), "KNOWS", n1, n2, map[string]any{"since": int64(2024)})
	if err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}
	if r.ID() != types.RelID(relID) {
		t.Errorf("ID = %d, want %d", r.ID(), relID)
	}

	// Verify retrievable.
	got, err := g.GetRelationship(types.RelID(relID))
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2024) {
		t.Errorf("since = %v (ok=%v), want 2024", since, ok)
	}
}

func TestImportRelationshipWithID_Collision(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n1, _ := g.AddNode([]string{"A"}, nil)
	n2, _ := g.AddNode([]string{"B"}, nil)

	relID := snowflake.ID(99999)
	_, err := g.ImportRelationshipWithID(context.Background(), types.RelID(relID), "KNOWS", n1, n2, nil)
	if err != nil {
		t.Fatalf("first import: %v", err)
	}

	_, err = g.ImportRelationshipWithID(context.Background(), types.RelID(relID), "LIKES", n1, n2, nil)
	if !errors.Is(err, ErrRelExists) {
		t.Errorf("err = %v, want ErrRelExists", err)
	}
}

func TestGraphTx_ImportNodeWithID(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	tx := g.BeginTx()
	defer tx.Rollback()

	id := snowflake.ID(55555)
	n, err := tx.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}
	if n.ID() != types.NodeID(id) {
		t.Errorf("ID = %d, want %d", n.ID(), id)
	}

	// Tracked for rollback.
	created := tx.CreatedNodeIDs()
	if len(created) != 1 || created[0] != types.NodeID(id) {
		t.Errorf("CreatedNodeIDs = %v, want [%d]", created, id)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify persisted.
	got, err := g.GetNode(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNode after commit: %v", err)
	}
	name, _ := got.GetProperty("name")
	if name != "Bob" {
		t.Errorf("name = %v, want Bob", name)
	}
}

func TestGraphTx_ImportNodeWithID_Rollback(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	id := snowflake.ID(77777)

	tx := g.BeginTx()
	_, err := tx.ImportNodeWithID(context.Background(), types.NodeID(id), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Node should be gone after rollback.
	_, err = g.GetNode(types.NodeID(id))
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("after rollback: err = %v, want ErrNodeNotFound", err)
	}
}
