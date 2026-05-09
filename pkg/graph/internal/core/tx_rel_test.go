package core

import (
	"testing"
)

func TestGraphTx_DeleteRel_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n1, n2, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx, _ := g.BeginTx()
	if err := tx.DeleteRelationship(relID); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Relationship should be restored.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count after rollback: got %d, want 1", rc)
	}

	got, err := g.Rels.Get(relID)
	if err != nil {
		t.Fatalf("GetRelationship after rollback: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2020) {
		t.Errorf("since = %v, want 2020", since)
	}
}

func TestGraphTx_UpdateRelationship_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add("KNOWS", n1, n2, map[string]any{"weight": int64(5)})
	if err != nil {
		t.Fatal(err)
	}
	relID := r.ID()

	tx, _ := g.BeginTx()
	if _, err := tx.UpdateRelationship(relID, map[string]any{"weight": int64(99)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	got, err := g.Rels.Get(relID)
	if err != nil {
		t.Fatal(err)
	}
	weight, _ := got.GetProperty("weight")
	if weight != int64(5) {
		t.Errorf("weight = %v, want 5", weight)
	}
}

func TestTxAddRelationshipByID(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	r, err := tx.AddRelationshipByID("KNOWS", n1.ID(), n2.ID(), map[string]any{"since": int64(2024)})
	if err != nil {
		t.Fatalf("AddRelationshipByID: %v", err)
	}
	if r == nil {
		t.Fatal("relationship is nil")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Verify relationship persisted.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count after commit: got %d, want 1", rc)
	}
	got, err := g.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	since, ok := got.GetProperty("since")
	if !ok || since != int64(2024) {
		t.Errorf("since = %v, want 2024", since)
	}
}

func TestTxAddRelationshipByID_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	tx, _ := g.BeginTx()
	_, err = tx.AddRelationshipByID("KNOWS", n1.ID(), n2.ID(), nil)
	if err != nil {
		t.Fatalf("AddRelationshipByID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Relationship should be gone after rollback.
	rc, _ := g.Rels.Count()
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}

func TestTxAddRelationshipByIDIfAbsent(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID()
	id2 := n2.ID()

	// First: create in tx and commit.
	tx, _ := g.BeginTx()
	r, created, err := tx.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatalf("AddRelationshipByIDIfAbsent: %v", err)
	}
	if !created {
		t.Error("first call: want created=true")
	}
	if r == nil {
		t.Fatal("relationship is nil")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Second: same call should find existing and return created=false.
	tx2, _ := g.BeginTx()
	r2, created2, err := tx2.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatalf("second AddRelationshipByIDIfAbsent: %v", err)
	}
	if created2 {
		t.Error("second call: want created=false")
	}
	if r2 == nil {
		t.Fatal("second call: relationship is nil")
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	// Should still have exactly 1 relationship.
	rc, _ := g.Rels.Count()
	if rc != 1 {
		t.Errorf("rel count: got %d, want 1", rc)
	}
}

func TestTxAddRelationshipByIDIfAbsent_Rollback(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n1, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	id1 := n1.ID()
	id2 := n2.ID()

	// Create in tx then rollback.
	tx, _ := g.BeginTx()
	_, created, err := tx.AddRelationshipByIDIfAbsent("KNOWS", id1, id2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Error("want created=true")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Should be absent after rollback.
	rc, _ := g.Rels.Count()
	if rc != 0 {
		t.Errorf("rel count after rollback: got %d, want 0", rc)
	}
}
