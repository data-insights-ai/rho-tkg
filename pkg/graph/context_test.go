package graph

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newTestGraph is declared in batch_test.go (same package).

// --- Group A: Pre-flight cancellation (8 tests) ---
// Context already cancelled at method entry → returns context.Canceled, no side effects.

func TestAddNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.AddNodeWithContext(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AddNodeWithContext err = %v, want context.Canceled", err)
	}

	// No node should have been created.
	count, _ := g.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount = %d after cancelled add, want 0", count)
	}
}

func TestAddRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)

	_, err := g.AddRelationshipWithContext(ctx, "KNOWS", a, b, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AddRelationshipWithContext err = %v, want context.Canceled", err)
	}

	count, _ := g.RelationshipCount()
	if count != 0 {
		t.Errorf("RelationshipCount = %d after cancelled add, want 0", count)
	}
}

func TestUpdateNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.UpdateNodeWithContext(ctx, n.InternalID().SnowflakeID(), map[string]any{"name": "Bob"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateNodeWithContext err = %v, want context.Canceled", err)
	}
}

func TestUpdateRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"since": 2020})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.UpdateRelationshipWithContext(ctx, r.InternalID().SnowflakeID(), map[string]any{"since": 2025})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateRelationshipWithContext err = %v, want context.Canceled", err)
	}
}

func TestDeleteNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.DeleteNodeWithContext(ctx, n.InternalID().SnowflakeID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteNodeWithContext err = %v, want context.Canceled", err)
	}

	// Node should still exist.
	_, err = g.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Errorf("GetNode after cancelled delete returned error: %v", err)
	}
}

func TestDeleteRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.DeleteRelationshipWithContext(ctx, r.InternalID().SnowflakeID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteRelationshipWithContext err = %v, want context.Canceled", err)
	}

	// Relationship should still exist.
	_, err = g.GetRelationship(r.InternalID().SnowflakeID())
	if err != nil {
		t.Errorf("GetRelationship after cancelled delete returned error: %v", err)
	}
}

func TestGetNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.GetNodeWithContext(ctx, n.InternalID().SnowflakeID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetNodeWithContext err = %v, want context.Canceled", err)
	}
}

func TestGetRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.GetRelationshipWithContext(ctx, r.InternalID().SnowflakeID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetRelationshipWithContext err = %v, want context.Canceled", err)
	}
}

// --- Group B: Happy path with valid context (8 tests) ---
// Non-cancelled context → identical behavior to non-context methods.

func TestAddNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.AddNodeWithContext(context.Background(), []string{"Person", "Employee"}, map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNodeWithContext error: %v", err)
	}
	if n == nil {
		t.Fatal("AddNodeWithContext returned nil node")
	}

	labels := g.NodeLabels(n)
	if len(labels) != 2 || labels[0] != "Person" || labels[1] != "Employee" {
		t.Errorf("labels = %v, want [Person Employee]", labels)
	}
	if v, _ := n.GetProperty("name"); v != "Alice" {
		t.Errorf("name = %v, want Alice", v)
	}
	if ig := n.Integrity(); ig == nil || ig.Hash == "" {
		t.Error("integrity hash not set on created node")
	}
}

func TestAddRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})

	r, err := g.AddRelationshipWithContext(context.Background(), "KNOWS", a, b, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationshipWithContext error: %v", err)
	}
	if r == nil {
		t.Fatal("AddRelationshipWithContext returned nil rel")
	}

	typeName := g.RelationshipType(r)
	if typeName != "KNOWS" {
		t.Errorf("type = %q, want KNOWS", typeName)
	}
	if v, _ := r.GetProperty("since"); v != 2020 {
		t.Errorf("since = %v (%T), want 2020", v, v)
	}
	if ig := r.Integrity(); ig == nil || ig.Hash == "" {
		t.Error("integrity hash not set on created rel")
	}
}

func TestUpdateNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	updated, err := g.UpdateNodeWithContext(context.Background(), id, map[string]any{"name": "Bob", "age": 25})
	if err != nil {
		t.Fatalf("UpdateNodeWithContext error: %v", err)
	}
	if v, _ := updated.GetProperty("name"); v != "Bob" {
		t.Errorf("name = %v, want Bob", v)
	}
	if updated.Version() != 1 {
		t.Errorf("version = %d, want 1", updated.Version())
	}

	// Verify version history was saved.
	history, _ := g.GetNodeHistory(id)
	if len(history) != 1 {
		t.Errorf("history len = %d, want 1", len(history))
	}
}

func TestUpdateRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.InternalID().SnowflakeID()

	updated, err := g.UpdateRelationshipWithContext(context.Background(), id, map[string]any{"since": 2025})
	if err != nil {
		t.Fatalf("UpdateRelationshipWithContext error: %v", err)
	}
	if v, _ := updated.GetProperty("since"); v != 2025 {
		t.Errorf("since = %v (%T), want 2025", v, v)
	}
	if updated.Version() != 1 {
		t.Errorf("version = %d, want 1", updated.Version())
	}

	// Verify version history was saved.
	history, _ := g.GetRelHistory(id)
	if len(history) != 1 {
		t.Errorf("history len = %d, want 1", len(history))
	}
}

func TestDeleteNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"Person"}, nil)
	b, _ := g.AddNode([]string{"Person"}, nil)
	g.AddRelationship("KNOWS", a, b, nil)

	err := g.DeleteNodeWithContext(context.Background(), a.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("DeleteNodeWithContext error: %v", err)
	}

	// Node and cascade-deleted rels should be gone.
	_, err = g.GetNode(a.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: err = %v, want ErrNodeNotFound", err)
	}

	relCount, _ := g.RelationshipCount()
	if relCount != 0 {
		t.Errorf("RelationshipCount = %d after cascade delete, want 0", relCount)
	}
}

func TestDeleteRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, nil)

	err := g.DeleteRelationshipWithContext(context.Background(), r.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("DeleteRelationshipWithContext error: %v", err)
	}

	_, err = g.GetRelationship(r.InternalID().SnowflakeID())
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: err = %v, want ErrRelNotFound", err)
	}
}

func TestGetNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	got, err := g.GetNodeWithContext(context.Background(), id)
	if err != nil {
		t.Fatalf("GetNodeWithContext error: %v", err)
	}
	if v, _ := got.GetProperty("name"); v != "Alice" {
		t.Errorf("name = %v, want Alice", v)
	}
}

func TestGetRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.InternalID().SnowflakeID()

	got, err := g.GetRelationshipWithContext(context.Background(), id)
	if err != nil {
		t.Fatalf("GetRelationshipWithContext error: %v", err)
	}
	if v, _ := got.GetProperty("since"); v != 2020 {
		t.Errorf("since = %v (%T), want 2020", v, v)
	}
}

// --- Group C: Deadline exceeded (4 tests) ---

func TestAddNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	_, err := g.AddNodeWithContext(ctx, []string{"Person"}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestUpdateNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	_, err := g.UpdateNodeWithContext(ctx, n.InternalID().SnowflakeID(), map[string]any{"x": 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestDeleteNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	err := g.DeleteNodeWithContext(ctx, n.InternalID().SnowflakeID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestGetNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.AddNode([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	_, err := g.GetNodeWithContext(ctx, n.InternalID().SnowflakeID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// --- Group D: Delegation regression (4 tests) ---
// Non-context methods still work after refactoring.

func TestAddNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode error: %v", err)
	}
	if n == nil {
		t.Fatal("AddNode returned nil")
	}
	if v, _ := n.GetProperty("name"); v != "Alice" {
		t.Errorf("name = %v, want Alice", v)
	}
}

func TestUpdateNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	updated, err := g.UpdateNode(n.InternalID().SnowflakeID(), map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("UpdateNode error: %v", err)
	}
	if v, _ := updated.GetProperty("name"); v != "Bob" {
		t.Errorf("name = %v, want Bob", v)
	}
}

func TestDeleteNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, nil)
	err := g.DeleteNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("DeleteNode error: %v", err)
	}
	_, err = g.GetNode(n.InternalID().SnowflakeID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("GetNode after delete: err = %v, want ErrNodeNotFound", err)
	}
}

func TestGetNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	got, err := g.GetNode(n.InternalID().SnowflakeID())
	if err != nil {
		t.Fatalf("GetNode error: %v", err)
	}
	if v, _ := got.GetProperty("name"); v != "Alice" {
		t.Errorf("name = %v, want Alice", v)
	}
}

// --- Group E: Edge cases (4 tests) ---

func TestUpdateNodeWithContextEmptyUpdatesNoop(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.InternalID().SnowflakeID()

	got, err := g.UpdateNodeWithContext(context.Background(), id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateNodeWithContext empty updates error: %v", err)
	}
	// Version should not be bumped.
	if got.Version() != 0 {
		t.Errorf("version = %d, want 0 (no bump on empty updates)", got.Version())
	}
}

func TestUpdateRelWithContextEmptyUpdatesNoop(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.InternalID().SnowflakeID()

	got, err := g.UpdateRelationshipWithContext(context.Background(), id, map[string]any{})
	if err != nil {
		t.Fatalf("UpdateRelationshipWithContext empty updates error: %v", err)
	}
	if got.Version() != 0 {
		t.Errorf("version = %d, want 0 (no bump on empty updates)", got.Version())
	}
}

func TestAddNodeWithContextValidationError(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	// Validation error (bad property value — struct) takes priority over context.
	// This verifies validation runs before context checks on the store path.
	type badStruct struct{ X int }
	_, err := g.AddNodeWithContext(context.Background(), []string{"Person"}, map[string]any{"bad": badStruct{1}})
	if err == nil {
		t.Fatal("AddNodeWithContext with bad property should fail")
	}

	count, _ := g.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount = %d after failed add, want 0", count)
	}
}

func TestCheckCtxBackground(t *testing.T) {
	t.Parallel()
	if err := checkCtx(context.Background()); err != nil {
		t.Fatalf("checkCtx(Background) = %v, want nil", err)
	}
}

// --- Helpers ---

// deadlineInPast returns a time in the past to create an already-expired deadline.
func deadlineInPast() time.Time {
	return time.Now().Add(-time.Second)
}
