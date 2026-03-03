package graph

import (
	"context"
	"errors"
	"sync"
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

// --- Fix 1: DeleteRelationshipWithContext entity lock tests ---

func TestDeleteRelWithContext_UsesEntityLock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"count": 0})
	rID := r.InternalID().SnowflakeID()

	// Concurrent delete + update on same rel. Under -race, this would fail
	// without the entity lock in DeleteRelationshipWithContext.
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				_ = g.DeleteRelationshipWithContext(context.Background(), rID)
			} else {
				_, _ = g.UpdateRelationshipWithContext(context.Background(), rID, map[string]any{"count": idx})
			}
		}(i)
	}
	wg.Wait()

	// Rel should be gone (at least one delete succeeded).
	_, err := g.GetRelationship(rID)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound after concurrent delete, got %v", err)
	}
}

func TestDeleteRelWithContext_ConcurrentUpdate(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"x": 1})
	rID := r.InternalID().SnowflakeID()

	// Delete wins, rel is gone, tombstone version exists.
	err := g.DeleteRelationshipWithContext(context.Background(), rID)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify deleted.
	_, err = g.GetRelationship(rID)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}

	// Verify tombstone history preserved.
	hist, err := g.GetRelHistory(rID)
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if len(hist) == 0 {
		t.Fatal("expected tombstone version in history")
	}
	last := hist[len(hist)-1]
	if tm := last.Temporal(); tm == nil || tm.DeletedAt == 0 {
		t.Fatal("last version should have DeletedAt set (tombstone)")
	}
}

// --- Fix 3: DeleteNodeWithContext relationship lock tests ---

func TestDeleteNodeWithContext_LocksRelationships(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("KNOWS", a, b, map[string]any{"v": 0})
	aID := a.InternalID().SnowflakeID()
	rID := r.InternalID().SnowflakeID()

	// Concurrent delete node + update connected rel — race detector catches issues.
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if idx == 0 {
				_ = g.DeleteNodeWithContext(context.Background(), aID)
			} else {
				_, _ = g.UpdateRelationshipWithContext(context.Background(), rID, map[string]any{"v": idx})
			}
		}(i)
	}
	wg.Wait()

	// Node and rel should be gone.
	_, err := g.GetNode(aID)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
	_, err = g.GetRelationship(rID)
	if !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("expected ErrRelNotFound, got %v", err)
	}
}

func TestDeleteNodeWithContext_ConcurrentAddRel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	aID := a.InternalID().SnowflakeID()

	// Pre-create some rels.
	for range 5 {
		_, _ = g.AddRelationship("EDGE", a, b, nil)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: delete node (cascade deletes all rels).
	go func() {
		defer wg.Done()
		_ = g.DeleteNodeWithContext(context.Background(), aID)
	}()

	// Goroutine 2: try to add more rels while delete is happening.
	go func() {
		defer wg.Done()
		for range 10 {
			_, _ = g.AddRelationship("EDGE", a, b, nil)
		}
	}()

	wg.Wait()

	// Node should be gone.
	_, err := g.GetNode(aID)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}
}

// --- Group F: extractProvenance bounds checks (v3.0.53) ---

func TestExtractProvenance_OutOfBoundsInt(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := extractProvenance(map[string]any{"tkg_auth_level": int(256)})
	if err == nil {
		t.Fatal("expected error for tkg_auth_level=256, got nil")
	}
}

func TestExtractProvenance_NegativeInt(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := extractProvenance(map[string]any{"tkg_auth_level": int(-1)})
	if err == nil {
		t.Fatal("expected error for tkg_auth_level=-1, got nil")
	}
}

func TestExtractProvenance_OutOfBoundsFloat(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := extractProvenance(map[string]any{"tkg_auth_level": float64(300)})
	if err == nil {
		t.Fatal("expected error for tkg_auth_level=300.0, got nil")
	}
}

func TestExtractProvenance_InvalidType(t *testing.T) {
	t.Parallel()
	_, _, _, _, _, err := extractProvenance(map[string]any{"tkg_auth_level": "5"})
	if err == nil {
		t.Fatal("expected error for tkg_auth_level=string(\"5\"), got nil")
	}
}

func TestExtractProvenance_ValidBoundary(t *testing.T) {
	t.Parallel()
	// int(0) — minimum valid value
	_, _, _, level0, _, err := extractProvenance(map[string]any{"tkg_auth_level": int(0)})
	if err != nil {
		t.Fatalf("int(0): unexpected error: %v", err)
	}
	if level0 != 0 {
		t.Errorf("int(0): got authLevel=%d, want 0", level0)
	}

	// int(255) — maximum valid value
	_, _, _, level255, _, err := extractProvenance(map[string]any{"tkg_auth_level": int(255)})
	if err != nil {
		t.Fatalf("int(255): unexpected error: %v", err)
	}
	if level255 != 255 {
		t.Errorf("int(255): got authLevel=%d, want 255", level255)
	}
}

// --- Helpers ---

// deadlineInPast returns a time in the past to create an already-expired deadline.
func deadlineInPast() time.Time {
	return time.Now().Add(-time.Second)
}
