package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestGraph is declared in batch_test.go (same package).

type cancelDuringStoreReadStore struct {
	*memory.Store

	afterGetNode  func()
	afterGetRel   func()
	afterIncoming func()
	afterOutgoing func()
}

func (s *cancelDuringStoreReadStore) GetNode(id types.NodeID) (*types.Node, error) {
	n, err := s.Store.GetNode(id)
	if err == nil && s.afterGetNode != nil {
		s.afterGetNode()
	}
	return n, err
}

func (s *cancelDuringStoreReadStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	r, err := s.Store.GetRelationship(id)
	if err == nil && s.afterGetRel != nil {
		s.afterGetRel()
	}
	return r, err
}

func (s *cancelDuringStoreReadStore) IncomingRelationships(id types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	rels, err := s.Store.IncomingRelationships(id, typeToken)
	if err == nil && s.afterIncoming != nil {
		s.afterIncoming()
	}
	return rels, err
}

func (s *cancelDuringStoreReadStore) OutgoingRelationships(id types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	rels, err := s.Store.OutgoingRelationships(id, typeToken)
	if err == nil && s.afterOutgoing != nil {
		s.afterOutgoing()
	}
	return rels, err
}

func onceFunc(fn func()) func() {
	var once sync.Once
	return func() {
		once.Do(fn)
	}
}

// --- Group A: Pre-flight cancellation (8 tests) ---
// Context already cancelled at method entry → returns context.Canceled, no side effects.

func TestAddNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Nodes.AddWithContext(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AddNodeWithContext err = %v, want context.Canceled", err)
	}

	// No node should have been created.
	count, _ := g.Nodes.Count()
	if count != 0 {
		t.Errorf("NodeCount = %d after cancelled add, want 0", count)
	}
}

func TestAddRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)

	_, err := g.Rels.AddWithContext(ctx, "KNOWS", a, b, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AddRelationshipWithContext err = %v, want context.Canceled", err)
	}

	count, _ := g.Rels.Count()
	if count != 0 {
		t.Errorf("RelationshipCount = %d after cancelled add, want 0", count)
	}
}

func TestUpdateNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Nodes.UpdateWithContext(ctx, n.ID(), map[string]any{"name": "Bob"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateNodeWithContext err = %v, want context.Canceled", err)
	}
}

func TestUpdateRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": 2020})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Rels.UpdateWithContext(ctx, r.ID(), map[string]any{"since": 2025})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateRelationshipWithContext err = %v, want context.Canceled", err)
	}
}

func TestDeleteNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.Nodes.DeleteWithContext(ctx, n.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteNodeWithContext err = %v, want context.Canceled", err)
	}

	// Node should still exist.
	_, err = g.Nodes.Get(n.ID())
	if err != nil {
		t.Errorf("GetNode after cancelled delete returned error: %v", err)
	}
}

func TestDeleteRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := g.Rels.DeleteWithContext(ctx, r.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteRelationshipWithContext err = %v, want context.Canceled", err)
	}

	// Relationship should still exist.
	_, err = g.Rels.Get(r.ID())
	if err != nil {
		t.Errorf("GetRelationship after cancelled delete returned error: %v", err)
	}
}

func TestDeleteNodeWithContextCanceledAfterAdjacencyReadDoesNotPersist(t *testing.T) {
	store := &cancelDuringStoreReadStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.afterIncoming = onceFunc(cancel)

	if err := g.Nodes.DeleteWithContext(ctx, a.ID()); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteWithContext after adjacency cancellation = %v, want context.Canceled", err)
	}
	if _, err := g.Nodes.Get(a.ID()); err != nil {
		t.Fatalf("node after canceled delete: %v", err)
	}
	if _, err := g.Rels.Get(r.ID()); err != nil {
		t.Fatalf("relationship after canceled cascade delete: %v", err)
	}
}

func TestDeleteRelWithContextCanceledAfterReadDoesNotPersist(t *testing.T) {
	store := &cancelDuringStoreReadStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.afterGetRel = onceFunc(cancel)

	if err := g.Rels.DeleteWithContext(ctx, r.ID()); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteWithContext after relationship read cancellation = %v, want context.Canceled", err)
	}
	if _, err := g.Rels.Get(r.ID()); err != nil {
		t.Fatalf("relationship after canceled delete: %v", err)
	}
}

func TestAddByIDIfAbsentWithContextCanceledAfterExistingProbeDoesNotSucceed(t *testing.T) {
	store := &cancelDuringStoreReadStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add B: %v", err)
	}
	existing, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.afterOutgoing = onceFunc(cancel)

	got, created, err := g.Rels.AddByIDIfAbsentWithContext(ctx, "KNOWS", a.ID(), b.ID(), nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AddByIDIfAbsentWithContext after existing probe cancellation = %v, want context.Canceled", err)
	}
	if got != nil || created {
		t.Fatalf("AddByIDIfAbsentWithContext returned (%v, created=%v), want nil false on cancellation", got, created)
	}
	if _, err := g.Rels.Get(existing.ID()); err != nil {
		t.Fatalf("existing relationship after canceled if-absent probe: %v", err)
	}
}

func TestGetNodeWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Nodes.GetWithContext(ctx, n.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetNodeWithContext err = %v, want context.Canceled", err)
	}
}

func TestGetRelWithContextCancelled(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := g.Rels.GetWithContext(ctx, r.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetRelationshipWithContext err = %v, want context.Canceled", err)
	}
}

func TestGetNodeWithContextCanceledAfterStoreReadDoesNotSucceed(t *testing.T) {
	store := &cancelDuringStoreReadStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	n, err := g.Nodes.Add([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.afterGetNode = onceFunc(cancel)

	got, err := g.Nodes.GetWithContext(ctx, n.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetWithContext after store-read cancellation = %v, want context.Canceled", err)
	}
	if got != nil {
		t.Fatalf("GetWithContext returned node %d on cancellation", got.ID())
	}
}

func TestGetRelWithContextCanceledAfterStoreReadDoesNotSucceed(t *testing.T) {
	store := &cancelDuringStoreReadStore{Store: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	a, err := g.Nodes.Add([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add A: %v", err)
	}
	b, err := g.Nodes.Add([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add B: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	store.afterGetRel = onceFunc(cancel)

	got, err := g.Rels.GetWithContext(ctx, r.ID())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetWithContext after store-read cancellation = %v, want context.Canceled", err)
	}
	if got != nil {
		t.Fatalf("GetWithContext returned relationship %d on cancellation", got.ID())
	}
}

func TestCanceledContextMutationEntryPointsDoNotWaitBehindTxLock(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add node: %v", err)
	}
	a, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("Add endpoint b: %v", err)
	}
	r, err := g.Rels.Add("KNOWS", a, b, map[string]any{"since": int64(2024)})
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	rolledBack := false
	defer func() {
		if !rolledBack {
			_ = tx.Rollback()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		call func() error
	}{
		{name: "Nodes.AddWithContext", call: func() error {
			_, err := g.Nodes.AddWithContext(ctx, []string{"Blocked"}, nil)
			return err
		}},
		{name: "Nodes.Import", call: func() error {
			_, err := g.Nodes.Import(ctx, types.NodeID(123456789), []string{"Blocked"}, nil)
			return err
		}},
		{name: "Nodes.UpdateWithContext", call: func() error {
			_, err := g.Nodes.UpdateWithContext(ctx, n.ID(), map[string]any{"name": "Bob"})
			return err
		}},
		{name: "Nodes.UpdateInPlaceWithContext", call: func() error {
			_, err := g.Nodes.UpdateInPlaceWithContext(ctx, n.ID(), map[string]any{"name": "Bob"})
			return err
		}},
		{name: "Nodes.DeleteWithContext", call: func() error {
			return g.Nodes.DeleteWithContext(ctx, n.ID())
		}},
		{name: "Nodes.CompareAndSetPropertyWithContext", call: func() error {
			_, err := g.Nodes.CompareAndSetPropertyWithContext(ctx, n.ID(), "name", "Alice", "Bob")
			return err
		}},
		{name: "Rels.AddWithContext", call: func() error {
			_, err := g.Rels.AddWithContext(ctx, "BLOCKED", a, b, nil)
			return err
		}},
		{name: "Rels.AddByIDWithContext", call: func() error {
			_, err := g.Rels.AddByIDWithContext(ctx, "BLOCKED", a.ID(), b.ID(), nil)
			return err
		}},
		{name: "Rels.AddByIDIfAbsentWithContext", call: func() error {
			_, _, err := g.Rels.AddByIDIfAbsentWithContext(ctx, "BLOCKED", a.ID(), b.ID(), nil)
			return err
		}},
		{name: "Rels.Import", call: func() error {
			_, err := g.Rels.Import(ctx, types.RelID(987654321), "BLOCKED", a, b, nil)
			return err
		}},
		{name: "Rels.UpdateWithContext", call: func() error {
			_, err := g.Rels.UpdateWithContext(ctx, r.ID(), map[string]any{"since": int64(2025)})
			return err
		}},
		{name: "Rels.UpdateInPlaceWithContext", call: func() error {
			_, err := g.Rels.UpdateInPlaceWithContext(ctx, r.ID(), map[string]any{"since": int64(2025)})
			return err
		}},
		{name: "Rels.DeleteWithContext", call: func() error {
			return g.Rels.DeleteWithContext(ctx, r.ID())
		}},
		{name: "Rels.CompareAndSetPropertyWithContext", call: func() error {
			_, err := g.Rels.CompareAndSetPropertyWithContext(ctx, r.ID(), "since", int64(2024), int64(2025))
			return err
		}},
	}

	for _, tc := range cases {
		done := make(chan error, 1)
		go func() {
			done <- tc.call()
		}()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("%s = %v, want context.Canceled", tc.name, err)
			}
		case <-time.After(50 * time.Millisecond):
			rolledBack = true
			_ = tx.Rollback()
			t.Fatalf("%s waited behind active transaction despite pre-canceled context", tc.name)
		}
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	rolledBack = true
}

// --- Group B: Happy path with valid context (8 tests) ---
// Non-cancelled context → identical behavior to non-context methods.

func TestAddNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.AddWithContext(context.Background(), []string{"Person", "Employee"}, map[string]any{"name": "Alice", "age": 30})
	if err != nil {
		t.Fatalf("AddNodeWithContext error: %v", err)
	}
	if n == nil {
		t.Fatal("AddNodeWithContext returned nil node")
	}

	labels := g.Nodes.Labels(n)
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

	a, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	b, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Bob"})

	r, err := g.Rels.AddWithContext(context.Background(), "KNOWS", a, b, map[string]any{"since": 2020})
	if err != nil {
		t.Fatalf("AddRelationshipWithContext error: %v", err)
	}
	if r == nil {
		t.Fatal("AddRelationshipWithContext returned nil rel")
	}

	typeName := g.Rels.Type(r)
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

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	updated, err := g.Nodes.UpdateWithContext(context.Background(), id, map[string]any{"name": "Bob", "age": 25})
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
	history, _ := g.Nodes.History(id)
	if len(history) != 1 {
		t.Errorf("history len = %d, want 1", len(history))
	}
}

func TestUpdateRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.ID()

	updated, err := g.Rels.UpdateWithContext(context.Background(), id, map[string]any{"since": 2025})
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
	history, _ := g.Rels.History(id)
	if len(history) != 1 {
		t.Errorf("history len = %d, want 1", len(history))
	}
}

func TestDeleteNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"Person"}, nil)
	b, _ := g.Nodes.Add([]string{"Person"}, nil)
	g.Rels.Add("KNOWS", a, b, nil)

	err := g.Nodes.DeleteWithContext(context.Background(), a.ID())
	if err != nil {
		t.Fatalf("DeleteNodeWithContext error: %v", err)
	}

	// Node and cascade-deleted rels should be gone.
	_, err = g.Nodes.Get(a.ID())
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("GetNode after delete: err = %v, want storepkg.ErrNodeNotFound", err)
	}

	relCount, _ := g.Rels.Count()
	if relCount != 0 {
		t.Errorf("RelationshipCount = %d after cascade delete, want 0", relCount)
	}
}

func TestDeleteRelWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, nil)

	err := g.Rels.DeleteWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("DeleteRelationshipWithContext error: %v", err)
	}

	_, err = g.Rels.Get(r.ID())
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Errorf("GetRelationship after delete: err = %v, want storepkg.ErrRelNotFound", err)
	}
}

func TestGetNodeWithContextSuccess(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	got, err := g.Nodes.GetWithContext(context.Background(), id)
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

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.ID()

	got, err := g.Rels.GetWithContext(context.Background(), id)
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

	_, err := g.Nodes.AddWithContext(ctx, []string{"Person"}, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestUpdateNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	_, err := g.Nodes.UpdateWithContext(ctx, n.ID(), map[string]any{"x": 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestDeleteNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	err := g.Nodes.DeleteWithContext(ctx, n.ID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestGetNodeWithContextDeadline(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	ctx, cancel := context.WithDeadline(context.Background(), deadlineInPast())
	defer cancel()

	_, err := g.Nodes.GetWithContext(ctx, n.ID())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
}

// --- Group D: Delegation regression (4 tests) ---
// Non-context methods still work after refactoring.

func TestAddNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
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

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	updated, err := g.Nodes.Update(n.ID(), map[string]any{"name": "Bob"})
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

	n, _ := g.Nodes.Add([]string{"Person"}, nil)
	err := g.Nodes.Delete(n.ID())
	if err != nil {
		t.Fatalf("DeleteNode error: %v", err)
	}
	_, err = g.Nodes.Get(n.ID())
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Errorf("GetNode after delete: err = %v, want storepkg.ErrNodeNotFound", err)
	}
}

func TestGetNodeDelegatesToContext(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	got, err := g.Nodes.Get(n.ID())
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

	n, _ := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	id := n.ID()

	got, err := g.Nodes.UpdateWithContext(context.Background(), id, map[string]any{})
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

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"since": 2020})
	id := r.ID()

	got, err := g.Rels.UpdateWithContext(context.Background(), id, map[string]any{})
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
	_, err := g.Nodes.AddWithContext(context.Background(), []string{"Person"}, map[string]any{"bad": badStruct{1}})
	if err == nil {
		t.Fatal("AddNodeWithContext with bad property should fail")
	}

	count, _ := g.Nodes.Count()
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

func TestCheckCtxNilReturnsSentinel(t *testing.T) {
	t.Parallel()
	var ctx context.Context
	if err := checkCtx(ctx); !errors.Is(err, ErrNilContext) {
		t.Fatalf("checkCtx(nil) = %v, want ErrNilContext", err)
	}
}

func TestMutationWithContextNilReturnsSentinel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, err := g.Nodes.Add([]string{"NilContextA"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add node a: %v", err)
	}
	b, err := g.Nodes.Add([]string{"NilContextB"}, nil)
	if err != nil {
		t.Fatalf("Add node b: %v", err)
	}
	rel, err := g.Rels.Add("NIL_CONTEXT_REL", a, b, map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("Add relationship: %v", err)
	}

	var ctx context.Context
	cases := []struct {
		name string
		run  func() error
	}{
		{name: "Nodes.AddWithContext", run: func() error {
			_, err := g.Nodes.AddWithContext(ctx, []string{"Blocked"}, nil) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Nodes.UpdateWithContext", run: func() error {
			_, err := g.Nodes.UpdateWithContext(ctx, a.ID(), map[string]any{"name": "b"}) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Nodes.UpdateInPlaceWithContext", run: func() error {
			_, err := g.Nodes.UpdateInPlaceWithContext(ctx, a.ID(), map[string]any{"name": "b"}) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Nodes.DeleteWithContext", run: func() error {
			return g.Nodes.DeleteWithContext(ctx, a.ID()) //nolint:staticcheck // intentional nil context boundary test
		}},
		{name: "Rels.AddWithContext", run: func() error {
			_, err := g.Rels.AddWithContext(ctx, "BLOCKED_REL", a, b, nil) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Rels.AddByIDWithContext", run: func() error {
			_, err := g.Rels.AddByIDWithContext(ctx, "BLOCKED_REL", a.ID(), b.ID(), nil) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Rels.AddByIDIfAbsentWithContext", run: func() error {
			_, _, err := g.Rels.AddByIDIfAbsentWithContext(ctx, "BLOCKED_REL", a.ID(), b.ID(), nil) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Rels.UpdateWithContext", run: func() error {
			_, err := g.Rels.UpdateWithContext(ctx, rel.ID(), map[string]any{"weight": int64(2)}) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Rels.UpdateInPlaceWithContext", run: func() error {
			_, err := g.Rels.UpdateInPlaceWithContext(ctx, rel.ID(), map[string]any{"weight": int64(2)}) //nolint:staticcheck // intentional nil context boundary test
			return err
		}},
		{name: "Rels.DeleteWithContext", run: func() error {
			return g.Rels.DeleteWithContext(ctx, rel.ID()) //nolint:staticcheck // intentional nil context boundary test
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrNilContext) {
				t.Fatalf("%s nil context = %v, want ErrNilContext", tc.name, err)
			}
		})
	}
}

// --- Fix 1: DeleteRelationshipWithContext entity lock tests ---

func TestDeleteRelWithContext_UsesEntityLock(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"count": 0})
	rID := r.ID()

	// Concurrent delete + update on same rel. Under -race, this would fail
	// without the entity lock in DeleteRelationshipWithContext.
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if idx%2 == 0 {
				_ = g.Rels.DeleteWithContext(context.Background(), rID)
			} else {
				_, _ = g.Rels.UpdateWithContext(context.Background(), rID, map[string]any{"count": idx})
			}
		}(i)
	}
	wg.Wait()

	// Rel should be gone (at least one delete succeeded).
	_, err := g.Rels.Get(rID)
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound after concurrent delete, got %v", err)
	}
}

func TestDeleteRelWithContext_ConcurrentUpdate(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"x": 1})
	rID := r.ID()

	// Delete wins, rel is gone, tombstone version exists.
	err := g.Rels.DeleteWithContext(context.Background(), rID)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Verify deleted.
	_, err = g.Rels.Get(rID)
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound, got %v", err)
	}

	// Verify tombstone history preserved.
	hist, err := g.Rels.History(rID)
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

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("KNOWS", a, b, map[string]any{"v": 0})
	aID := a.ID()
	rID := r.ID()

	// Concurrent delete node + update connected rel — race detector catches issues.
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			if idx == 0 {
				_ = g.Nodes.DeleteWithContext(context.Background(), aID)
			} else {
				_, _ = g.Rels.UpdateWithContext(context.Background(), rID, map[string]any{"v": idx})
			}
		}(i)
	}
	wg.Wait()

	// Node and rel should be gone.
	_, err := g.Nodes.Get(aID)
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
	}
	_, err = g.Rels.Get(rID)
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("expected storepkg.ErrRelNotFound, got %v", err)
	}
}

func TestDeleteNodeWithContext_ConcurrentAddRel(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	aID := a.ID()

	// Pre-create some rels.
	for range 5 {
		_, _ = g.Rels.Add("EDGE", a, b, nil)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: delete node (cascade deletes all rels).
	go func() {
		defer wg.Done()
		_ = g.Nodes.DeleteWithContext(context.Background(), aID)
	}()

	// Goroutine 2: try to add more rels while delete is happening.
	go func() {
		defer wg.Done()
		for range 10 {
			_, _ = g.Rels.Add("EDGE", a, b, nil)
		}
	}()

	wg.Wait()

	// Node should be gone.
	_, err := g.Nodes.Get(aID)
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("expected storepkg.ErrNodeNotFound, got %v", err)
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

func TestExtractProvenance_InvalidReservedValueTypes(t *testing.T) {
	t.Parallel()
	tests := []map[string]any{
		{"tkg_author_id": 123},
		{"tkg_signature": "signed"},
		{"tkg_authorized_by": []byte("admin")},
	}
	for _, props := range tests {
		_, _, _, _, _, err := extractProvenance(props)
		if err == nil {
			t.Fatalf("extractProvenance(%v) returned nil error", props)
		}
	}
}

func TestAddNodeRejectsInvalidProvenanceTypeWithoutDroppingReservedKey(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	if _, err := g.Nodes.Add([]string{"Person"}, map[string]any{"tkg_author_id": 123}); err == nil {
		t.Fatal("Nodes.Add returned nil error for non-string tkg_author_id")
	}
	count, err := g.Nodes.Count()
	if err != nil {
		t.Fatalf("Nodes.Count: %v", err)
	}
	if count != 0 {
		t.Fatalf("Nodes.Count = %d, want 0 after rejected provenance", count)
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

func TestExtractProvenance_ValidAuthLevelNumericTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want uint8
	}{
		{name: "int8", val: int8(7), want: 7},
		{name: "int16", val: int16(8), want: 8},
		{name: "uint", val: uint(9), want: 9},
		{name: "uint16", val: uint16(10), want: 10},
		{name: "uint32", val: uint32(11), want: 11},
		{name: "uint64", val: uint64(12), want: 12},
		{name: "float32 whole number", val: float32(13), want: 13},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, got, _, err := extractProvenance(map[string]any{"tkg_auth_level": tt.val})
			if err != nil {
				t.Fatalf("extractProvenance(%T(%v)): %v", tt.val, tt.val, err)
			}
			if got != tt.want {
				t.Fatalf("authLevel = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestExtractProvenance_RejectsOutOfRangeAuthLevelNumericTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
	}{
		{name: "int8 negative", val: int8(-1)},
		{name: "int16 high", val: int16(256)},
		{name: "uint high", val: uint(256)},
		{name: "uint16 high", val: uint16(256)},
		{name: "uint32 high", val: uint32(256)},
		{name: "uint64 high", val: uint64(256)},
		{name: "float32 fractional", val: float32(5.5)},
		{name: "float32 high", val: float32(256)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, _, _, _, err := extractProvenance(map[string]any{"tkg_auth_level": tt.val})
			if err == nil {
				t.Fatalf("extractProvenance(%T(%v)) returned nil error", tt.val, tt.val)
			}
		})
	}
}

// --- Group: Temporal metadata via tkg_ props ---

func TestParseInstantAcceptsSafeNumericTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
		want types.Instant
	}{
		{name: "types.Instant", val: types.Instant(1000), want: 1000},
		{name: "int", val: int(1001), want: 1001},
		{name: "int8", val: int8(12), want: 12},
		{name: "int16", val: int16(1002), want: 1002},
		{name: "int32", val: int32(1003), want: 1003},
		{name: "int64", val: int64(1004), want: 1004},
		{name: "uint", val: uint(1005), want: 1005},
		{name: "uint8", val: uint8(13), want: 13},
		{name: "uint16", val: uint16(1006), want: 1006},
		{name: "uint32", val: uint32(1007), want: 1007},
		{name: "uint64", val: uint64(1008), want: 1008},
		{name: "float32 whole number", val: float32(1009), want: 1009},
		{name: "float64 whole number", val: float64(1010), want: 1010},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInstant(tt.val, "tkg_valid_from")
			if err != nil {
				t.Fatalf("parseInstant(%T(%v)): %v", tt.val, tt.val, err)
			}
			if got != tt.want {
				t.Fatalf("parseInstant = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestParseInstantRejectsUnsafeNumericTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		val  any
	}{
		{name: "uint64 over int64", val: uint64(maxInt64Value) + 1},
		{name: "float32 fractional", val: float32(10.5)},
		{name: "float32 outside exact range", val: float32(maxExactFloat32Int * 2)},
		{name: "float64 fractional", val: float64(10.5)},
		{name: "float64 outside exact range", val: maxExactFloat64Int * 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseInstant(tt.val, "tkg_valid_from")
			if err == nil {
				t.Fatalf("parseInstant(%T(%v)) returned nil error", tt.val, tt.val)
			}
		})
	}
}

func TestAddNodeWithTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	eventTime := types.Instant(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC).UnixMilli())
	farFuture := types.Instant(time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli())

	n, err := g.Nodes.AddWithContext(context.Background(), []string{"Signal"}, map[string]any{
		"name":           "brute-force",
		"tkg_valid_from": int64(eventTime),
		"tkg_valid_to":   int64(farFuture),
		"tkg_created_at": int64(eventTime),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tm := n.Temporal()
	if tm == nil {
		t.Fatal("Temporal() is nil")
	}
	if tm.ValidFrom != eventTime {
		t.Errorf("ValidFrom = %d, want %d", tm.ValidFrom, eventTime)
	}
	if tm.ValidTo != farFuture {
		t.Errorf("ValidTo = %d, want %d", tm.ValidTo, farFuture)
	}
	if tm.CreatedAt != eventTime {
		t.Errorf("CreatedAt = %d, want %d", tm.CreatedAt, eventTime)
	}
	if tm.TxFrom == 0 {
		t.Error("TxFrom should be auto-set, got 0")
	}

	// Verify tkg_ keys are NOT stored as regular properties.
	if v, _ := n.GetProperty("tkg_valid_from"); v != nil {
		t.Error("tkg_valid_from should not be a regular property")
	}
}

func TestAddNodeWithoutTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	n, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	tm := n.Temporal()
	if tm == nil {
		t.Fatal("Temporal() is nil (TxFrom should always be set)")
	}
	if tm.TxFrom == 0 {
		t.Error("TxFrom should be auto-set")
	}
	if tm.ValidFrom != 0 {
		t.Errorf("ValidFrom = %d, want 0 (not set)", tm.ValidFrom)
	}
	if tm.ValidTo != 0 {
		t.Errorf("ValidTo = %d, want 0 (not set)", tm.ValidTo)
	}
}

func TestAddRelationshipWithTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	eventTime := types.Instant(time.Date(2026, 3, 9, 12, 0, 0, 0, time.UTC).UnixMilli())

	src, _ := g.Nodes.Add([]string{"Host"}, map[string]any{"ip": "10.0.0.1"})
	dst, _ := g.Nodes.Add([]string{"Host"}, map[string]any{"ip": "10.0.0.2"})

	r, err := g.Rels.AddWithContext(context.Background(), "CONNECTED_TO", src, dst, map[string]any{
		"port":           int64(22),
		"tkg_valid_from": int64(eventTime),
		"tkg_created_at": int64(eventTime),
	})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	tm := r.Temporal()
	if tm == nil {
		t.Fatal("Temporal() is nil")
	}
	if tm.ValidFrom != eventTime {
		t.Errorf("ValidFrom = %d, want %d", tm.ValidFrom, eventTime)
	}
	if tm.CreatedAt != eventTime {
		t.Errorf("CreatedAt = %d, want %d", tm.CreatedAt, eventTime)
	}
	if tm.TxFrom == 0 {
		t.Error("TxFrom should be auto-set")
	}
}

func TestTemporalFloat64Accepted(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	eventTime := types.Instant(1741521600000) // 2025-03-09T12:00:00Z

	// float64 — common from JSON round-trip.
	n, err := g.Nodes.Add([]string{"Event"}, map[string]any{
		"tkg_valid_from": float64(eventTime),
	})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	if tm := n.Temporal(); tm.ValidFrom != eventTime {
		t.Errorf("ValidFrom = %d, want %d", tm.ValidFrom, eventTime)
	}
}

func TestTemporalInvalidTypeRejected(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, err := g.Nodes.Add([]string{"Event"}, map[string]any{
		"tkg_valid_from": "not-a-number",
	})
	if err == nil {
		t.Fatal("expected error for string tkg_valid_from")
	}
}

func TestTemporalNonIntegerFloat64Rejected(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	_, err := g.Nodes.Add([]string{"Event"}, map[string]any{
		"tkg_valid_from": 123.456,
	})
	if err == nil {
		t.Fatal("expected error for non-integer float64 tkg_valid_from")
	}
}

func TestTemporalExplicitInvalidRangeRejected(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	props := map[string]any{
		"tkg_valid_from": types.Instant(20),
		"tkg_valid_to":   types.Instant(20),
	}
	if _, err := g.Nodes.Add([]string{"Event"}, props); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("Nodes.Add equal temporal range = %v, want ErrInvalidTimeRange", err)
	}

	start, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Endpoint"}, nil)
	if err != nil {
		t.Fatalf("AddNode end: %v", err)
	}
	relProps := map[string]any{
		"tkg_valid_from": types.Instant(30),
		"tkg_valid_to":   types.Instant(10),
	}
	if _, err := g.Rels.Add("LINKS", start, end, relProps); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("Rels.Add reversed temporal range = %v, want ErrInvalidTimeRange", err)
	}

	batch, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := batch.AddNode([]string{"Queued"}, props); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("Batch.AddNode equal temporal range = %v, want ErrInvalidTimeRange", err)
	}
	if _, err := batch.AddRelationship("QUEUED", start, end, relProps); !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("Batch.AddRelationship reversed temporal range = %v, want ErrInvalidTimeRange", err)
	}
}

func TestBatchAddNodeWithTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	eventTime := types.Instant(1741521600000)
	batch, _ := NewBatchBuilder(g)

	n, err := batch.AddNode([]string{"Alert"}, map[string]any{
		"name":           "suspicious",
		"tkg_valid_from": int64(eventTime),
		"tkg_created_at": int64(eventTime),
	})
	if err != nil {
		t.Fatalf("batch AddNode: %v", err)
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed > 0 {
		t.Fatalf("batch failed: %v", result.Errors)
	}

	// Re-read from store to verify temporal survived PutNodesBatch + DeepCopy.
	stored, err := g.Nodes.GetWithContext(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	tm := stored.Temporal()
	if tm == nil {
		t.Fatal("stored Temporal() is nil")
	}
	if tm.ValidFrom != eventTime {
		t.Errorf("stored ValidFrom = %d, want %d", tm.ValidFrom, eventTime)
	}
	if tm.CreatedAt != eventTime {
		t.Errorf("stored CreatedAt = %d, want %d", tm.CreatedAt, eventTime)
	}
}

func TestBatchAddRelationshipWithTemporal(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)

	eventTime := types.Instant(1741521600000)
	batch, _ := NewBatchBuilder(g)

	src, _ := batch.AddNode([]string{"Host"}, map[string]any{"ip": "10.0.0.1"})
	dst, _ := batch.AddNode([]string{"Host"}, map[string]any{"ip": "10.0.0.2"})
	r, err := batch.AddRelationship("ATTACKS", src, dst, map[string]any{
		"tkg_valid_from": int64(eventTime),
	})
	if err != nil {
		t.Fatalf("batch AddRelationship: %v", err)
	}

	result, err := batch.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Failed > 0 {
		t.Fatalf("batch failed: %v", result.Errors)
	}

	stored, err := g.Rels.GetWithContext(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	tm := stored.Temporal()
	if tm == nil {
		t.Fatal("stored Temporal() is nil")
	}
	if tm.ValidFrom != eventTime {
		t.Errorf("stored ValidFrom = %d, want %d", tm.ValidFrom, eventTime)
	}
}

// --- Helpers ---

// deadlineInPast returns a time in the past to create an already-expired deadline.
func deadlineInPast() time.Time {
	return time.Now().Add(-time.Second)
}
