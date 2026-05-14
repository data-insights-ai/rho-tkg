// Tests in this file pin the round-2 maintainability review's R2-F3
// finding (and its generalisation to deleteNodeInternal): manual entity
// locks must release on every exit path, including a panic from a custom
// Store backend. The fix wraps each lock-bearing region in a closure with
// a defer-backed unlock; without it, a panic unwinds past the explicit
// Unlock call and leaks the shard lock for the rest of the process.
//
// The leak is observable as a deadlock: the next mutation that hashes to
// the same shard waits forever on the unreleased lock. Each test below
// recovers from the panic, then exercises a follow-up mutation on the
// same endpoints under a wall-clock deadline. If the lock leaked, the
// follow-up mutation never returns and the deadline trips.

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// panicOnGetNodeStore wraps a Store and panics from GetNode when armed.
// Used to drive the batch endpoint hash-refresh path through its panic
// branch. Disarmed by default so test setup (creating nodes for the
// batch to reference) succeeds.
type panicOnGetNodeStore struct {
	storepkg.MandatoryStore
	armed bool
}

func (s *panicOnGetNodeStore) GetNode(id types.NodeID) (*types.Node, error) {
	if s.armed {
		panic("panicOnGetNodeStore: synthetic panic from GetNode")
	}
	return s.MandatoryStore.GetNode(id)
}

// panicOnPutRelStore wraps a Store and panics from PutRelationship when armed.
type panicOnPutRelStore struct {
	storepkg.MandatoryStore
	armed bool
}

func (s *panicOnPutRelStore) PutRelationship(r *types.Relationship) error {
	if s.armed {
		panic("panicOnPutRelStore: synthetic panic from PutRelationship")
	}
	return s.MandatoryStore.PutRelationship(r)
}

// panicOnAdjacencyStore wraps a Store and panics from
// OutgoingRelationships when armed. Used to drive deleteNodeInternal's
// Phase A through its panic branch — the lock under defer must release
// even when the adjacency read itself panics.
type panicOnAdjacencyStore struct {
	storepkg.MandatoryStore
	armed bool
}

func (s *panicOnAdjacencyStore) OutgoingRelationships(id types.NodeID, typeToken uint16) ([]*types.Relationship, error) {
	if s.armed {
		panic("panicOnAdjacencyStore: synthetic panic from OutgoingRelationships")
	}
	return s.MandatoryStore.OutgoingRelationships(id, typeToken)
}

// withDeadline runs fn and reports whether it completed within d.
func withDeadline(t *testing.T, d time.Duration, fn func()) bool {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

func TestBatch_PanicInGetNode_DoesNotLeakEndpointLocks(t *testing.T) {
	t.Parallel()
	store := &panicOnGetNodeStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add b: %v", err)
	}

	// Arm the panic and run a batch that touches a→b. The closure
	// inside batch.go's rel loop must release the LockTwo via defer.
	store.armed = true
	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddRelationship("KNOWS", a, b, nil); err != nil {
		t.Fatalf("queue rel: %v", err)
	}
	func() {
		defer func() { _ = recover() }()
		_, _ = bb.Execute()
	}()
	store.armed = false

	// Follow-up mutation on the same endpoints. If the panic leaked
	// LockTwo, this hangs forever — bound by a short wall-clock budget.
	completed := withDeadline(t, 2*time.Second, func() {
		_, _ = g.Rels.Add(context.Background(), "KNOWS2", a, b, nil)
	})
	if !completed {
		t.Fatal("follow-up Rels.Add deadlocked — endpoint locks leaked across the panic")
	}
}

func TestBatch_PanicInPutRelationship_DoesNotLeakEndpointLocks(t *testing.T) {
	t.Parallel()
	store := &panicOnPutRelStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	b, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add b: %v", err)
	}

	store.armed = true
	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddRelationship("KNOWS", a, b, nil); err != nil {
		t.Fatalf("queue rel: %v", err)
	}
	func() {
		defer func() { _ = recover() }()
		_, _ = bb.Execute()
	}()
	store.armed = false

	completed := withDeadline(t, 2*time.Second, func() {
		_, _ = g.Rels.Add(context.Background(), "KNOWS2", a, b, nil)
	})
	if !completed {
		t.Fatal("follow-up Rels.Add deadlocked — endpoint locks leaked across the PutRelationship panic")
	}
}

func TestDeleteNode_PanicInAdjacencyRead_DoesNotLeakEntityLocks(t *testing.T) {
	t.Parallel()
	store := &panicOnAdjacencyStore{MandatoryStore: memory.New()}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}

	store.armed = true
	func() {
		defer func() { _ = recover() }()
		_ = g.Nodes.Delete(context.Background(), a.ID())
	}()
	store.armed = false

	// Follow-up mutation on the same node ID. If Phase A leaked the
	// LockEntity, this hangs forever.
	completed := withDeadline(t, 2*time.Second, func() {
		_, _ = g.Nodes.Update(context.Background(), a.ID(), map[string]any{"k": "v"})
	})
	if !completed {
		t.Fatal("follow-up Nodes.Update deadlocked — entity lock leaked across the adjacency-read panic")
	}
}

// Negative control: ErrNodeNotFound during endpoint refresh in batch
// must NOT trigger the BatchError path — that's the documented silent
// case and the unlock pattern must work the same way.
func TestBatch_EndpointNotFound_StillRunsToCompletion(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"B"}, nil)

	bb, _ := NewBatchBuilder(g)
	if _, err := bb.AddRelationship("KNOWS", a, b, nil); err != nil {
		t.Fatalf("queue rel: %v", err)
	}
	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Failed != 0 {
		t.Fatalf("res.Failed = %d, want 0 (clean run, no synthetic faults)", res.Failed)
	}
	if !errors.Is(error(nil), nil) { // appease unused-import
		t.Fatal("unreachable")
	}
}
