package core

import (
	"context"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- Fix A: GraphTx.Rollback releases write lock even on store panic ---

// deleteRelPanicStore wraps a Store and panics on DeleteRelationship.
// This simulates a store panic during the "delete created rels" rollback phase.
type deleteRelPanicStore struct {
	storepkg.MandatoryStore
}

func (s *deleteRelPanicStore) DeleteRelationship(_ types.RelID) error {
	panic("injected store panic during DeleteRelationship")
}

func TestGraphTx_RollbackPanicSafe(t *testing.T) {
	// Not parallel: we mutate g.store directly.
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Create two nodes before the transaction so we can add a relationship.
	n1, err := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode n1: %v", err)
	}
	n2, err := g.Nodes.Add(context.Background(), []string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode n2: %v", err)
	}

	tx, _ := g.BeginTx()

	// Create a relationship inside the transaction so tx.createdRels is non-empty.
	_, err = tx.AddRelationship("LINKS", n1, n2, nil)
	if err != nil {
		// If AddRelationship fails here, skip — we only need createdRels non-empty.
		t.Fatalf("AddRelationship in tx: %v", err)
	}

	// Inject a panicking store so Rollback panics when deleting created rels.
	g.store = &deleteRelPanicStore{g.store}

	// Call Rollback from a goroutine so the panic is recoverable.
	panicCaught := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCaught <- true
			} else {
				panicCaught <- false
			}
		}()
		_ = tx.Rollback() //nolint:errcheck // panic expected; error path never reached
	}()

	caught := <-panicCaught
	if !caught {
		t.Fatal("expected a panic from the injected store, but none occurred")
	}

	// The deferred tx.g.mu.Unlock() must have run, so BeginTx must not block.
	// Use a timeout to detect deadlock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		tx2, _ := g.BeginTx()
		_ = tx2.Commit()
	}()

	select {
	case <-done:
		// BeginTx completed — lock was properly released after the panic.
	case <-time.After(2 * time.Second):
		t.Fatal("BeginTx blocked for 2s after panicking Rollback; graph write lock leaked")
	}
}

// ─── Issue 2: Batch panic lock-leak ───────────────────────────────────────────

// panicStore is a Store that panics on PutNodesBatch to test batch panic recovery.
type panicStore struct {
	*memory.Store
}

func (ps *panicStore) PutNodesBatch(_ []*types.Node) error {
	panic("test panic in PutNodesBatch")
}

func TestBatchExecute_PanicRecovery(t *testing.T) {
	t.Parallel()

	ms := memory.New()
	ps := &panicStore{Store: ms}
	g, err := New(Config{Store: ps})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	b, _ := NewBatchBuilder(g)
	_, err = b.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Execute should panic; recover and verify lock released.
	func() {
		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic from PutNodesBatch")
			}
		}()
		b.Execute()
	}()

	// Lock must be released: this should not deadlock.
	done := make(chan struct{})
	go func() {
		g.mu.Lock()
		g.mu.Unlock() //nolint:staticcheck // intentional empty critical section: test verifies the lock is acquirable (i.e., released by the prior holder)
		close(done)
	}()

	select {
	case <-done:
		// success — lock was released
	case <-time.After(2 * time.Second):
		t.Fatal("g.mu.Lock() deadlocked — batch panic leaked the lock")
	}

	// txEventBuffer must be nil after panic cleanup.
	g.mu.Lock()
	if g.txEventBuffer != nil {
		t.Error("txEventBuffer not nil after panic recovery")
	}
	g.mu.Unlock()
}
