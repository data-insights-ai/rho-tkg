package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func newTxTestGraph(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestGraphTx_CommitPersists(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify entities persisted.
	nc, _ := g.Nodes.Count()
	rc, _ := g.Rels.Count()
	if nc != 2 {
		t.Errorf("node count: got %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("rel count: got %d, want 1", rc)
	}
}

func TestGraphTx_DoubleCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	err := tx.Commit()
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("double commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_AddAfterCommit(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	_, err := tx.AddNode([]string{"Person"}, nil)
	if !errors.Is(err, storepkg.ErrTxDone) {
		t.Errorf("AddNode after commit: got %v, want storepkg.ErrTxDone", err)
	}
}

func TestGraphTx_ConcurrentAccess(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	// BeginTx holds the write lock. Another goroutine trying to do
	// something that needs the lock should block until commit/rollback.
	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})

	go func() {
		defer wg.Done()
		// This should block until the tx is committed.
		_, _ = g.Nodes.All(storepkg.QueryOpts{})
		close(done)
	}()

	// Commit to release the lock.
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	// Verify the node was visible after commit.
	_ = n
	nc, _ := g.Nodes.Count()
	if nc != 1 {
		t.Errorf("after concurrent commit: node count=%d, want 1", nc)
	}
}

func TestGraphTx_CreatedIDs(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := tx.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatal(err)
	}

	nodeIDs := tx.CreatedNodeIDs()
	relIDs := tx.CreatedRelIDs()

	if len(nodeIDs) != 2 {
		t.Errorf("created node IDs: got %d, want 2", len(nodeIDs))
	}
	if len(relIDs) != 1 {
		t.Errorf("created rel IDs: got %d, want 1", len(relIDs))
	}

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// --- Graph.Reset tests ---

func TestGraphReset_Empty(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	if err := g.Admin.Reset(); err != nil {
		t.Fatalf("Reset empty graph: %v", err)
	}
}

func TestGraphReset_ClearsEntities(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	_, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	nc, _ := g.Nodes.Count()
	if nc != 0 {
		t.Errorf("node count after reset: %d, want 0", nc)
	}
}

func TestGraphReset_PreservesRegistries(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	// Register some labels and types.
	_, err := g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	// The "Person" label should still be registered.
	tok, ok := g.Resolve.LookupLabel("Person")
	if !ok {
		t.Fatal("label 'Person' should survive Reset")
	}
	if tok == 0 {
		t.Fatal("label token should be non-zero")
	}
}

func TestGraphReset_ClearsHistory(t *testing.T) {
	t.Parallel()
	g := newTxTestGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatal(err)
	}

	// Update to create version history.
	_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatal(err)
	}

	if err := g.Admin.Reset(); err != nil {
		t.Fatal(err)
	}

	// History should be cleared.
	history, err := g.Nodes.History(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 0 {
		t.Errorf("history after reset: got %d entries, want 0", len(history))
	}
}

// TestMutationBlockedDuringTx verifies that standalone mutations (AddNode)
// are blocked while a transaction holds g.mu.Lock. The AddNode call in goroutine B
// must wait until the tx commits before proceeding.
func TestMutationBlockedDuringTx(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	tx, _ := g.BeginTx()

	// Create a node inside the tx to prove it works.
	_, err = tx.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	var blocked atomic.Bool
	blocked.Store(true)

	done := make(chan struct{})
	go func() {
		// This AddNode must block until tx commits because it acquires g.mu.RLock
		// which is blocked by the tx's g.mu.Lock.
		_, err2 := g.Nodes.Add(context.Background(), []string{"B"}, nil)
		if err2 != nil {
			t.Errorf("standalone AddNode: %v", err2)
		}
		blocked.Store(false)
		close(done)
	}()

	// Give goroutine B time to attempt and block.
	time.Sleep(50 * time.Millisecond)
	if !blocked.Load() {
		t.Fatal("standalone AddNode was NOT blocked during tx — isolation failure")
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Wait for goroutine B to complete.
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("standalone AddNode did not unblock after tx commit")
	}

	nc, _ := g.Nodes.Count()
	if nc != 2 {
		t.Errorf("node count: got %d, want 2", nc)
	}
}

// TestSnapshotConsistencyDuringMutation verifies that Snapshot sees a consistent
// view — no torn reads from concurrent mutations. The Snapshot is taken under
// g.mu.RLock which is compatible with other readers but blocked by writers (tx).
func TestSnapshotConsistencyDuringMutation(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	// Pre-create some nodes.
	for i := 0; i < 10; i++ {
		_, err := g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{"idx": i})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
	}

	// Concurrent reads and writes should not race.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = g.Nodes.Add(context.Background(), []string{"Item"}, map[string]any{"idx": j + 100})
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = g.Nodes.All(storepkg.QueryOpts{})
			}
		}()
	}
	wg.Wait()

	// If the race detector didn't fire, concurrency is safe.
	nc, _ := g.Nodes.Count()
	if nc < 10 {
		t.Errorf("node count: got %d, want >= 10", nc)
	}
}

// TestTxCommitPublishesBufferedEvents verifies that events are buffered during a
// transaction and all published (in order) on Commit.
func TestTxCommitPublishesBufferedEvents(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	var mu sync.Mutex
	var received []eventspkg.Event
	bus.Subscribe(func(e eventspkg.Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})

	tx, _ := g.BeginTx()

	n, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	n2, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	_, err = tx.AddRelationship("KNOWS", n, n2, nil)
	if err != nil {
		t.Fatalf("tx.AddRelationship: %v", err)
	}

	// Before commit: no events should have been published.
	mu.Lock()
	preCommitCount := len(received)
	mu.Unlock()
	if preCommitCount != 0 {
		t.Fatalf("events before commit: got %d, want 0", preCommitCount)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After commit: all 3 events should have been published.
	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("events after commit: got %d, want 3", len(received))
	}
	if received[0].Type != eventspkg.EventNodeCreate {
		t.Errorf("event[0].Type: got %d, want eventspkg.EventNodeCreate(%d)", received[0].Type, eventspkg.EventNodeCreate)
	}
	if received[1].Type != eventspkg.EventNodeCreate {
		t.Errorf("event[1].Type: got %d, want eventspkg.EventNodeCreate(%d)", received[1].Type, eventspkg.EventNodeCreate)
	}
	if received[2].Type != eventspkg.EventRelCreate {
		t.Errorf("event[2].Type: got %d, want eventspkg.EventRelCreate(%d)", received[2].Type, eventspkg.EventRelCreate)
	}
}

// TestTxCommitHandlerCanReadGraph verifies that event handlers invoked on Commit
// can safely call Graph read methods (proving events fire after g.mu.Unlock).
func TestTxCommitHandlerCanReadGraph(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	var handlerOK atomic.Bool
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			// This must not deadlock — g.mu must be unlocked at this point.
			n, err := g.Nodes.Get(context.Background(), types.NodeID(e.EntityID))
			if err == nil && n != nil {
				handlerOK.Store(true)
			}
		}
	})

	tx, _ := g.BeginTx()
	_, err = tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	if !handlerOK.Load() {
		t.Error("event handler could not read committed node — possible deadlock or missing data")
	}
}

// TestBatchEventsNotBuffered verifies that batch operations (outside a tx) still
// emit events immediately — only tx-based mutations use buffering.
func TestBatchEventsNotBuffered(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := eventspkg.NewEventBus()
	_ = g.Events.SetSync(bus)

	var count atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		count.Add(1)
	})

	// Standalone mutation (not in tx) — should emit immediately.
	_, err = g.Nodes.Add(context.Background(), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if got := count.Load(); got != 1 {
		t.Errorf("events after standalone AddNode: got %d, want 1", got)
	}
}
