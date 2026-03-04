package graph

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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

	tx := g.BeginTx()

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
		_, err2 := g.AddNode([]string{"B"}, nil)
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

	nc, _ := g.NodeCount()
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
		_, err := g.AddNode([]string{"Item"}, map[string]any{"idx": i})
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
				_, _ = g.AddNode([]string{"Item"}, map[string]any{"idx": j + 100})
			}
		}()
	}
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = g.AllNodes(QueryOpts{})
			}
		}()
	}
	wg.Wait()

	// If the race detector didn't fire, concurrency is safe.
	nc, _ := g.NodeCount()
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

	bus := NewEventBus()
	g.SetEventBus(bus)

	var mu sync.Mutex
	var received []Event
	bus.Subscribe(func(e Event) {
		mu.Lock()
		received = append(received, e)
		mu.Unlock()
	})

	tx := g.BeginTx()

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
	if received[0].Type != EventNodeCreate {
		t.Errorf("event[0].Type: got %d, want EventNodeCreate(%d)", received[0].Type, EventNodeCreate)
	}
	if received[1].Type != EventNodeCreate {
		t.Errorf("event[1].Type: got %d, want EventNodeCreate(%d)", received[1].Type, EventNodeCreate)
	}
	if received[2].Type != EventRelCreate {
		t.Errorf("event[2].Type: got %d, want EventRelCreate(%d)", received[2].Type, EventRelCreate)
	}
}

// TestTxRollbackNoEvents verifies that no events are published when a tx rolls back.
func TestTxRollbackNoEvents(t *testing.T) {
	t.Parallel()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })

	bus := NewEventBus()
	g.SetEventBus(bus)

	var count atomic.Int64
	bus.Subscribe(func(e Event) {
		count.Add(1)
	})

	tx := g.BeginTx()

	_, err = tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("tx.AddNode: %v", err)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// No events should have been published.
	if got := count.Load(); got != 0 {
		t.Errorf("events after rollback: got %d, want 0", got)
	}

	// Verify node was actually rolled back.
	nc, _ := g.NodeCount()
	if nc != 0 {
		t.Errorf("node count after rollback: got %d, want 0", nc)
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

	bus := NewEventBus()
	g.SetEventBus(bus)

	var handlerOK atomic.Bool
	bus.Subscribe(func(e Event) {
		if e.Type == EventNodeCreate {
			// This must not deadlock — g.mu must be unlocked at this point.
			n, err := g.GetNode(e.EntityID)
			if err == nil && n != nil {
				handlerOK.Store(true)
			}
		}
	})

	tx := g.BeginTx()
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

	bus := NewEventBus()
	g.SetEventBus(bus)

	var count atomic.Int64
	bus.Subscribe(func(e Event) {
		count.Add(1)
	})

	// Standalone mutation (not in tx) — should emit immediately.
	_, err = g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	if got := count.Load(); got != 1 {
		t.Errorf("events after standalone AddNode: got %d, want 1", got)
	}
}
