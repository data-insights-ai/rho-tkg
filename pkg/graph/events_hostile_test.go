package graph_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Hostile-handler tests: the event system invokes USER code on the mutation
// path. A panicking handler, a handler that re-enters the graph, and a
// rolled-back transaction are the three classic ways subscribers break the
// engine (or get lied to).

// A handler that panics on every event must not kill the mutation, the bus,
// or delivery to well-behaved handlers.
func TestPanickingHandlerDoesNotBreakMutationsOrOtherHandlers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewEventBus()
	var delivered atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		panic("hostile handler")
	})
	bus.Subscribe(func(e eventspkg.Event) {
		delivered.Add(1)
	})
	if err := g.Events().SetSync(bus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := g.Nodes().Add(ctx, []string{"Ev"}, nil); err != nil {
			t.Fatalf("Add %d failed because a subscriber panicked: %v", i, err)
		}
	}
	if got := delivered.Load(); got != 5 {
		t.Fatalf("well-behaved handler received %d events, want 5 — the panicking sibling broke delivery", got)
	}
	// The graph state must be intact despite the panics.
	rows, err := g.Nodes().ByLabel("Ev", storepkg.QueryOpts{})
	if err != nil || len(rows) != 5 {
		t.Fatalf("graph state damaged: %v (%d rows)", err, len(rows))
	}
}

// A handler that re-enters the graph (reads AND writes) on every event must
// not deadlock — handlers are invoked outside the engine locks, and a
// re-entrant write triggers a nested event, which must also terminate.
func TestReentrantHandlerDoesNotDeadlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewEventBus()
	var reads, writes atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) {
		// Re-entrant read on every event.
		if _, err := g.Stats().NodeCount(); err == nil {
			reads.Add(1)
		}
		// Re-entrant WRITE on node-create events only (bounded depth: the
		// nested write is a SetProperty, which emits an update event whose
		// handler only reads).
		if e.Type == eventspkg.EventNodeCreate {
			if err := g.Nodes().SetProperty(ctx, types.NodeID(e.EntityID), "touched", true); err == nil {
				writes.Add(1)
			}
		}
	})
	if err := g.Events().SetSync(bus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 5; i++ {
			if _, err := g.Nodes().Add(ctx, []string{"Re"}, nil); err != nil {
				t.Errorf("Add: %v", err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("re-entrant handler deadlocked the mutation path")
	}
	if writes.Load() != 5 {
		t.Fatalf("re-entrant writes = %d, want 5", writes.Load())
	}
	// Every node must carry the re-entrant property.
	rows, err := g.Nodes().ByLabel("Re", storepkg.QueryOpts{})
	if err != nil || len(rows) != 5 {
		t.Fatalf("scan: %v (%d)", err, len(rows))
	}
	for _, n := range rows {
		if _, ok := n.GetProperty("touched"); !ok {
			t.Fatalf("re-entrant write lost on node %v", n.ID())
		}
	}
}

// Subscribers must NEVER observe rolled-back mutations, and must observe a
// committed transaction's events exactly once each.
func TestTxRollbackEventsNeverDelivered(t *testing.T) {
	t.Parallel()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 7})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewEventBus()
	var events atomic.Int64
	bus.Subscribe(func(e eventspkg.Event) { events.Add(1) })
	if err := g.Events().SetSync(bus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	// Rolled back: three mutations, zero events.
	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	n, err := tx.AddNode([]string{"TxEv"}, nil)
	if err != nil {
		t.Fatalf("tx add: %v", err)
	}
	if _, err := tx.UpdateNode(n.ID(), map[string]any{"x": 1}); err != nil {
		t.Fatalf("tx update: %v", err)
	}
	if err := tx.DeleteNode(n.ID()); err != nil {
		t.Fatalf("tx delete: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if got := events.Load(); got != 0 {
		t.Fatalf("subscriber observed %d events from a ROLLED-BACK transaction", got)
	}

	// Committed: exactly the buffered events, no duplicates.
	tx2, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	if _, err := tx2.AddNode([]string{"TxEv"}, nil); err != nil {
		t.Fatalf("tx2 add: %v", err)
	}
	if _, err := tx2.AddNode([]string{"TxEv"}, nil); err != nil {
		t.Fatalf("tx2 add 2: %v", err)
	}
	if events.Load() != 0 {
		t.Fatalf("events leaked BEFORE commit")
	}
	if err := tx2.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := events.Load(); got != 2 {
		t.Fatalf("committed tx delivered %d events, want exactly 2", got)
	}
}
