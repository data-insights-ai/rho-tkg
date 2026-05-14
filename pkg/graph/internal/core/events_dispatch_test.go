package core

import (
	"context"
	"sync"
	"testing"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// captureBus is a sync EventBus that records every Publish for assertion.
type captureBus struct {
	mu     sync.Mutex
	events []eventspkg.Event
	*eventspkg.EventBus
}

func newCaptureBus() *captureBus {
	cb := &captureBus{EventBus: eventspkg.NewEventBus()}
	cb.EventBus.Subscribe(func(ev eventspkg.Event) {
		cb.mu.Lock()
		cb.events = append(cb.events, ev)
		cb.mu.Unlock()
	})
	return cb
}

func (cb *captureBus) snapshot() []eventspkg.Event {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	out := make([]eventspkg.Event, len(cb.events))
	copy(out, cb.events)
	return out
}

// TestPublishCascadeDeleteEvents_FiresRelDeletesThenNodeDelete asserts the
// publish order: every relationship delete fires first (one event per rel),
// followed by exactly one node delete event. The function is reached via the
// transaction-path delete that internally calls publishCascadeDeleteEvents.
func TestPublishCascadeDeleteEvents_FiresRelDeletesThenNodeDelete(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	bus := newCaptureBus()
	if err := g.Events.SetSync(bus.EventBus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	ctx := context.Background()
	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	c, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	r1, _ := g.Rels.Add(ctx, "KNOWS", a, b, nil)
	r2, _ := g.Rels.Add(ctx, "FOLLOWS", a, c, nil)
	r3, _ := g.Rels.Add(ctx, "LIKES", c, a, nil) // incoming on a

	// Reset bus to only see the cascade events.
	bus.mu.Lock()
	bus.events = nil
	bus.mu.Unlock()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.DeleteNode(a.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx DeleteNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx Commit: %v", err)
	}

	events := bus.snapshot()
	if len(events) != 4 {
		t.Fatalf("expected 4 events (3 rel deletes + 1 node delete), got %d: %+v", len(events), events)
	}

	// Final event must be the node delete.
	last := events[len(events)-1]
	if last.Type != eventspkg.EventNodeDelete {
		t.Fatalf("last event = %v, want EventNodeDelete", last.Type)
	}
	if last.EntityID != types.EntityID(a.ID()) {
		t.Fatalf("last event entity = %v, want %v", last.EntityID, a.ID())
	}
	if last.Priority != eventspkg.PriorityCritical {
		t.Fatalf("last event priority = %v, want PriorityCritical", last.Priority)
	}

	// First three must all be rel deletes covering r1, r2, r3.
	seen := map[types.EntityID]bool{}
	for _, ev := range events[:3] {
		if ev.Type != eventspkg.EventRelDelete {
			t.Fatalf("expected EventRelDelete, got %v", ev.Type)
		}
		if ev.Priority != eventspkg.PriorityCritical {
			t.Fatalf("rel delete event priority = %v, want PriorityCritical", ev.Priority)
		}
		seen[ev.EntityID] = true
	}
	for _, want := range []types.EntityID{types.EntityID(r1.ID()), types.EntityID(r2.ID()), types.EntityID(r3.ID())} {
		if !seen[want] {
			t.Fatalf("missing rel delete for %v; got %v", want, events)
		}
	}
}

// TestPublishCascadeDeleteEvents_EmptyAdjacencyStillFiresNodeDelete asserts
// that even when a node has zero connected relationships, the function
// publishes the single EventNodeDelete (the loop is a no-op but the final
// publish must run).
func TestPublishCascadeDeleteEvents_EmptyAdjacencyStillFiresNodeDelete(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	bus := newCaptureBus()
	if err := g.Events.SetSync(bus.EventBus); err != nil {
		t.Fatalf("SetSync: %v", err)
	}

	ctx := context.Background()
	isolated, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)

	bus.mu.Lock()
	bus.events = nil
	bus.mu.Unlock()

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.DeleteNode(isolated.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx DeleteNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx Commit: %v", err)
	}

	events := bus.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d: %+v", len(events), events)
	}
	if events[0].Type != eventspkg.EventNodeDelete {
		t.Fatalf("event type = %v, want EventNodeDelete", events[0].Type)
	}
	if events[0].EntityID != types.EntityID(isolated.ID()) {
		t.Fatalf("event entity = %v, want %v", events[0].EntityID, isolated.ID())
	}
	if events[0].Priority != eventspkg.PriorityCritical {
		t.Fatalf("event priority = %v, want PriorityCritical", events[0].Priority)
	}
}

// TestPublishCascadeDeleteEvents_NilBusIsNoOp asserts the early-return branch
// when no event publisher is configured. The cascade-delete path must remain
// safe to invoke on a graph with c.events == nil.
func TestPublishCascadeDeleteEvents_NilBusIsNoOp(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	// Explicitly: no SetSync / SetAsync — c.events stays nil.

	ctx := context.Background()
	a, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"Person"}, nil)
	_, _ = g.Rels.Add(ctx, "KNOWS", a, b, nil)

	// Must not panic, must not block.
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.DeleteNode(a.ID()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx DeleteNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx Commit: %v", err)
	}
}
