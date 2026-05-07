package core

import (
	"sync"
	"testing"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// TestEventBus_Subscribe_Unsubscribe verifies that subscribing two handlers and
// unsubscribing one results in only the remaining handler receiving events.
func TestEventBus_Subscribe_Unsubscribe(t *testing.T) {
	bus := eventspkg.NewEventBus()

	var got1, got2 []eventspkg.EventType
	unsub1 := bus.Subscribe(func(e eventspkg.Event) { got1 = append(got1, e.Type) })
	_ = bus.Subscribe(func(e eventspkg.Event) { got2 = append(got2, e.Type) })

	// Unsubscribe first handler.
	unsub1()

	bus.Publish(eventspkg.Event{Type: eventspkg.EventNodeCreate})

	if len(got1) != 0 {
		t.Fatalf("unsubscribed handler received %d events", len(got1))
	}
	if len(got2) != 1 || got2[0] != eventspkg.EventNodeCreate {
		t.Fatalf("remaining handler: expected [eventspkg.EventNodeCreate], got %v", got2)
	}
}

// TestEventBus_Unsubscribe_Idempotent verifies that calling the unsubscribe
// function multiple times is safe and does not panic.
func TestEventBus_Unsubscribe_Idempotent(t *testing.T) {
	bus := eventspkg.NewEventBus()
	unsub := bus.Subscribe(func(eventspkg.Event) {})
	unsub()
	unsub() // second call must not panic
}

// TestEventBus_MultipleHandlers verifies all registered handlers receive every event.
func TestEventBus_MultipleHandlers(t *testing.T) {
	bus := eventspkg.NewEventBus()

	counts := make([]int, 3)
	for i := range counts {
		i := i
		bus.Subscribe(func(e eventspkg.Event) { counts[i]++ })
	}

	bus.Publish(eventspkg.Event{Type: eventspkg.EventRelCreate})
	bus.Publish(eventspkg.Event{Type: eventspkg.EventRelDelete})

	for i, c := range counts {
		if c != 2 {
			t.Fatalf("handler %d received %d events, expected 2", i, c)
		}
	}
}

// TestGraph_EventBus_NilDefault verifies that a freshly created Graph has no
// eventspkg.EventBus attached and does not panic on mutations.
func TestGraph_EventBus_NilDefault(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = g.Close() }()

	if g.GetEventBus() != nil {
		t.Fatal("expected nil eventspkg.EventBus by default")
	}

	// Should not panic even without an event bus.
	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode without event bus: %v", err)
	}
	if _, err := g.UpdateNode(n.ID(), map[string]any{"x": "y"}); err != nil {
		t.Fatalf("UpdateNode without event bus: %v", err)
	}
}

// TestGraph_NodeCreate_Event verifies that AddNode fires eventspkg.EventNodeCreate with
// the correct EntityID and a non-zero Timestamp.
func TestGraph_NodeCreate_Event(t *testing.T) {
	g := newTestGraphForEvents(t)
	events := collectEvents(g, eventspkg.EventNodeCreate)

	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	got := drain(events)
	if len(got) != 1 {
		t.Fatalf("expected 1 event, got %d", len(got))
	}
	if got[0].Type != eventspkg.EventNodeCreate {
		t.Fatalf("expected eventspkg.EventNodeCreate, got %v", got[0].Type)
	}
	if got[0].EntityID != types.EntityID(n.ID()) {
		t.Fatalf("EntityID mismatch: expected %d, got %d", n.ID(), got[0].EntityID)
	}
	if got[0].Timestamp == 0 {
		t.Fatal("Timestamp must be non-zero")
	}
}

// TestGraph_NodeUpdate_Event verifies that UpdateNode fires eventspkg.EventNodeUpdate.
func TestGraph_NodeUpdate_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	n, err := g.AddNode([]string{"Person"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	events := collectEvents(g, eventspkg.EventNodeUpdate)

	if _, err := g.UpdateNode(id, map[string]any{"name": "Alice"}); err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventNodeUpdate || got[0].EntityID != types.EntityID(id) {
		t.Fatalf("expected 1 eventspkg.EventNodeUpdate for %d, got %v", id, got)
	}
}

// TestGraph_NodeDelete_Event verifies that DeleteNode fires eventspkg.EventNodeDelete.
func TestGraph_NodeDelete_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	n, err := g.AddNode([]string{"Thing"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	events := collectEvents(g, eventspkg.EventNodeDelete)

	if err := g.DeleteNode(id); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventNodeDelete || got[0].EntityID != types.EntityID(id) {
		t.Fatalf("expected 1 eventspkg.EventNodeDelete for %d, got %v", id, got)
	}
}

// TestGraph_RelCreate_Event verifies that AddRelationship fires eventspkg.EventRelCreate.
func TestGraph_RelCreate_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)

	events := collectEvents(g, eventspkg.EventRelCreate)

	r, err := g.AddRelationship("LINKS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventRelCreate || got[0].EntityID != types.EntityID(r.ID()) {
		t.Fatalf("expected 1 eventspkg.EventRelCreate, got %v", got)
	}
}

// TestGraph_RelDelete_Event verifies that DeleteRelationship fires eventspkg.EventRelDelete.
func TestGraph_RelDelete_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("LINKS", a, b, nil)
	rid := r.ID()

	events := collectEvents(g, eventspkg.EventRelDelete)

	if err := g.DeleteRelationship(rid); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventRelDelete || got[0].EntityID != types.EntityID(rid) {
		t.Fatalf("expected 1 eventspkg.EventRelDelete, got %v", got)
	}
}

// TestGraph_RelUpdate_Event verifies that UpdateRelationship fires eventspkg.EventRelUpdate.
func TestGraph_RelUpdate_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("LINKS", a, b, nil)
	rid := r.ID()

	events := collectEvents(g, eventspkg.EventRelUpdate)

	if _, err := g.UpdateRelationship(rid, map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("UpdateRelationship: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventRelUpdate || got[0].EntityID != types.EntityID(rid) {
		t.Fatalf("expected 1 eventspkg.EventRelUpdate, got %v", got)
	}
}

// TestEventBus_AsyncHandler verifies that a handler pushing to a buffered channel
// receives all events without dropping any.
func TestEventBus_AsyncHandler(t *testing.T) {
	g := newTestGraphForEvents(t)

	ch := make(chan eventspkg.Event, 10)
	g.GetEventBus().Subscribe(func(e eventspkg.Event) { ch <- e })

	const n = 5
	for i := range n {
		if _, err := g.AddNode([]string{"X"}, map[string]any{"i": int64(i)}); err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
	}

	// Give any async processing a moment, then drain.
	time.Sleep(time.Millisecond)
	close(ch)
	var got []eventspkg.Event
	for e := range ch {
		got = append(got, e)
	}
	if len(got) != n {
		t.Fatalf("expected %d events, got %d", n, got)
	}
}

// TestGraph_CloseNodeVersion_Event verifies CloseNodeVersion fires eventspkg.EventNodeUpdate.
func TestGraph_CloseNodeVersion_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	n, err := g.AddNode([]string{"E"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	events := collectEvents(g, eventspkg.EventNodeUpdate)

	closeTime := types.Instant(time.Now().UnixMilli()) + 2000
	if err := g.CloseNodeVersion(id, closeTime); err != nil {
		t.Fatalf("CloseNodeVersion: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventNodeUpdate || got[0].EntityID != types.EntityID(id) {
		t.Fatalf("expected 1 eventspkg.EventNodeUpdate for %d, got %v", id, got)
	}
}

// TestGraph_CloseRelVersion_Event verifies CloseRelVersion fires eventspkg.EventRelUpdate.
func TestGraph_CloseRelVersion_Event(t *testing.T) {
	g := newTestGraphForEvents(t)

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("E", a, b, nil)
	rid := r.ID()

	events := collectEvents(g, eventspkg.EventRelUpdate)

	closeTime := types.Instant(time.Now().UnixMilli()) + 2000
	if err := g.CloseRelVersion(rid, closeTime); err != nil {
		t.Fatalf("CloseRelVersion: %v", err)
	}

	got := drain(events)
	if len(got) != 1 || got[0].Type != eventspkg.EventRelUpdate || got[0].EntityID != types.EntityID(rid) {
		t.Fatalf("expected 1 eventspkg.EventRelUpdate for %d, got %v", rid, got)
	}
}

// --- test helpers ---

// newTestGraphForEvents returns a Graph with an eventspkg.EventBus already attached.
func newTestGraphForEvents(t *testing.T) *Core {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	g.SetEventBus(eventspkg.NewEventBus())
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// collectEvents subscribes to ALL events and returns a slice pointer that
// accumulates them as they arrive. Use drain() to read the slice safely.
func collectEvents(g *Core, _ eventspkg.EventType) *[]eventspkg.Event {
	var mu sync.Mutex
	result := &[]eventspkg.Event{}
	g.GetEventBus().Subscribe(func(e eventspkg.Event) {
		mu.Lock()
		*result = append(*result, e)
		mu.Unlock()
	})
	return result
}

// drain returns the current contents of the collected events slice and clears it.
func drain(events *[]eventspkg.Event) []eventspkg.Event {
	// Events are synchronous — no wait needed.
	return *events
}

// TestEventBus_PanicHandler verifies that a panicking handler does not crash
// the publish call and does not prevent other handlers from receiving the event.
func TestEventBus_PanicHandler(t *testing.T) {
	bus := eventspkg.NewEventBus()

	// First handler panics.
	bus.Subscribe(func(eventspkg.Event) { panic("intentional panic in handler") })

	// Second handler records received events.
	var got []eventspkg.EventType
	bus.Subscribe(func(e eventspkg.Event) { got = append(got, e.Type) })

	// publish must not panic.
	bus.Publish(eventspkg.Event{Type: eventspkg.EventNodeCreate})

	if len(got) != 1 || got[0] != eventspkg.EventNodeCreate {
		t.Fatalf("expected surviving handler to receive eventspkg.EventNodeCreate, got %v", got)
	}
}
