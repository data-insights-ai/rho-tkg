package core

import (
	"context"
	"testing"
	"time"

	eventspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
)

// newAsyncBusGraph is a memory graph whose events go to a one-worker async bus;
// every event of type want reaches the returned channel.
func newAsyncBusGraph(t *testing.T, want eventspkg.EventType) (*Core, *eventspkg.AsyncEventBus, <-chan eventspkg.Event) {
	t.Helper()
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	bus := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{Workers: 1, QueueSize: 64})
	t.Cleanup(func() { bus.Close() })
	if err := g.Events.SetAsync(bus); err != nil {
		t.Fatalf("SetAsync: %v", err)
	}
	ch := make(chan eventspkg.Event, 16)
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == want {
			select {
			case ch <- e:
			default:
			}
		}
	})
	return g, bus, ch
}

// awaitEvent returns the first event on ch, or fails with "never delivered"
// after 2 s.
func awaitEvent(t *testing.T, ch <-chan eventspkg.Event, what string) eventspkg.Event {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(2 * time.Second):
		t.Fatalf("async bus never delivered %s", what)
		return eventspkg.Event{}
	}
}

// TestPriority_GraphEventPriorities: through an async bus a node delete is
// delivered with PriorityCritical and a node create with PriorityHigh.
func TestPriority_GraphEventPriorities(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cases := []struct {
		name  string
		typ   eventspkg.EventType
		write func(t *testing.T, g *Core)
		want  eventspkg.EventPriority
	}{
		{"delete-is-critical", eventspkg.EventNodeDelete, func(t *testing.T, g *Core) {
			n, err := g.Nodes.Add(ctx, []string{"X"}, nil)
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if err := g.Nodes.Delete(ctx, n.ID()); err != nil {
				t.Fatalf("DeleteNode: %v", err)
			}
		}, eventspkg.PriorityCritical},
		{"create-is-high", eventspkg.EventNodeCreate, func(t *testing.T, g *Core) {
			if _, err := g.Nodes.Add(ctx, []string{"Y"}, nil); err != nil {
				t.Fatalf("AddNode: %v", err)
			}
		}, eventspkg.PriorityHigh},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g, _, ch := newAsyncBusGraph(t, tc.typ)
			tc.write(t, g)
			if e := awaitEvent(t, ch, tc.name); e.Priority != tc.want {
				t.Errorf("%v priority = %v, want %v", tc.typ, e.Priority, tc.want)
			}
		})
	}
}

// TestSetAsyncEventBus_GraphIntegration: SetAsync installs the bus as the
// async bus (GetAsync returns it) and leaves no sync bus; a create reaches it.
func TestSetAsyncEventBus_GraphIntegration(t *testing.T) {
	t.Parallel()
	g, bus, ch := newAsyncBusGraph(t, eventspkg.EventNodeCreate)
	if got := g.Events.GetAsync(); got != bus {
		t.Fatalf("GetAsync = %p, want %p", got, bus)
	}
	if got := g.Events.GetSync(); got != nil {
		t.Fatalf("GetSync with async bus = %p, want nil", got)
	}
	if _, err := g.Nodes.Add(context.Background(), []string{"Test"}, nil); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	awaitEvent(t, ch, "EventNodeCreate")
}
