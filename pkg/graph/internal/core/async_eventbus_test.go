package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

func TestPriority_GraphDeleteIsCritical(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{Workers: 1, QueueSize: 64})
	defer bus.Close()
	_ = g.Events.SetAsync(bus)

	var deleteEvent eventspkg.Event
	var mu sync.Mutex
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeDelete {
			mu.Lock()
			deleteEvent = e
			mu.Unlock()
		}
	})

	n, _ := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	nid := n.ID()
	_ = g.Nodes.Delete(context.Background(), nid)

	// Wait for delivery
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := deleteEvent
		mu.Unlock()
		if got.Type == eventspkg.EventNodeDelete {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if deleteEvent.Priority != eventspkg.PriorityCritical {
		t.Errorf("eventspkg.EventNodeDelete priority = %v, want eventspkg.PriorityCritical", deleteEvent.Priority)
	}
}

func TestPriority_GraphCreateIsHigh(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{Workers: 1, QueueSize: 64})
	defer bus.Close()
	_ = g.Events.SetAsync(bus)

	var createEvent eventspkg.Event
	var mu sync.Mutex
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			mu.Lock()
			createEvent = e
			mu.Unlock()
		}
	})

	_, err = g.Nodes.Add(context.Background(), []string{"Y"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := createEvent
		mu.Unlock()
		if got.Type == eventspkg.EventNodeCreate {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if createEvent.Priority != eventspkg.PriorityHigh {
		t.Errorf("eventspkg.EventNodeCreate priority = %v, want eventspkg.PriorityHigh", createEvent.Priority)
	}
}

func TestSetAsyncEventBus_GraphIntegration(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := eventspkg.NewAsyncEventBus(eventspkg.AsyncEventBusConfig{Workers: 1, QueueSize: 32})
	defer bus.Close()

	_ = g.Events.SetAsync(bus)
	if got := g.Events.GetAsync(); got != bus {
		t.Fatalf("GetAsync = %p, want %p", got, bus)
	}
	if got := g.Events.GetSync(); got != nil {
		t.Fatalf("GetSync with async bus = %p, want nil", got)
	}

	var received atomic.Int32
	bus.Subscribe(func(e eventspkg.Event) {
		if e.Type == eventspkg.EventNodeCreate {
			received.Add(1)
		}
	})

	_, err = g.Nodes.Add(context.Background(), []string{"Test"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	// Wait for async delivery
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if received.Load() == 0 {
		t.Fatal("async handler never received eventspkg.EventNodeCreate")
	}
}
