package graph

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPriority_GraphDeleteIsCritical(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 64})
	defer bus.Close()
	g.SetAsyncEventBus(bus)

	var deleteEvent Event
	var mu sync.Mutex
	bus.Subscribe(func(e Event) {
		if e.Type == EventNodeDelete {
			mu.Lock()
			deleteEvent = e
			mu.Unlock()
		}
	})

	n, _ := g.AddNode([]string{"X"}, nil)
	nid := n.ID()
	_ = g.DeleteNode(nid)

	// Wait for delivery
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := deleteEvent
		mu.Unlock()
		if got.Type == EventNodeDelete {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if deleteEvent.Priority != PriorityCritical {
		t.Errorf("EventNodeDelete priority = %v, want PriorityCritical", deleteEvent.Priority)
	}
}

func TestPriority_GraphCreateIsHigh(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 64})
	defer bus.Close()
	g.SetAsyncEventBus(bus)

	var createEvent Event
	var mu sync.Mutex
	bus.Subscribe(func(e Event) {
		if e.Type == EventNodeCreate {
			mu.Lock()
			createEvent = e
			mu.Unlock()
		}
	})

	_, err = g.AddNode([]string{"Y"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := createEvent
		mu.Unlock()
		if got.Type == EventNodeCreate {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if createEvent.Priority != PriorityHigh {
		t.Errorf("EventNodeCreate priority = %v, want PriorityHigh", createEvent.Priority)
	}
}

func TestSetAsyncEventBus_GraphIntegration(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 32})
	defer bus.Close()

	g.SetAsyncEventBus(bus)

	var received atomic.Int32
	bus.Subscribe(func(e Event) {
		if e.Type == EventNodeCreate {
			received.Add(1)
		}
	})

	_, err = g.AddNode([]string{"Test"}, nil)
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
		t.Fatal("async handler never received EventNodeCreate")
	}
}
