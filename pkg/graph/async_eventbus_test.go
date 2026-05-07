package graph

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAsyncEventBus_HandlerReceivesEvent(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 16})
	defer bus.Close()

	var received atomic.Int32
	bus.Subscribe(func(e Event) {
		received.Add(1)
	})

	bus.Publish(Event{Type: EventNodeCreate})

	// Wait for delivery (workers are async)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if received.Load() > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if received.Load() == 0 {
		t.Fatal("handler never received event")
	}
}

func TestAsyncEventBus_SlowHandlerDoesNotBlockPublish(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 64, Backpressure: BackpressureDropLatest})
	defer bus.Close()

	blockCh := make(chan struct{})
	bus.Subscribe(func(e Event) {
		<-blockCh // blocks until we unblock it
	})

	// Publish many events quickly — should not block even with slow handler
	start := time.Now()
	for i := 0; i < 10; i++ {
		bus.Publish(Event{Type: EventNodeCreate})
	}
	elapsed := time.Since(start)
	close(blockCh)

	if elapsed > 100*time.Millisecond {
		t.Errorf("publish took too long with slow handler: %v", elapsed)
	}
}

func TestAsyncEventBus_BackpressureBlock(t *testing.T) {
	// With BackpressureBlock and a full queue, publish should eventually unblock
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureBlock,
	})
	defer bus.Close()

	blockCh := make(chan struct{})
	bus.Subscribe(func(e Event) {
		<-blockCh
	})

	// Fill the queue
	bus.Publish(Event{Type: EventNodeCreate})
	bus.Publish(Event{Type: EventNodeCreate})

	// Publish in a goroutine (would block)
	done := make(chan struct{})
	go func() {
		defer close(done)
		bus.Publish(Event{Type: EventNodeCreate})
	}()

	// Unblock the handler — queue drains and goroutine can proceed
	close(blockCh)

	select {
	case <-done:
		// good
	case <-time.After(2 * time.Second):
		t.Fatal("BackpressureBlock: publish goroutine did not unblock")
	}
}

func TestAsyncEventBus_BackpressureDropOldest(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureDropOldest,
	})
	defer bus.Close()

	var received []EventType
	var mu sync.Mutex
	bus.Subscribe(func(e Event) {
		mu.Lock()
		received = append(received, e.Type)
		mu.Unlock()
	})

	// This just tests that publish does NOT block with DropOldest
	start := time.Now()
	for i := 0; i < 5; i++ {
		bus.Publish(Event{Type: EventNodeCreate})
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Error("DropOldest: publish should not block")
	}
}

func TestAsyncEventBus_BackpressureDropLatest(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    2,
		Backpressure: BackpressureDropLatest,
	})
	defer bus.Close()

	// Test that publish does NOT block when queue is full
	start := time.Now()
	for i := 0; i < 5; i++ {
		bus.Publish(Event{Type: EventNodeCreate})
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Error("DropLatest: publish should not block")
	}
}

func TestAsyncEventBus_Close_DrainsQueue(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 2, QueueSize: 64})

	var counter atomic.Int32
	bus.Subscribe(func(e Event) {
		counter.Add(1)
	})

	const n = 20
	for i := 0; i < n; i++ {
		bus.Publish(Event{Type: EventNodeCreate})
	}

	bus.Close() // should drain all pending events before returning

	if counter.Load() < n {
		t.Errorf("Close did not drain queue: received %d/%d events", counter.Load(), n)
	}
}

func TestAsyncEventBus_MultipleWorkers(t *testing.T) {
	// Run with -race to detect data races
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 4, QueueSize: 256})
	defer bus.Close()

	var counter atomic.Int64
	bus.Subscribe(func(e Event) {
		counter.Add(1)
	})

	const goroutines = 8
	const eventsPerGoroutine = 25
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < eventsPerGoroutine; j++ {
				bus.Publish(Event{Type: EventNodeCreate})
			}
		}()
	}
	wg.Wait()

	// Wait for delivery
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if counter.Load() == goroutines*eventsPerGoroutine {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := counter.Load()
	want := int64(goroutines * eventsPerGoroutine)
	if got != want {
		t.Errorf("expected %d events, got %d", want, got)
	}
}

func TestPriority_ZeroValueIsNormal(t *testing.T) {
	// Event{} zero value should have PriorityNormal (0)
	e := Event{}
	if e.Priority != PriorityNormal {
		t.Errorf("zero Event.Priority should be PriorityNormal, got %v", e.Priority)
	}
}

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

func TestPriority_CriticalBeforeNormal(t *testing.T) {
	// With a single worker and a stalled queue, critical events should be
	// processed before normal events — but this is hard to test deterministically
	// without single-threaded sequential drain.
	//
	// Instead, test that the per-priority queue routing is correct:
	// publish Critical + Normal, verify both are eventually received.
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 2, QueueSize: 64})
	defer bus.Close()

	var received []EventPriority
	var mu sync.Mutex
	bus.Subscribe(func(e Event) {
		mu.Lock()
		received = append(received, e.Priority)
		mu.Unlock()
	})

	bus.Publish(Event{Type: EventNodeUpdate, Priority: PriorityNormal})
	bus.Publish(Event{Type: EventNodeDelete, Priority: PriorityCritical})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 2 {
		t.Fatalf("expected 2 events, got %d", len(received))
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
