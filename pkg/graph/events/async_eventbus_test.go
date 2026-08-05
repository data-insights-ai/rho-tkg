package events

import (
	"fmt"
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

	// Wait for async dispatcher delivery.
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

func TestAsyncEventBusSubscribeNilIsNoop(t *testing.T) {
	var bus AsyncEventBus

	unsub := bus.Subscribe(nil)
	unsub()
	unsub()

	bus.mu.RLock()
	handlerCount := len(bus.handlers)
	bus.mu.RUnlock()
	if handlerCount != 0 {
		t.Fatalf("Subscribe(nil) installed %d handlers, want 0", handlerCount)
	}
	if bus.stopCh != nil {
		t.Fatal("Subscribe(nil) started zero-value AsyncEventBus")
	}

	bus.Publish(Event{Type: EventNodeCreate})
	bus.Close()
}

func TestAsyncEventBusNilReceiverMethodsAreNoop(t *testing.T) {
	var bus *AsyncEventBus

	unsub := bus.Subscribe(func(Event) {
		t.Fatal("nil AsyncEventBus should not install handlers")
	})
	unsub()
	unsub()

	bus.Publish(Event{Type: EventNodeCreate})
	bus.PublishBatch(Event{Type: EventRelDelete})
	bus.Close()
}

func TestAsyncEventBusZeroValueStartsOnFirstUse(t *testing.T) {
	var bus AsyncEventBus
	defer bus.Close()

	received := make(chan EventType, 2)
	bus.Subscribe(func(e Event) {
		received <- e.Type
	})

	bus.Publish(Event{Type: EventNodeCreate})
	bus.PublishBatch(Event{Type: EventRelDelete})

	for _, want := range []EventType{EventNodeCreate, EventRelDelete} {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("zero-value AsyncEventBus delivered %v, want %v", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("zero-value AsyncEventBus did not deliver %v", want)
		}
	}
}

func TestAsyncEventBusZeroValueCloseBeforeUse(t *testing.T) {
	var bus AsyncEventBus
	bus.Close()
	bus.Close()

	done := make(chan struct{})
	go func() {
		bus.Publish(Event{Type: EventNodeCreate})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish after zero-value Close blocked")
	}
}

func TestAsyncEventBusSubscribeAfterCloseIsNoop(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 1, QueueSize: 4})
	bus.Close()

	unsub := bus.Subscribe(func(Event) {
		t.Fatal("handler registered after Close should not be invoked")
	})
	unsub()
	unsub()

	bus.mu.RLock()
	handlerCount := len(bus.handlers)
	bus.mu.RUnlock()
	if handlerCount != 0 {
		t.Fatalf("Subscribe after Close installed %d handlers, want 0", handlerCount)
	}
}

func TestAsyncEventBusInvalidBackpressureDefaultsToBlock(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    16,
		Backpressure: BackpressureStrategy(99),
	})
	defer bus.Close()

	if got := bus.backpressure; got != BackpressureBlock {
		t.Fatalf("normalized backpressure = %d, want BackpressureBlock", got)
	}

	received := make(chan EventType, 1)
	bus.Subscribe(func(e Event) {
		received <- e.Type
	})

	bus.Publish(Event{Type: EventNodeCreate})
	select {
	case got := <-received:
		if got != EventNodeCreate {
			t.Fatalf("delivered event = %v, want %v", got, EventNodeCreate)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("invalid backpressure dropped published event")
	}
}

func TestAsyncEventBusPublishAfterCloseDoesNotEnqueue(t *testing.T) {
	for _, strategy := range []BackpressureStrategy{
		BackpressureBlock,
		BackpressureDropOldest,
		BackpressureDropLatest,
	} {
		t.Run(fmt.Sprintf("strategy_%d", strategy), func(t *testing.T) {
			bus := NewAsyncEventBus(AsyncEventBusConfig{
				Workers:      1,
				QueueSize:    4,
				Backpressure: strategy,
			})
			bus.Close()

			bus.Publish(Event{Type: EventNodeCreate, Priority: PriorityCritical})
			bus.PublishBatch(
				Event{Type: EventNodeUpdate, Priority: PriorityHigh},
				Event{Type: EventRelDelete, Priority: PriorityDeferred},
			)

			for i, q := range bus.queues {
				if got := len(q); got != 0 {
					t.Fatalf("queue %d length after post-close publish = %d, want 0", i, got)
				}
			}
		})
	}
}

func TestAsyncEventBusCloseUnblocksBackpressureBlockPublisher(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureBlock,
	})

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var startOnce sync.Once
	bus.Subscribe(func(Event) {
		startOnce.Do(func() { close(handlerStarted) })
		<-releaseHandler
	})

	bus.Publish(Event{Type: EventNodeCreate})
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		close(releaseHandler)
		t.Fatal("first event handler did not start")
	}

	bus.Publish(Event{Type: EventNodeUpdate})

	publishDone := make(chan struct{})
	go func() {
		bus.Publish(Event{Type: EventRelDelete})
		close(publishDone)
	}()

	select {
	case <-publishDone:
		close(releaseHandler)
		t.Fatal("third publish returned before the queue was full")
	case <-time.After(50 * time.Millisecond):
	}

	closeStarted := make(chan struct{})
	closeDone := make(chan struct{})
	go func() {
		close(closeStarted)
		bus.Close()
		close(closeDone)
	}()

	select {
	case <-closeStarted:
	case <-time.After(time.Second):
		close(releaseHandler)
		t.Fatal("Close goroutine did not start")
	}

	select {
	case <-publishDone:
	case <-time.After(time.Second):
		close(releaseHandler)
		t.Fatal("Close did not unblock publisher waiting on full queue")
	}

	select {
	case <-closeDone:
		close(releaseHandler)
		t.Fatal("Close returned while handler was still blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHandler)
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not finish after handler was released")
	}
}

func TestAsyncEventBusPublishBatchStrictPriorityDuringClose(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      2,
		QueueSize:    16,
		Backpressure: BackpressureBlock,
	})
	if got := bus.workers; got != 1 {
		t.Fatalf("configured Workers > 1 started %d dispatchers, want 1", got)
	}

	criticalStarted := make(chan struct{})
	releaseCritical := make(chan struct{})
	lowDelivered := make(chan struct{}, 1)

	bus.Subscribe(func(e Event) {
		switch e.Priority {
		case PriorityCritical:
			close(criticalStarted)
			<-releaseCritical
		case PriorityLow:
			lowDelivered <- struct{}{}
		}
	})

	bus.PublishBatch(
		Event{Type: EventNodeDelete, Priority: PriorityCritical},
		Event{Type: EventNodeUpdate, Priority: PriorityLow},
	)

	closed := make(chan struct{})
	go func() {
		bus.Close()
		close(closed)
	}()

	select {
	case <-criticalStarted:
	case <-time.After(2 * time.Second):
		close(releaseCritical)
		t.Fatal("critical event was not delivered")
	}

	select {
	case <-lowDelivered:
		close(releaseCritical)
		t.Fatal("low-priority batch event was delivered before critical handler completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCritical)

	select {
	case <-lowDelivered:
	case <-time.After(2 * time.Second):
		t.Fatal("low-priority event was not delivered after critical handler completed")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after draining events")
	}
}

func TestAsyncEventBusPublishBatchBlockWakesBeforeFullQueueWait(t *testing.T) {
	t.Parallel()

	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureBlock,
	})

	var delivered atomic.Int32
	bus.Subscribe(func(Event) {
		delivered.Add(1)
	})

	done := make(chan struct{})
	go func() {
		bus.PublishBatch(
			Event{Type: EventNodeCreate, Priority: PriorityNormal},
			Event{Type: EventNodeUpdate, Priority: PriorityNormal},
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		bus.Close()
		t.Fatal("PublishBatch blocked after filling the priority queue before waking the dispatcher")
	}

	bus.Close()
	if got := delivered.Load(); got != 2 {
		t.Fatalf("delivered events = %d, want 2", got)
	}
}

// TestAsyncEventBusPublishBatch_PriorityCeiling pins the W1 contract: when a
// PublishBatch saturates a priority queue, the in-batch wake-up MUST NOT
// cause the dispatcher to dispatch a pre-existing lower-priority event
// before the batch's later higher-priority events have been made visible.
//
// Setup forces the race window deterministically:
//
//  1. Subscribe a handler that blocks the first event (drains the
//     dispatcher's first wake-up slot).
//  2. Publish a Normal to load the dispatcher's handler — gets stuck there.
//  3. Publish a SECOND Normal, which now sits in the Normal queue while
//     the dispatcher is busy.
//  4. PublishBatch of 3 Criticals with QueueSize=1 — every enqueue past
//     the first will block, firing the in-batch wake-up.
//  5. Release the handler. Without the priority ceiling the dispatcher
//     would alternate Critical/Normal (priority order picks Critical
//     when both are queued, but as soon as Critical drains and the next
//     batch event hasn't enqueued yet, Normal sneaks in). With the
//     ceiling the dispatcher skips Normal until the batch completes.
func TestAsyncEventBusPublishBatch_PriorityCeiling(t *testing.T) {
	t.Parallel()

	bus := NewAsyncEventBus(AsyncEventBusConfig{
		Workers:      1,
		QueueSize:    1,
		Backpressure: BackpressureBlock,
	})

	gate := make(chan struct{})
	firstSeen := make(chan struct{})
	var firstOnce sync.Once
	var order []EventType
	var orderMu sync.Mutex
	bus.Subscribe(func(e Event) {
		firstOnce.Do(func() {
			close(firstSeen)
			<-gate
		})
		orderMu.Lock()
		order = append(order, e.Type)
		orderMu.Unlock()
	})

	// First Normal: dispatcher takes it, enters handler, blocks on gate.
	bus.Publish(Event{Type: EventNodeUpdate, Priority: PriorityNormal})
	<-firstSeen

	// Second Normal now sits in the Normal queue waiting.
	bus.Publish(Event{Type: EventNodeUpdate, Priority: PriorityNormal})

	// Critical batch — 3 events, QueueSize=1 ⇒ batch saturates and fires
	// the in-batch wake-up.
	done := make(chan struct{})
	go func() {
		bus.PublishBatch(
			Event{Type: EventNodeCreate, Priority: PriorityCritical},
			Event{Type: EventNodeCreate, Priority: PriorityCritical},
			Event{Type: EventNodeCreate, Priority: PriorityCritical},
		)
		close(done)
	}()

	// Give the batch goroutine time to enqueue the first Critical and
	// start the saturating second.
	time.Sleep(50 * time.Millisecond)

	close(gate) // release the dispatcher

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		bus.Close()
		t.Fatal("PublishBatch did not complete")
	}
	bus.Close()

	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != 5 {
		t.Fatalf("delivered = %d events (%v), want 5", len(order), order)
	}
	// order[0] is the staged Normal (the one in the handler).
	if order[0] != EventNodeUpdate {
		t.Fatalf("order[0] = %v, want EventNodeUpdate", order[0])
	}
	// The next THREE events must be Criticals — the ceiling prevents the
	// queued-Normal from interleaving.
	for i := 1; i <= 3; i++ {
		if order[i] != EventNodeCreate {
			t.Fatalf("order[%d] = %v, want EventNodeCreate (W1 ceiling violated: Normal interleaved with batch)", i, order[i])
		}
	}
	if order[4] != EventNodeUpdate {
		t.Fatalf("order[4] = %v, want EventNodeUpdate (queued Normal flushed last)", order[4])
	}
}

// publishBlockedTimeout bounds LIVENESS, not speed.
//
// It is deliberately enormous compared to the ~microseconds a non-blocking Publish
// takes, and that is the point. These probes assert "Publish does not block", and
// the earlier form — measure elapsed, fail if it exceeds 100ms — made a slow machine
// indistinguishable from a blocked one, so a loaded CI runner could fail code that
// was perfectly correct. Requiring the publishes to COMPLETE inverts the failure
// mode: a slow runner simply uses more of this budget and still passes, while a
// genuine block never completes and fails no matter how fast the machine is.
const publishBlockedTimeout = 30 * time.Second

// mustNotBlock runs publish on its own goroutine and requires it to finish.
//
// Returns only once the publishes are done, so a caller can safely release whatever
// was holding the handler afterwards — the ordering IS the assertion in the
// slow-handler probe.
func mustNotBlock(t *testing.T, what string, publish func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		publish()
	}()
	select {
	case <-done:
	case <-time.After(publishBlockedTimeout):
		t.Fatalf("%s: Publish did not return within %v — it BLOCKED. A non-blocking "+
			"backpressure policy must drop rather than wait.", what, publishBlockedTimeout)
	}
}

// stalledBus returns a bus whose every worker is BLOCKED inside the handler, and a
// release function.
//
// This is what makes a queue-full probe real. Publishing "more events than
// QueueSize" does NOT fill the queue while a worker is draining it concurrently —
// the drop path is simply never reached, and a Publish that blocks on a full queue
// would pass anyway. Verified: with a live worker, a mutation making DropOldest
// block on the send passed all four probes.
//
// With every worker parked in the handler, exactly Workers events are in flight and
// QueueSize more fit in the channel; everything beyond that MUST be dropped, so
// publishing well past that boundary is the only way to exercise the policy.
func stalledBus(t *testing.T, cfg AsyncEventBusConfig) (bus *AsyncEventBus, release func()) {
	t.Helper()
	bus = NewAsyncEventBus(cfg)
	blockCh := make(chan struct{})
	entered := make(chan struct{}, cfg.Workers)
	bus.Subscribe(func(e Event) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-blockCh
	})
	var once sync.Once
	release = func() { once.Do(func() { close(blockCh) }) }
	t.Cleanup(release) // never leave workers parked, even on a failed assertion

	// Park every worker before the probe publishes, so the queue state under test is
	// deterministic rather than a race with worker start-up.
	for i := 0; i < cfg.Workers; i++ {
		bus.Publish(Event{Type: EventNodeCreate})
	}
	for i := 0; i < cfg.Workers; i++ {
		select {
		case <-entered:
		case <-time.After(publishBlockedTimeout):
			t.Fatalf("worker %d never entered the handler; the probe cannot fill the queue", i)
		}
	}
	return bus, release
}

func TestAsyncEventBus_SlowHandlerDoesNotBlockPublish(t *testing.T) {
	const queueSize = 64
	bus, release := stalledBus(t, AsyncEventBusConfig{
		Workers: 1, QueueSize: queueSize, Backpressure: BackpressureDropLatest,
	})
	defer bus.Close()
	defer release()

	// Publish PAST the queue capacity while the only worker is parked. The previous
	// version sent 10 events into a queue of 64 and could never reach the full-queue
	// path it meant to test.
	mustNotBlock(t, "slow handler", func() {
		for i := 0; i < queueSize*2; i++ {
			bus.Publish(Event{Type: EventNodeCreate})
		}
	})
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
	bus, release := stalledBus(t, AsyncEventBusConfig{
		Workers: 1, QueueSize: 2, Backpressure: BackpressureDropOldest,
	})
	defer bus.Close()
	defer release()

	// The worker is parked and the queue holds 2, so events past that MUST be
	// evicted. A Publish that waits for space instead would never return.
	mustNotBlock(t, "DropOldest", func() {
		for i := 0; i < 50; i++ {
			bus.Publish(Event{Type: EventNodeCreate})
		}
	})
}

func TestAsyncEventBus_BackpressureDropLatest(t *testing.T) {
	bus, release := stalledBus(t, AsyncEventBusConfig{
		Workers: 1, QueueSize: 2, Backpressure: BackpressureDropLatest,
	})
	defer bus.Close()
	defer release()

	// The worker is parked and the queue holds 2, so events past that MUST be
	// discarded. A Publish that waits for space instead would never return.
	mustNotBlock(t, "DropLatest", func() {
		for i := 0; i < 50; i++ {
			bus.Publish(Event{Type: EventNodeCreate})
		}
	})
}

func TestAsyncEventBusEnqueueLockedDefensiveBranches(t *testing.T) {
	t.Run("invalid priority uses normal queue", func(t *testing.T) {
		var bus AsyncEventBus
		bus.backpressure = BackpressureDropLatest
		bus.stopCh = make(chan struct{})
		bus.queues[PriorityNormal] = make(chan Event, 1)

		bus.publishMu.Lock()
		wrote := bus.enqueueLocked(Event{Type: EventNodeCreate, Priority: EventPriority(numPriorityLevels + 1)}, nil)
		bus.publishMu.Unlock()
		if !wrote {
			t.Fatal("enqueueLocked dropped invalid-priority event, want normal-priority enqueue")
		}

		select {
		case got := <-bus.queues[PriorityNormal]:
			if got.Type != EventNodeCreate {
				t.Fatalf("event type = %v, want %v", got.Type, EventNodeCreate)
			}
		default:
			t.Fatal("normal-priority queue did not receive invalid-priority event")
		}
	})

	t.Run("drop oldest stops while contended", func(t *testing.T) {
		var bus AsyncEventBus
		bus.backpressure = BackpressureDropOldest
		bus.stopCh = make(chan struct{})
		bus.queues[PriorityNormal] = make(chan Event)

		done := make(chan bool, 1)
		go func() {
			bus.publishMu.Lock()
			done <- bus.enqueueLocked(Event{Type: EventNodeCreate}, nil)
			bus.publishMu.Unlock()
		}()

		time.Sleep(10 * time.Millisecond)
		close(bus.stopCh)

		select {
		case wrote := <-done:
			if wrote {
				t.Fatal("enqueueLocked reported a write after stopCh closed")
			}
		case <-time.After(time.Second):
			t.Fatal("enqueueLocked did not return after stopCh closed")
		}
	})

	t.Run("unknown backpressure drops defensively", func(t *testing.T) {
		var bus AsyncEventBus
		bus.backpressure = BackpressureStrategy(99)
		bus.stopCh = make(chan struct{})
		bus.queues[PriorityNormal] = make(chan Event, 1)

		bus.publishMu.Lock()
		wrote := bus.enqueueLocked(Event{Type: EventNodeCreate}, nil)
		bus.publishMu.Unlock()
		if wrote {
			t.Fatal("enqueueLocked wrote with an unknown backpressure strategy")
		}
		if got := len(bus.queues[PriorityNormal]); got != 0 {
			t.Fatalf("normal-priority queue length = %d, want 0", got)
		}
	})
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

func TestAsyncEventBusCloseFinalDrainAfterPublisherGate(t *testing.T) {
	var bus AsyncEventBus
	var delivered atomic.Int32
	wrongType := make(chan EventType, 1)
	bus.handlers = map[int]EventHandler{
		1: func(e Event) {
			if e.Type != EventNodeCreate {
				wrongType <- e.Type
				return
			}
			delivered.Add(1)
		},
	}
	bus.stopCh = make(chan struct{})
	bus.wakeupCh = make(chan struct{}, 1)
	for i := range bus.queues {
		bus.queues[i] = make(chan Event, 1)
	}
	bus.startOnce.Do(func() {})

	bus.publishMu.Lock()
	closed := make(chan struct{})
	go func() {
		bus.Close()
		close(closed)
	}()

	select {
	case <-bus.stopCh:
	case <-time.After(time.Second):
		bus.publishMu.Unlock()
		t.Fatal("Close did not signal stop")
	}

	bus.queues[PriorityNormal] <- Event{Type: EventNodeCreate}
	bus.publishMu.Unlock()

	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not return after publisher gate released")
	}
	if got := delivered.Load(); got != 1 {
		t.Fatalf("delivered events after final drain = %d, want 1", got)
	}
	select {
	case got := <-wrongType:
		t.Fatalf("event type = %v, want EventNodeCreate", got)
	default:
	}
}

func TestAsyncEventBusConcurrentPublishers(t *testing.T) {
	// Run with -race to detect data races across concurrent publishers.
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

func TestPriority_CriticalBeforeNormal(t *testing.T) {
	bus := NewAsyncEventBus(AsyncEventBusConfig{Workers: 2, QueueSize: 64})
	defer bus.Close()

	received := make(chan EventPriority, 2)
	bus.Subscribe(func(e Event) {
		received <- e.Priority
	})

	bus.PublishBatch(
		Event{Type: EventNodeUpdate, Priority: PriorityNormal},
		Event{Type: EventNodeDelete, Priority: PriorityCritical},
	)

	for _, want := range []EventPriority{PriorityCritical, PriorityNormal} {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("delivery priority = %v, want %v", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for priority %v", want)
		}
	}
}
