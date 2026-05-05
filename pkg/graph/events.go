package graph

import (
	"log/slog"
	"runtime"
	"sync"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// EventType classifies a graph lifecycle event.
type EventType uint8

const (
	EventNodeCreate EventType = iota + 1
	EventNodeUpdate
	EventNodeDelete
	EventRelCreate
	EventRelUpdate
	EventRelDelete
)

// EventPriority controls the delivery queue for AsyncEventBus.
// Zero value is PriorityNormal — all existing Event{} literals remain valid.
type EventPriority uint8

const (
	PriorityNormal   EventPriority = iota // 0 — zero value, backward-compatible
	PriorityHigh                          // 1
	PriorityCritical                      // 2
	PriorityLow                           // 3
	PriorityDeferred                      // 4
)

// numPriorityLevels is the count of EventPriority constants.
// Used to size the per-priority queue array in AsyncEventBus.
const numPriorityLevels = 5

// Event is a lifecycle notification emitted after a successful graph mutation.
type Event struct {
	Type      EventType
	EntityID  types.EntityID
	Timestamp types.Instant
	Priority  EventPriority // zero value = PriorityNormal
}

// EventHandler is a callback invoked for each published event.
// Handlers are called synchronously inline with the mutation.
// For async delivery, push to a buffered channel inside the handler.
type EventHandler func(Event)

// EventBus dispatches graph lifecycle events to registered subscribers.
// Multiple handlers may be registered; all receive every event.
//
// Handlers are invoked outside the EventBus lock to prevent deadlocks
// when a handler re-enters the Graph (e.g., reads the mutated entity).
//
// Zero value is not usable — use NewEventBus().
type EventBus struct {
	mu       sync.RWMutex
	nextID   int
	handlers map[int]EventHandler
}

// NewEventBus creates an EventBus ready for use.
func NewEventBus() *EventBus {
	return &EventBus{handlers: make(map[int]EventHandler)}
}

// Subscribe registers a handler and returns an unsubscribe function.
// The returned function may be called at any time to deregister the handler.
// Calling it multiple times is safe.
func (eb *EventBus) Subscribe(h EventHandler) func() {
	eb.mu.Lock()
	id := eb.nextID
	eb.nextID++
	eb.handlers[id] = h
	eb.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			eb.mu.Lock()
			delete(eb.handlers, id)
			eb.mu.Unlock()
		})
	}
}

// publish delivers e to all registered handlers.
// Copies the handler slice under RLock, then invokes each handler outside
// the lock to prevent deadlocks when handlers re-enter the Graph.
//
// Each handler is invoked via safeInvoke so that a panic inside a handler
// cannot crash the graph mutation that triggered the event. Panics are
// logged at Error level and do not prevent subsequent handlers from running.
func (eb *EventBus) publish(e Event) {
	eb.mu.RLock()
	if len(eb.handlers) == 0 {
		eb.mu.RUnlock()
		return
	}
	local := make([]EventHandler, 0, len(eb.handlers))
	for _, h := range eb.handlers {
		local = append(local, h)
	}
	eb.mu.RUnlock()

	for _, h := range local {
		safeInvoke(h, e)
	}
}

// safeInvoke calls h(e) and recovers from any panic, logging it via slog.
// This isolates a misbehaving subscriber from crashing the mutation caller.
func safeInvoke(h EventHandler, e Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("graph: event handler panicked", "panic", r,
				"eventType", e.Type, "entityID", e.EntityID)
		}
	}()
	h(e)
}

// eventPublisher is the internal interface for event dispatch.
// Both *EventBus (sync) and *AsyncEventBus (async) implement this.
type eventPublisher interface {
	publish(Event)
}

// BackpressureStrategy controls AsyncEventBus behavior when the queue is full.
type BackpressureStrategy uint8

const (
	// BackpressureBlock blocks the caller until queue space is available.
	BackpressureBlock BackpressureStrategy = iota
	// BackpressureDropOldest evicts the oldest queued event and enqueues the new one.
	BackpressureDropOldest
	// BackpressureDropLatest discards the new event (no-op) when the queue is full.
	BackpressureDropLatest
)

// AsyncEventBusConfig configures an AsyncEventBus.
type AsyncEventBusConfig struct {
	Workers      int                  // number of worker goroutines (default 1)
	QueueSize    int                  // channel capacity (default 256)
	Backpressure BackpressureStrategy // behavior when queue is full
}

// AsyncEventBus delivers graph lifecycle events asynchronously via a worker pool.
// Handlers are invoked in worker goroutines, not on the caller's goroutine.
// This decouples slow handler latency from graph write latency.
//
// Events are routed to per-priority queues. Workers drain queues in priority
// order: Critical > High > Normal > Low > Deferred.
//
// Zero value is not usable — use NewAsyncEventBus().
type AsyncEventBus struct {
	mu       sync.RWMutex
	nextID   int
	handlers map[int]EventHandler

	queues       [numPriorityLevels]chan Event // one channel per priority level
	backpressure BackpressureStrategy

	wg        sync.WaitGroup
	stopCh    chan struct{}
	closeOnce sync.Once
}

// NewAsyncEventBus creates and starts an AsyncEventBus with the given configuration.
// Workers are started immediately and consume events until Close() is called.
func NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 256
	}

	ab := &AsyncEventBus{
		handlers:     make(map[int]EventHandler),
		backpressure: cfg.Backpressure,
		stopCh:       make(chan struct{}),
	}
	for i := range ab.queues {
		ab.queues[i] = make(chan Event, cfg.QueueSize)
	}

	for i := 0; i < cfg.Workers; i++ {
		ab.wg.Add(1)
		go ab.worker()
	}
	return ab
}

// Subscribe registers a handler and returns an unsubscribe function.
// Safe to call concurrently. Calling the unsubscribe function multiple times is safe.
func (ab *AsyncEventBus) Subscribe(h EventHandler) func() {
	ab.mu.Lock()
	id := ab.nextID
	ab.nextID++
	ab.handlers[id] = h
	ab.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			ab.mu.Lock()
			delete(ab.handlers, id)
			ab.mu.Unlock()
		})
	}
}

// publish enqueues an event for async delivery into the per-priority queue.
// Behavior when the target queue is full is determined by BackpressureStrategy.
func (ab *AsyncEventBus) publish(e Event) {
	p := e.Priority
	if int(p) >= numPriorityLevels {
		p = PriorityNormal
	}
	q := ab.queues[p]

	switch ab.backpressure {
	case BackpressureBlock:
		select {
		case q <- e:
		case <-ab.stopCh:
		}
	case BackpressureDropOldest:
		for {
			select {
			case q <- e:
				return
			default:
				select {
				case <-q:
				default:
					// Queue is full and drain attempt also contended; yield to the
					// scheduler so workers draining the queue get CPU time.
					// Without this, a tight spin under high contention can livelock.
					runtime.Gosched()
				}
			}
		}
	case BackpressureDropLatest:
		select {
		case q <- e:
		default:
			// Queue full — discard this event
		}
	}
}

// priorityOrder is the drain order for worker goroutines: highest priority first.
var priorityOrder = [numPriorityLevels]EventPriority{
	PriorityCritical,
	PriorityHigh,
	PriorityNormal,
	PriorityLow,
	PriorityDeferred,
}

// worker drains per-priority queues in priority order and invokes handlers.
// Priority ordering: Critical > High > Normal > Low > Deferred.
// Go's select does not guarantee order when multiple channels are ready, so a
// non-blocking check per priority implements the strict drain order.
func (ab *AsyncEventBus) worker() {
	defer ab.wg.Done()
	for {
		// Non-blocking stop check first.
		select {
		case <-ab.stopCh:
			ab.drainAll()
			return
		default:
		}

		// Try priority queues in order: non-blocking check per priority.
		served := false
		for _, p := range priorityOrder {
			select {
			case e := <-ab.queues[p]:
				ab.dispatch(e)
				served = true
			default:
			}
			if served {
				break
			}
		}

		if !served {
			// All queues empty — block until any event or stop signal arrives.
			select {
			case <-ab.stopCh:
				ab.drainAll()
				return
			case e := <-ab.queues[PriorityCritical]:
				ab.dispatch(e)
			case e := <-ab.queues[PriorityHigh]:
				ab.dispatch(e)
			case e := <-ab.queues[PriorityNormal]:
				ab.dispatch(e)
			case e := <-ab.queues[PriorityLow]:
				ab.dispatch(e)
			case e := <-ab.queues[PriorityDeferred]:
				ab.dispatch(e)
			}
		}
	}
}

// drainAll processes all remaining events in priority order before worker exits.
func (ab *AsyncEventBus) drainAll() {
	for {
		drained := false
		for _, p := range priorityOrder {
			select {
			case e := <-ab.queues[p]:
				ab.dispatch(e)
				drained = true
			default:
			}
		}
		if !drained {
			return
		}
	}
}

// dispatch copies handlers under RLock and invokes each via safeInvoke.
// Uses copy-outside-lock pattern (B15) to prevent deadlocks when handlers
// re-enter the Graph.
func (ab *AsyncEventBus) dispatch(e Event) {
	ab.mu.RLock()
	if len(ab.handlers) == 0 {
		ab.mu.RUnlock()
		return
	}
	local := make([]EventHandler, 0, len(ab.handlers))
	for _, h := range ab.handlers {
		local = append(local, h)
	}
	ab.mu.RUnlock()

	for _, h := range local {
		safeInvoke(h, e)
	}
}

// Close signals workers to stop, waits for them to drain the queue and finish.
// Safe to call multiple times (B11). Blocks until all in-flight events are delivered.
func (ab *AsyncEventBus) Close() {
	ab.closeOnce.Do(func() {
		close(ab.stopCh)
		ab.wg.Wait()
	})
}
