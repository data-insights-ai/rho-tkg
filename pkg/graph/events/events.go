// Package events provides the graph lifecycle event types and the two
// publisher implementations (sync EventBus, async AsyncEventBus) used by
// pkg/graph. This is a public sub-package: external callers (notably tkgd-v3)
// import it directly to construct buses (events.NewEventBus,
// events.NewAsyncEventBus) and observe events (events.Event, events.EventType,
// the EventNode*/EventRel* constants, and the BackpressureStrategy variants).
package events

import (
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// EventType classifies a graph lifecycle event.
type EventType uint8

// Event types — one per Graph mutation kind.
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

// Priority levels for AsyncEventBus delivery ordering.
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
// The zero value is ready to use.
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
// A nil handler is ignored and returns a no-op unsubscribe.
// The returned function may be called at any time to deregister the handler.
// Calling it multiple times is safe.
func (eb *EventBus) Subscribe(h EventHandler) func() {
	if eb == nil || h == nil {
		return func() {}
	}
	eb.mu.Lock()
	if eb.handlers == nil {
		eb.handlers = make(map[int]EventHandler)
	}
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

// Publish delivers e to all registered handlers.
// Copies the handler slice under RLock, then invokes each handler outside
// the lock to prevent deadlocks when handlers re-enter the Graph.
//
// Each handler is invoked via safeInvoke so that a panic inside a handler
// cannot crash the graph mutation that triggered the event. Panics are
// logged at Error level and do not prevent subsequent handlers from running.
func (eb *EventBus) Publish(e Event) {
	if eb == nil {
		return
	}
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

// PublishBatch dispatches each event to every handler, in order. The
// sync bus invokes handlers on the caller's goroutine and snapshots the
// handler set once for the whole batch, so subscription changes made by
// a handler do not affect later events in the same batch. Mirrors the
// AsyncEventBus contract so the Publisher interface is uniform.
func (eb *EventBus) PublishBatch(events ...Event) {
	if eb == nil {
		return
	}
	if len(events) == 0 {
		return
	}
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

	for _, e := range events {
		for _, h := range local {
			safeInvoke(h, e)
		}
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

// Publisher is the cross-package interface for event dispatch.
// Both *EventBus (sync) and *AsyncEventBus (async) implement this. The Graph
// layer holds an instance of this interface and never depends on the concrete
// bus type at the call site.
type Publisher interface {
	Publish(Event)
	// PublishBatch enqueues a sequence of events atomically. Async
	// implementations guarantee strict priority ordering across the
	// batch; sync implementations dispatch each event in order.
	PublishBatch(events ...Event)
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
	Workers      int                  // requested worker count; capped at 1 to preserve priority, default 1
	QueueSize    int                  // channel capacity (default 256)
	Backpressure BackpressureStrategy // behavior when queue is full; invalid values use BackpressureBlock
}

// AsyncEventBus delivers graph lifecycle events asynchronously via a dispatcher.
// Handlers are invoked in a dispatcher goroutine, not on the caller's goroutine.
// This decouples slow handler latency from graph write latency.
//
// Events are routed to per-priority queues. The dispatcher drains queues in priority
// order: Critical > High > Normal > Low > Deferred. PublishBatch enqueues
// multiple events atomically with a single wake-up at the end, so the dispatcher
// observes the full burst before its first scan and dispatches in strict
// priority order. Sequential Publish calls do not provide that guarantee
// because the dispatcher can wake from the first call's signal and dispatch
// before later events enqueue.
//
// The zero value starts lazily with the default configuration on first use.
type AsyncEventBus struct {
	mu       sync.RWMutex
	nextID   int
	handlers map[int]EventHandler

	// publishMu serializes Publish/PublishBatch so a batch's enqueues
	// land together and no per-event Publish can interleave a wake-up
	// signal mid-batch. The dispatcher does NOT take publishMu — it
	// observes the queues only via channel receives, and the
	// channels' own internal locking guarantees safe concurrent
	// access.
	publishMu sync.Mutex

	queues       [numPriorityLevels]chan Event // one channel per priority level
	backpressure BackpressureStrategy

	// wakeupCh is a 1-buffered "events available" signal. Each
	// Publish/PublishBatch does ONE non-blocking send after all
	// enqueues are complete. The dispatcher only learns "events are
	// available" via wakeupCh — never directly from a per-priority
	// channel in its blocking branch — so when it wakes it always
	// re-runs the priority-ordered scan and picks the highest
	// priority event among everything queued by the burst.
	wakeupCh chan struct{}

	wg        sync.WaitGroup
	stopCh    chan struct{}
	workers   int
	queueSize int
	closing   atomic.Bool
	startOnce sync.Once
	closeOnce sync.Once
}

// NewAsyncEventBus creates and starts an AsyncEventBus with the given configuration.
// The dispatcher is started immediately and consumes events until Close() is called.
func NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus {
	ab := &AsyncEventBus{
		backpressure: normalizeBackpressure(cfg.Backpressure),
		workers:      cfg.Workers,
		queueSize:    cfg.QueueSize,
	}
	ab.start()
	return ab
}

func normalizeBackpressure(strategy BackpressureStrategy) BackpressureStrategy {
	switch strategy {
	case BackpressureBlock, BackpressureDropOldest, BackpressureDropLatest:
		return strategy
	default:
		return BackpressureBlock
	}
}

func (ab *AsyncEventBus) start() {
	ab.startOnce.Do(func() {
		workers := ab.workers
		if workers <= 0 {
			workers = 1
		}
		if workers > 1 {
			workers = 1
		}
		ab.workers = workers
		queueSize := ab.queueSize
		if queueSize <= 0 {
			queueSize = 256
		}
		ab.backpressure = normalizeBackpressure(ab.backpressure)

		ab.handlers = make(map[int]EventHandler)
		ab.stopCh = make(chan struct{})
		ab.wakeupCh = make(chan struct{}, 1)
		for i := range ab.queues {
			ab.queues[i] = make(chan Event, queueSize)
		}

		for i := 0; i < workers; i++ {
			ab.wg.Add(1)
			go ab.worker()
		}
	})
}

// Subscribe registers a handler and returns an unsubscribe function.
// A nil handler is ignored and returns a no-op unsubscribe.
// Safe to call concurrently. Calling the unsubscribe function multiple times is safe.
func (ab *AsyncEventBus) Subscribe(h EventHandler) func() {
	if ab == nil || h == nil {
		return func() {}
	}
	ab.start()
	if ab.closing.Load() {
		return func() {}
	}
	ab.mu.Lock()
	if ab.closing.Load() {
		ab.mu.Unlock()
		return func() {}
	}
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

// Publish enqueues an event for async delivery into the per-priority queue.
// Behavior when the target queue is full is determined by BackpressureStrategy.
//
// Sequential Publish calls do NOT guarantee strict priority ordering across
// the burst: the dispatcher may wake from the first call's signal and dispatch
// before later events enqueue. Use PublishBatch when strict ordering across
// multiple events is required.
func (ab *AsyncEventBus) Publish(e Event) {
	if ab == nil {
		return
	}
	ab.start()
	if ab.closing.Load() {
		return
	}
	ab.publishMu.Lock()
	if ab.closing.Load() {
		ab.publishMu.Unlock()
		return
	}
	wrote := ab.enqueueLocked(e)
	ab.publishMu.Unlock()
	if wrote {
		ab.signalWakeup()
	}
}

// PublishBatch enqueues a sequence of events atomically: the publish mutex is
// held across every enqueue and the dispatcher is woken exactly once at the end.
// Events are enqueued by priority, preserving caller order within each priority
// level. That way, even if the dispatcher is already in a non-blocking scan
// while the batch is being enqueued, it cannot observe a lower-priority event
// before all higher-priority events from the same batch have been made visible.
// Use this for the Tx / Batch event-flush path or any caller that needs
// deterministic ordering across multiple events.
//
// Backpressure applies per event: if BackpressureBlock would block on
// a single event, the batch waits there before enqueueing later
// events. BackpressureDropOldest / DropLatest never block.
func (ab *AsyncEventBus) PublishBatch(events ...Event) {
	if ab == nil {
		return
	}
	if len(events) == 0 {
		return
	}
	ab.start()
	if ab.closing.Load() {
		return
	}
	ab.publishMu.Lock()
	if ab.closing.Load() {
		ab.publishMu.Unlock()
		return
	}
	wroteAny := false
	for _, p := range priorityOrder {
		for _, e := range events {
			if normalizePriority(e.Priority) != p {
				continue
			}
			if ab.enqueueLocked(e) {
				wroteAny = true
			}
		}
	}
	ab.publishMu.Unlock()
	if wroteAny {
		ab.signalWakeup()
	}
}

// enqueueLocked enqueues a single event into the appropriate priority
// channel, applying the configured backpressure strategy. Returns true
// if the event was successfully enqueued, false if it was dropped
// (DropLatest with full queue) or the bus is stopping. Caller must
// hold ab.publishMu.
func (ab *AsyncEventBus) enqueueLocked(e Event) bool {
	p := normalizePriority(e.Priority)
	q := ab.queues[p]

	switch ab.backpressure {
	case BackpressureBlock:
		select {
		case q <- e:
			return true
		default:
		}
		// A blocking batch send can fill a priority queue before
		// PublishBatch reaches its final wake-up. Signal here so the
		// dispatcher can drain space instead of sleeping behind a full queue.
		ab.signalWakeup()
		select {
		case q <- e:
			return true
		case <-ab.stopCh:
			return false
		}
	case BackpressureDropOldest:
		for {
			select {
			case q <- e:
				return true
			default:
				select {
				case <-ab.stopCh:
					return false
				case <-q:
				default:
					// Queue is full and drain attempt also contended;
					// yield so the dispatcher draining the queue gets CPU time.
					runtime.Gosched()
				}
			}
		}
	case BackpressureDropLatest:
		select {
		case q <- e:
			return true
		default:
			// Queue full — drop this event.
			return false
		}
	}
	return false
}

func normalizePriority(p EventPriority) EventPriority {
	if int(p) >= numPriorityLevels {
		return PriorityNormal
	}
	return p
}

// signalWakeup pokes the dispatcher via the coalescing wakeupCh. Buffered
// cap 1 means repeated Publishes against an already-pending wake-up
// collapse into a single signal; the dispatcher observes there is work and
// re-runs the priority-ordered drain.
func (ab *AsyncEventBus) signalWakeup() {
	select {
	case ab.wakeupCh <- struct{}{}:
	default:
	}
}

// priorityOrder is the dispatcher drain order: highest priority first.
var priorityOrder = [numPriorityLevels]EventPriority{
	PriorityCritical,
	PriorityHigh,
	PriorityNormal,
	PriorityLow,
	PriorityDeferred,
}

// worker runs the dispatcher loop and drains per-priority queues via a
// non-blocking scan; when all queues are empty it blocks ONLY on
// stopCh + wakeupCh (R4-F16). Critically, the blocking branch never
// receives an event directly — it only learns "events are now
// available" and loops back to the priority scan. This guarantees
// that when multiple priority queues become ready together, the
// scan picks the highest-priority one even though Go's select on
// multiple ready channels is pseudo-random.
//
// Priority ordering: Critical > High > Normal > Low > Deferred.
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
			// All queues empty — wait for a wake-up or stop. Drain any
			// pending wake-up signal that accumulated while we were
			// dispatching; the next iteration runs the priority scan.
			select {
			case <-ab.stopCh:
				ab.drainAll()
				return
			case <-ab.wakeupCh:
			}
		}
	}
}

// drainAll processes all remaining events in priority order before the dispatcher exits.
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

// Close signals the dispatcher to stop, waits for it to drain the queue and finish.
// Safe to call multiple times (B11). Blocks until all in-flight events are delivered.
func (ab *AsyncEventBus) Close() {
	if ab == nil {
		return
	}
	ab.start()
	ab.closeOnce.Do(func() {
		ab.closing.Store(true)
		close(ab.stopCh)
		ab.publishMu.Lock()
		// publishMu is a lifecycle gate here: taking it after closing waits for
		// any publisher that entered before the closing flag became visible.
		ab.publishMu.Unlock() //nolint:staticcheck
		ab.wg.Wait()
		ab.drainAll()
	})
}
