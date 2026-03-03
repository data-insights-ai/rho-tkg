package graph

import (
	"log/slog"
	"sync"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg-v3/pkg/types"
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

// Event is a lifecycle notification emitted after a successful graph mutation.
type Event struct {
	Type      EventType
	EntityID  snowflake.ID
	Timestamp types.Instant
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
