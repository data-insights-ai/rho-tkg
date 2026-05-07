package graph

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/events"
)

// Lifecycle event types and buses are defined in `pkg/graph/internal/events`.
// The aliases below ARE the public API; external callers (notably tkgd-v3)
// depend on `graph.Event`, `graph.EventBus`, `graph.AsyncEventBus`, the
// `EventNode*`/`EventRel*` constants, the `Priority*` constants, and the
// `Backpressure*` constants. The Graph-coupled dispatch wiring (`SetEventBus`,
// `SetAsyncEventBus`, internal `dispatchEvent`/`publishEvent`) lives in
// `events_dispatch.go`.

type (
	// Event is a lifecycle notification emitted after a successful graph mutation.
	Event = events.Event
	// EventType classifies a graph lifecycle event.
	EventType = events.EventType
	// EventPriority controls the delivery queue for AsyncEventBus.
	EventPriority = events.EventPriority
	// EventHandler is a callback invoked for each published event.
	EventHandler = events.EventHandler
	// EventBus dispatches graph lifecycle events to registered subscribers synchronously.
	EventBus = events.EventBus
	// AsyncEventBus delivers graph lifecycle events asynchronously via a worker pool.
	AsyncEventBus = events.AsyncEventBus
	// AsyncEventBusConfig configures an AsyncEventBus.
	AsyncEventBusConfig = events.AsyncEventBusConfig
	// BackpressureStrategy controls AsyncEventBus behaviour when the queue is full.
	BackpressureStrategy = events.BackpressureStrategy
)

// EventType constants.
const (
	EventNodeCreate = events.EventNodeCreate
	EventNodeUpdate = events.EventNodeUpdate
	EventNodeDelete = events.EventNodeDelete
	EventRelCreate  = events.EventRelCreate
	EventRelUpdate  = events.EventRelUpdate
	EventRelDelete  = events.EventRelDelete
)

// EventPriority constants.
const (
	PriorityNormal   = events.PriorityNormal
	PriorityHigh     = events.PriorityHigh
	PriorityCritical = events.PriorityCritical
	PriorityLow      = events.PriorityLow
	PriorityDeferred = events.PriorityDeferred
)

// BackpressureStrategy constants.
const (
	BackpressureBlock      = events.BackpressureBlock
	BackpressureDropOldest = events.BackpressureDropOldest
	BackpressureDropLatest = events.BackpressureDropLatest
)

// NewEventBus creates an EventBus ready for use.
func NewEventBus() *EventBus { return events.NewEventBus() }

// NewAsyncEventBus creates and starts an AsyncEventBus with the given configuration.
func NewAsyncEventBus(cfg AsyncEventBusConfig) *AsyncEventBus {
	return events.NewAsyncEventBus(cfg)
}
