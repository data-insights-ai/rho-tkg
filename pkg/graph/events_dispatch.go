package graph

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- eventspkg.Event bus ---

// SetEventBus attaches a synchronous eventspkg.EventBus to the graph.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (g *Graph) SetEventBus(bus *eventspkg.EventBus) {
	g.mu.Lock()
	if bus == nil {
		g.events = nil
	} else {
		g.events = bus
	}
	g.mu.Unlock()
}

// GetEventBus returns the attached synchronous eventspkg.EventBus, or nil if none is set
// (including when an eventspkg.AsyncEventBus is attached instead).
func (g *Graph) GetEventBus() *eventspkg.EventBus {
	g.mu.RLock()
	ep := g.events
	g.mu.RUnlock()
	if eb, ok := ep.(*eventspkg.EventBus); ok {
		return eb
	}
	return nil
}

// SetAsyncEventBus attaches an eventspkg.AsyncEventBus to the graph for async event delivery.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (g *Graph) SetAsyncEventBus(bus *eventspkg.AsyncEventBus) {
	g.mu.Lock()
	if bus == nil {
		g.events = nil
	} else {
		g.events = bus
	}
	g.mu.Unlock()
}

// publishEvent delivers a lifecycle event to the attached eventspkg.EventBus.
// During a transaction (txEventBuffer != nil), events are buffered instead of
// dispatched — published on Commit, discarded on Rollback.
// No-op if no eventspkg.EventBus is attached (nil-safe).
//
// IMPORTANT: callers must hold g.mu.RLock or g.mu.Lock when calling this method,
// because it reads g.events and g.txEventBuffer without additional synchronization.
// For dispatch AFTER releasing g.mu, capture ep := g.events under lock and call
// dispatchEvent(ep, ...) instead.
func (g *Graph) publishEvent(typ eventspkg.EventType, id types.EntityID, t types.Instant, priority eventspkg.EventPriority) {
	if g.events == nil {
		return
	}
	e := eventspkg.Event{Type: typ, EntityID: id, Timestamp: t, Priority: priority}
	if g.txEventBuffer != nil {
		*g.txEventBuffer = append(*g.txEventBuffer, e)
		return
	}
	g.events.Publish(e)
}

// dispatchEvent delivers an event to the given publisher. No-op if ep is nil.
// Use this for event dispatch after releasing g.mu — the caller captures g.events
// while the lock is held, then calls dispatchEvent with the captured reference.
func dispatchEvent(ep eventspkg.Publisher, e eventspkg.Event) {
	if ep == nil {
		return
	}
	ep.Publish(e)
}
