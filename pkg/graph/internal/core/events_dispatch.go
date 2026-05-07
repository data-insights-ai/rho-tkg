package core

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- eventspkg.Event bus ---

// SetEventBus attaches a synchronous eventspkg.EventBus to the graph.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (c *Core) SetEventBus(bus *eventspkg.EventBus) {
	c.mu.Lock()
	if bus == nil {
		c.events = nil
	} else {
		c.events = bus
	}
	c.mu.Unlock()
}

// GetEventBus returns the attached synchronous eventspkg.EventBus, or nil if none is set
// (including when an eventspkg.AsyncEventBus is attached instead).
func (c *Core) GetEventBus() *eventspkg.EventBus {
	c.mu.RLock()
	ep := c.events
	c.mu.RUnlock()
	if eb, ok := ep.(*eventspkg.EventBus); ok {
		return eb
	}
	return nil
}

// SetAsyncEventBus attaches an eventspkg.AsyncEventBus to the graph for async event delivery.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (c *Core) SetAsyncEventBus(bus *eventspkg.AsyncEventBus) {
	c.mu.Lock()
	if bus == nil {
		c.events = nil
	} else {
		c.events = bus
	}
	c.mu.Unlock()
}

// publishEvent delivers a lifecycle event to the attached eventspkg.EventBus.
// During a transaction (txEventBuffer != nil), events are buffered instead of
// dispatched — published on Commit, discarded on Rollback.
// No-op if no eventspkg.EventBus is attached (nil-safe).
//
// IMPORTANT: callers must hold c.mu.RLock or c.mu.Lock when calling this method,
// because it reads c.events and c.txEventBuffer without additional synchronization.
// For dispatch AFTER releasing c.mu, capture ep := c.events under lock and call
// dispatchEvent(ep, ...) instead.
func (c *Core) publishEvent(typ eventspkg.EventType, id types.EntityID, t types.Instant, priority eventspkg.EventPriority) {
	if c.events == nil {
		return
	}
	e := eventspkg.Event{Type: typ, EntityID: id, Timestamp: t, Priority: priority}
	if c.txEventBuffer != nil {
		*c.txEventBuffer = append(*c.txEventBuffer, e)
		return
	}
	c.events.Publish(e)
}

// dispatchEvent delivers an event to the given publisher. No-op if ep is nil.
// Use this for event dispatch after releasing c.mu — the caller captures c.events
// while the lock is held, then calls dispatchEvent with the captured reference.
func dispatchEvent(ep eventspkg.Publisher, e eventspkg.Event) {
	if ep == nil {
		return
	}
	ep.Publish(e)
}
