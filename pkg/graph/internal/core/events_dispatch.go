package core

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- eventspkg.Event bus ---

// SetSync attaches a synchronous eventspkg.EventBus to the graph.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (e *EventOps) SetSync(bus *eventspkg.EventBus) error {
	c := e.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if bus == nil {
		c.events = nil
	} else {
		c.events = bus
	}
	return nil
}

// GetSync returns the attached synchronous eventspkg.EventBus, or nil if none is set
// (including when an eventspkg.AsyncEventBus is attached instead).
func (e *EventOps) GetSync() *eventspkg.EventBus {
	c := e.c
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return nil
	}
	ep := c.events
	c.mu.RUnlock()
	if eb, ok := ep.(*eventspkg.EventBus); ok {
		return eb
	}
	return nil
}

// GetAsync returns the attached asynchronous eventspkg.AsyncEventBus, or nil if none is set
// (including when a synchronous eventspkg.EventBus is attached instead).
func (e *EventOps) GetAsync() *eventspkg.AsyncEventBus {
	c := e.c
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return nil
	}
	ep := c.events
	c.mu.RUnlock()
	if eb, ok := ep.(*eventspkg.AsyncEventBus); ok {
		return eb
	}
	return nil
}

// SetAsync attaches an eventspkg.AsyncEventBus to the graph for async event delivery.
// All subsequent mutations publish lifecycle events to the bus.
// Pass nil to detach and disable event publishing.
func (e *EventOps) SetAsync(bus *eventspkg.AsyncEventBus) error {
	c := e.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	if bus == nil {
		c.events = nil
	} else {
		c.events = bus
	}
	return nil
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

func (c *Core) publishCascadeDeleteEvents(nodeID types.NodeID, relIDs []types.RelID, t types.Instant) {
	if c.events == nil {
		return
	}
	for _, relID := range relIDs {
		c.publishEvent(eventspkg.EventRelDelete, types.EntityID(relID), t, eventspkg.PriorityCritical)
	}
	c.publishEvent(eventspkg.EventNodeDelete, types.EntityID(nodeID), t, eventspkg.PriorityCritical)
}

func cascadeDeleteEvents(nodeID types.NodeID, relIDs []types.RelID, t types.Instant) []eventspkg.Event {
	events := make([]eventspkg.Event, 0, len(relIDs)+1)
	for _, relID := range relIDs {
		events = append(events, eventspkg.Event{Type: eventspkg.EventRelDelete, EntityID: types.EntityID(relID), Timestamp: t, Priority: eventspkg.PriorityCritical})
	}
	events = append(events, eventspkg.Event{Type: eventspkg.EventNodeDelete, EntityID: types.EntityID(nodeID), Timestamp: t, Priority: eventspkg.PriorityCritical})
	return events
}

// dispatchEvent delivers events to the given publisher. No-op if ep is nil.
// Use this for event dispatch after releasing c.mu — the caller captures c.events
// while the lock is held, then calls dispatchEvent with the captured reference.
func dispatchEvent(ep eventspkg.Publisher, events ...eventspkg.Event) {
	if ep == nil || len(events) == 0 {
		return
	}
	if len(events) == 1 {
		ep.Publish(events[0])
		return
	}
	ep.PublishBatch(events...)
}
