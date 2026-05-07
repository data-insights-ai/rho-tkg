// Package eventsapi is a sub-API accessor exposing event-bus management
// methods. The underlying EventBus / AsyncEventBus types live in
// pkg/graph/events.
package eventsapi

import (
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
)

// Core is the subset of *graph.Graph methods the events sub-API forwards to.
type Core interface {
	SetEventBus(bus *eventspkg.EventBus)
	SetAsyncEventBus(bus *eventspkg.AsyncEventBus)
	GetEventBus() *eventspkg.EventBus
}

// API is the events sub-API accessor.
type API struct{ c Core }

// New constructs an events sub-API.
func New(c Core) *API { return &API{c: c} }

// SetSync installs a synchronous EventBus on the graph. Forwards to Graph.SetEventBus.
func (a *API) SetSync(bus *eventspkg.EventBus) { a.c.SetEventBus(bus) }

// SetAsync installs an asynchronous EventBus on the graph. Forwards to Graph.SetAsyncEventBus.
func (a *API) SetAsync(bus *eventspkg.AsyncEventBus) { a.c.SetAsyncEventBus(bus) }

// GetSync returns the currently installed synchronous EventBus, if any. Forwards to Graph.GetEventBus.
func (a *API) GetSync() *eventspkg.EventBus { return a.c.GetEventBus() }
