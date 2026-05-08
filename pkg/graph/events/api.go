// API accessors for event-bus management. Lives in the same package as the
// underlying EventBus / AsyncEventBus types — collapsed in v3.4.0
// post-cleanup from the previous pkg/graph/eventsapi sibling.
package events

// Ops is the subset of *core.EventOps the events sub-API forwards to.
type Ops interface {
	SetSync(bus *EventBus)
	SetAsync(bus *AsyncEventBus)
	GetSync() *EventBus
}

// API is the events sub-API accessor.
type API struct{ ops Ops }

// New constructs an events sub-API.
func New(ops Ops) *API { return &API{ops: ops} }

// SetSync installs a synchronous EventBus on the graph.
func (a *API) SetSync(bus *EventBus) { a.ops.SetSync(bus) }

// SetAsync installs an asynchronous EventBus on the graph.
func (a *API) SetAsync(bus *AsyncEventBus) { a.ops.SetAsync(bus) }

// GetSync returns the currently installed synchronous EventBus, if any.
func (a *API) GetSync() *EventBus { return a.ops.GetSync() }
