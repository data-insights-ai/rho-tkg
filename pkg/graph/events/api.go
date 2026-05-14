// API accessors for event-bus management. Lives in the same package as the
// underlying EventBus / AsyncEventBus types — collapsed in v3.4.0
// post-cleanup from the previous pkg/graph/eventsapi sibling.
package events

import "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/grapherr"

// Ops is the subset of *core.EventOps the events sub-API forwards to.
type Ops interface {
	SetSync(bus *EventBus) error
	SetAsync(bus *AsyncEventBus) error
	GetSync() *EventBus
	GetAsync() *AsyncEventBus
}

// API is the events sub-API accessor.
type API struct {
	ops Ops
	ok  bool
}

// New constructs an events sub-API.
func New(ops Ops) *API { return &API{ops: ops, ok: !grapherr.IsNil(ops)} }

func (a *API) ready() (Ops, error) {
	if a == nil || !a.ok {
		return nil, grapherr.ErrNilGraph
	}
	return a.ops, nil
}

// SetSync installs a synchronous EventBus on the graph.
func (a *API) SetSync(bus *EventBus) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetSync(bus)
}

// SetAsync installs an asynchronous EventBus on the graph.
func (a *API) SetAsync(bus *AsyncEventBus) error {
	ops, err := a.ready()
	if err != nil {
		return err
	}
	return ops.SetAsync(bus)
}

// GetSync returns the currently installed synchronous EventBus, if any.
func (a *API) GetSync() *EventBus {
	if a == nil || !a.ok {
		return nil
	}
	return a.ops.GetSync()
}

// GetAsync returns the currently installed asynchronous EventBus, if any.
func (a *API) GetAsync() *AsyncEventBus {
	if a == nil || !a.ok {
		return nil
	}
	return a.ops.GetAsync()
}
