// Package events mirrors the v4 import path pkg/graph/events: the event bus. Every method keeps its
// v4 signature and reports ErrNotSupportedInV5 (declined or not in the first
// batch of WP-11); the types are placeholders so consumers compile.
package events

import (
	"fmt"
	"github.com/data-insights-ai/rho-tkg/v5compat/pkg/graph/store"
)

// ErrNotSupportedInV5 is the class of every declined door.
var ErrNotSupportedInV5 = store.ErrNotSupportedInV5

func declined(door string) error { return fmt.Errorf("%w: "+door, ErrNotSupportedInV5) }

// API is the sub-API handle returned by the graph accessor.
type API struct{}

// EventBus is a placeholder for the v4 type of the same name.
type EventBus struct{}

// AsyncEventBus is a placeholder for the v4 type of the same name.
type AsyncEventBus struct{}

// SetSync is declined.
func (a *API) SetSync(bus *EventBus) error {
	return declined("events.SetSync")
}

// SetAsync is declined.
func (a *API) SetAsync(bus *AsyncEventBus) error {
	return declined("events.SetAsync")
}

// GetSync is declined.
func (a *API) GetSync() (r0 *EventBus) {
	return
}

// GetAsync is declined.
func (a *API) GetAsync() (r0 *AsyncEventBus) {
	return
}
