package events_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/events"
)

// ExampleAPI_SetSync demonstrates installing a synchronous event bus and
// subscribing a handler that observes every node creation.
func ExampleAPI_SetSync() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	bus := events.NewEventBus()
	bus.Subscribe(func(e events.Event) {
		// Custom event handler.
		_ = e.Type
	})
	_ = g.Events.SetSync(bus)
}

// ExampleAPI_GetSync demonstrates retrieving the currently installed sync
// event bus.
func ExampleAPI_GetSync() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_ = g.Events.SetSync(events.NewEventBus())
	bus := g.Events.GetSync()
	_ = bus
}
