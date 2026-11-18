package events_test

import (
	"context"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/events"
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
		// A sync bus runs the handler before the mutation returns.
		fmt.Println("node created:", e.Type == events.EventNodeCreate)
	})
	if err := g.Events().SetSync(bus); err != nil {
		panic(err)
	}
	if _, err := g.Nodes().Add(context.Background(), []string{"Person"}, nil); err != nil {
		panic(err)
	}
	// Output: node created: true
}

// ExampleAPI_GetSync demonstrates retrieving the currently installed sync
// event bus.
func ExampleAPI_GetSync() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	installed := events.NewEventBus()
	if err := g.Events().SetSync(installed); err != nil {
		panic(err)
	}
	fmt.Println(g.Events().GetSync() == installed)
	// Output: true
}
