package index_test

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/index" // godoc anchor: ExampleAPI_<method> resolves against index.API
)

// ExampleAPI_CreateProperty demonstrates installing a property index that
// accelerates lookups via NodesByLabelAndProperty.
func ExampleAPI_CreateProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if err := g.Index().CreateProperty("Person", "email"); err != nil {
		panic(err)
	}
	has, _ := g.Index().HasProperty("Person", "email")
	other, _ := g.Index().HasProperty("Person", "name")
	fmt.Println(has, other)
	// Output: true false
}

// ExampleAPI_CreateTemporal demonstrates installing a temporal index for a
// label, accelerating ValidAt / ValidStart-ValidEnd queries.
func ExampleAPI_CreateTemporal() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if err := g.Index().CreateTemporal("Event"); err != nil {
		panic(err)
	}
	has, _ := g.Index().HasTemporal("Event")
	fmt.Println(has)
	// Output: true
}

// ExampleAPI_Providers demonstrates listing registered IndexProvider names.
func ExampleAPI_Providers() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	// A new graph has no providers registered.
	names := g.Index().Providers()
	fmt.Println(len(names))
	// Output: 0
}
