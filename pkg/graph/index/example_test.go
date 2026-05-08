package index_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index" // godoc anchor: ExampleAPI_<method> resolves against index.API
)

// ExampleAPI_CreateProperty demonstrates installing a property index that
// accelerates lookups via NodesByLabelAndProperty.
func ExampleAPI_CreateProperty() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if err := g.Index.CreateProperty("Person", "email"); err != nil {
		panic(err)
	}
	defer func() { _ = g.Index.DropProperty("Person", "email") }()
}

// ExampleAPI_CreateTemporal demonstrates installing a temporal index for a
// label, accelerating ValidAt / ValidStart-ValidEnd queries.
func ExampleAPI_CreateTemporal() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	if err := g.Index.CreateTemporal("Event"); err != nil {
		panic(err)
	}
	defer func() { _ = g.Index.DropTemporal("Event") }()
}

// ExampleAPI_Providers demonstrates listing registered IndexProvider names.
func ExampleAPI_Providers() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	names := g.Index.Providers()
	_ = names
}
