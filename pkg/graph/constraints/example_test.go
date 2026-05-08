package constraints_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/constraints" // godoc anchor: ExampleAPI_<method> resolves against constraints.API
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
)

// ExampleAPI_Add demonstrates registering a temporal constraint that
// enforces "relationships must be valid within both endpoints' validity".
func ExampleAPI_Add() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	g.Constraints.Add(temporal.TemporalConstraint{
		Kind: temporal.ConstraintRelWithinEndpoints,
	})
}

// ExampleAPI_Set demonstrates replacing the entire constraint set in one
// call (useful for resetting or for atomic configuration updates).
func ExampleAPI_Set() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	cs := temporal.NewConstraintSet(temporal.TemporalConstraint{
		Kind: temporal.ConstraintRelWithinEndpoints,
	})
	g.Constraints.Set(cs)
}

// ExampleAPI_Get demonstrates inspecting the configured constraints.
func ExampleAPI_Get() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	cs := g.Constraints.Get()
	_ = cs.Len()
}
