package admin_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin" // godoc anchor: ExampleAPI_<method> resolves against admin.API
)

// ExampleAPI_DecomposeNodeID demonstrates extracting the timestamp and
// generator-node identifier from a NodeID. Works on every store backend,
// not just tiered.Store.
func ExampleAPI_DecomposeNodeID() {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 7})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	n, _ := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	comp := g.Admin().DecomposeNodeID(n.ID())
	// NodeID is the generator slot: nodes mint on slot 2*SnowflakeNodeID,
	// relationships on 2*SnowflakeNodeID+1. CreatedAt is the minting time.
	fmt.Println(comp.NodeID, !comp.CreatedAt.IsZero())
	// Output: 14 true
}

// ExampleAPI_Reset demonstrates clearing all entities while preserving
// label and relationship-type registries. Reset is a whole-graph destructive
// wipe, so it must be explicitly opted into via Config.AllowReset — a graph
// opened without it fails closed with ErrResetDisabled.
func ExampleAPI_Reset() {
	g, err := graph.New(graph.Config{AllowReset: true})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	_, _ = g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err := g.Admin().Reset(); err != nil {
		panic(err)
	}
	n, _ := g.Nodes().Count()

	locked, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer locked.Close()
	fmt.Println(n, errors.Is(locked.Admin().Reset(), graph.ErrResetDisabled))
	// Output: 0 true
}
