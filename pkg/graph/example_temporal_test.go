package graph_test

import (
	"context"
	"fmt"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

func Example_temporalQueries() {
	g, _ := graphpkg.New(graphpkg.Config{})
	defer g.Close()
	user, _ := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"status": "active"})
	// NowTx reserves a transaction instant strictly between the two writes.
	t0, _ := g.Temporal().NowTx()
	_, _ = g.Nodes().Update(context.Background(), user.ID(), map[string]any{"status": "inactive"})
	users, _ := g.Temporal().NodesAsOf(t0)
	if len(users) > 0 {
		if s, ok := users[0].GetProperty("status"); ok {
			fmt.Println("User status at t0:", s)
		}
	}
	current, _ := g.Nodes().Get(context.Background(), user.ID())
	if current != nil {
		if s, ok := current.GetProperty("status"); ok {
			fmt.Println("User status now:", s)
		}
	}
	// Output:
	// User status at t0: active
	// User status now: inactive
}
