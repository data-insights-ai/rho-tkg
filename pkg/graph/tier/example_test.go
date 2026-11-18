package tier_test

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	_ "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/tier" // godoc anchor: ExampleAPI_<method> resolves against tier.API
)

// ExampleAPI_ListShards lists shard metadata. On a store that is not tiered
// (the in-memory default here) every tier method refuses with
// graph.ErrNotTieredStore and returns no shards.
func ExampleAPI_ListShards() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	shards, err := g.Tier().ListShards()
	fmt.Println(len(shards), errors.Is(err, graph.ErrNotTieredStore))
	// Output: 0 true
}
