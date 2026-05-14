package tier_test

import (
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	_ "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/tier" // godoc anchor: ExampleAPI_<method> resolves against tier.API
)

// ExampleAPI_ListShards demonstrates inspecting shard metadata. Returns
// ErrNotTieredStore on non-tiered backends; the example handles that gracefully.
func ExampleAPI_ListShards() {
	g, err := graph.New(graph.Config{})
	if err != nil {
		panic(err)
	}
	defer g.Close()

	shards, _ := g.Tier.ListShards() // not a tiered store in this example — returns ErrNotTieredStore
	_ = shards
}
