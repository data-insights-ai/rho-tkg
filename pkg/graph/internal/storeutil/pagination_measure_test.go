//go:build measure

// Measurement level (René 4 Nov, `make measure`): testing.AllocsPerRun reads
// the process-wide malloc count, and this package runs parallel tests, so the
// bound runs by hand, not in a gate.

package storeutil

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestSortByIDSingletonsDoNotAllocate(t *testing.T) {
	node := types.NewNode(types.NodeID(10), 1, nil)
	rel := types.NewRelationship(types.RelID(10), 1, 1, 2)
	nodes := []*types.Node{node}
	rels := []*types.Relationship{rel}
	nodeIDs := []types.NodeID{10}
	relIDs := []types.RelID{10}
	rawIDs := []snowflake.ID{10}

	nodeAllocs := testing.AllocsPerRun(1000, func() {
		SortNodesByID(nodes)
	})
	if nodeAllocs != 0 {
		t.Fatalf("SortNodesByID singleton allocations = %v, want 0", nodeAllocs)
	}

	relAllocs := testing.AllocsPerRun(1000, func() {
		SortRelsByID(rels)
	})
	if relAllocs != 0 {
		t.Fatalf("SortRelsByID singleton allocations = %v, want 0", relAllocs)
	}

	nodeIDAllocs := testing.AllocsPerRun(1000, func() {
		SortNodeIDs(nodeIDs)
	})
	if nodeIDAllocs != 0 {
		t.Fatalf("SortNodeIDs singleton allocations = %v, want 0", nodeIDAllocs)
	}

	relIDAllocs := testing.AllocsPerRun(1000, func() {
		SortRelIDs(relIDs)
	})
	if relIDAllocs != 0 {
		t.Fatalf("SortRelIDs singleton allocations = %v, want 0", relIDAllocs)
	}

	rawIDAllocs := testing.AllocsPerRun(1000, func() {
		SortSnowflakeIDs(rawIDs)
	})
	if rawIDAllocs != 0 {
		t.Fatalf("SortSnowflakeIDs singleton allocations = %v, want 0", rawIDAllocs)
	}
}
