package tiered

import (
	"reflect"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredMergeNodeSlicesPreservesSortedOrder(t *testing.T) {
	got := mergeNodeSlices([][]*types.Node{
		{tieredMergeNode(1), tieredMergeNode(4)},
		nil,
		{tieredMergeNode(2), tieredMergeNode(6)},
		{tieredMergeNode(3), tieredMergeNode(5)},
	})
	if ids := tieredMergeNodeIDs(got); !reflect.DeepEqual(ids, []snowflake.ID{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("mergeNodeSlices IDs = %v, want sorted 1..6", ids)
	}
}

func TestTieredMergeRelSlicesPreservesSortedOrder(t *testing.T) {
	got := mergeRelSlices([][]*types.Relationship{
		{tieredMergeRel(2), tieredMergeRel(5)},
		{tieredMergeRel(1), tieredMergeRel(4)},
		{tieredMergeRel(3), tieredMergeRel(6)},
	})
	if ids := tieredMergeRelIDs(got); !reflect.DeepEqual(ids, []snowflake.ID{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("mergeRelSlices IDs = %v, want sorted 1..6", ids)
	}
}

func TestTieredMergeNodeIDSlicesPreservesSortedOrderAndFastPaths(t *testing.T) {
	if got := mergeNodeIDSlices(nil); got != nil {
		t.Fatalf("mergeNodeIDSlices(nil) = %v, want nil", got)
	}
	one := []types.NodeID{1, 3}
	if got := mergeNodeIDSlices([][]types.NodeID{nil, one}); !reflect.DeepEqual(got, one) {
		t.Fatalf("mergeNodeIDSlices one non-empty = %v, want %v", got, one)
	}
	two := mergeNodeIDSlices([][]types.NodeID{{1, 4}, {2, 3}})
	if !reflect.DeepEqual(two, []types.NodeID{1, 2, 3, 4}) {
		t.Fatalf("mergeNodeIDSlices two-way = %v, want sorted 1..4", two)
	}
	many := mergeNodeIDSlices([][]types.NodeID{{1, 7}, {2, 6}, {3, 5}, {4}})
	if !reflect.DeepEqual(many, []types.NodeID{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("mergeNodeIDSlices k-way = %v, want sorted 1..7", many)
	}
	duplicates := mergeNodeIDSlices([][]types.NodeID{{1, 4}, {1, 3}, {2, 4}})
	if !reflect.DeepEqual(duplicates, []types.NodeID{1, 1, 2, 3, 4, 4}) {
		t.Fatalf("mergeNodeIDSlices duplicate IDs = %v, want stable sorted duplicates", duplicates)
	}
}

func TestTieredMergeRelIDSlicesPreservesSortedOrder(t *testing.T) {
	got := mergeRelIDSlices([][]types.RelID{{1, 4}, {1, 3}, {2, 4}})
	if !reflect.DeepEqual(got, []types.RelID{1, 1, 2, 3, 4, 4}) {
		t.Fatalf("mergeRelIDSlices = %v, want stable sorted duplicates", got)
	}
}

func TestTieredPaginationHelpersApplyCursorAndLimit(t *testing.T) {
	rels := []*types.Relationship{
		tieredMergeRel(10),
		tieredMergeRel(20),
		tieredMergeRel(30),
	}
	gotRels := applyRelPagination(rels, QueryOpts{
		After: types.EntityID(snowflake.ID(10)),
		Limit: 1,
	})
	if ids := tieredMergeRelIDs(gotRels); !reflect.DeepEqual(ids, []snowflake.ID{20}) {
		t.Fatalf("applyRelPagination IDs = %v, want [20]", ids)
	}

	nodeIDs := []types.NodeID{10, 20, 30}
	gotNodeIDs := applyNodeIDPagination(nodeIDs, QueryOpts{
		After: types.EntityID(snowflake.ID(20)),
		Limit: 5,
	})
	if !reflect.DeepEqual(gotNodeIDs, []types.NodeID{30}) {
		t.Fatalf("applyNodeIDPagination = %v, want [30]", gotNodeIDs)
	}
	if got := applyNodeIDPagination(nodeIDs, QueryOpts{After: types.EntityID(snowflake.ID(30))}); got != nil {
		t.Fatalf("applyNodeIDPagination after end = %v, want nil", got)
	}
	relIDs := []types.RelID{10, 20, 30}
	if got := applyRelIDPagination(relIDs, QueryOpts{After: types.EntityID(snowflake.ID(10)), Limit: 1}); !reflect.DeepEqual(got, []types.RelID{20}) {
		t.Fatalf("applyRelIDPagination = %v, want [20]", got)
	}
}

func tieredMergeNode(id snowflake.ID) *types.Node {
	return types.NewNode(types.NodeID(id), 1, nil)
}

func tieredMergeRel(id snowflake.ID) *types.Relationship {
	return types.NewRelationship(types.RelID(id), 1, types.NodeID(1), types.NodeID(2))
}

func tieredMergeNodeIDs(nodes []*types.Node) []snowflake.ID {
	ids := make([]snowflake.ID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID().SnowflakeID()
	}
	return ids
}

func tieredMergeRelIDs(rels []*types.Relationship) []snowflake.ID {
	ids := make([]snowflake.ID, len(rels))
	for i, r := range rels {
		ids[i] = r.ID().SnowflakeID()
	}
	return ids
}
