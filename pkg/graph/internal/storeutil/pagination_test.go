package storeutil

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestPaginateIDs_EmptyInput(t *testing.T) {
	result := PaginateIDs(nil, 0, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_NoLimitNoCursor(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 0, 0)
	if len(result) != 5 {
		t.Fatalf("expected 5, got %d", len(result))
	}
	for i, id := range ids {
		if result[i] != id {
			t.Fatalf("index %d: expected %d, got %d", i, id, result[i])
		}
	}
}

func TestPaginateIDs_LimitOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 0, 3)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 10 || result[1] != 20 || result[2] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorOnly(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 20, 0)
	if len(result) != 3 {
		t.Fatalf("expected 3, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 || result[2] != 50 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorAndLimit(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30, 40, 50}
	result := PaginateIDs(ids, 20, 2)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 30 || result[1] != 40 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func TestPaginateIDs_CursorBeyondAll(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 100, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_CursorAtExactID(t *testing.T) {
	// Cursor at exactly the last ID should return nil.
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 30, 0)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestPaginateIDs_LimitLargerThanRemaining(t *testing.T) {
	ids := []snowflake.ID{10, 20, 30}
	result := PaginateIDs(ids, 10, 100)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0] != 20 || result[1] != 30 {
		t.Fatalf("unexpected result: %v", result)
	}
}

func nodeSlice(ids ...types.NodeID) []*types.Node {
	nodes := make([]*types.Node, len(ids))
	for i, id := range ids {
		nodes[i] = types.NewNode(id, 1, nil)
	}
	return nodes
}

func relSlice(ids ...types.RelID) []*types.Relationship {
	rels := make([]*types.Relationship, len(ids))
	for i, id := range ids {
		rels[i] = types.NewRelationship(id, 1, 1, 2)
	}
	return rels
}

func requireNodeIDs(t *testing.T, got []*types.Node, want ...types.NodeID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d; got %v", len(got), len(want), got)
	}
	for i, wantID := range want {
		if got[i].ID() != wantID {
			t.Fatalf("node[%d].ID = %d, want %d", i, got[i].ID(), wantID)
		}
	}
}

func requireRelIDs(t *testing.T, got []*types.Relationship, want ...types.RelID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d; got %v", len(got), len(want), got)
	}
	for i, wantID := range want {
		if got[i].ID() != wantID {
			t.Fatalf("rel[%d].ID = %d, want %d", i, got[i].ID(), wantID)
		}
	}
}

func TestPaginateNodes(t *testing.T) {
	nodes := nodeSlice(10, 20, 30, 40)
	tests := []struct {
		name  string
		input []*types.Node
		after types.EntityID
		limit int
		want  []types.NodeID
	}{
		{name: "empty", input: nil, after: 0, limit: 0, want: nil},
		{name: "all", input: nodes, after: 0, limit: 0, want: []types.NodeID{10, 20, 30, 40}},
		{name: "limit", input: nodes, after: 0, limit: 2, want: []types.NodeID{10, 20}},
		{name: "cursor exact", input: nodes, after: 20, limit: 0, want: []types.NodeID{30, 40}},
		{name: "cursor between IDs", input: nodes, after: 25, limit: 0, want: []types.NodeID{30, 40}},
		{name: "cursor and limit", input: nodes, after: 10, limit: 2, want: []types.NodeID{20, 30}},
		{name: "cursor at last", input: nodes, after: 40, limit: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PaginateNodes(tt.input, tt.after, tt.limit)
			requireNodeIDs(t, got, tt.want...)
		})
	}
}

func TestPaginateRels(t *testing.T) {
	rels := relSlice(10, 20, 30, 40)
	tests := []struct {
		name  string
		input []*types.Relationship
		after types.EntityID
		limit int
		want  []types.RelID
	}{
		{name: "empty", input: nil, after: 0, limit: 0, want: nil},
		{name: "all", input: rels, after: 0, limit: 0, want: []types.RelID{10, 20, 30, 40}},
		{name: "limit", input: rels, after: 0, limit: 2, want: []types.RelID{10, 20}},
		{name: "cursor exact", input: rels, after: 20, limit: 0, want: []types.RelID{30, 40}},
		{name: "cursor between IDs", input: rels, after: 25, limit: 0, want: []types.RelID{30, 40}},
		{name: "cursor and limit", input: rels, after: 10, limit: 2, want: []types.RelID{20, 30}},
		{name: "cursor at last", input: rels, after: 40, limit: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PaginateRels(tt.input, tt.after, tt.limit)
			requireRelIDs(t, got, tt.want...)
		})
	}
}

func TestPaginateNodesInOrder(t *testing.T) {
	nodes := nodeSlice(30, 10, 20)
	tests := []struct {
		name  string
		input []*types.Node
		after types.EntityID
		limit int
		want  []types.NodeID
	}{
		{name: "empty", input: nil, after: 0, limit: 0, want: nil},
		{name: "preserves input order", input: nodes, after: 0, limit: 0, want: []types.NodeID{30, 10, 20}},
		{name: "limit preserves input order", input: nodes, after: 0, limit: 2, want: []types.NodeID{30, 10}},
		{name: "cursor found", input: nodes, after: 30, limit: 0, want: []types.NodeID{10, 20}},
		{name: "cursor found with limit", input: nodes, after: 10, limit: 1, want: []types.NodeID{20}},
		{name: "cursor at last", input: nodes, after: 20, limit: 0, want: nil},
		{name: "cursor absent", input: nodes, after: 99, limit: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PaginateNodesInOrder(tt.input, tt.after, tt.limit)
			requireNodeIDs(t, got, tt.want...)
		})
	}
}

func TestToNodeIDs(t *testing.T) {
	if got := ToNodeIDs(nil); got != nil {
		t.Fatalf("ToNodeIDs(nil) = %v, want nil", got)
	}

	got := ToNodeIDs([]snowflake.ID{10, 20, 30})
	want := []types.NodeID{types.NodeID(10), types.NodeID(20), types.NodeID(30)}
	if len(got) != len(want) {
		t.Fatalf("ToNodeIDs len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ToNodeIDs[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestToRelIDs(t *testing.T) {
	if got := ToRelIDs(nil); got != nil {
		t.Fatalf("ToRelIDs(nil) = %v, want nil", got)
	}

	got := ToRelIDs([]snowflake.ID{10, 20, 30})
	want := []types.RelID{types.RelID(10), types.RelID(20), types.RelID(30)}
	if len(got) != len(want) {
		t.Fatalf("ToRelIDs len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ToRelIDs[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestPaginateNodeIDs(t *testing.T) {
	ids := []types.NodeID{10, 20, 30, 40}
	tests := []struct {
		name  string
		input []types.NodeID
		after types.EntityID
		limit int
		want  []types.NodeID
	}{
		{name: "empty", input: nil, after: 0, limit: 0, want: nil},
		{name: "all", input: ids, after: 0, limit: 0, want: []types.NodeID{10, 20, 30, 40}},
		{name: "limit", input: ids, after: 0, limit: 2, want: []types.NodeID{10, 20}},
		{name: "cursor exact", input: ids, after: 20, limit: 0, want: []types.NodeID{30, 40}},
		{name: "cursor between IDs", input: ids, after: 25, limit: 0, want: []types.NodeID{30, 40}},
		{name: "cursor and limit", input: ids, after: 10, limit: 2, want: []types.NodeID{20, 30}},
		{name: "cursor at last", input: ids, after: 40, limit: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PaginateNodeIDs(tt.input, tt.after, tt.limit)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d; got %v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got[%d] = %d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestPaginateRelIDs(t *testing.T) {
	ids := []types.RelID{10, 20, 30, 40}
	tests := []struct {
		name  string
		input []types.RelID
		after types.EntityID
		limit int
		want  []types.RelID
	}{
		{name: "empty", input: nil, after: 0, limit: 0, want: nil},
		{name: "all", input: ids, after: 0, limit: 0, want: []types.RelID{10, 20, 30, 40}},
		{name: "limit", input: ids, after: 0, limit: 2, want: []types.RelID{10, 20}},
		{name: "cursor exact", input: ids, after: 20, limit: 0, want: []types.RelID{30, 40}},
		{name: "cursor between IDs", input: ids, after: 25, limit: 0, want: []types.RelID{30, 40}},
		{name: "cursor and limit", input: ids, after: 10, limit: 2, want: []types.RelID{20, 30}},
		{name: "cursor at last", input: ids, after: 40, limit: 0, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PaginateRelIDs(tt.input, tt.after, tt.limit)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d; got %v", len(got), len(tt.want), got)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("got[%d] = %d, want %d", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestSortNodesByID(t *testing.T) {
	nodes := []*types.Node{
		types.NewNode(types.NodeID(30), 1, nil),
		types.NewNode(types.NodeID(10), 1, nil),
		types.NewNode(types.NodeID(20), 1, nil),
	}
	SortNodesByID(nodes)
	for i, want := range []types.NodeID{10, 20, 30} {
		if got := nodes[i].ID(); got != want {
			t.Fatalf("nodes[%d].ID = %d, want %d", i, got, want)
		}
	}
}

func TestSortRelsByID(t *testing.T) {
	rels := []*types.Relationship{
		types.NewRelationship(types.RelID(30), 1, 1, 2),
		types.NewRelationship(types.RelID(10), 1, 1, 2),
		types.NewRelationship(types.RelID(20), 1, 1, 2),
	}
	SortRelsByID(rels)
	for i, want := range []types.RelID{10, 20, 30} {
		if got := rels[i].ID(); got != want {
			t.Fatalf("rels[%d].ID = %d, want %d", i, got, want)
		}
	}
}

func TestSortIDHelpers(t *testing.T) {
	nodeIDs := []types.NodeID{30, 10, 20}
	SortNodeIDs(nodeIDs)
	for i, want := range []types.NodeID{10, 20, 30} {
		if got := nodeIDs[i]; got != want {
			t.Fatalf("nodeIDs[%d] = %d, want %d", i, got, want)
		}
	}

	relIDs := []types.RelID{30, 10, 20}
	SortRelIDs(relIDs)
	for i, want := range []types.RelID{10, 20, 30} {
		if got := relIDs[i]; got != want {
			t.Fatalf("relIDs[%d] = %d, want %d", i, got, want)
		}
	}

	rawIDs := []snowflake.ID{30, 10, 20}
	SortSnowflakeIDs(rawIDs)
	for i, want := range []snowflake.ID{10, 20, 30} {
		if got := rawIDs[i]; got != want {
			t.Fatalf("rawIDs[%d] = %d, want %d", i, got, want)
		}
	}
}

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
