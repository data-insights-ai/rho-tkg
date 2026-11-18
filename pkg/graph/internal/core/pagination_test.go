package core

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ─── memory.Store pagination ─────────────────────────────────────────────────

// seedPaginationStore puts 10 nodes (IDs 1000..1009, label 10, name=Alice)
// and 5 relationships (IDs 100..104, type 5) between the first two nodes,
// optionally indexing name.
func seedPaginationStore(t *testing.T, indexed bool) *memory.Store {
	t.Helper()
	ms := memory.New()
	for i := range 10 {
		n := types.NewNode(types.NodeID(snowflake.ID(1000+i)), 10, nil)
		ps, err := types.NewPropertySlice(map[string]any{"name": "Alice"})
		if err != nil {
			t.Fatal(err)
		}
		n.SetProperties(ps)
		if err := ms.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	for i := range 5 {
		r := types.NewRelationship(types.RelID(snowflake.ID(100+i)), 5, types.NodeID(snowflake.ID(1000)), types.NodeID(snowflake.ID(1001)))
		if err := ms.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
	}
	if indexed {
		if err := ms.CreatePropertyIndex(10, "name"); err != nil {
			t.Fatalf("CreatePropertyIndex: %v", err)
		}
	}
	return ms
}

func idRange(from, n int64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = from + int64(i)
	}
	return out
}

func nodeIDList(ns []*types.Node) []int64 {
	out := make([]int64, len(ns))
	for i, n := range ns {
		out[i] = int64(n.ID())
	}
	return out
}

func relIDList(rs []*types.Relationship) []int64 {
	out := make([]int64, len(rs))
	for i, r := range rs {
		out[i] = int64(r.ID())
	}
	return out
}

// checkPagination asserts that zero opts return want, and that a Limit-3
// walk with After returns want in ID order: each page exactly the next three,
// no duplicates, no gaps, and an empty page after the last.
func checkPagination(t *testing.T, call func(storepkg.QueryOpts) ([]int64, error), want []int64) {
	t.Helper()
	all, err := call(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("zero opts: %v", err)
	}
	if !slices.Equal(all, want) {
		t.Fatalf("zero opts = %v, want %v", all, want)
	}
	const limit = 3
	var after int64
	for start := 0; ; start += limit {
		page, err := call(storepkg.QueryOpts{Limit: limit, After: types.EntityID(after)})
		if err != nil {
			t.Fatalf("page at %d: %v", start, err)
		}
		wantPage := want[min(start, len(want)):min(start+limit, len(want))]
		if !slices.Equal(page, wantPage) {
			t.Fatalf("page at %d (After %d) = %v, want %v", start, after, page, wantPage)
		}
		if len(page) == 0 {
			return
		}
		after = page[len(page)-1]
	}
}

// TestMemoryStorePagination: every memory.Store list door honours Limit and
// After with exact IDs in ID order (the property door on its index and on
// its scan fallback).
func TestMemoryStorePagination(t *testing.T) {
	t.Parallel()
	plain := seedPaginationStore(t, false)
	indexed := seedPaginationStore(t, true)
	nodes, rels := idRange(1000, 10), idRange(100, 5)
	for _, tc := range []struct {
		name string
		call func(storepkg.QueryOpts) ([]int64, error)
		want []int64
	}{
		{"NodesByLabel", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := plain.NodesByLabel(10, o)
			return nodeIDList(ns), err
		}, nodes},
		{"AllNodes", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := plain.AllNodes(o)
			return nodeIDList(ns), err
		}, nodes},
		{"RelationshipsByType", func(o storepkg.QueryOpts) ([]int64, error) {
			rs, err := plain.RelationshipsByType(5, o)
			return relIDList(rs), err
		}, rels},
		{"AllRelationships", func(o storepkg.QueryOpts) ([]int64, error) {
			rs, err := plain.AllRelationships(o)
			return relIDList(rs), err
		}, rels},
		{"NodesByLabelAndProperty fallback", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := plain.NodesByLabelAndProperty(10, "name", "Alice", o)
			return nodeIDList(ns), err
		}, nodes},
		{"NodesByLabelAndProperty indexed", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := indexed.NodesByLabelAndProperty(10, "name", "Alice", o)
			return nodeIDList(ns), err
		}, nodes},
	} {
		t.Run(tc.name, func(t *testing.T) { checkPagination(t, tc.call, tc.want) })
	}
}

// ─── Graph-layer pagination ──────────────────────────────────────────────────

// Graph.Nodes.ByLabel and Rels.ByType with Limit and After are covered, with
// exact IDs on every backend, by TestStoreContract_Pagination
// (store_contract_test.go). TestGraphPagination covers the doors it does not:
// Nodes.All, Rels.All and Nodes.ByLabelAndProperty.
func TestGraphPagination(t *testing.T) {
	t.Parallel()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	ctx := context.Background()

	var people, nodes, rels []int64
	for i := range 5 {
		n, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "Alice", "i": i})
		if err != nil {
			t.Fatal(err)
		}
		people = append(people, int64(n.ID()))
		other, err := g.Nodes.Add(ctx, []string{fmt.Sprintf("Type%d", i)}, map[string]any{"name": "Bob"})
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, int64(n.ID()), int64(other.ID()))
		r, err := g.Rels.Add(ctx, "LINK", n, other, nil)
		if err != nil {
			t.Fatal(err)
		}
		rels = append(rels, int64(r.ID()))
	}
	slices.Sort(people)
	slices.Sort(nodes)
	slices.Sort(rels)

	for _, tc := range []struct {
		name string
		call func(storepkg.QueryOpts) ([]int64, error)
		want []int64
	}{
		{"Nodes.All", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := g.Nodes.All(o)
			return nodeIDList(ns), err
		}, nodes},
		{"Rels.All", func(o storepkg.QueryOpts) ([]int64, error) {
			rs, err := g.Rels.All(o)
			return relIDList(rs), err
		}, rels},
		{"Nodes.ByLabelAndProperty", func(o storepkg.QueryOpts) ([]int64, error) {
			ns, err := g.Nodes.ByLabelAndProperty("Person", "name", "Alice", o)
			return nodeIDList(ns), err
		}, people},
	} {
		t.Run(tc.name, func(t *testing.T) { checkPagination(t, tc.call, tc.want) })
	}
}
