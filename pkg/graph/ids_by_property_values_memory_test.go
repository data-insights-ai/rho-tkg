package graph_test

import (
	"context"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RHOMEMIDS: on the memory store IDsByLabelAndPropertyValues answers from the
// property index (NodePropertyKeysIDsCapability) instead of loading and
// rechecking every candidate row, and returns the IDs ByLabelAndPropertyValues
// returns: two labels on one key (an Elements and a plain definition), three
// values, a deleted node, a node moved to another value, a node that left the
// label, equal numeric forms, a label with no index, After+Limit, and a time
// coordinate (rows, whose versions answer; parity only), and a label of 64
// rows of 64-element lists. The allocation bound that fails while each
// candidate's list is walked (one key string per element) is
// TestIDsByLabelAndPropertyValuesMemoryAllocs, at the measurement level.
func TestIDsByLabelAndPropertyValuesMemoryFromIndex(t *testing.T) {
	ctx := context.Background()
	ms := memory.New()
	if _, ok := any(ms).(storepkg.NodePropertyKeysIDsCapability); !ok {
		t.Error("memory.Store has no NodePropertyKeysIDsCapability: the door loads rows")
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: ms})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	for _, def := range []struct {
		label, key string
		opts       storepkg.PropertyIndexOptions
	}{{"Block", "bk", storepkg.PropertyIndexOptions{Elements: true}}, {"Other", "bk", storepkg.PropertyIndexOptions{}},
		{"Plain", "pk", storepkg.PropertyIndexOptions{}}, {"Long", "lk", storepkg.PropertyIndexOptions{Elements: true}}} {
		if err := g.Index().CreatePropertyWithOptions(def.label, def.key, def.opts); err != nil {
			t.Fatal(err)
		}
	}
	add := func(labels []string, props map[string]any) types.NodeID {
		t.Helper()
		n, err := g.Nodes().Add(ctx, labels, props)
		if err != nil {
			t.Fatal(err)
		}
		return n.ID()
	}
	b1 := add([]string{"Block"}, map[string]any{"bk": []int64{1, 2}})
	b2 := add([]string{"Block"}, map[string]any{"bk": []int64{2, 3}})
	b3 := add([]string{"Block"}, map[string]any{"bk": int64(3)})
	b4 := add([]string{"Block"}, map[string]any{"bk": []int64{2, 4}})
	b6 := add([]string{"Block"}, map[string]any{"bk": []int64{2}})
	o1 := add([]string{"Other"}, map[string]any{"bk": int64(2)})
	m := add([]string{"Block", "Other"}, map[string]any{"bk": int64(2)})
	add([]string{"Other"}, map[string]any{"bk": []int64{2}}) // a list under a plain definition
	p1 := add([]string{"Plain"}, map[string]any{"pk": int64(5)})
	p2 := add([]string{"Plain"}, map[string]any{"pk": 5.0})
	add([]string{"Plain"}, map[string]any{"pk": int64(6)})
	n1 := add([]string{"Bare"}, map[string]any{"nk": int64(1)})
	add([]string{"Bare"}, map[string]any{"nk": int64(2)})
	add([]string{"Bare"}, map[string]any{"nk": []int64{1}})
	if err := g.Nodes().Delete(ctx, b4); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(ctx, b6, map[string]any{"bk": []int64{9}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().RemoveLabel(ctx, m, "Block"); err != nil {
		t.Fatal(err)
	}
	now := types.Instant(time.Now().UnixMilli())
	three := []any{int64(2), int64(3), int64(9)}
	for _, c := range []struct {
		name     string
		label, k string
		values   []any
		opts     storepkg.QueryOpts
		want     []types.NodeID
	}{
		{"Elements, three values", "Block", "bk", three, storepkg.QueryOpts{}, []types.NodeID{b1, b2, b3, b6}},
		{"Elements, the moved node's old value", "Block", "bk", []any{int64(4)}, storepkg.QueryOpts{}, nil},
		{"plain beside Elements", "Other", "bk", three, storepkg.QueryOpts{}, []types.NodeID{o1, m}},
		{"equal forms", "Plain", "pk", []any{5.0}, storepkg.QueryOpts{}, []types.NodeID{p1, p2}},
		{"After and Limit", "Block", "bk", three, storepkg.QueryOpts{After: types.EntityID(b1), Limit: 2}, []types.NodeID{b2, b3}},
		{"Limit", "Block", "bk", three, storepkg.QueryOpts{Limit: 1}, []types.NodeID{b1}},
		{"no index", "Bare", "nk", []any{int64(1)}, storepkg.QueryOpts{}, []types.NodeID{n1}},
		{"no value", "Block", "bk", nil, storepkg.QueryOpts{}, nil},
		{"unknown label", "Nope", "bk", three, storepkg.QueryOpts{}, nil},
		{"time coordinate", "Block", "bk", three, storepkg.QueryOpts{ValidAt: now, ValidAtSet: true}, nil},
	} {
		rows, err := g.Nodes().ByLabelAndPropertyValues(c.label, c.k, c.values, c.opts)
		if err != nil {
			t.Fatalf("%s: rows: %v", c.name, err)
		}
		got, err := g.Nodes().IDsByLabelAndPropertyValues(c.label, c.k, c.values, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		parityOnly := c.opts.ValidAtSet
		if (!parityOnly && !slices.Equal(got, c.want)) || !slices.Equal(got, nodeIDsOf(rows)) {
			t.Errorf("%s: IDs %v, rows %v, want %v", c.name, got, nodeIDsOf(rows), c.want)
		}
	}

	// 64 rows of 64-element lists, 10 values a call, each row's last element.
	const width = 64
	for i := range width {
		l := make([]int64, width)
		for j := range l {
			l[j] = int64(1000 + i*width + j)
		}
		add([]string{"Long"}, map[string]any{"lk": l})
	}
	values := make([]any, 10)
	for i := range values {
		values[i] = int64(1000 + i*width + width - 1) // each row's last element
	}
	got, err := g.Nodes().IDsByLabelAndPropertyValues("Long", "lk", values, storepkg.QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(values) {
		t.Fatalf("Long: %d IDs, want %d", len(got), len(values))
	}
}
