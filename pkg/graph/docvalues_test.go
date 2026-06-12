package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// eachBackend runs fn against both the memory and badger-in-memory backends, so
// the DocValues capability (column build + the per-backend node-mutation epoch
// wiring) is exercised on each store.
func eachBackend(t *testing.T, fn func(t *testing.T, g *graphpkg.Graph)) {
	t.Helper()
	for _, bc := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{SnowflakeNodeID: 7}},
		{"badger", graphpkg.Config{SnowflakeNodeID: 7, BadgerInMemory: true}},
	} {
		t.Run(bc.name, func(t *testing.T) {
			g, err := graphpkg.New(bc.cfg)
			if err != nil {
				t.Fatalf("graph.New(%s): %v", bc.name, err)
			}
			defer g.Close()
			fn(t, g)
		})
	}
}

// drainDocValues collects ForEachDocValues into a per-node value map.
func drainDocValues(t *testing.T, g *graphpkg.Graph, label string, keys []string) (map[types.NodeID][]any, uint64, bool) {
	t.Helper()
	out := map[types.NodeID][]any{}
	gen, ok, err := g.Nodes().ForEachDocValues(label, keys, func(id types.NodeID, vals []any, present []bool) bool {
		row := make([]any, len(vals))
		for i := range vals {
			if present[i] {
				row[i] = vals[i]
			}
		}
		out[id] = row
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	return out, gen, ok
}

// TestDocValues_FullMembershipAndTypes pins that the column covers every label
// member (including one lacking the property → nil) and preserves the stored Go
// type — the store-level contract the cypher sink relies on, on both backends.
func TestDocValues_FullMembershipAndTypes(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"city": "berlin", "age": int64(30)})
		b, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"city": "munich", "age": int64(40)})
		c, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"city": "berlin"}) // no age

		rows, _, ok := drainDocValues(t, g, "P", []string{"city", "age"})
		if !ok {
			t.Fatal("column path declined; want usable")
		}
		if len(rows) != 3 {
			t.Fatalf("rows = %d, want 3 (full membership)", len(rows))
		}
		if rows[a.ID()][0] != "berlin" || rows[a.ID()][1].(int64) != 30 {
			t.Fatalf("node a = %v, want [berlin 30]", rows[a.ID()])
		}
		if rows[b.ID()][1].(int64) != 40 {
			t.Fatalf("node b age = %v, want int64 40", rows[b.ID()][1])
		}
		if rows[c.ID()][1] != nil {
			t.Fatalf("node c age = %v, want nil (absent property still a row)", rows[c.ID()][1])
		}
	})
}

// TestDocValues_StaleOnPropertyEdit is the critique-H2 regression: a property edit
// (SET → Update → ReplaceNodeWithHistory, which never touches the label index)
// MUST advance the node-mutation epoch so the cached column is rebuilt with the new
// value. A counter hooked only on label-index changes would serve the stale value.
func TestDocValues_StaleOnPropertyEdit(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		n, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(30)})

		rows, gen0, _ := drainDocValues(t, g, "P", []string{"age"})
		if rows[n.ID()][0].(int64) != 30 {
			t.Fatalf("initial age = %v, want 30", rows[n.ID()][0])
		}
		if err := g.Nodes().SetProperty(ctx, n.ID(), "age", int64(99)); err != nil {
			t.Fatalf("SetProperty: %v", err)
		}
		if g.Nodes().NodeMutationEpoch() == gen0 {
			t.Fatal("epoch did NOT advance after a property edit (critique H2): the column would serve a stale value")
		}
		rows2, _, _ := drainDocValues(t, g, "P", []string{"age"})
		if rows2[n.ID()][0].(int64) != 99 {
			t.Fatalf("after edit, column age = %v, want 99 (rebuilt fresh)", rows2[n.ID()][0])
		}
	})
}

// TestDocValues_ExpiredNodeStillCounted is the critique-C1 regression: a node whose
// valid_to is set to the past is logically expired but NOT deleted, so it stays in
// the (unfiltered) label set — and a non-temporal aggregation counts it. The column
// membership must match: it must still yield that node's row.
func TestDocValues_ExpiredNodeStillCounted(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		live, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(1)})
		exp, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(2)})
		// Close the valid interval in the past — expired but not deleted.
		if _, err := g.Nodes().Update(ctx, exp.ID(), map[string]any{"tkg_valid_to": int64(1000)}); err != nil {
			t.Fatalf("close valid_to: %v", err)
		}

		// Oracle: the unfiltered label scan still includes the expired node.
		all, err := g.Nodes().ByLabel("P", graphpkg.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel: %v", err)
		}
		rows, _, ok := drainDocValues(t, g, "P", []string{"age"})
		if !ok {
			t.Fatal("column declined")
		}
		if len(rows) != len(all) {
			t.Fatalf("column rows = %d, unfiltered scan = %d — membership must match (C1)", len(rows), len(all))
		}
		if _, present := rows[live.ID()]; !present {
			t.Fatal("live node missing from column")
		}
		if _, present := rows[exp.ID()]; !present {
			t.Fatal("expired-but-not-deleted node missing from column (C1: column wrongly filtered valid-time)")
		}
	})
}

// TestDocValues_EpochBumpsOnEveryMutationDoor is the completeness guard: every door
// the cypher engine drives a write through MUST advance the epoch, on both
// backends. A new write path added without hooking the epoch fails a sub-case (this
// is how the missed ReplaceNodeWithHistory and badger delete doors were caught).
func TestDocValues_EpochBumpsOnEveryMutationDoor(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		n, _ := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(1)})
		doors := []struct {
			name string
			do   func() error
		}{
			{"Add", func() error { _, e := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(2)}); return e }},
			{"SetProperty(Update)", func() error { return g.Nodes().SetProperty(ctx, n.ID(), "age", int64(3)) }},
			{"AddLabel", func() error { return g.Nodes().AddLabel(ctx, n.ID(), "Extra") }},
			{"RemoveLabel", func() error { return g.Nodes().RemoveLabel(ctx, n.ID(), "Extra") }},
			{"Delete", func() error { return g.Nodes().Delete(ctx, n.ID()) }},
		}
		for _, d := range doors {
			before := g.Nodes().NodeMutationEpoch()
			if err := d.do(); err != nil {
				t.Fatalf("%s: %v", d.name, err)
			}
			if g.Nodes().NodeMutationEpoch() == before {
				t.Fatalf("mutation door %q did not advance the node-mutation epoch — a column would serve a stale aggregate after it", d.name)
			}
		}
	})
}
