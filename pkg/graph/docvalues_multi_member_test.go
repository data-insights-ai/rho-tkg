package graph_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// multiCounter is the telemetry the member-column path is observed by.
type multiCounter interface{ MultiColumnFromMemberCount() uint64 }

// renderDocRows renders the rows a DocValues door handed out, numbers
// normalized to int64 / float64 as the columns store them.
func renderDocRows(t *testing.T, call func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)) (string, bool) {
	t.Helper()
	var b strings.Builder
	_, ok, err := call(func(id types.NodeID, vals []any, present []bool) bool {
		fmt.Fprintf(&b, "%d:", id)
		for i, v := range vals {
			fmt.Fprintf(&b, " %v/%T/%v", present[i], v, v)
		}
		b.WriteString("\n")
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return b.String(), ok
}

// docOracle renders the rows of the nodes carrying every label, from the
// nodes themselves, in ID order.
func docOracle(t *testing.T, g *graphpkg.Graph, labels, keys []string) string {
	t.Helper()
	var rows []*types.Node
	if err := g.Nodes().ForEachByLabel(labels[0], storepkg.QueryOpts{}, func(n *types.Node) bool {
		for _, l := range labels[1:] {
			if !g.Nodes().HasLabel(n, l) {
				return true
			}
		}
		rows = append(rows, n)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(rows, func(a, b *types.Node) int { return int(a.ID() - b.ID()) })
	var b strings.Builder
	for _, n := range rows {
		fmt.Fprintf(&b, "%d:", n.ID())
		for _, k := range keys {
			v, ok := n.GetProperty(k)
			switch x := v.(type) {
			case int:
				v = int64(x)
			case float32:
				v = float64(x)
			}
			if !ok {
				v = nil
			}
			fmt.Fprintf(&b, " %v/%T/%v", ok, v, v)
		}
		b.WriteString("\n")
	}
	return b.String()
}

// TestDocValuesMultiFromMemberColumns (sigma-tkgd round 7 request 6): on
// memory and badger a label intersection's columns are taken from a fresh
// member label's columns when it holds every key, and hand out exactly the
// rows a read of the nodes gives — through a write to the other label, a
// label added and removed, an update and a delete. Where the member's column
// for a key is unbuildable (strings and numbers on the whole label, numbers
// only on the intersection) or no member column is fresh, the nodes are read
// and the answer is the same.
func TestDocValuesMultiFromMemberColumns(t *testing.T) {
	for _, backend := range []string{"memory", "badger"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			var st storepkg.Store
			if backend == "memory" {
				st = memory.New()
			} else {
				bs, err := badger.New(badger.Config{InMemory: true, CacheCapacity: 16})
				if err != nil {
					t.Fatal(err)
				}
				st = bs
			}
			g, err := graphpkg.New(graphpkg.Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			defer g.Close()
			counter := st.(multiCounter)
			var nodes []*types.Node
			for i := range 300 {
				labels := []string{"A"}
				if i%3 == 0 {
					labels = append(labels, "B")
				}
				props := map[string]any{"n": int64(i % 7), "s": fmt.Sprintf("s%d", i%5)}
				if i%4 == 0 {
					props["f"] = float64(i) / 2
				} else if i%4 == 1 {
					props["f"] = int64(i)
				}
				// mixed: a string on A-only nodes, a number on A:B nodes.
				if i%3 == 0 {
					props["mixed"] = int64(i)
				} else {
					props["mixed"] = "x"
				}
				n, err := g.Nodes().Add(ctx, labels, props)
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n)
			}
			for i := range 20 {
				if _, err := g.Nodes().Add(ctx, []string{"B"}, map[string]any{"n": int64(i)}); err != nil {
					t.Fatal(err)
				}
			}
			keys := []string{"n", "s", "f"}
			check := func(step string, wantMember bool, keys []string) {
				t.Helper()
				single := func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
					return g.Nodes().ForEachDocValues("A", keys, fn)
				}
				if wantMember { // else A's column is left stale
					if _, ok := renderDocRows(t, single); !ok {
						t.Fatalf("%s: the A column declined", step)
					}
				}
				before := counter.MultiColumnFromMemberCount()
				got, ok := renderDocRows(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
					return g.Nodes().ForEachDocValuesMulti([]string{"A", "B"}, keys, fn)
				})
				if !ok {
					t.Fatalf("%s: the A:B column declined", step)
				}
				if want := docOracle(t, g, []string{"A", "B"}, keys); got != want {
					t.Fatalf("%s: A:B rows differ from the nodes:\n got\n%s\n want\n%s", step, got, want)
				}
				if used := counter.MultiColumnFromMemberCount() > before; used != wantMember {
					t.Fatalf("%s: built from a member column = %v, want %v", step, used, wantMember)
				}
			}
			check("fresh", true, keys)

			if _, err := g.Nodes().Add(ctx, []string{"B"}, map[string]any{"n": int64(99)}); err != nil {
				t.Fatal(err)
			}
			check("after a B-only write", true, keys)
			if err := g.Nodes().AddLabel(ctx, nodes[1].ID(), "B"); err != nil {
				t.Fatal(err)
			}
			check("after a label add", true, keys)
			if err := g.Nodes().RemoveLabel(ctx, nodes[3].ID(), "B"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, nodes[6].ID(), map[string]any{"n": 2.5, "s": "new"}); err != nil {
				t.Fatal(err)
			}
			check("after an update, A's column stale", false, keys)
			if _, err := g.Nodes().Update(ctx, nodes[12].ID(), map[string]any{"n": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().Delete(ctx, nodes[9].ID()); err != nil {
				t.Fatal(err)
			}
			check("after a label removal, an update and a delete", true, keys)

			// The C column of "mixed" is unbuildable (strings and numbers); on C:D
			// it is numbers only, which only the nodes can tell.
			for i := range 30 {
				labels, v := []string{"C"}, any("x")
				if i%2 == 0 {
					labels, v = append(labels, "D"), int64(i)
				}
				if _, err := g.Nodes().Add(ctx, labels, map[string]any{"mixed": v}); err != nil {
					t.Fatal(err)
				}
			}
			if _, ok := renderDocRows(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
				return g.Nodes().ForEachDocValues("C", []string{"mixed"}, fn)
			}); ok {
				t.Fatal("the C column of mixed values did not decline")
			}
			before := counter.MultiColumnFromMemberCount()
			got, ok := renderDocRows(t, func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
				return g.Nodes().ForEachDocValuesMulti([]string{"C", "D"}, []string{"mixed"}, fn)
			})
			if want := docOracle(t, g, []string{"C", "D"}, []string{"mixed"}); !ok || got != want {
				t.Fatalf("mixed key: ok=%v\n got\n%s\n want\n%s", ok, got, want)
			}
			if counter.MultiColumnFromMemberCount() != before {
				t.Fatal("mixed key: built from the member's unbuildable column")
			}

		})
	}
}
