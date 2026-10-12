package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestForEachDocValuesMultiServesAnEmptyIntersection (round 5 request 3,
// sigma-tkgd C4r): an intersection of two labels that both have members but
// share none is served (ok=true, no rows), not declined like an intersection
// the store cannot build — the caller cannot tell an empty intersection
// cheaply, and a decline sent it to check every label for a common member.
// Two-phase: a node joining both labels is then one row, and leaving one of
// them empties the intersection again. Every backend. A label the graph does
// not know still declines (ok=false), as before.
func TestForEachDocValuesMultiServesAnEmptyIntersection(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		la, lb := "A", "B"
		add := func(labels ...string) *types.Node {
			n, err := g.Nodes().Add(ctx, labels, map[string]any{"v": int64(len(labels))})
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		add(la)
		add(la)
		add(lb)
		read := func(step string, labels ...string) (int, bool) {
			t.Helper()
			rows := 0
			_, ok, err := g.Nodes().ForEachDocValuesMulti(labels, []string{"v"}, func(types.NodeID, []any, []bool) bool {
				rows++
				return true
			})
			if err != nil {
				t.Fatalf("%s: %v", step, err)
			}
			return rows, ok
		}
		if rows, ok := read("disjoint", la, lb); !ok || rows != 0 {
			t.Fatalf("disjoint labels: rows=%d ok=%v, want an empty served intersection", rows, ok)
		}
		both := add(la, lb)
		if rows, ok := read("one member", la, lb); !ok || rows != 1 {
			t.Fatalf("one common member: rows=%d ok=%v", rows, ok)
		}
		if err := g.Nodes().RemoveLabel(ctx, both.ID(), lb); err != nil {
			t.Fatal(err)
		}
		if rows, ok := read("left again", la, lb); !ok || rows != 0 {
			t.Fatalf("after the member left: rows=%d ok=%v, want an empty served intersection", rows, ok)
		}
		if _, ok := read("unknown", la, "NoSuchLabel"); ok {
			t.Fatal("an unknown label was served; it declines")
		}
	})
}
