package graph_test

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestForEachByLabelPropertyRange pins the ordered-view ordered-view range scan:
// candidate completeness against a ByLabel reference filter (the view may
// over-select but must never under-select within widened bounds), bound
// inclusivity, mixed int/float values on one key, removal maintenance, and
// the ErrIndexNotFound fallback signal when no index exists.
func TestForEachByLabelPropertyRange(t *testing.T) {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()

	var ids []types.NodeID
	for i := 0; i < 50; i++ {
		var age any = int64(20 + i)
		if i%5 == 0 {
			age = float64(20+i) + 0.5 // mixed int/float on one key
		}
		n, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": age})
		if err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
		ids = append(ids, n.ID())
	}
	// Label registered but no property index yet: the range scan must
	// signal fallback, not guess.
	err = g.Nodes().ForEachByLabelPropertyRange("P", "age", 0, 100, true, true, graph.QueryOpts{}, func(*types.Node) bool { return true })
	if !errors.Is(err, graph.ErrIndexNotFound) {
		t.Fatalf("no-index range scan err = %v, want ErrIndexNotFound", err)
	}
	if err := g.Index().CreateProperty("P", "age"); err != nil {
		t.Fatalf("CreateProperty: %v", err)
	}

	countRange := func(min, max float64, inclMin, inclMax bool) map[types.NodeID]bool {
		got := map[types.NodeID]bool{}
		if err := g.Nodes().ForEachByLabelPropertyRange("P", "age", min, max, inclMin, inclMax, graph.QueryOpts{}, func(n *types.Node) bool {
			got[n.ID()] = true
			return true
		}); err != nil {
			t.Fatalf("range scan: %v", err)
		}
		return got
	}
	reference := func(pred func(float64) bool) map[types.NodeID]bool {
		want := map[types.NodeID]bool{}
		nodes, err := g.Nodes().ByLabel("P", graph.QueryOpts{})
		if err != nil {
			t.Fatalf("ByLabel: %v", err)
		}
		for _, n := range nodes {
			v, _ := n.GetProperty("age")
			var f float64
			switch x := v.(type) {
			case int64:
				f = float64(x)
			case float64:
				f = x
			}
			if pred(f) {
				want[n.ID()] = true
			}
		}
		return want
	}

	// Candidate completeness: every reference match must be in the
	// candidate set (over-selection allowed, under-selection is the bug).
	got := countRange(30, 40, true, true)
	for id := range reference(func(f float64) bool { return f >= 30 && f <= 40 }) {
		if !got[id] {
			t.Fatalf("range [30,40] missing node %d", id)
		}
	}
	// Exclusive bounds: the view returns the SAME candidate set as the
	// inclusive form — boundary buckets are never skipped, because int64
	// values past 2^53 collide onto the bound's float64 sort key and
	// skipping would under-select (review finding). Bound inclusivity is
	// the CALLER's exact post-filter's job; the view only guarantees
	// completeness.
	gotExcl := countRange(31, 39, false, false)
	for id := range reference(func(f float64) bool { return f > 31 && f < 39 }) {
		if !gotExcl[id] {
			t.Fatalf("range (31,39) missing node %d", id)
		}
	}

	// Removal maintenance: delete a matching node, the candidates follow.
	victim := ids[15] // age 35
	if err := g.Nodes().Delete(ctx, victim); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if countRange(30, 40, true, true)[victim] {
		t.Fatal("deleted node still produced by the ordered view")
	}

	// Early stop.
	seen := 0
	if err := g.Nodes().ForEachByLabelPropertyRange("P", "age", 0, 1000, true, true, graph.QueryOpts{}, func(*types.Node) bool {
		seen++
		return seen < 3
	}); err != nil {
		t.Fatalf("early stop: %v", err)
	}
	if seen != 3 {
		t.Fatalf("early stop saw %d", seen)
	}
}
