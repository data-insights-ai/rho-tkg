package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestRelRangeCardinality_Parity is the rule-2 mirror of the node RangeCardinality
// test: g.Rels().RangeCardinality and its g.Stats().RelRangeCardinality alias forward
// to the SAME core op — identical results — on both memory and badger; declines
// (exact=false) with no index, is exact once the rel property index is built, and
// declines under a temporal filter.
func TestRelRangeCardinality_Parity(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		// Two endpoints + 20 KNOWS rels with weight 0..19.
		a, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatalf("add a: %v", err)
		}
		b, err := g.Nodes().Add(ctx, []string{"P"}, nil)
		if err != nil {
			t.Fatalf("add b: %v", err)
		}
		for i := 0; i < 20; i++ {
			if _, err := g.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"weight": int64(i)}); err != nil {
				t.Fatalf("add rel %d: %v", i, err)
			}
		}

		// No index yet: both doors decline identically.
		rCount, rExact, rErr := g.Rels().RangeCardinality("KNOWS", "weight", 5, 15, true, true, graphpkg.QueryOpts{})
		sCount, sExact, sErr := g.Stats().RelRangeCardinality("KNOWS", "weight", 5, 15, true, true, graphpkg.QueryOpts{})
		if rErr != nil || sErr != nil {
			t.Fatalf("pre-index errors: rels=%v stats=%v", rErr, sErr)
		}
		if rExact || sExact {
			t.Fatalf("pre-index exact = (rels=%v stats=%v), want both false", rExact, sExact)
		}
		if rCount != sCount {
			t.Fatalf("pre-index count mismatch: rels=%d stats=%d", rCount, sCount)
		}

		if err := g.Index().CreateRelProperty("KNOWS", "weight"); err != nil {
			t.Fatalf("CreateRelProperty: %v", err)
		}

		// With the index: both doors exact, count == 11 (weights 5..15 inclusive).
		rCount, rExact, rErr = g.Rels().RangeCardinality("KNOWS", "weight", 5, 15, true, true, graphpkg.QueryOpts{})
		sCount, sExact, sErr = g.Stats().RelRangeCardinality("KNOWS", "weight", 5, 15, true, true, graphpkg.QueryOpts{})
		if rErr != nil || sErr != nil {
			t.Fatalf("post-index errors: rels=%v stats=%v", rErr, sErr)
		}
		if !rExact || !sExact {
			t.Fatalf("post-index exact = (rels=%v stats=%v), want both true", rExact, sExact)
		}
		if rCount != 11 || sCount != 11 {
			t.Fatalf("post-index count = (rels=%d stats=%d), want 11", rCount, sCount)
		}

		// Exclusive bounds: (5,15) → weights 6..14 = 9.
		if c, exact, _ := g.Rels().RangeCardinality("KNOWS", "weight", 5, 15, false, false, graphpkg.QueryOpts{}); !exact || c != 9 {
			t.Fatalf("exclusive bounds count = %d exact=%v, want 9 true", c, exact)
		}

		// A temporal filter declines the index fast path (valid-time agnostic).
		if _, exact, _ := g.Rels().RangeCardinality("KNOWS", "weight", 5, 15, true, true, graphpkg.QueryOpts{ValidAt: 1000}); exact {
			t.Fatal("temporal RangeCardinality returned exact=true, want decline")
		}

		// Unknown type declines (finds zero).
		if c, exact, _ := g.Rels().RangeCardinality("NOPE", "weight", 0, 100, true, true, graphpkg.QueryOpts{}); exact || c != 0 {
			t.Fatalf("unknown-type count = %d exact=%v, want 0 false", c, exact)
		}
	})
}
