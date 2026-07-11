package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// TestStatsRangeCardinality_MatchesNodesAlias pins 's additive alias
// contract: g.Stats().RangeCardinality forwards to the SAME core op as
// g.Nodes().RangeCardinality — identical signature, identical results — on
// both the memory and badger backends (Testing Rule 1: direct test of the
// new public method, not delegation-assumption).
func TestStatsRangeCardinality_MatchesNodesAlias(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		for i := 0; i < 20; i++ {
			if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(i)}); err != nil {
				t.Fatalf("Add %d: %v", i, err)
			}
		}

		// No property index yet: both doors decline identically (fast path
		// unusable, caller must scan-and-count).
		nCount, nExact, nErr := g.Nodes().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{})
		sCount, sExact, sErr := g.Stats().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{})
		if nErr != nil || sErr != nil {
			t.Fatalf("pre-index RangeCardinality errors: nodes=%v stats=%v", nErr, sErr)
		}
		if nExact || sExact {
			t.Fatalf("pre-index RangeCardinality exact = (nodes=%v, stats=%v), want both false", nExact, sExact)
		}
		if nCount != sCount {
			t.Fatalf("pre-index RangeCardinality count mismatch: nodes=%d stats=%d", nCount, sCount)
		}

		if err := g.Index().CreateProperty("P", "age"); err != nil {
			t.Fatalf("CreateProperty: %v", err)
		}

		// With the index built, both doors must agree — exact, and count == 11
		// (ages 5..15 inclusive).
		nCount, nExact, nErr = g.Nodes().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{})
		sCount, sExact, sErr = g.Stats().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{})
		if nErr != nil || sErr != nil {
			t.Fatalf("post-index RangeCardinality errors: nodes=%v stats=%v", nErr, sErr)
		}
		if !nExact || !sExact {
			t.Fatalf("post-index RangeCardinality exact = (nodes=%v, stats=%v), want both true", nExact, sExact)
		}
		if nCount != 11 || sCount != 11 {
			t.Fatalf("post-index RangeCardinality count = (nodes=%d, stats=%d), want both 11", nCount, sCount)
		}

		// Exclusive bounds must also agree between the two doors.
		nCount, nExact, nErr = g.Nodes().RangeCardinality("P", "age", 5, 15, false, false, graphpkg.QueryOpts{})
		sCount, sExact, sErr = g.Stats().RangeCardinality("P", "age", 5, 15, false, false, graphpkg.QueryOpts{})
		if nErr != nil || sErr != nil {
			t.Fatalf("exclusive-bounds RangeCardinality errors: nodes=%v stats=%v", nErr, sErr)
		}
		if !nExact || !sExact {
			t.Fatalf("exclusive-bounds RangeCardinality exact = (nodes=%v, stats=%v), want both true", nExact, sExact)
		}
		if nCount != 9 || sCount != 9 {
			t.Fatalf("exclusive-bounds RangeCardinality count = (nodes=%d, stats=%d), want both 9", nCount, sCount)
		}

		// A temporal filter in opts is a documented decline condition (the BSI
		// is valid-time agnostic): both doors must decline identically even
		// though the index now exists.
		nCount, nExact, nErr = g.Nodes().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{ValidAt: 1})
		sCount, sExact, sErr = g.Stats().RangeCardinality("P", "age", 5, 15, true, true, graphpkg.QueryOpts{ValidAt: 1})
		if nErr != nil || sErr != nil {
			t.Fatalf("temporal-opts RangeCardinality errors: nodes=%v stats=%v", nErr, sErr)
		}
		if nExact || sExact {
			t.Fatalf("temporal-opts RangeCardinality exact = (nodes=%v, stats=%v), want both false (BSI is valid-time agnostic)", nExact, sExact)
		}
		if nCount != 0 || sCount != 0 {
			t.Fatalf("temporal-opts RangeCardinality count = (nodes=%d, stats=%d), want both 0", nCount, sCount)
		}
	})
}

// TestStatsRangeCardinality_DeclinesWithSentinelOnClosedGraph is the decline
// case documented for the capability story: on a closed graph the call must
// both decline (exact=false) AND surface the documented graph.ErrGraphClosed
// sentinel, verifiable with errors.Is (Testing Rule 4) — exactly like every
// other Stats accessor.
func TestStatsRangeCardinality_DeclinesWithSentinelOnClosedGraph(t *testing.T) {
	eachBackend(t, func(t *testing.T, g *graphpkg.Graph) {
		ctx := context.Background()
		if _, err := g.Nodes().Add(ctx, []string{"P"}, map[string]any{"age": int64(5)}); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := g.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		count, exact, err := g.Stats().RangeCardinality("P", "age", 0, 100, true, true, graphpkg.QueryOpts{})
		if !errors.Is(err, graphpkg.ErrGraphClosed) {
			t.Fatalf("RangeCardinality on closed graph err = %v, want ErrGraphClosed", err)
		}
		if exact {
			t.Fatal("RangeCardinality on closed graph exact = true, want false (declined)")
		}
		if count != 0 {
			t.Fatalf("RangeCardinality on closed graph count = %d, want 0", count)
		}
	})
}
