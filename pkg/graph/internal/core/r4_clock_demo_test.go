// Demonstration test for the deterministic test clock introduced as
// part of R4-F20. Replaces the wall-clock sleeps that 96+ tests use to
// force a millisecond gap between consecutive mutations with a
// per-Core mock clock that returns strictly-increasing timestamps on
// every call.
//
// New tests should follow the pattern shown here. Existing tests will
// be migrated incrementally — the legacy `time.Sleep(2 * time.Millisecond)`
// pattern still works because c.clock defaults to time.Now in
// production and tests that don't call useTestClock get the legacy
// behaviour.
package core

import (
	"context"
	"testing"
	"time"
)

// TestR4_TestClock_AssignsStrictlyIncreasingInstants verifies that
// every mutation that calls c.now() observes a strictly-increasing
// timestamp under useTestClock. Without the clock, the test would need
// time.Sleep between Add and each Update — exactly the pattern
// R4-F20 flagged as a CI flake source.
func TestR4_TestClock_AssignsStrictlyIncreasingInstants(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	useTestClock(t, g)

	// Three mutations in rapid succession (no sleeps).
	a, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tm0 := a.Temporal().TxFrom

	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"v": 1}); err != nil {
		t.Fatal(err)
	}
	v1, _ := g.Nodes.Get(context.Background(), a.ID())
	tm1 := v1.Temporal().UpdatedAt

	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	v2, _ := g.Nodes.Get(context.Background(), a.ID())
	tm2 := v2.Temporal().UpdatedAt

	if tm0 >= tm1 || tm1 >= tm2 {
		t.Fatalf("timestamps not strictly increasing: tm0=%d tm1=%d tm2=%d", tm0, tm1, tm2)
	}
}

// TestR4_TestClock_AdvanceJumpsAhead shows the explicit Advance API for
// scenarios where a test wants to skip a validity window in one step
// (e.g. "the entity expired at start+1h, query at start+2h"), without
// real-time waiting.
func TestR4_TestClock_AdvanceJumpsAhead(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	clk := useTestClock(t, g)

	a, err := g.Nodes.Add(context.Background(), []string{"X"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t0 := a.Temporal().TxFrom

	clk.Advance(time.Hour)

	if _, err := g.Nodes.Update(context.Background(), a.ID(), map[string]any{"v": 1}); err != nil {
		t.Fatal(err)
	}
	v1, _ := g.Nodes.Get(context.Background(), a.ID())
	t1 := v1.Temporal().UpdatedAt

	gap := int64(t1) - int64(t0)
	wantMin := time.Hour.Milliseconds() // at least one hour gap
	if gap < wantMin {
		t.Fatalf("Advance(1h) produced gap of %dms, want >= %dms", gap, wantMin)
	}
}
