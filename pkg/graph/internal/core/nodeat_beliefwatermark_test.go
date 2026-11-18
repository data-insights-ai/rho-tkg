package core

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeAt_BoundedCascadeResumption_NodeAtResolvesThroughRestoredFastPath
// is a regression test for the restored fast path exercised via
// a REAL cascade sequence (rather than a direct injection — see
// nodeat_beliefwatermark_invariant_test.go for the adversarial invariant
// proof).
//
// A bounded correction (SetNodeVersionInterval(nid, 6000, 7000, ...)) against
// a single untouched, still-open current row does NOT leave current
// unchanged: cascadeNodeVersionInterval also constructs a "resumption" row
// (re-asserting the pre-correction belief from newVT=7000 onward) whose own
// interval is OPEN (nodeResumptionEnd finds no later boundary in a
// single-row preChain) — so the resumption, carrying the NEWEST belief
// (TxFrom = the cascade's timestamp), wins the "own-open" comparison and
// REPLACES current outright (current's ValidFrom effectively shifts from
// 5000 to 7000, same state "A", newer TxFrom). This is the tiling discipline
// the cascade's own resumption-boundary logic guarantees: a bounded
// correction can never leave a GEOMETRICALLY OVERLAPPING, higher-belief row
// stranded in history underneath an untouched current for THIS specific
// single-cascade shape (see the invariant test file for why the
// current-answers-alone fast path still needs the watermark gate regardless
// — other write paths / bug classes are not bound by this tiling).
//
// This test proves both arms after a real cascade. At validAt=7500 (at or
// after the resumption's start) the cascade has advanced the belief watermark
// to the resumption's TxFrom, so the current row alone answers A and NodeAt
// takes no history read (headStats.nodeAt stays put). At validAt=6500 (the
// correction's window, state B) and 5500 (the original belief, state A) the
// probe lies before current's ValidFrom, so the shortcut declines on that
// check and NodeAt reads history (one history read each). The watermark gate
// against a history row that outranks current is proved by
// nodeat_beliefwatermark_invariant_test.go.
func TestNodeAt_BoundedCascadeResumption_NodeAtResolvesThroughRestoredFastPath(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testNodeAtBoundedCascadeUnderOpenCurrent(t, cfg)
		})
	}
}

func testNodeAtBoundedCascadeUnderOpenCurrent(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(5000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	txT0 := n.Temporal().TxFrom

	// A bounded correction strictly AFTER current's own valid-from, with a
	// strictly newer belief.
	corrected, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 6000, 7000, map[string]any{
		"state": "B",
	})
	if err != nil {
		t.Fatalf("cascade SetNodeVersionInterval: %v", err)
	}
	txT1 := corrected.Temporal().TxFrom
	if txT1 <= txT0 {
		t.Fatalf("test setup invalid: want txT1 (%d) > txT0 (%d)", txT1, txT0)
	}

	// The resumption row (re-asserting "A" from newVT=7000 onward) becomes
	// the new current: newer belief (txT1), open own-interval, ValidFrom
	// shifted to 7000 — this is the cascade's documented tiling behavior, not
	// a fast-path artifact.
	cur, err := g.Nodes.Get(ctx, n.ID())
	if err != nil {
		t.Fatalf("Get current: %v", err)
	}
	if v, _ := cur.GetProperty("state"); v != "A" {
		t.Fatalf("current state = %v, want A (the resumption row's re-asserted pre-correction value)", v)
	}
	tm := cur.Temporal()
	if tm == nil || tm.ValidTo != 0 {
		t.Fatalf("current must be open (ValidTo == 0), got %+v", tm)
	}
	if tm.ValidFrom != 7000 {
		t.Fatalf("current ValidFrom = %d, want 7000 (the resumption's start, not the original 5000)", tm.ValidFrom)
	}
	if tm.TxFrom != txT1 {
		t.Fatalf("current TxFrom = %d, want %d (the resumption carries the cascade's timestamp, not the original create's)", tm.TxFrom, txT1)
	}

	// NodeAt(7500): at or after the resumption's start, the current row is the
	// newest belief in the chain (the cascade moved the watermark to txT1), so
	// the shortcut answers A without a history read.
	if !g.nodeCurrentAnswersAt(cur, 7500, 0) {
		t.Fatalf("nodeCurrentAnswersAt(cur, 7500) = false, want true (the cascade must advance the belief watermark to the resumption's TxFrom)")
	}
	histReads := g.headStats.nodeAt.Load()
	gotAfter, err := g.Temporal.NodeAt(n.ID(), 7500)
	if err != nil {
		t.Fatalf("NodeAt(7500): %v", err)
	}
	if v, _ := gotAfter.GetProperty("state"); v != "A" {
		t.Fatalf("NodeAt(7500) state = %v, want A (the resumption row)", v)
	}
	if d := g.headStats.nodeAt.Load() - histReads; d != 0 {
		t.Fatalf("NodeAt(7500) took %d history reads, want 0 (the current row alone answers)", d)
	}

	// NodeAt(6500) must find the BOUNDED CORRECTION (B) from history — this
	// validAt is inside the correction's own window [6000,7000) and BEFORE
	// current's own valid-from (7000), so the shortcut declines on the
	// validAt-before-ValidFrom check and NodeAt reads history.
	histReads = g.headStats.nodeAt.Load()
	got, err := g.Temporal.NodeAt(n.ID(), 6500)
	if err != nil {
		t.Fatalf("NodeAt(6500): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAt(6500) state = %v, want B (the bounded correction)", v)
	}
	if d := g.headStats.nodeAt.Load() - histReads; d != 1 {
		t.Fatalf("NodeAt(6500) took %d history reads, want 1", d)
	}

	// NodeAt(5500) must find the ORIGINAL pre-correction belief (A) — before
	// the correction's own window starts and before current's new
	// (post-resumption) valid-from too, so this ALSO falls through to history
	// (the demoted original row).
	histReads = g.headStats.nodeAt.Load()
	gotBefore, err := g.Temporal.NodeAt(n.ID(), 5500)
	if err != nil {
		t.Fatalf("NodeAt(5500): %v", err)
	}
	if v, _ := gotBefore.GetProperty("state"); v != "A" {
		t.Fatalf("NodeAt(5500) state = %v, want A (the original pre-correction belief, from history)", v)
	}
	if d := g.headStats.nodeAt.Load() - histReads; d != 1 {
		t.Fatalf("NodeAt(5500) took %d history reads, want 1", d)
	}
}

// TestNodeAt_BeliefWatermarkFastPath_ActuallyEngagesWhenSafe proves the
// current-row shortcut skips the history fetch in the common (no-cascade)
// case: after a plain Update, current IS the newest belief in the whole
// chain, so nodeCurrentAnswersAt returns true and NodeAt answers without a
// history read (headStats.nodeAt, counted on the history arm, stays put).
// Covers a node with no history (single Add) and one with an ordinary history
// entry (via Update); the watermark equals current's TxFrom in both cases. A
// probe before current's ValidFrom is the control: it must read history.
func TestNodeAt_BeliefWatermarkFastPath_ActuallyEngagesWhenSafe(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]Config{
		"memory": {AllowTxBackfill: true},
		"badger": {BadgerInMemory: true, AllowTxBackfill: true},
	} {
		t.Run(name, func(t *testing.T) {
			testNodeAtBeliefWatermarkFastPathEngages(t, cfg)
		})
	}
}

func testNodeAtBeliefWatermarkFastPathEngages(t *testing.T, cfg Config) {
	g, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// No history yet: nodeCurrentAnswersAt must be true (watermark == current's
	// own TxFrom, since current is the ONLY row ever written).
	c := g
	current, err := c.getCurrentNode(n.ID())
	if err != nil {
		t.Fatalf("getCurrentNode: %v", err)
	}
	if !c.nodeCurrentAnswersAt(current, 2000, 0) {
		t.Fatalf("nodeCurrentAnswersAt = false for a node with no history, want true (fast path should engage on the common no-cascade case)")
	}

	// After an ordinary Update (fresh TxFrom, strictly newer than the create),
	// the fast path must STILL engage — current is again the max-belief row.
	// Re-asserting tkg_valid_from (strictly greater than the previous
	// version's, as the store requires) keeps the effective valid-from well
	// before validAt=2000 — an Update with NO explicit tkg_valid_from resets
	// the effective valid-from to "now" via UpdatedAt (correct, pre-existing
	// behavior unrelated to this fix), which would otherwise make validAt=2000
	// legitimately precede the entity's new effective valid-from.
	updated, err := g.Nodes.Update(ctx, n.ID(), map[string]any{
		"state":          "B",
		"tkg_valid_from": types.Instant(1500),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !c.nodeCurrentAnswersAt(updated, 2000, 0) {
		t.Fatalf("nodeCurrentAnswersAt = false after an ordinary Update, want true (an ordinary update keeps current as the max-belief row)")
	}
	histReads := g.headStats.nodeAt.Load()
	got, err := g.Temporal.NodeAt(n.ID(), 2000)
	if err != nil {
		t.Fatalf("NodeAt(2000): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAt(2000) state = %v, want B", v)
	}
	if d := g.headStats.nodeAt.Load() - histReads; d != 0 {
		t.Fatalf("NodeAt(2000) took %d history reads, want 0 (the current row alone answers)", d)
	}

	// Control: a probe before current's ValidFrom (1500) must read history and
	// find the first version.
	histReads = g.headStats.nodeAt.Load()
	got, err = g.Temporal.NodeAt(n.ID(), 1200)
	if err != nil {
		t.Fatalf("NodeAt(1200): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "A" {
		t.Fatalf("NodeAt(1200) state = %v, want A", v)
	}
	if d := g.headStats.nodeAt.Load() - histReads; d != 1 {
		t.Fatalf("NodeAt(1200) took %d history reads, want 1", d)
	}
}
