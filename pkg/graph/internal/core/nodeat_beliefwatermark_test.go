package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestNodeAt_BoundedCascadeResumption_NodeAtResolvesThroughRestoredFastPath
// is a regression test for the restored (BACKLOG 10c) fast path exercised via
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
// the 10b fix's own resumption-boundary logic guarantees: a bounded
// correction can never leave a GEOMETRICALLY OVERLAPPING, higher-belief row
// stranded in history underneath an untouched current for THIS specific
// single-cascade shape (see the invariant test file for why the
// current-answers-alone fast path still needs the watermark gate regardless
// — other write paths / bug classes are not bound by this tiling).
//
// This test instead proves the practical, everyday-shape regression: NodeAt
// correctly resolves BOTH the bounded correction's own window (state B, at
// validAt=6500) AND the pre-correction belief (state A, at validAt=5500,
// before the correction's own window even starts) once the fast path is
// restored — i.e. restoring the shortcut for the (much more common) case
// where current legitimately IS the max-belief row does not regress the
// correctness of a query that must fall through to history.
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

	// NodeAt(6500) must find the BOUNDED CORRECTION (B) from history — this
	// validAt is inside the correction's own window [6000,7000) and BEFORE
	// current's own valid-from (7000), so the fast path correctly declines
	// on the pre-existing validAt-vs-sortValidFrom check alone (current
	// cannot answer for a validAt before its own start) and falls through to
	// the full chain scan.
	got, err := g.Temporal.NodeAt(n.ID(), 6500)
	if err != nil {
		t.Fatalf("NodeAt(6500): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAt(6500) state = %v, want B (the bounded correction)", v)
	}

	// NodeAt(5500) must find the ORIGINAL pre-correction belief (A) — before
	// the correction's own window starts and before current's new
	// (post-resumption) valid-from too, so this ALSO falls through to history
	// (the demoted original row).
	gotBefore, err := g.Temporal.NodeAt(n.ID(), 5500)
	if err != nil {
		t.Fatalf("NodeAt(5500): %v", err)
	}
	if v, _ := gotBefore.GetProperty("state"); v != "A" {
		t.Fatalf("NodeAt(5500) state = %v, want A (the original pre-correction belief, from history)", v)
	}
}

// TestNodeAt_BeliefWatermarkFastPath_ActuallyEngagesWhenSafe proves the
// restored fast path is not merely harmless but genuinely SKIPS the history
// fetch in the common (no-cascade) case: after a plain, ordinary Update (not
// a cascade), current IS the newest belief in the whole chain, so
// nodeCurrentAnswersAt must return true and NodeAt must resolve without
// ever touching history. Uses a node with NO history at all (single Add, no
// Update) as the simplest possible positive case, plus one WITH an ordinary
// history entry (via Update) to prove the fast path also engages after a
// normal edit — the watermark equals current's TxFrom in both cases.
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
	time.Sleep(time.Millisecond)
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
	got, err := g.Temporal.NodeAt(n.ID(), 2000)
	if err != nil {
		t.Fatalf("NodeAt(2000): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAt(2000) state = %v, want B", v)
	}
}
