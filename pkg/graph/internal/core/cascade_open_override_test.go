package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 10b: an open-ended cascade correction starting BEFORE an untouched
// open "current" row was silently capped and never won, even though it is the
// newer belief. Root cause: nodeVersionBounds/relVersionBounds derive a
// version's effective end POSITIONALLY (the next sorted entry's ValidFrom),
// not by belief recency, so the untouched current row's ValidFrom incorrectly
// truncated the new correction's interval. Fixed via own-interval bounds
// (nodeOwnBounds/relOwnBounds) + the existing newest-belief tiebreak, scoped
// to the cascade slow path and newCurrent selection only — see temporal.go
// and temporal_cascade.go. See tasks/backlog.md history / CHANGELOG for the
// full design rationale (two prior fix attempts were reverted before this
// scoped design).

// TestCascade_OpenCorrectionBeforeUntouchedOpenCurrent is the original 10b
// repro: current v0 ValidFrom=2000 open TxFrom=T0; a cascade correction at
// T1>T0 creates v1 ValidFrom=1000 open TxFrom=T1 (newer belief, WIDER valid
// range covering v0's start). Both v0 and v1 cover t=2500; v1 must win
// (newest belief), and v1 must also become the new "current" row.
func TestCascade_OpenCorrectionBeforeUntouchedOpenCurrent(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	txT0 := n.Temporal().TxFrom

	clk.Advance(time.Millisecond)
	v1, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 1000, 0, map[string]any{
		"state": "B",
	})
	if err != nil {
		t.Fatalf("cascade: %v", err)
	}
	txT1 := v1.Temporal().TxFrom
	if !(txT1 > txT0) {
		t.Fatalf("test setup invalid: want txT1 (%d) > txT0 (%d)", txT1, txT0)
	}

	// Point read (no TX pin): v1 (B) must win at t=2500 — it is the newer
	// belief and its own interval [1000,+inf) covers 2500.
	got, err := g.Temporal.NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt(2500): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAt(2500) state=%v, want B (10b regression: the untouched older row A must not win)", v)
	}

	// Bitemporal read pinned at/after T1: same expectation.
	gotTx, err := g.Temporal.NodeAtTx(n.ID(), 2500, txT1)
	if err != nil {
		t.Fatalf("NodeAtTx(2500, txT1): %v", err)
	}
	if v, _ := gotTx.GetProperty("state"); v != "B" {
		t.Fatalf("NodeAtTx(2500, txT1) state=%v, want B", v)
	}

	// Bitemporal read pinned BEFORE T1 (at/after T0, before the correction):
	// the belief state as of T0 never saw the correction, so A must still be
	// what's returned at t=2500 (A's own interval [2000,+inf) covers 2500).
	gotPre, err := g.Temporal.NodeAtTx(n.ID(), 2500, txT0)
	if err != nil {
		t.Fatalf("NodeAtTx(2500, txT0): %v", err)
	}
	if v, _ := gotPre.GetProperty("state"); v != "A" {
		t.Fatalf("NodeAtTx(2500, txT0) state=%v, want A (pre-correction belief must be unaffected)", v)
	}

	// The corrected row must also be the new "current" (Get by ID / no
	// temporal filter resolves to the live current row).
	cur, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get (current): %v", err)
	}
	if v, _ := cur.GetProperty("state"); v != "B" {
		t.Fatalf("current row state=%v, want B (the corrected, newer-belief open row must become current)", v)
	}
}

// TestCascadeRel_OpenCorrectionBeforeUntouchedOpenCurrent is the relationship
// mirror of TestCascade_OpenCorrectionBeforeUntouchedOpenCurrent (rule 2).
func TestCascadeRel_OpenCorrectionBeforeUntouchedOpenCurrent(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)
	r, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	txT0 := r.Temporal().TxFrom

	clk.Advance(time.Millisecond)
	v1, err := g.Temporal.SetRelVersionInterval(context.Background(), r.ID(), 1000, 0, map[string]any{
		"state": "B",
	})
	if err != nil {
		t.Fatalf("cascade: %v", err)
	}
	txT1 := v1.Temporal().TxFrom
	if !(txT1 > txT0) {
		t.Fatalf("test setup invalid: want txT1 (%d) > txT0 (%d)", txT1, txT0)
	}

	got, err := g.Temporal.RelAt(r.ID(), 2500)
	if err != nil {
		t.Fatalf("RelAt(2500): %v", err)
	}
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Fatalf("RelAt(2500) state=%v, want B (10b regression)", v)
	}

	gotTx, err := g.Temporal.RelAtTx(r.ID(), 2500, txT1)
	if err != nil {
		t.Fatalf("RelAtTx(2500, txT1): %v", err)
	}
	if v, _ := gotTx.GetProperty("state"); v != "B" {
		t.Fatalf("RelAtTx(2500, txT1) state=%v, want B", v)
	}

	gotPre, err := g.Temporal.RelAtTx(r.ID(), 2500, txT0)
	if err != nil {
		t.Fatalf("RelAtTx(2500, txT0): %v", err)
	}
	if v, _ := gotPre.GetProperty("state"); v != "A" {
		t.Fatalf("RelAtTx(2500, txT0) state=%v, want A (pre-correction belief must be unaffected)", v)
	}

	cur, err := g.Rels.Get(context.Background(), r.ID())
	if err != nil {
		t.Fatalf("Get (current rel): %v", err)
	}
	if v, _ := cur.GetProperty("state"); v != "B" {
		t.Fatalf("current rel state=%v, want B", v)
	}
}

// TestCascade_NestedMultiCascadeAccumulation is the exact multi-cascade
// accumulation scenario that broke the FIRST reverted 10b fix attempt (a
// pairwise-adjacent TxFrom comparison inside nodeVersionBounds could not
// track "which cascade batch" a candidate pair belonged to once multiple
// cascades' newVer/resumption pairs accumulated on one chain). Own-interval
// bounds + a global belief tiebreak has no pairwise comparison to trap:
//
//	genesis  [1000, +inf)         A   TxFrom=T0
//	cascade1 [2000, 4000)         B   TxFrom=T1  (resumption1 = [4000,+inf) A — no later boundary)
//	cascade2 [2500, 3000)         C   TxFrom=T2  (resumption2 = [3000,4000) B — bounded by cascade1's own resumption)
//
// Painted timeline: [1000,2000)A, [2000,2500)B, [2500,3000)C, [3000,4000)B, [4000,+inf)A.
// Asserts ABSOLUTE values (rule 16), not merely "engine agrees with itself".
func TestCascade_NestedMultiCascadeAccumulation(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 4000, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade1 (B): %v", err)
	}

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2500, 3000, map[string]any{
		"state": "C",
	}); err != nil {
		t.Fatalf("cascade2 (C): %v", err)
	}

	for _, tc := range []struct {
		at   types.Instant
		want string
	}{
		{1500, "A"},
		{1999, "A"},
		{2000, "B"},
		{2200, "B"},
		{2499, "B"},
		{2500, "C"},
		{2700, "C"},
		{2999, "C"},
		{3000, "B"},
		{3500, "B"},
		{3999, "B"},
		{4000, "A"},
		{4500, "A"},
	} {
		got, err := g.Temporal.NodeAt(n.ID(), tc.at)
		if err != nil {
			t.Errorf("at %d: %v", tc.at, err)
			continue
		}
		if v, _ := got.GetProperty("state"); v != tc.want {
			t.Errorf("at %d state=%v, want %s", tc.at, v, tc.want)
		}
	}
}

// TestCascade_TwoOpenCascadesStacked proves belief recency alone decides an
// overlap among own-open rows, even when a LATER-belief correction's own
// interval starts EARLIER than an intermediate not-most-recent open row's
// start (i.e. interval "specificity" never overrides belief recency):
//
//	genesis  [1000, +inf)  A  TxFrom=T0
//	cascade1 [1500, +inf)  B  TxFrom=T1
//	cascade2 [1200, +inf)  C  TxFrom=T2  (newest belief, starts BEFORE B)
//
// At t=1600 all three rows cover; C must win (newest belief) even though B's
// interval [1500,+inf) is "more specific" to 1600 than C's [1200,+inf). C
// also becomes the new current row.
func TestCascade_TwoOpenCascadesStacked(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 1500, 0, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade1 (B): %v", err)
	}

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 1200, 0, map[string]any{
		"state": "C",
	}); err != nil {
		t.Fatalf("cascade2 (C): %v", err)
	}

	for _, tc := range []struct {
		at   types.Instant
		want string
	}{
		{1100, "A"}, // only A covers
		{1300, "C"}, // A and C cover; C is newer belief
		{1600, "C"}, // A, B, and C all cover; C is newest belief regardless of B's later start
	} {
		got, err := g.Temporal.NodeAt(n.ID(), tc.at)
		if err != nil {
			t.Errorf("at %d: %v", tc.at, err)
			continue
		}
		if v, _ := got.GetProperty("state"); v != tc.want {
			t.Errorf("at %d state=%v, want %s", tc.at, v, tc.want)
		}
	}

	cur, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get (current): %v", err)
	}
	if v, _ := cur.GetProperty("state"); v != "C" {
		t.Fatalf("current row state=%v, want C (newest-belief open row must be current)", v)
	}
}

// TestCascade_ResumptionAgainstOpenTailStaysOpen exercises the BACKLOG 10b
// resumptionEnd computation's "src is the open tail with nothing after it in
// preChain" branch directly: a single genesis row [1000,+inf)A, a BOUNDED
// cascade [2000,3000)Wipe entirely inside it. The resumption resolves
// against src=A at newVT=3000; since A has no later boundary in the
// PRE-correction chain, the resumption's own ValidTo must stay 0 (open), not
// some other value — so [3000,+inf) resolves to A again, not Wipe or absent.
func TestCascade_ResumptionAgainstOpenTailStaysOpen(t *testing.T) {
	g := newTxTimeGraph(t)

	n, err := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 3000, map[string]any{
		"state": "Wipe",
	}); err != nil {
		t.Fatalf("cascade: %v", err)
	}

	for _, tc := range []struct {
		at   types.Instant
		want string
	}{
		{1500, "A"},
		{2500, "Wipe"},
		{3500, "A"},  // resumption must be open — must still cover far beyond 3000
		{50000, "A"}, // arbitrarily far in the future — resumption is NOT closed
	} {
		got, err := g.Temporal.NodeAt(n.ID(), tc.at)
		if err != nil {
			t.Errorf("at %d: %v", tc.at, err)
			continue
		}
		if v, _ := got.GetProperty("state"); v != tc.want {
			t.Errorf("at %d state=%v, want %s", tc.at, v, tc.want)
		}
	}

	cur, err := g.Nodes.Get(context.Background(), n.ID())
	if err != nil {
		t.Fatalf("Get (current): %v", err)
	}
	if v, _ := cur.GetProperty("state"); v != "A" {
		t.Fatalf("current row state=%v, want A (the resumption row, open-ended, must be current)", v)
	}
}
