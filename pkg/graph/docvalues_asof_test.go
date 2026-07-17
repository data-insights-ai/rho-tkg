package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestDocValuesSnapshotAsOf_TimeTravel is the two-phase temporal proof (Testing
// Rule 15): a columnar snapshot pinned at t0 must reflect the state AS BELIEVED at
// t0, not the post-mutation state. It also proves the door works where the
// current-state scanner does and that current vs as-of diverge.
func TestDocValuesSnapshotAsOf_TimeTravel(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Phase 1: create the node in state X (score=10) at t0.
	n, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": int64(10)})
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	txfRaw, ok := g.Resolve().NodeProperty(n, "tkg_tx_from")
	if !ok {
		t.Fatal("no tkg_tx_from on created node")
	}
	t0 := txfRaw.(types.Instant)

	// Advance the clock so the mutation is stamped strictly AFTER t0, then mutate.
	if _, err := g.Temporal().AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"score": int64(99)}); err != nil {
		t.Fatalf("update: %v", err)
	}

	keys := []string{"score"}

	// AS OF t0: the columnar snapshot must report the OLD score (10).
	snap, gen, ok, err := g.Nodes().DocValuesSnapshotAsOf("Metric", keys, t0)
	if err != nil {
		t.Fatalf("DocValuesSnapshotAsOf: %v", err)
	}
	if !ok {
		t.Fatal("as-of snapshot unusable (ok=false) for a numeric column")
	}
	// gen is deliberately 0 for an as-of (frozen, no current-state staleness signal).
	if gen != 0 {
		t.Fatalf("as-of gen = %d, want 0 (frozen point-in-time read)", gen)
	}
	vals := make([]any, 1)
	present := make([]bool, 1)
	if !snap.Row(n.ID(), vals, present) {
		t.Fatal("as-of snapshot: node not a member at t0, want member")
	}
	if !present[0] || vals[0] != int64(10) {
		t.Fatalf("as-of score = %v (present=%v), want 10 — snapshot did not time-travel", vals[0], present[0])
	}

	// Current-state snapshot must report the NEW score (99) — proves they diverge.
	cur, _, curOK, err := g.Nodes().DocValuesSnapshot("Metric", keys)
	if err != nil {
		t.Fatalf("DocValuesSnapshot: %v", err)
	}
	if curOK {
		cv := make([]any, 1)
		cp := make([]bool, 1)
		if cur.Row(n.ID(), cv, cp) && cv[0] != int64(99) {
			t.Fatalf("current score = %v, want 99", cv[0])
		}
	}
}

// TestDocValuesSnapshotAsOf_MembershipAtPin proves as-of MEMBERSHIP: a node that
// acquired the label only AFTER the pin is not a member of the as-of snapshot.
func TestDocValuesSnapshotAsOf_MembershipAtPin(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	early, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("add early: %v", err)
	}
	txfRaw, _ := g.Resolve().NodeProperty(early, "tkg_tx_from")
	t0 := txfRaw.(types.Instant)

	// A second Metric node created AFTER t0.
	if _, err := g.Temporal().AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	late, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": int64(2)})
	if err != nil {
		t.Fatalf("add late: %v", err)
	}

	snap, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("Metric", []string{"score"}, t0)
	if err != nil || !ok {
		t.Fatalf("as-of snapshot: ok=%v err=%v", ok, err)
	}
	vals := make([]any, 1)
	present := make([]bool, 1)
	if !snap.Row(early.ID(), vals, present) {
		t.Fatal("early node should be a member at t0")
	}
	if snap.Row(late.ID(), vals, present) {
		t.Fatal("late node (created after t0) must NOT be a member of the as-of snapshot")
	}
}

// TestForEachDocValuesAsOf_StreamingAggregation proves the streaming as-of door:
// it drives a SUM aggregation over the label's members as believed at t0, so a
// post-t0 mutation is not reflected — the ~124,000x time-travel aggregation win.
func TestForEachDocValuesAsOf_StreamingAggregation(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Three metrics: scores 10, 20, 30 (sum 60). t0 is the LAST one's tx-time, so
	// all three are believed to exist at t0.
	var first, last *types.Node
	for i, s := range []int64{10, 20, 30} {
		n, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": s})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if first == nil {
			first = n
		}
		last = n
	}
	txfRaw, _ := g.Resolve().NodeProperty(last, "tkg_tx_from")
	t0 := txfRaw.(types.Instant)

	// After t0, bump the first metric's score to 999.
	if _, err := g.Temporal().AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, first.ID(), map[string]any{"score": int64(999)}); err != nil {
		t.Fatalf("update: %v", err)
	}

	// AS OF t0: streaming SUM must be 60 (the pre-mutation belief), not 1049.
	var sum, count int64
	gen, ok, err := g.Nodes().ForEachDocValuesAsOf("Metric", []string{"score"}, t0, func(_ types.NodeID, vals []any, present []bool) bool {
		if present[0] {
			sum += vals[0].(int64)
			count++
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValuesAsOf: %v", err)
	}
	if !ok {
		t.Fatal("stream unusable (ok=false) for a numeric column")
	}
	if gen != 0 {
		t.Fatalf("as-of gen = %d, want 0", gen)
	}
	if count != 3 {
		t.Fatalf("streamed %d members at t0, want 3", count)
	}
	if sum != 60 {
		t.Fatalf("as-of SUM(score) = %d, want 60 (stream did not time-travel; current would be 1049)", sum)
	}

	// Early-stop: fn returning false halts the scan.
	seen := 0
	if _, _, err := g.Nodes().ForEachDocValuesAsOf("Metric", []string{"score"}, t0, func(_ types.NodeID, _ []any, _ []bool) bool {
		seen++
		return false // stop after the first row
	}); err != nil {
		t.Fatalf("early-stop stream: %v", err)
	}
	if seen != 1 {
		t.Fatalf("early-stop streamed %d rows, want 1", seen)
	}
}

// TestForEachDocValuesAsOf_ContractEdges covers the door's two non-happy contracts:
// a non-positive txAt is rejected, and a non-uniform column (mixed value types)
// yields ok=false streaming nothing — the "caller falls back" signal.
func TestForEachDocValuesAsOf_ContractEdges(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Two Metric nodes whose "score" is int on one, string on the other → the
	// column is not uniformly numeric/string, so it is unbuildable.
	a, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": "not-a-number"}); err != nil {
		t.Fatalf("add b: %v", err)
	}
	txfRaw, _ := g.Resolve().NodeProperty(a, "tkg_tx_from")
	t0 := txfRaw.(types.Instant)
	if _, err := g.Temporal().AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}

	// txAt <= 0 → ErrInvalidTimeRange.
	if _, _, err := g.Nodes().ForEachDocValuesAsOf("Metric", []string{"score"}, 0, func(types.NodeID, []any, []bool) bool {
		return true
	}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatalf("txAt=0 err=%v, want ErrInvalidTimeRange", err)
	}

	// Non-uniform column → ok=false, zero rows streamed.
	rows := 0
	_, ok, err := g.Nodes().ForEachDocValuesAsOf("Metric", []string{"score"}, t0+1, func(types.NodeID, []any, []bool) bool {
		rows++
		return true
	})
	if err != nil {
		t.Fatalf("ForEachDocValuesAsOf mixed column: %v", err)
	}
	if ok {
		t.Fatal("mixed-type column reported ok=true, want ok=false (unbuildable)")
	}
	if rows != 0 {
		t.Fatalf("ok=false stream emitted %d rows, want 0", rows)
	}
}

// TestDocValuesSnapshotAsOf_ContractEdges mirrors the ForEachDocValuesAsOf contract
// test: a non-positive txAt is rejected, and a non-uniform (mixed-type) column yields
// ok=false with a nil snapshot — the caller falls back to the row path.
func TestDocValuesSnapshotAsOf_ContractEdges(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	a, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": int64(1)})
	if err != nil {
		t.Fatalf("add a: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Metric"}, map[string]any{"score": "not-a-number"}); err != nil {
		t.Fatalf("add b: %v", err)
	}
	txfRaw, _ := g.Resolve().NodeProperty(a, "tkg_tx_from")
	t0 := txfRaw.(types.Instant)
	if _, err := g.Temporal().AdvanceClock(t0 + 1_000_000); err != nil {
		t.Fatalf("AdvanceClock: %v", err)
	}

	// txAt <= 0 → ErrInvalidTimeRange.
	if _, _, _, err := g.Nodes().DocValuesSnapshotAsOf("Metric", []string{"score"}, 0); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatalf("txAt=0 err=%v, want ErrInvalidTimeRange", err)
	}

	// Non-uniform column → ok=false, nil snapshot.
	snap, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("Metric", []string{"score"}, t0+1)
	if err != nil {
		t.Fatalf("DocValuesSnapshotAsOf mixed column: %v", err)
	}
	if ok || snap != nil {
		t.Fatalf("mixed-type column reported ok=%v snap!=nil=%v, want ok=false + nil snap", ok, snap != nil)
	}
}
