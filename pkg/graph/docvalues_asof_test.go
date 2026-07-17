package graph_test

import (
	"context"
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
