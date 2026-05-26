package core

import (
	"context"
	"errors"
	"testing"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Phase 0 — bitemporal queries (valid time × transaction time).
// Rule 15: two-phase tests for history-aware methods.
// Rule 2: Node and Rel parity.

func TestNodeAtTx_BitemporalTwoPhase(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	// Phase 1: create node with property X=1 at T1.
	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	t1 := n.Temporal().TxFrom

	// Phase 2: advance, update to X=2 at T2.
	clk.Advance(2 * time.Millisecond)
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"x": int64(2)})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	t2 := updated.Temporal().TxFrom

	// Query bitemporally: valid-at = now (any), tx-at = T1.5 → should see X=1.
	mid := t1 + (t2-t1)/2
	if mid == t1 {
		mid = t1 + 1
	}
	got, err := g.Temporal.NodeAtTx(n.ID(), clk.PeekInstant(), mid)
	if err != nil {
		t.Fatalf("NodeAtTx mid: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(1) {
		t.Fatalf("at TxAt=mid x = %v, want 1", v)
	}

	// Query bitemporally: tx-at = T2 → should see X=2 (the post-update version).
	got, err = g.Temporal.NodeAtTx(n.ID(), clk.PeekInstant(), t2)
	if err != nil {
		t.Fatalf("NodeAtTx t2: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("at TxAt=t2 x = %v, want 2", v)
	}
}

func TestRelAtTx_BitemporalTwoPhase(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	t1 := r.Temporal().TxFrom

	clk.Advance(2 * time.Millisecond)
	updated, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"w": int64(2)})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	t2 := updated.Temporal().TxFrom

	mid := t1 + (t2-t1)/2
	if mid == t1 {
		mid = t1 + 1
	}
	got, err := g.Temporal.RelAtTx(r.ID(), clk.PeekInstant(), mid)
	if err != nil {
		t.Fatalf("RelAtTx mid: %v", err)
	}
	if v, _ := got.GetProperty("w"); v != int64(1) {
		t.Fatalf("at TxAt=mid w = %v, want 1", v)
	}

	got, err = g.Temporal.RelAtTx(r.ID(), clk.PeekInstant(), t2)
	if err != nil {
		t.Fatalf("RelAtTx t2: %v", err)
	}
	if v, _ := got.GetProperty("w"); v != int64(2) {
		t.Fatalf("at TxAt=t2 w = %v, want 2", v)
	}
}

func TestNodeAtTx_BeforeCreateReturnsNoVersion(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	clk.Advance(100 * time.Millisecond)
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)

	// TxAt = before node existed; should return ErrNoVersionValidAt.
	_, err := g.Temporal.NodeAtTx(n.ID(), clk.PeekInstant(), 1)
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("NodeAtTx before-create = %v, want ErrNoVersionValidAt", err)
	}
}

func TestRelAtTx_BeforeCreateReturnsNoVersion(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	clk.Advance(100 * time.Millisecond)
	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), nil)

	_, err := g.Temporal.RelAtTx(r.ID(), clk.PeekInstant(), 1)
	if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
		t.Fatalf("RelAtTx before-create = %v, want ErrNoVersionValidAt", err)
	}
}

func TestNodeAtTx_TxAtZeroEquivalentToNodeAt(t *testing.T) {
	// Backward-compat: TxAt == 0 produces identical result to NodeAt.
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"x": int64(1)})
	clk.Advance(time.Millisecond)
	_, _ = g.Nodes.Update(context.Background(), n.ID(), map[string]any{"x": int64(2)})

	at := clk.PeekInstant()

	a, err := g.Temporal.NodeAt(n.ID(), at)
	if err != nil {
		t.Fatalf("NodeAt: %v", err)
	}
	b, err := g.Temporal.NodeAtTx(n.ID(), at, 0)
	if err != nil {
		t.Fatalf("NodeAtTx: %v", err)
	}
	if a.Version() != b.Version() {
		t.Fatalf("TxAt=0 mismatch: NodeAt v=%d, NodeAtTx v=%d", a.Version(), b.Version())
	}
}

func TestNodesAtTx_FiltersByBothAxes(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	midTime := clk.PeekInstant()
	clk.Advance(time.Millisecond)
	n2, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)

	// NodesAtTx at midTime: should see n1 only (n2 didn't exist yet).
	got, err := g.Temporal.NodesAtTx(clk.PeekInstant(), midTime)
	if err != nil {
		t.Fatalf("NodesAtTx: %v", err)
	}
	found1, found2 := false, false
	for _, n := range got {
		if n.ID() == n1.ID() {
			found1 = true
		}
		if n.ID() == n2.ID() {
			found2 = true
		}
	}
	if !found1 {
		t.Error("n1 should be present at TxAt=midTime")
	}
	if found2 {
		t.Error("n2 must NOT be present at TxAt=midTime — created after")
	}
}

func TestRelsAtTx_FiltersByBothAxes(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r1, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), nil)
	midTime := clk.PeekInstant()
	clk.Advance(time.Millisecond)
	r2, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), nil)

	got, err := g.Temporal.RelsAtTx(clk.PeekInstant(), midTime)
	if err != nil {
		t.Fatalf("RelsAtTx: %v", err)
	}
	found1, found2 := false, false
	for _, r := range got {
		if r.ID() == r1.ID() {
			found1 = true
		}
		if r.ID() == r2.ID() {
			found2 = true
		}
	}
	if !found1 {
		t.Error("r1 should be present at TxAt=midTime")
	}
	if found2 {
		t.Error("r2 must NOT be present at TxAt=midTime")
	}
}

func TestQueryOptsTxAt_NodesByLabel(t *testing.T) {
	// QueryOpts.TxAt threads through opts-based queries (e.g. NodesByLabel).
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n1, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)
	midTime := clk.PeekInstant()
	clk.Advance(time.Millisecond)
	n2, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)

	// At TxAt=midTime, only n1 should match.
	got, err := g.Nodes.ByLabel("L", storepkg.QueryOpts{
		ValidAt: clk.PeekInstant(),
		TxAt:    midTime,
	})
	if err != nil {
		t.Fatalf("ByLabel TxAt: %v", err)
	}
	found1, found2 := false, false
	for _, n := range got {
		if n.ID() == n1.ID() {
			found1 = true
		}
		if n.ID() == n2.ID() {
			found2 = true
		}
	}
	if !found1 {
		t.Error("n1 should be present in TxAt-filtered result")
	}
	if found2 {
		t.Error("n2 must NOT be present in TxAt-filtered result")
	}
}

func TestQueryOptsTxAt_RelsByType(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r1, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), nil)
	midTime := clk.PeekInstant()
	clk.Advance(time.Millisecond)
	r2, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), nil)

	got, err := g.Rels.ByType("T", storepkg.QueryOpts{
		ValidAt: clk.PeekInstant(),
		TxAt:    midTime,
	})
	if err != nil {
		t.Fatalf("ByType TxAt: %v", err)
	}
	found1, found2 := false, false
	for _, r := range got {
		if r.ID() == r1.ID() {
			found1 = true
		}
		if r.ID() == r2.ID() {
			found2 = true
		}
	}
	if !found1 {
		t.Error("r1 should be present in TxAt-filtered result")
	}
	if found2 {
		t.Error("r2 must NOT be present in TxAt-filtered result")
	}
}

// Phase 1: Update must NOT inherit ValidFrom from the previous version.
// New non-genesis version should have ValidFrom == 0 (resolver derives from UpdatedAt).
// Rule 2: Node + Rel parity.

func TestUpdateNode_NoValidFromInheritance(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got := n.Temporal().ValidFrom; got != 1000 {
		t.Fatalf("genesis ValidFrom = %d, want 1000", got)
	}

	clk.Advance(time.Millisecond)
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"x": int64(1)})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := updated.Temporal().ValidFrom; got != 0 {
		t.Fatalf("post-Update ValidFrom = %d, want 0 (no inheritance)", got)
	}
}

func TestUpdateRel_NoValidFromInheritance(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if got := r.Temporal().ValidFrom; got != 1000 {
		t.Fatalf("genesis Rel ValidFrom = %d, want 1000", got)
	}

	clk.Advance(time.Millisecond)
	updated, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{"w": int64(1)})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := updated.Temporal().ValidFrom; got != 0 {
		t.Fatalf("post-Update Rel ValidFrom = %d, want 0 (no inheritance)", got)
	}
}

func TestUpdateNode_ResolverUsesUpdatedAtForNonGenesis(t *testing.T) {
	// After Phase 1, the resolver computes vStart from UpdatedAt for non-genesis
	// versions (the old inheritance pattern no longer creates false ValidFrom values).
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{"x": int64(1)})
	t1 := n.Temporal().TxFrom

	clk.Advance(2 * time.Millisecond)
	updated, _ := g.Nodes.Update(context.Background(), n.ID(), map[string]any{"x": int64(2)})
	t2 := updated.Temporal().UpdatedAt

	// Query at t1 + 1: should see version 0 (X=1).
	got, err := g.Temporal.NodeAt(n.ID(), t1+1)
	if err != nil {
		t.Fatalf("NodeAt t1+1: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(1) {
		t.Fatalf("at t1+1 x = %v, want 1", v)
	}

	// Query at t2: should see version 1 (X=2). t2 is the new version's UpdatedAt
	// — resolver's vStart for v1.
	got, err = g.Temporal.NodeAt(n.ID(), t2)
	if err != nil {
		t.Fatalf("NodeAt t2: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("at t2 x = %v, want 2", v)
	}
}

func TestVersionVisibleAtTx_Helper(t *testing.T) {
	// Direct unit test of the helper. Rule 4: sentinel test (no errors here,
	// just predicate verification on boundary conditions).
	tests := []struct {
		name string
		tm   *types.TemporalMetadata
		txAt types.Instant
		want bool
	}{
		{"nil tm always visible", nil, 100, true},
		{"txAt=0 no filter", &types.TemporalMetadata{TxFrom: 10, TxTo: 20}, 0, true},
		{"before TxFrom invisible", &types.TemporalMetadata{TxFrom: 10}, 5, false},
		{"at TxFrom visible", &types.TemporalMetadata{TxFrom: 10}, 10, true},
		{"between visible", &types.TemporalMetadata{TxFrom: 10, TxTo: 20}, 15, true},
		{"at TxTo invisible (half-open)", &types.TemporalMetadata{TxFrom: 10, TxTo: 20}, 20, false},
		{"after TxTo invisible", &types.TemporalMetadata{TxFrom: 10, TxTo: 20}, 25, false},
		{"open-ended TxTo visible far future", &types.TemporalMetadata{TxFrom: 10, TxTo: 0}, 1000, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := versionVisibleAtTx(tc.tm, tc.txAt)
			if got != tc.want {
				t.Fatalf("versionVisibleAtTx = %v, want %v", got, tc.want)
			}
		})
	}
}
