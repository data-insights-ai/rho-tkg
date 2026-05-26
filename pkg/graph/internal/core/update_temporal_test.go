package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Phase 2 — Update accepts tkg_valid_from / tkg_valid_to.
// Rules: 2 (Node/Rel parity), 4 (errors.Is at every call layer),
// 15 (two-phase: write→update→query at intermediate VT).

func TestUpdateNode_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	clk.Advance(time.Millisecond)
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"x":              int64(2),
	})
	if err != nil {
		t.Fatalf("Update with valid_from: %v", err)
	}
	if got := updated.Temporal().ValidFrom; got != 2000 {
		t.Fatalf("new version ValidFrom = %d, want 2000", got)
	}
}

func TestUpdateRel_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, err := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"w":              int64(1),
	})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}

	clk.Advance(time.Millisecond)
	updated, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"w":              int64(2),
	})
	if err != nil {
		t.Fatalf("Update with valid_from: %v", err)
	}
	if got := updated.Temporal().ValidFrom; got != 2000 {
		t.Fatalf("new rel version ValidFrom = %d, want 2000", got)
	}
}

func TestUpdateNode_AcceptsValidTo(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})

	clk.Advance(time.Millisecond)
	updated, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"tkg_valid_to":   types.Instant(3000),
	})
	if err != nil {
		t.Fatalf("Update with valid_to: %v", err)
	}
	if got := updated.Temporal().ValidTo; got != 3000 {
		t.Fatalf("new version ValidTo = %d, want 3000", got)
	}
}

func TestUpdateNode_RejectsValidFromBeforePrevious(t *testing.T) {
	// Rule 4: sentinel test with errors.Is.
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	_, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(5000),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(5000),
	})

	clk.Advance(time.Millisecond)
	_, err = g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
	})
	if !errors.Is(err, ErrValidFromBeforePrevious) {
		t.Fatalf("Update backwards = %v, want ErrValidFromBeforePrevious", err)
	}
}

func TestUpdateRel_RejectsValidFromBeforePrevious(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(5000),
	})

	clk.Advance(time.Millisecond)
	_, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
	})
	if !errors.Is(err, ErrValidFromBeforePrevious) {
		t.Fatalf("Update backwards = %v, want ErrValidFromBeforePrevious", err)
	}
}

func TestUpdateNode_RejectsValidFromGteValidTo(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})

	clk.Advance(time.Millisecond)
	_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
		"tkg_valid_to":   types.Instant(2000),
	})
	if !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("Update valid_from >= valid_to = %v, want ErrInvalidTimeRange", err)
	}
}

func TestUpdateNode_RejectsCreatedAtAsReserved(t *testing.T) {
	// tkg_created_at is not in the temporal-update allowlist (per-entity, not
	// per-version). Should still error with ErrReservedPrefix.
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_created_at": types.Instant(1000),
	})
	if !errors.Is(err, types.ErrReservedPrefix) {
		t.Fatalf("Update with tkg_created_at = %v, want ErrReservedPrefix", err)
	}
}

func TestUpdateNode_TwoPhaseBitemporalRoundTrip(t *testing.T) {
	// Rule 15: two-phase. Create at VT=1000, Update with VT=2000.
	// Query NodeAt(1500) → original. Query NodeAt(2500) → updated.
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})

	clk.Advance(time.Millisecond)
	_, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"x":              int64(2),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := g.Temporal.NodeAt(n.ID(), 1500)
	if err != nil {
		t.Fatalf("NodeAt 1500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(1) {
		t.Fatalf("NodeAt 1500 x = %v, want 1 (genesis)", v)
	}

	got, err = g.Temporal.NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt 2500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("NodeAt 2500 x = %v, want 2 (updated)", v)
	}
}

func TestUpdateRel_TwoPhaseBitemporalRoundTrip(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"w":              int64(1),
	})

	clk.Advance(time.Millisecond)
	_, err := g.Rels.Update(context.Background(), r.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"w":              int64(2),
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := g.Temporal.RelAt(r.ID(), 1500)
	if err != nil {
		t.Fatalf("RelAt 1500: %v", err)
	}
	if v, _ := got.GetProperty("w"); v != int64(1) {
		t.Fatalf("RelAt 1500 w = %v, want 1", v)
	}

	got, err = g.Temporal.RelAt(r.ID(), 2500)
	if err != nil {
		t.Fatalf("RelAt 2500: %v", err)
	}
	if v, _ := got.GetProperty("w"); v != int64(2) {
		t.Fatalf("RelAt 2500 w = %v, want 2", v)
	}
}

func TestUpdateNode_TxPath_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.UpdateNode(n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"x":              int64(1),
	}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.UpdateNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, _ := g.Nodes.Get(context.Background(), n.ID())
	if vf := got.Temporal().ValidFrom; vf != 2000 {
		t.Fatalf("post-tx ValidFrom = %d, want 2000", vf)
	}
}

// UpdateInPlace temporal — report 6.2 followup. Rule 2 parity Node/Rel.

func TestUpdateInPlaceNode_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})

	// UpdateInPlace preserves version, but the row's TemporalMetadata is
	// rewritten directly.
	updated, err := g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"tkg_valid_to":   types.Instant(5000),
		"x":              int64(2),
	})
	if err != nil {
		t.Fatalf("UpdateInPlace: %v", err)
	}
	if updated.Version() != n.Version() {
		t.Fatalf("UpdateInPlace bumped version: %d → %d", n.Version(), updated.Version())
	}
	tm := updated.Temporal()
	if tm.ValidFrom != 2000 {
		t.Fatalf("ValidFrom = %d, want 2000", tm.ValidFrom)
	}
	if tm.ValidTo != 5000 {
		t.Fatalf("ValidTo = %d, want 5000", tm.ValidTo)
	}
	if v, _ := updated.GetProperty("x"); v != int64(2) {
		t.Fatalf("x = %v, want 2", v)
	}
}

func TestUpdateInPlaceRel_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)
	a, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"L"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"w":              int64(1),
	})

	updated, err := g.Rels.UpdateInPlace(context.Background(), r.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"tkg_valid_to":   types.Instant(5000),
		"w":              int64(2),
	})
	if err != nil {
		t.Fatalf("UpdateInPlace rel: %v", err)
	}
	if updated.Version() != r.Version() {
		t.Fatalf("UpdateInPlace bumped rel version: %d → %d", r.Version(), updated.Version())
	}
	tm := updated.Temporal()
	if tm.ValidFrom != 2000 {
		t.Fatalf("rel ValidFrom = %d, want 2000", tm.ValidFrom)
	}
	if tm.ValidTo != 5000 {
		t.Fatalf("rel ValidTo = %d, want 5000", tm.ValidTo)
	}
	if v, _ := updated.GetProperty("w"); v != int64(2) {
		t.Fatalf("w = %v, want 2", v)
	}
}

func TestUpdateInPlaceNode_TemporalOnlyMutationStillWrites(t *testing.T) {
	// Even a temporal-only update (no business-property changes) must be
	// applied — the tmp.present gate in the mutates check covers this.
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	updated, err := g.Nodes.UpdateInPlace(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
	})
	if err != nil {
		t.Fatalf("UpdateInPlace temporal-only: %v", err)
	}
	if updated.Temporal().ValidFrom != 3000 {
		t.Fatalf("ValidFrom = %d, want 3000", updated.Temporal().ValidFrom)
	}
}

func TestUpdateNode_BatchPath_AcceptsValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := b.UpdateNode(n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(2000),
		"x":              int64(1),
	}); err != nil {
		t.Fatalf("batch UpdateNode: %v", err)
	}
	result, err := b.Execute()
	if err != nil {
		t.Fatalf("batch Execute: %v", err)
	}
	if result.Failed != 0 {
		t.Fatalf("batch failures: %d: %v", result.Failed, result.Errors)
	}

	got, _ := g.Nodes.Get(context.Background(), n.ID())
	if vf := got.Temporal().ValidFrom; vf != 2000 {
		t.Fatalf("post-batch ValidFrom = %d, want 2000", vf)
	}
}
