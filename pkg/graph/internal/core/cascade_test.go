package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

// Phase 3 — cascade / timeline tile. After Update with explicit ValidFrom,
// the previous version's effective interval closes at the new ValidFrom
// (resolver vEnd uses next.ValidFrom). Rule 15 two-phase + rule 2 parity.

func TestSetNodeVersionInterval_TimelineTiles(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	// Genesis at VT=1000 with state X=1.
	n, err := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"x":              int64(1),
	})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}

	// SetNodeVersionInterval: state X=2 for [2000, ∞).
	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 0, map[string]any{
		"x": int64(2),
	}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	// Query at VT=1500 → state X=1 (genesis interval [1000, 2000)).
	got, err := g.Temporal.NodeAt(n.ID(), 1500)
	if err != nil {
		t.Fatalf("NodeAt 1500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(1) {
		t.Fatalf("at VT=1500 x = %v, want 1", v)
	}

	// Query at VT=2500 → state X=2 (new interval [2000, ∞)).
	got, err = g.Temporal.NodeAt(n.ID(), 2500)
	if err != nil {
		t.Fatalf("NodeAt 2500: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(2) {
		t.Fatalf("at VT=2500 x = %v, want 2", v)
	}

	// Critical: query at VT=1999 (just before new ValidFrom) → still X=1.
	// This proves the resolver tiles: genesis vEnd = 2000 (next.ValidFrom).
	got, err = g.Temporal.NodeAt(n.ID(), 1999)
	if err != nil {
		t.Fatalf("NodeAt 1999: %v", err)
	}
	if v, _ := got.GetProperty("x"); v != int64(1) {
		t.Fatalf("at VT=1999 x = %v, want 1 (tile boundary)", v)
	}
}

func TestSetRelVersionInterval_TimelineTiles(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	a, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"A"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "KNOWS", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"w":              int64(1),
	})

	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetRelVersionInterval(context.Background(), r.ID(), 2000, 0, map[string]any{
		"w": int64(2),
	}); err != nil {
		t.Fatalf("SetRelVersionInterval: %v", err)
	}

	got, _ := g.Temporal.RelAt(r.ID(), 1500)
	if v, _ := got.GetProperty("w"); v != int64(1) {
		t.Fatalf("Rel at 1500 w = %v, want 1", v)
	}
	got, _ = g.Temporal.RelAt(r.ID(), 2500)
	if v, _ := got.GetProperty("w"); v != int64(2) {
		t.Fatalf("Rel at 2500 w = %v, want 2", v)
	}
	got, _ = g.Temporal.RelAt(r.ID(), 1999)
	if v, _ := got.GetProperty("w"); v != int64(1) {
		t.Fatalf("Rel at 1999 w = %v, want 1 (tile boundary)", v)
	}
}

// Full cascade — five overlap classifications. Each test sets up an existing
// timeline then runs SetNodeVersionInterval with a target that exercises one
// classification, then asserts post-cascade resolver behaviour.

func TestCascade_CloseRight_OpenEndedTarget(t *testing.T) {
	// Existing: [1000, ∞) state=A.
	// Cascade: [2000, ∞) state=B.
	// Expected: A closes at 2000, B is current open-ended.
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 0, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade: %v", err)
	}

	for _, tc := range []struct {
		at   types.Instant
		want string
	}{{1500, "A"}, {1999, "A"}, {2000, "B"}, {5000, "B"}} {
		got, _ := g.Temporal.NodeAt(n.ID(), tc.at)
		if v, _ := got.GetProperty("state"); v != tc.want {
			t.Errorf("at %d state=%v, want %s", tc.at, v, tc.want)
		}
	}
}

func TestCascade_Eclipse(t *testing.T) {
	g := newTxTimeGraph(t)

	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	// Add C as open-ended after A via cascade close-right.
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 3000, 0, map[string]any{
		"state": "C",
	}); err != nil {
		t.Fatalf("cascade to C: %v", err)
	}

	// Cascade [500, 4000) Wipe over both.
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 500, 4000, map[string]any{
		"state": "Wipe",
	}); err != nil {
		t.Fatalf("cascade Wipe: %v", err)
	}

	got, _ := g.Temporal.NodeAt(n.ID(), 1500)
	if v, _ := got.GetProperty("state"); v != "Wipe" {
		t.Errorf("at 1500 state=%v, want Wipe (A eclipsed)", v)
	}
	got, _ = g.Temporal.NodeAt(n.ID(), 3500)
	if v, _ := got.GetProperty("state"); v != "Wipe" {
		t.Errorf("at 3500 state=%v, want Wipe (C eclipsed)", v)
	}
}

func TestCascade_OpenLeft(t *testing.T) {
	// Existing: [1000, 5000) state=A.
	// Cascade: [800, 2000) state=B.
	// A: starts at 1000 (>= 800), ends at 5000 (> 2000) → open-left:
	// A becomes [2000, 5000).
	// Resolver expected:
	//   500 → no version (before B and A)
	//   1500 → B (cascade interval)
	//   3000 → A (re-opened from 2000)
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_valid_to":   types.Instant(5000),
		"state":          "A",
	})
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 800, 2000, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade: %v", err)
	}
	got, _ := g.Temporal.NodeAt(n.ID(), 1500)
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Errorf("at 1500 state=%v, want B", v)
	}
	got, _ = g.Temporal.NodeAt(n.ID(), 3000)
	if v, _ := got.GetProperty("state"); v != "A" {
		t.Errorf("at 3000 state=%v, want A (open-left)", v)
	}
}

func TestCascade_Split(t *testing.T) {
	// Existing: [1000, 5000) state=A.
	// Cascade: [2000, 3000) state=B.
	// A spans [1000, 5000) which contains [2000, 3000) — split.
	// Resolver expected:
	//   1500 → A (left fragment)
	//   2500 → B (cascade interval)
	//   3500 → A (right fragment)
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_valid_to":   types.Instant(5000),
		"state":          "A",
	})
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 3000, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade: %v", err)
	}
	for _, tc := range []struct {
		at   types.Instant
		want string
	}{{1500, "A"}, {1999, "A"}, {2000, "B"}, {2999, "B"}, {3000, "A"}, {4500, "A"}} {
		got, _ := g.Temporal.NodeAt(n.ID(), tc.at)
		if v, _ := got.GetProperty("state"); v != tc.want {
			t.Errorf("at %d state=%v, want %s (split)", tc.at, v, tc.want)
		}
	}
}

func TestCascade_MidHistoryInsertion(t *testing.T) {
	// Existing two-tile timeline: [1000, 3000) A, [3000, ∞) C.
	// Cascade insert at [1500, 2500) B.
	// Expected:
	//   1500 → B (cascade)
	//   2500 → A (right of B, still in A's original range)
	// Wait: A's right is closed by C at 3000. So A's right fragment is [2500, 3000).
	// Then C is [3000, ∞).
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})
	clk.Advance(time.Millisecond)
	if _, err := g.Nodes.Update(context.Background(), n.ID(), map[string]any{
		"tkg_valid_from": types.Instant(3000),
		"state":          "C",
	}); err != nil {
		t.Fatalf("update to C: %v", err)
	}

	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 1500, 2500, map[string]any{
		"state": "B",
	}); err != nil {
		t.Fatalf("cascade insert B: %v", err)
	}

	for _, tc := range []struct {
		at   types.Instant
		want string
	}{{1200, "A"}, {1500, "B"}, {2499, "B"}, {2500, "A"}, {2999, "A"}, {3000, "C"}, {9999, "C"}} {
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

func TestCascade_RejectsZeroValidFrom(t *testing.T) {
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	_, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 0, 0, nil)
	if !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("zero validFrom = %v, want ErrInvalidTimeRange", err)
	}
}

func TestCascade_RejectsInvertedInterval(t *testing.T) {
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
	})
	_, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 1500, nil)
	if !errors.Is(err, ErrInvalidTimeRange) {
		t.Fatalf("inverted interval = %v, want ErrInvalidTimeRange", err)
	}
}

// Rel parity (rule 2): repeat the closeRight, eclipse, split classifications.

func TestCascadeRel_CloseRight(t *testing.T) {
	g := newTxTimeGraph(t)
	a, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"s":              "A",
	})
	if _, err := g.Temporal.SetRelVersionInterval(context.Background(), r.ID(), 2000, 0, map[string]any{"s": "B"}); err != nil {
		t.Fatalf("cascade rel: %v", err)
	}
	got, _ := g.Temporal.RelAt(r.ID(), 1500)
	if v, _ := got.GetProperty("s"); v != "A" {
		t.Errorf("rel at 1500 s=%v, want A", v)
	}
	got, _ = g.Temporal.RelAt(r.ID(), 2500)
	if v, _ := got.GetProperty("s"); v != "B" {
		t.Errorf("rel at 2500 s=%v, want B", v)
	}
}

func TestCascadeRel_Split(t *testing.T) {
	g := newTxTimeGraph(t)
	a, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"tkg_valid_to":   types.Instant(5000),
		"s":              "A",
	})
	if _, err := g.Temporal.SetRelVersionInterval(context.Background(), r.ID(), 2000, 3000, map[string]any{"s": "B"}); err != nil {
		t.Fatalf("cascade split rel: %v", err)
	}
	for _, tc := range []struct {
		at   types.Instant
		want string
	}{{1500, "A"}, {2500, "B"}, {3500, "A"}} {
		got, _ := g.Temporal.RelAt(r.ID(), tc.at)
		if v, _ := got.GetProperty("s"); v != tc.want {
			t.Errorf("rel at %d s=%v, want %s", tc.at, v, tc.want)
		}
	}
}

func TestCascade_BatchPath(t *testing.T) {
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := b.SetNodeVersionInterval(n.ID(), 2000, 0, map[string]any{"state": "B"}); err != nil {
		t.Fatalf("batch SetNodeVersionInterval: %v", err)
	}
	res, err := b.Execute()
	if err != nil {
		t.Fatalf("batch Execute: %v", err)
	}
	if res.Failed != 0 || res.Updated != 1 {
		t.Fatalf("batch result: Failed=%d Updated=%d, errors=%v", res.Failed, res.Updated, res.Errors)
	}

	got, _ := g.Temporal.NodeAt(n.ID(), 1500)
	if v, _ := got.GetProperty("state"); v != "A" {
		t.Errorf("at 1500 state=%v, want A", v)
	}
	got, _ = g.Temporal.NodeAt(n.ID(), 2500)
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Errorf("at 2500 state=%v, want B", v)
	}
}

func TestCascade_TxPath(t *testing.T) {
	g := newTxTimeGraph(t)
	n, _ := g.Nodes.Add(context.Background(), []string{"L"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"state":          "A",
	})

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.SetNodeVersionInterval(n.ID(), 2000, 0, map[string]any{"state": "B"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx SetNodeVersionInterval: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, _ := g.Temporal.NodeAt(n.ID(), 2500)
	if v, _ := got.GetProperty("state"); v != "B" {
		t.Errorf("at 2500 state=%v, want B", v)
	}
}

func TestCascadeRel_BatchPath(t *testing.T) {
	g := newTxTimeGraph(t)
	a, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	b, _ := g.Nodes.Add(context.Background(), []string{"N"}, nil)
	r, _ := g.Rels.AddByID(context.Background(), "T", a.ID(), b.ID(), map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"s":              "A",
	})

	bb, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if err := bb.SetRelVersionInterval(r.ID(), 2000, 0, map[string]any{"s": "B"}); err != nil {
		t.Fatalf("batch SetRelVersionInterval: %v", err)
	}
	res, err := bb.Execute()
	if err != nil {
		t.Fatalf("batch Execute: %v", err)
	}
	if res.Failed != 0 {
		t.Fatalf("batch failures: %v", res.Errors)
	}
	got, _ := g.Temporal.RelAt(r.ID(), 2500)
	if v, _ := got.GetProperty("s"); v != "B" {
		t.Errorf("rel at 2500 s=%v, want B", v)
	}
}

func TestSetNodeVersionInterval_AdversarialMultiVersion(t *testing.T) {
	// Rule 16: adversarial. Multiple historical transitions tile correctly.
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)

	n, _ := g.Nodes.Add(context.Background(), []string{"A"}, map[string]any{
		"tkg_valid_from": types.Instant(1000),
		"role":           "engineer",
	})
	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 2000, 0, map[string]any{
		"role": "manager",
	}); err != nil {
		t.Fatalf("SVI to manager: %v", err)
	}
	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(context.Background(), n.ID(), 3000, 0, map[string]any{
		"role": "director",
	}); err != nil {
		t.Fatalf("SVI to director: %v", err)
	}

	// Timeline should be:
	//   [1000, 2000) engineer
	//   [2000, 3000) manager
	//   [3000, ∞)    director
	cases := []struct {
		at   types.Instant
		want string
	}{
		{1500, "engineer"},
		{1999, "engineer"},
		{2000, "manager"},
		{2500, "manager"},
		{2999, "manager"},
		{3000, "director"},
		{9999, "director"},
	}
	for _, tc := range cases {
		got, err := g.Temporal.NodeAt(n.ID(), tc.at)
		if err != nil {
			t.Fatalf("NodeAt %d: %v", tc.at, err)
		}
		role, _ := got.GetProperty("role")
		if role != tc.want {
			t.Errorf("at VT=%d role=%v, want %s", tc.at, role, tc.want)
		}
	}
}
