package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestCascade_BeliefIsConsistentPerTxAt encodes the bitemporal law that at any
// transaction time txAt the DB believes ONE consistent world (a complete
// valid-time timeline). A valid-time correction recorded at tx=Tc must be
// invisible to NodeAtTx(_, txAt) for any txAt < Tc — before Tc the DB had not
// decided the correction, so the world believed then is the pre-correction
// world, with NO holes and NO leaked corrected values.
//
// Before the append-only cascade fix, SetNodeVersionInterval rewrote/split
// existing rows in place while preserving their original TxFrom, which left a
// hole in the pre-correction belief (the inserted interval's slot returned
// nothing). Runs node + rel across all three backends — the resolver/cascade
// must agree everywhere (rule 2 parity, MR protocol phase 2).
func TestCascade_BeliefIsConsistentPerTxAt(t *testing.T) {
	for _, backend := range storeContractBackends() {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			g := backend.newGraph(t, backend.snowflakeNodeID)
			clk := useTestClock(t, g)
			ctx := context.Background()

			// Genesis: state x=1 valid over [1000, ∞).
			n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"x":              int64(1),
			})
			if err != nil {
				t.Fatalf("Add node: %v", err)
			}
			txBefore := clk.PeekInstant()

			// Correction recorded later: x=2 for [2000, 3000), splitting the open
			// genesis into [1000,2000)→1, [2000,3000)→2, [3000,∞)→1.
			clk.Advance(time.Millisecond)
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 3000, map[string]any{"x": int64(2)}); err != nil {
				t.Fatalf("SetNodeVersionInterval: %v", err)
			}
			txAfter := clk.PeekInstant()

			nodeX := func(validAt, txAt types.Instant) (int64, bool) {
				node, err := g.Temporal.NodeAtTx(n.ID(), validAt, txAt)
				if err != nil || node == nil {
					return 0, false
				}
				v, _ := node.Properties().Get("x")
				iv, ok := v.(int64)
				return iv, ok
			}
			assertWorld(t, "node", txBefore, txAfter, nodeX)
		})
	}
}

// TestCascade_BeliefIsConsistentPerTxAt_Rel is the relationship mirror.
func TestCascade_BeliefIsConsistentPerTxAt_Rel(t *testing.T) {
	for _, backend := range storeContractBackends() {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			g := backend.newGraph(t, backend.snowflakeNodeID)
			clk := useTestClock(t, g)
			ctx := context.Background()

			a, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
			if err != nil {
				t.Fatalf("Add node a: %v", err)
			}
			b, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
			if err != nil {
				t.Fatalf("Add node b: %v", err)
			}
			r, err := g.Rels.Add(ctx, "LINKS", a, b, map[string]any{
				"tkg_valid_from": types.Instant(1000),
				"x":              int64(1),
			})
			if err != nil {
				t.Fatalf("Add rel: %v", err)
			}
			txBefore := clk.PeekInstant()

			clk.Advance(time.Millisecond)
			if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), 2000, 3000, map[string]any{"x": int64(2)}); err != nil {
				t.Fatalf("SetRelVersionInterval: %v", err)
			}
			txAfter := clk.PeekInstant()

			relX := func(validAt, txAt types.Instant) (int64, bool) {
				rel, err := g.Temporal.RelAtTx(r.ID(), validAt, txAt)
				if err != nil || rel == nil {
					return 0, false
				}
				v, _ := rel.Properties().Get("x")
				iv, ok := v.(int64)
				return iv, ok
			}
			assertWorld(t, "rel", txBefore, txAfter, relX)
		})
	}
}

// TestCascade_CorrectionSpanningBoundary exercises the overlay path: a
// correction whose interval spans an EXISTING valid-time boundary, so an older
// still-covering tile and the newer correction both cover part of the range. The
// newer belief (higher TxFrom) must win — resolveNodeVersionAt's overlap rule.
func TestCascade_CorrectionSpanningBoundary(t *testing.T) {
	g := newTxTimeGraph(t)
	clk := useTestClock(t, g)
	ctx := context.Background()

	// Genesis [1000,∞)→1.
	n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(1)})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Normal update tiles a second segment: [2000,∞)→2, [1000,2000)→1.
	clk.Advance(time.Millisecond)
	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"tkg_valid_from": types.Instant(2000), "x": int64(2)}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	txBefore := clk.PeekInstant()
	// Correction [1500,2500)→3 spans the 2000 boundary.
	clk.Advance(time.Millisecond)
	if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1500, 2500, map[string]any{"x": int64(3)}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	x := func(validAt, txAt types.Instant) (int64, bool) {
		node, err := g.Temporal.NodeAtTx(n.ID(), validAt, txAt)
		if err != nil || node == nil {
			return 0, false
		}
		v, _ := node.Properties().Get("x")
		iv, ok := v.(int64)
		return iv, ok
	}

	// Post-correction world: [1000,1500)→1, [1500,2500)→3, [2500,∞)→2.
	for _, tc := range []struct {
		validAt types.Instant
		want    int64
	}{{1200, 1}, {1800, 3}, {2200, 3}, {2700, 2}} {
		if got, ok := x(tc.validAt, 0); !ok || got != tc.want {
			t.Errorf("post NodeAt(%d)=%d ok=%v, want %d", tc.validAt, got, ok, tc.want)
		}
	}
	// Pre-correction world (the original two-segment timeline), no leak:
	// 1800→1, 2200→2.
	if got, ok := x(1800, txBefore); !ok || got != 1 {
		t.Errorf("pre NodeAtTx(1800,txBefore)=%d ok=%v, want 1", got, ok)
	}
	if got, ok := x(2200, txBefore); !ok || got != 2 {
		t.Errorf("pre NodeAtTx(2200,txBefore)=%d ok=%v, want 2 (the boundary tile, pre-correction)", got, ok)
	}
}

// assertWorld checks the post-correction tiled world [1,2,1] at txAfter and the
// untouched pre-correction world (all 1, no holes) at txBefore.
func assertWorld(t *testing.T, kind string, txBefore, txAfter types.Instant, xAt func(validAt, txAt types.Instant) (int64, bool)) {
	t.Helper()
	for _, tc := range []struct {
		validAt types.Instant
		want    int64
	}{{1500, 1}, {2500, 2}, {3500, 1}} {
		if got, ok := xAt(tc.validAt, txAfter); !ok || got != tc.want {
			t.Errorf("%s post-correction AtTx(validAt=%d, txAfter)=%d ok=%v, want %d", kind, tc.validAt, got, ok, tc.want)
		}
	}
	for _, validAt := range []types.Instant{1500, 2500, 3500} {
		got, ok := xAt(validAt, txBefore)
		if !ok {
			t.Errorf("%s pre-correction AtTx(validAt=%d, txBefore) returned nothing — HOLE; world was state 1 there before the cascade", kind, validAt)
			continue
		}
		if got != 1 {
			t.Errorf("%s pre-correction AtTx(validAt=%d, txBefore)=%d, want 1 (correction not yet recorded — must not leak)", kind, validAt, got)
		}
	}
}
