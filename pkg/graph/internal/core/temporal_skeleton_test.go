package core

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests pin the selection-skeleton fast path (TemporalMetaHistoryCapability,
// the sigma-tkgd valid-time depth ask) against the full-chain fold on IDENTICAL
// data: badger implements the capability, so the public doors take the skeleton
// path; nilling c.temporalMetaHistory forces the same graph through the
// full-chain arm. Any divergence is a fast-path bug by construction.

// skeletonABNode runs NodeAt/NodeAtTx across a probe grid with and without the
// capability and asserts identical (version, error-class) outcomes.
func skeletonABNode(t *testing.T, g *Core, id types.NodeID, validAts, txAts []types.Instant) {
	t.Helper()
	for _, va := range validAts {
		for _, ta := range txAts {
			fast, fastErr := g.Temporal.NodeAtTx(id, va, ta)
			saved := g.temporalMetaHistory
			g.temporalMetaHistory = nil
			full, fullErr := g.Temporal.NodeAtTx(id, va, ta)
			g.temporalMetaHistory = saved
			if (fastErr == nil) != (fullErr == nil) {
				t.Fatalf("NodeAtTx(%d,%d): fast err=%v, full err=%v", va, ta, fastErr, fullErr)
			}
			if fastErr != nil {
				continue
			}
			if fast.Version() != full.Version() {
				t.Fatalf("NodeAtTx(%d,%d): fast v%d, full v%d", va, ta, fast.Version(), full.Version())
			}
			// The fast path must return a HYDRATED row, never a skeleton:
			// property parity with the full row proves it.
			fv, _ := full.GetProperty("k")
			gv, ok := fast.GetProperty("k")
			if !ok || fmt.Sprintf("%v", gv) != fmt.Sprintf("%v", fv) {
				t.Fatalf("NodeAtTx(%d,%d): fast row k=%v (ok=%v), full k=%v — skeleton leak?", va, ta, gv, ok, fv)
			}
		}
	}
}

// TestSkeletonResolve_CascadeReopenDelete is the exact divergence class the
// bitemporal oracle caught during development: a node CREATED CLOSED
// ([1629,2484)), reopened by an append-only cascade to [535,∞) (non-monotonic
// chain — the resolver's cascade arm), then hard-deleted. Resolving twice on
// the same slice (the first run's in-place sort feeding the second run) flips
// the monotonic-vs-cascade branch and loses the cascade row — the fast path
// must hand each resolve its own pristine ascending-version copy.
func TestSkeletonResolve_CascadeReopenDelete(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()

	n, err := g.Nodes.Add(ctx, []string{"A"}, map[string]any{
		"k": 0, "tkg_valid_from": types.Instant(1629), "tkg_valid_to": types.Instant(2484),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := n.ID()
	if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 535, 0, map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	// An update AFTER the cascade so the OPEN cascade row survives the later
	// delete un-stamped in history (the delete tombstones the then-current
	// update row, not the cascade row) — the oracle's exact shape.
	if _, err := g.Nodes.Update(ctx, id, map[string]any{"k": 2}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	now := types.Instant(g.now())

	if g.temporalMetaHistory == nil {
		t.Fatal("badger must implement TemporalMetaHistoryCapability")
	}
	probes := []types.Instant{1, 535, 536, 1628, 1629, 2000, 2483, 2484, 3000, now, now + 1_000_000}
	skeletonABNode(t, g, id, probes, []types.Instant{0, now})

	// Exact-set pin (rule 16): far-future probe resolves the reopened cascade
	// row (k=1), NOT no-match and NOT the closed genesis row.
	got, err := g.Temporal.NodeAt(id, now+1_000_000)
	if err != nil {
		t.Fatalf("NodeAt(future): %v", err)
	}
	if v, _ := got.GetProperty("k"); fmt.Sprintf("%v", v) != "1" {
		t.Fatalf("NodeAt(future) k = %v, want 1 (the cascade-reopened row)", v)
	}
	// At validAt=2000 both the genesis row and the cascade row cover; the
	// cascade row has the newer belief and must win.
	if v, _ := mustNodeProp(t, g, id, 2000, 0); fmt.Sprintf("%v", v) != "1" {
		t.Fatalf("NodeAt(2000) k = %v, want 1 (newest belief wins on overlap)", v)
	}
}

func mustNodeProp(t *testing.T, g *Core, id types.NodeID, va, ta types.Instant) (any, error) {
	t.Helper()
	n, err := g.Temporal.NodeAtTx(id, va, ta)
	if err != nil {
		t.Fatalf("NodeAtTx(%d,%d): %v", va, ta, err)
	}
	v, _ := n.GetProperty("k")
	return v, nil
}

// TestSkeletonResolve_RelCascadeMirror is the relationship mirror (rule 2) of
// the cascade-reopen-delete scenario.
func TestSkeletonResolve_RelCascadeMirror(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()

	a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
	r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{
		"k": 0, "tkg_valid_from": types.Instant(1629), "tkg_valid_to": types.Instant(2484),
	})
	if err != nil {
		t.Fatal(err)
	}
	id := r.ID()
	if _, err := g.Temporal.SetRelVersionInterval(ctx, id, 535, 0, map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels.Update(ctx, id, map[string]any{"k": 2}); err != nil {
		t.Fatal(err)
	}
	if err := g.Rels.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	now := types.Instant(g.now())

	for _, va := range []types.Instant{1, 535, 1629, 2000, 2484, now + 1_000_000} {
		for _, ta := range []types.Instant{0, now} {
			fast, fastErr := g.Temporal.RelAtTx(id, va, ta)
			saved := g.temporalMetaHistory
			g.temporalMetaHistory = nil
			full, fullErr := g.Temporal.RelAtTx(id, va, ta)
			g.temporalMetaHistory = saved
			if (fastErr == nil) != (fullErr == nil) {
				t.Fatalf("RelAtTx(%d,%d): fast err=%v, full err=%v", va, ta, fastErr, fullErr)
			}
			if fastErr != nil {
				continue
			}
			if fast.Version() != full.Version() {
				t.Fatalf("RelAtTx(%d,%d): fast v%d, full v%d", va, ta, fast.Version(), full.Version())
			}
			fv, _ := full.GetProperty("k")
			if gv, ok := fast.GetProperty("k"); !ok || fmt.Sprintf("%v", gv) != fmt.Sprintf("%v", fv) {
				t.Fatalf("RelAtTx(%d,%d): fast row k=%v (ok=%v), full k=%v — skeleton leak?", va, ta, gv, ok, fv)
			}
			// Endpoints must be real (skeleton endpoints are zero) — a leak check.
			if fast.StartNodeID() != a.ID() || fast.EndNodeID() != b.ID() {
				t.Fatalf("RelAtTx(%d,%d): endpoints %v->%v, want %v->%v (skeleton leak?)",
					va, ta, fast.StartNodeID(), fast.EndNodeID(), a.ID(), b.ID())
			}
		}
	}
}

// TestSkeletonResolve_RandomizedEquivalence fuzzes multi-entity lifecycles
// (updates with explicit tiling valid-from, closes, cascades, deletes) and
// asserts fast/full equivalence over a probe grid — the in-repo miniature of
// the bitemporal oracle focused on the skeleton path.
func TestSkeletonResolve_RandomizedEquivalence(t *testing.T) {
	t.Parallel()
	g, err := New(Config{BadgerInMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(0xC0FFEE)) // #nosec G404 — deterministic test fuzz

	var ids []types.NodeID
	for i := 0; i < 12; i++ {
		props := map[string]any{"k": 0}
		if rng.Intn(2) == 0 {
			vf := types.Instant(100 + rng.Intn(1000))
			props["tkg_valid_from"] = vf
			if rng.Intn(2) == 0 {
				props["tkg_valid_to"] = vf + types.Instant(1+rng.Intn(2000))
			}
		}
		n, err := g.Nodes.Add(ctx, []string{"F"}, props)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, n.ID())
		depth := rng.Intn(6)
		for v := 1; v <= depth; v++ {
			op := rng.Intn(3)
			switch op {
			case 0:
				_, _ = g.Nodes.Update(ctx, n.ID(), map[string]any{"k": v})
			case 1:
				_, _ = g.Nodes.Update(ctx, n.ID(), map[string]any{
					"k": v, "tkg_valid_from": types.Instant(3000 + 100*v + rng.Intn(50)),
				})
			case 2:
				_, _ = g.Temporal.SetNodeVersionInterval(ctx, n.ID(), types.Instant(200+rng.Intn(4000)), 0, map[string]any{"k": 100 + v})
			}
		}
		if rng.Intn(4) == 0 {
			_ = g.Nodes.Delete(ctx, n.ID())
		}
	}
	now := types.Instant(g.now())
	probes := []types.Instant{1, 150, 600, 1200, 2500, 3300, 4100, 7000, now, now + 500_000}
	for i, id := range ids {
		t.Run(fmt.Sprintf("entity=%d", i), func(t *testing.T) {
			skeletonABNode(t, g, id, probes, []types.Instant{0, now})
		})
	}
}
