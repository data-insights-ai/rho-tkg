package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestResolveNodeVersionAt_EveryChainPositionMatchesExpectedVersion guards
// BACKLOG 10m: resolveNodeVersionAt's monotonic-chain fast path now uses
// sort.Search to find the backward scan's STARTING index instead of always
// starting at the end (O(log n) instead of O(n) to locate an OLD t, the
// exact case this test targets — a query near the chain's genesis, forcing
// the pre-change linear scan to walk almost the entire history backward).
// The optimization only changes the scan's starting point, never its
// per-entry logic, but a chain-position edge case (off-by-one at a tile
// boundary, an incorrectly early stop) would still be a silent wrong-answer
// bug — the exact class this test exists to rule out by checking EVERY
// tiled version's boundary, not just one or two samples.
func TestResolveNodeVersionAt_EveryChainPositionMatchesExpectedVersion(t *testing.T) {
	g := newTestGraphForChain(t)
	ctx := context.Background()

	const versions = 40
	n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{"tkg_valid_from": types.Instant(1000), "seq": int64(0)})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	id := n.ID()
	validFroms := []types.Instant{1000}
	for i := 1; i < versions; i++ {
		vf := types.Instant(1000 + i*1000)
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"tkg_valid_from": vf, "seq": int64(i)}); err != nil {
			t.Fatalf("Update %d: %v", i, err)
		}
		validFroms = append(validFroms, vf)
	}

	// Before the very first version: no version valid.
	if _, err := g.Temporal.NodeAt(id, 500); err == nil {
		t.Fatal("NodeAt(before genesis) succeeded, want ErrNoVersionValidAt")
	}

	// At and just after each tile's own valid-from: that exact version.
	for seq, vf := range validFroms {
		for _, at := range []types.Instant{vf, vf + 500} {
			got, err := g.Temporal.NodeAt(id, at)
			if err != nil {
				t.Fatalf("NodeAt(%d) [tile %d]: %v", at, seq, err)
			}
			v, ok := got.GetProperty("seq")
			if !ok || v != int64(seq) {
				t.Fatalf("NodeAt(%d) seq = %v (ok=%v), want %d (tile boundary vf=%d)", at, v, ok, seq, vf)
			}
		}
	}

	// Well past the last tile's valid-from: the open-ended current tip.
	got, err := g.Temporal.NodeAt(id, validFroms[versions-1]+50_000)
	if err != nil {
		t.Fatalf("NodeAt(far future): %v", err)
	}
	if v, ok := got.GetProperty("seq"); !ok || v != int64(versions-1) {
		t.Fatalf("NodeAt(far future) seq = %v (ok=%v), want %d", v, ok, versions-1)
	}
}

// TestResolveRelVersionAt_EveryChainPositionMatchesExpectedVersion is the
// relationship-side mirror (rule 2) of the test above.
func TestResolveRelVersionAt_EveryChainPositionMatchesExpectedVersion(t *testing.T) {
	g := newTestGraphForChain(t)
	ctx := context.Background()

	start, err := g.Nodes.Add(ctx, []string{"A"}, nil)
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add(ctx, []string{"B"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}

	const versions = 40
	r, err := g.Rels.Add(ctx, "EDGE", start, end, map[string]any{"tkg_valid_from": types.Instant(1000), "seq": int64(0)})
	if err != nil {
		t.Fatalf("AddRel: %v", err)
	}
	id := r.ID()
	validFroms := []types.Instant{1000}
	for i := 1; i < versions; i++ {
		vf := types.Instant(1000 + i*1000)
		if _, err := g.Rels.Update(ctx, id, map[string]any{"tkg_valid_from": vf, "seq": int64(i)}); err != nil {
			t.Fatalf("UpdateRel %d: %v", i, err)
		}
		validFroms = append(validFroms, vf)
	}

	if _, err := g.Temporal.RelAt(id, 500); err == nil {
		t.Fatal("RelAt(before genesis) succeeded, want ErrNoVersionValidAt")
	}

	for seq, vf := range validFroms {
		for _, at := range []types.Instant{vf, vf + 500} {
			got, err := g.Temporal.RelAt(id, at)
			if err != nil {
				t.Fatalf("RelAt(%d) [tile %d]: %v", at, seq, err)
			}
			v, ok := got.GetProperty("seq")
			if !ok || v != int64(seq) {
				t.Fatalf("RelAt(%d) seq = %v (ok=%v), want %d (tile boundary vf=%d)", at, v, ok, seq, vf)
			}
		}
	}

	got, err := g.Temporal.RelAt(id, validFroms[versions-1]+50_000)
	if err != nil {
		t.Fatalf("RelAt(far future): %v", err)
	}
	if v, ok := got.GetProperty("seq"); !ok || v != int64(versions-1) {
		t.Fatalf("RelAt(far future) seq = %v (ok=%v), want %d", v, ok, versions-1)
	}
}

// buildSyntheticNodeChain constructs a tiled, monotonic []*types.Node chain
// directly (no store I/O) so BenchmarkResolveNodeVersionAt_OldTimestamp
// isolates resolveNodeVersionAt's own cost from history materialization —
// see BenchmarkNodeAt_DeepHistory (temporal_fastpath_test.go) for the
// store-backed variant, where decode cost dominates and masks this specific
// improvement.
func buildSyntheticNodeChain(size int) []*types.Node {
	chain := make([]*types.Node, size)
	for i := 0; i < size; i++ {
		n := types.NewNode(types.NodeID(int64(i+1)), 1, nil)
		n.SetVersion(uint32(i))
		n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant((i + 1) * 1000)})
		chain[i] = n
	}
	return chain
}

// BenchmarkResolveNodeVersionAt_OldTimestamp measures BACKLOG 10m's fix in
// isolation: querying near the chain's genesis forces the pre-fix linear
// scan to walk almost the entire chain; the fix locates the same answer via
// sort.Search instead. MEASURED (this machine, benchtime=2000x): size=100
// ~1529ns -> ~514ns (~3.0x), size=1000 ~12377ns -> ~4480ns (~2.8x),
// size=10000 ~84769ns -> ~29120ns (~2.9x) — a consistent ~3x constant-factor
// win, NOT an asymptotic one: resolveNodeVersionAt's overall cost is still
// bounded by sortNodeChainForResolve's own O(n) monotonicity-confirmation
// scan, which this change does not touch. The optimization targets
// specifically the resolution sub-step the finding named, not the whole
// function's complexity class.
func BenchmarkResolveNodeVersionAt_OldTimestamp(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		chain := buildSyntheticNodeChain(size)
		c, err := New(Config{})
		if err != nil {
			b.Fatal(err)
		}
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := c.resolveNodeVersionAt(chain, types.Instant(1500)); err != nil {
					b.Fatal(err)
				}
			}
		})
		_ = c.Close()
	}
}
