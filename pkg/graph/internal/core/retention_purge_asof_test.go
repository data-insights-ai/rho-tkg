package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRetentionPurgeAsOfBumpAfterRows (backlog 21, R14): the node purge must
// invalidate the as-of column cache AFTER it removes rows. A column built
// between the watermark advance and the chunks (at a pin at or above the
// boundary, which the gate answers) reads the not-yet-removed node; a bump
// before the chunks lets that column be cached as current, and the pin keeps
// answering a purged node. The hook runs that build exactly in the window.
func TestRetentionPurgeAsOfBumpAfterRows(t *testing.T) {
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, Config{AllowRetentionPurge: true}, false)
			old, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(3 * time.Millisecond)
			kept, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			before := kept.ID().MintInstant()
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if pin < before {
				t.Fatalf("pin %d below the boundary %d", pin, before)
			}
			warmAsOf(t, g, pin)
			assertMembers(t, "warm", asOfMembers(t, g, pin), old.ID(), kept.ID())

			builds := 0
			g.purgeChunkHook = func() {
				done := make(chan error, 1)
				go func() {
					_, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
					done <- err
				}()
				if err := <-done; err != nil {
					t.Errorf("interleaved build: %v", err)
				}
				builds++
			}
			if _, err := g.Admin.PurgeExpiredNodes(ctx, PurgePolicy{Label: pastDatedLabel, Before: before}); err != nil {
				t.Fatal(err)
			}
			g.purgeChunkHook = nil
			if builds == 0 {
				t.Fatal("the purge never reached the hook between the watermark and the chunks")
			}
			assertMembers(t, "after the purge", asOfMembers(t, g, pin), kept.ID())
		})
	}
}

// TestRelRetentionPurgeKeepsAsOfCache (backlog 21): the as-of cache holds node
// label columns only, so a relationship purge leaves a warm column cached (the
// same column object answers after the purge) and its answer unchanged.
// Faulty implementation caught: a relationship purge that bumps the cache
// (every dashboard pin goes cold at each overview cut).
func TestRelRetentionPurgeKeepsAsOfCache(t *testing.T) {
	ctx := context.Background()
	for _, be := range pastDatedBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, Config{AllowRetentionPurge: true}, false)
			a, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes.Add(ctx, []string{pastDatedLabel}, map[string]any{"k": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.AddByID(ctx, "EVENT", a.ID(), b.ID(), nil); err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			warmAsOf(t, g, pin)
			c1, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
			if err != nil {
				t.Fatal(err)
			}
			rep, err := g.Admin.PurgeExpiredRels(ctx, RelPurgePolicy{Type: "EVENT", Before: types.Instant(1 << 50)})
			if err != nil {
				t.Fatal(err)
			}
			if rep.RelsPurged != 1 {
				t.Fatalf("RelsPurged = %d, want 1", rep.RelsPurged)
			}
			c2, err := g.buildAsOfColumns(pastDatedLabel, []string{"k"}, pin)
			if err != nil {
				t.Fatal(err)
			}
			if c1 != c2 {
				t.Fatal("a relationship purge invalidated the node as-of column cache")
			}
			assertMembers(t, "after the rel purge", asOfMembers(t, g, pin), a.ID(), b.ID())
		})
	}
}
