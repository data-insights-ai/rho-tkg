package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRetentionReset_DoesNotLeakStalePerLabelWatermarkAcrossReset is the
// BACKLOG 13b regression: Admin.Reset zeroed only the graph-max retention
// watermark, leaving every per-label `retention_watermark/<token>` MetaKV key
// in place. Reset explicitly PRESERVES the label-token registry (unlike a
// compaction stub, which is keyed by entity ID and therefore naturally
// orphaned-and-irrelevant once Reset destroys every entity), so a label's
// token is REUSED across a reset — its stale pre-reset watermark can silently
// resurface and cause a cross-label false-positive ErrRetentionExpired for
// entities of a label that was NEVER purged post-reset, the moment ANY other
// post-reset purge (on a totally different label) raises the graph-max gate
// again.
func TestRetentionReset_DoesNotLeakStalePerLabelWatermarkAcrossReset(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true, AllowReset: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// Pre-reset: purge label "Foo" with a huge boundary, stamping a large
	// per-label watermark (and raising the graph max to match).
	if _, err := g.Nodes().Add(ctx, []string{"Foo"}, nil); err != nil {
		t.Fatalf("add pre-reset Foo: %v", err)
	}
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Foo", Mode: adminpkg.PurgeByAge, Before: farFuture}); err != nil {
		t.Fatalf("pre-reset purge Foo: %v", err)
	}

	// Reset: wipes all entities, PRESERVES the label registry (so "Foo" keeps
	// its same token number).
	if err := g.Admin().Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	// A brand-new post-reset Foo node.
	newFoo, err := g.Nodes().Add(ctx, []string{"Foo"}, nil)
	if err != nil {
		t.Fatalf("add post-reset Foo: %v", err)
	}

	// Purge a DIFFERENT label ("Bar") with a small boundary — small enough
	// that it purges nothing (no Bar entities exist), but its watermark
	// advance still raises the graph-max gate above 0, re-activating the
	// per-label check path in checkNodePointRetention.
	const smallBoundary = types.Instant(2000)
	if _, err := g.Nodes().Add(ctx, []string{"Bar"}, nil); err != nil {
		t.Fatalf("add Bar: %v", err)
	}
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Bar", Mode: adminpkg.PurgeByAge, Before: smallBoundary}); err != nil {
		t.Fatalf("purge Bar: %v", err)
	}

	// A point-in-tx-time read on the NEW Foo node, pinned below smallBoundary
	// (so it is legitimately before the node's own creation, and also below
	// the reactivated graph-max gate) must answer with the NATURAL "nothing
	// recorded that early" result (ErrNoVersionAsOf) — NOT ErrRetentionExpired,
	// which would mean Foo's STALE pre-reset watermark (farFuture) leaked
	// across the reset and is being consulted for a label that was never
	// purged post-reset.
	const pin = types.Instant(1000) // < smallBoundary, well below farFuture
	_, err = g.Temporal().NodeAsOf(newFoo.ID(), pin)
	if errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("NodeAsOf(post-reset Foo, pin=%d) = ErrRetentionExpired — stale pre-reset Foo watermark leaked across Reset (BACKLOG 13b regression)", pin)
	}
	if !errors.Is(err, graphpkg.ErrNoVersionAsOf) {
		t.Fatalf("NodeAsOf(post-reset Foo, pin=%d) err = %v, want ErrNoVersionAsOf", pin, err)
	}
}
