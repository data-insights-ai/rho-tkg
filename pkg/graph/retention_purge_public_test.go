package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// farFuture is safely above every 2026-epoch snowflake mint-instant, so a purge
// with this boundary selects every currently-minted node.
const farFuture = types.Instant(1 << 50)

// TestPurgeExpiredNodes_Gates covers the door's refusal contract.
func TestPurgeExpiredNodes_Gates(t *testing.T) {
	ctx := context.Background()

	// Gate 1: disabled by default.
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture}); !errors.Is(err, graphpkg.ErrRetentionPurgeDisabled) {
		t.Fatalf("disabled gate err=%v, want ErrRetentionPurgeDisabled", err)
	}

	// Gate 2: enabled but invalid policy.
	ge, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New enabled: %v", err)
	}
	defer ge.Close()
	if _, err := ge.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Before: farFuture}); !errors.Is(err, graphpkg.ErrInvalidPurgePolicy) {
		t.Fatalf("empty-label err=%v, want ErrInvalidPurgePolicy", err)
	}
	if _, err := ge.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: 0}); !errors.Is(err, graphpkg.ErrInvalidPurgePolicy) {
		t.Fatalf("zero-before err=%v, want ErrInvalidPurgePolicy", err)
	}

	// With a change-log enabled, the purge SUCCEEDS (R3) — the native badger store
	// also implements RangePurgeLogCapability, so it emits the predicate record
	// instead of refusing. (The ErrRetentionPurgeChangeLogEnabled refusal now only
	// guards a hypothetical change-log store lacking the log capability.)
	gc, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true, BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New changelog: %v", err)
	}
	defer gc.Close()
	if _, err := gc.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture}); err != nil {
		t.Fatalf("change-log purge should succeed under R3, got %v", err)
	}
}

// TestPurgeExpiredNodes_EndToEnd proves the happy path: below-boundary nodes of
// the target label are hard-removed, other labels survive, the per-label
// retention watermark advances so a temporal read pinned below the boundary fails
// closed with ErrRetentionExpired (the R1 guard, now fired by R2), and a re-run is
// an idempotent no-op.
func TestPurgeExpiredNodes_EndToEnd(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// 5 Event nodes (some with edges to a surviving Machine) + 2 Machine nodes.
	machine, err := g.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h1"})
	if err != nil {
		t.Fatalf("add machine: %v", err)
	}
	machine2, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
	for i := 0; i < 5; i++ {
		e, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		if _, err := g.Rels().AddByID(ctx, "ON", e.ID(), machine.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}

	res, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.NodesPurged != 5 {
		t.Fatalf("purged %d Event nodes, want 5", res.NodesPurged)
	}
	if res.RelsPurged != 5 {
		t.Fatalf("purged %d edges, want 5", res.RelsPurged)
	}

	// Event nodes are gone; Machine nodes survive with no phantom incoming edges.
	events, err := g.Nodes().ByLabel("Event", store.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel Event: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("%d Event nodes survived purge, want 0", len(events))
	}
	machines, _ := g.Nodes().ByLabel("Machine", store.QueryOpts{})
	if len(machines) != 2 {
		t.Fatalf("%d Machine nodes, want 2 (survivors)", len(machines))
	}
	if in, _ := g.Rels().Incoming(machine.ID(), "ON"); len(in) != 0 {
		t.Fatalf("machine has %d phantom incoming edges, want 0", len(in))
	}
	_ = machine2

	// The per-label watermark advanced: a temporal scan pinned below the boundary
	// now fails closed rather than returning a silently-incomplete set.
	if _, err := g.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}

	// Idempotent: purging again removes nothing.
	res2, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture})
	if err != nil {
		t.Fatalf("re-purge: %v", err)
	}
	if res2.NodesPurged != 0 || res2.RelsPurged != 0 {
		t.Fatalf("re-purge removed %+v, want zero", res2)
	}
}
