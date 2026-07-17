package graph_test

import (
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPurgeExpiredNodes_ByValidTo is the adversarial exact-set proof for the R5
// ByValidTo predicate (ADR-0008): a node is purged IFF its CURRENT-version world-
// time validity ended before the boundary (ValidTo != 0 && ValidTo < before). It
// diverges from ByAge deliberately — every node here is minted "now" (mint-time far
// above the boundary), so ByAge would purge NONE, yet ByValidTo purges exactly the
// two whose validity closed early. `closedViaUpdate` is the two-phase case (Rule 15
// flavor): created OPEN, then closed by a later Update, so a purge that read the
// genesis (open) state instead of the current (closed) state would wrongly keep it.
func TestPurgeExpiredNodes_ByValidTo(t *testing.T) {
	// Run on both native backends so badger's getNodeLocked selection path (a
	// cache/db read) is covered alongside memory's under-lock map read.
	for _, backend := range []struct {
		name string
		cfg  graphpkg.Config
	}{
		{"memory", graphpkg.Config{AllowRetentionPurge: true}},
		{"badger", graphpkg.Config{AllowRetentionPurge: true, BadgerInMemory: true}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			purgeByValidToExactSet(t, backend.cfg)
		})
	}
}

func purgeByValidToExactSet(t *testing.T, cfg graphpkg.Config) {
	ctx := context.Background()
	g, err := graphpkg.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	const boundary = types.Instant(5000)

	// closedAtCreate: validity ended at 1000 < boundary → PURGED.
	closedAtCreate, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{
		"seq": int64(1), "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("add closedAtCreate: %v", err)
	}
	// closedViaUpdate: created OPEN, then an Update closes it at 1000 < boundary →
	// PURGED. Proves the predicate reads the CURRENT version, not the genesis.
	closedViaUpdate, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{
		"seq": int64(2), "tkg_valid_from": types.Instant(100),
	})
	if err != nil {
		t.Fatalf("add closedViaUpdate: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, closedViaUpdate.ID(), map[string]any{"tkg_valid_to": types.Instant(1000)}); err != nil {
		t.Fatalf("close closedViaUpdate: %v", err)
	}
	// keptOpen: ValidTo == 0 (open interval) → KEPT (an open fact is never expired).
	keptOpen, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{
		"seq": int64(3), "tkg_valid_from": types.Instant(100),
	})
	if err != nil {
		t.Fatalf("add keptOpen: %v", err)
	}
	// keptLateClose: validity ends at 9000 > boundary → KEPT.
	keptLateClose, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{
		"seq": int64(4), "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(9000),
	})
	if err != nil {
		t.Fatalf("add keptLateClose: %v", err)
	}
	// A different label with early-closed validity must be untouched by the scan.
	machine, err := g.Nodes().Add(ctx, []string{"Machine"}, map[string]any{
		"host": "h1", "tkg_valid_from": types.Instant(100), "tkg_valid_to": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("add machine: %v", err)
	}

	res, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{
		Label: "Event", Mode: adminpkg.PurgeByValidTo, Before: boundary,
	})
	if err != nil {
		t.Fatalf("purge by valid-to: %v", err)
	}
	if res.NodesPurged != 2 {
		t.Fatalf("ByValidTo purged %d nodes, want exactly 2 (closedAtCreate, closedViaUpdate)", res.NodesPurged)
	}

	// Exact-set survivors.
	events, err := g.Nodes().ByLabel("Event", store.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel Event: %v", err)
	}
	survivors := map[types.NodeID]bool{}
	for _, n := range events {
		survivors[n.ID()] = true
	}
	for _, gone := range []struct {
		name string
		id   types.NodeID
	}{{"closedAtCreate", closedAtCreate.ID()}, {"closedViaUpdate", closedViaUpdate.ID()}} {
		if survivors[gone.id] {
			t.Fatalf("%s (ValidTo < boundary) survived, want purged", gone.name)
		}
	}
	for _, keep := range []struct {
		name string
		id   types.NodeID
	}{{"keptOpen", keptOpen.ID()}, {"keptLateClose", keptLateClose.ID()}} {
		if !survivors[keep.id] {
			t.Fatalf("%s was purged, want kept", keep.name)
		}
	}
	if machines, _ := g.Nodes().ByLabel("Machine", store.QueryOpts{}); len(machines) != 1 || machines[0].ID() != machine.ID() {
		t.Fatalf("Machine label was affected by an Event ByValidTo purge, want untouched")
	}

	// Idempotent re-run removes nothing.
	res2, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{
		Label: "Event", Mode: adminpkg.PurgeByValidTo, Before: boundary,
	})
	if err != nil {
		t.Fatalf("re-purge: %v", err)
	}
	if res2.NodesPurged != 0 {
		t.Fatalf("re-purge removed %d, want 0", res2.NodesPurged)
	}
}
