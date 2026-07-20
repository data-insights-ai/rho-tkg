package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
)

// TestPurge_ReapsUniqueForeverOwner (ADR-0008 R2 gotcha / ADR-0002) proves that
// purging a node which OWNS a UniqueForever value releases the claim, so the value
// is reusable afterward — rather than being barred forever by a ghost owner.
func TestPurge_ReapsUniqueForeverOwner(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}

	// First User claims the value forever.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("add first user: %v", err)
	}
	// A second User with the same value is barred (owned forever).
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"}); err == nil {
		t.Fatal("second user with owned value succeeded, want ErrUniqueViolation")
	}

	// Purge the User label — the owner node is removed AND its forever claim reaped.
	res, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "User", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.NodesPurged != 1 {
		t.Fatalf("purged %d users, want 1", res.NodesPurged)
	}

	// The value is now reusable — the claim was released with the purged owner.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com"}); err != nil {
		t.Fatalf("re-add user after owner purge failed (ghost owner?): %v", err)
	}
}

// TestPurge_ReapsAllForeverKeysOfMultiKeyOwner (BACKLOG 13h) is the
// multi-key companion to TestPurge_ReapsUniqueForeverOwner: a SINGLE node
// can own forever claims on MULTIPLE distinct constrained keys at once
// (here, two different labels' constraints, since UniqueForever is scoped
// per (label, key) and a purge-eligible node label is what's being tested —
// the ownership registry itself is a flat map keyed by (label, propKey,
// value), so nothing in reapForeverOwnersForPurged's loop shape special-
// cases "how many keys does this owner hold" — but that was previously
// unverified by any test, unlike the single-key case). Purges the owner and
// asserts BOTH claims are released, not just one.
func TestPurge_ReapsAllForeverKeysOfMultiKeyOwner(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever(email): %v", err)
	}
	if err := g.Constraints().CreateUniqueForever(ctx, "User", "ssn"); err != nil {
		t.Fatalf("CreateUniqueForever(ssn): %v", err)
	}

	// One node claims BOTH forever keys.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com", "ssn": "111-11-1111"}); err != nil {
		t.Fatalf("add user with both keys: %v", err)
	}
	// Both values are barred while the owner is alive.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com", "ssn": "222-22-2222"}); err == nil {
		t.Fatal("duplicate email succeeded, want ErrUniqueViolation")
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "b@x.com", "ssn": "111-11-1111"}); err == nil {
		t.Fatal("duplicate ssn succeeded, want ErrUniqueViolation")
	}

	res, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "User", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.NodesPurged != 1 {
		t.Fatalf("purged %d users, want 1", res.NodesPurged)
	}

	// BOTH values must be reusable — not just the first key the reap loop
	// happens to visit.
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "a@x.com", "ssn": "222-22-2222"}); err != nil {
		t.Fatalf("email claim not reaped after owner purge — BACKLOG 13h regression: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "b@x.com", "ssn": "111-11-1111"}); err != nil {
		t.Fatalf("ssn claim not reaped after owner purge — BACKLOG 13h regression: %v", err)
	}
}

// TestPurge_ForeverReapIsScoped ensures the reap only releases claims of purged
// owners — a SURVIVING owner of a different label keeps its claim.
func TestPurge_ForeverReapIsScoped(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if err := g.Constraints().CreateUniqueForever(ctx, "Account", "handle"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"handle": "keep"}); err != nil {
		t.Fatalf("add account: %v", err)
	}

	// Purge an UNRELATED label — the Account owner survives, its claim intact.
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture}); err != nil {
		t.Fatalf("purge event: %v", err)
	}

	// The surviving owner still holds "keep": a new Account is barred.
	_, err = g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"handle": "keep"})
	if err == nil {
		t.Fatal("value freed by an unrelated purge — reap not scoped to purged owners")
	}
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("want ErrUniqueViolation, got %v", err)
	}
}
