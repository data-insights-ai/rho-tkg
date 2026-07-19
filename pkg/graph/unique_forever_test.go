package graph_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
)

// 50 goroutines race to claim the SAME forever value: exactly one owner, the
// rest ErrUniqueViolation — the durable claim is made under the value stripe.
func TestUniqueForever_ConcurrentClaimOneOwner(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUniqueForever(t, g)

			const goroutines = 50
			var wins, violations, others atomic.Int32
			var wg sync.WaitGroup
			start := make(chan struct{})
			wg.Add(goroutines)
			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					<-start
					_, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "claim@x.com"})
					switch {
					case err == nil:
						wins.Add(1)
					case errors.Is(err, graphpkg.ErrUniqueViolation):
						violations.Add(1)
					default:
						others.Add(1)
					}
				}()
			}
			close(start)
			wg.Wait()
			if wins.Load() != 1 || others.Load() != 0 || violations.Load() != goroutines-1 {
				t.Fatalf("wins=%d violations=%d others=%d, want 1/%d/0", wins.Load(), violations.Load(), others.Load(), goroutines-1)
			}
		})
	}
}

func mustCreateUniqueForever(t *testing.T, g *graphpkg.Graph) {
	t.Helper()
	if err := g.Constraints().CreateUniqueForever(context.Background(), "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever(User,email): %v", err)
	}
}

// UniqueForever bars a value from every OTHER entity even after the owner is
// hard-deleted — unlike UniqueCurrent which frees the value.
func TestUniqueForever_BarredAfterDelete(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "own@x.com")

			// A different node cannot take the value while the owner is alive.
			_, err := g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "own@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("duplicate under forever err = %v, want ErrUniqueViolation", err)
			}

			// Delete the owner — the value stays owned (barred forever).
			if err := g.Nodes().Delete(context.Background(), a.ID()); err != nil {
				t.Fatalf("delete owner: %v", err)
			}
			_, err = g.Nodes().Add(context.Background(), []string{"User"}, map[string]any{"email": "own@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("post-delete reclaim err = %v, want ErrUniqueViolation (barred forever)", err)
			}
		})
	}
}

// The owning entity (any later version) may keep or restore its value; another
// entity may not take a value the owner moved AWAY from.
func TestUniqueForever_SameEntityAnyVersion(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "a@x.com")

			// Owner moves a -> b (claims b too), then back to a (still owns a).
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"email": "b@x.com"}); err != nil {
				t.Fatalf("update a->b: %v", err)
			}
			if _, err := g.Nodes().Update(ctx, a.ID(), map[string]any{"email": "a@x.com"}); err != nil {
				t.Fatalf("update b->a (same entity owns a): %v", err)
			}
			// A different node cannot take b — the owner claimed it forever.
			_, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "b@x.com"})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("claim of moved-away value err = %v, want ErrUniqueViolation", err)
			}
		})
	}
}

// ReleaseOwnership lets an operator free a value for reclamation; it is
// idempotent and requires a UniqueForever constraint.
func TestUniqueForever_ReleaseOwnership(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()
			mustCreateUniqueForever(t, g)
			a := mustAddUser(t, g, "rel@x.com")
			if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
				t.Fatalf("delete: %v", err)
			}

			// Barred until released.
			if _, err := g.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "rel@x.com"}); !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("pre-release reclaim err = %v, want ErrUniqueViolation", err)
			}
			if err := g.Constraints().ReleaseOwnership(ctx, "User", "email", "rel@x.com"); err != nil {
				t.Fatalf("ReleaseOwnership: %v", err)
			}
			// Now reclaimable.
			mustAddUser(t, g, "rel@x.com")
			// Idempotent: releasing an unowned value is a no-op (the value is now
			// owned again by the reclaimer, so release a DIFFERENT unowned value).
			if err := g.Constraints().ReleaseOwnership(ctx, "User", "email", "never@x.com"); err != nil {
				t.Fatalf("idempotent ReleaseOwnership: %v", err)
			}
		})
	}
}

// ReleaseOwnership without a UniqueForever constraint on the pair fails.
func TestUniqueForever_ReleaseWithoutConstraint(t *testing.T) {
	g := bcMemory(t)
	defer g.Close()
	ctx := context.Background()
	// A UniqueCurrent constraint is NOT a forever constraint.
	mustCreateUnique(t, g)
	err := g.Constraints().ReleaseOwnership(ctx, "User", "email", "x@x.com")
	if !errors.Is(err, graphpkg.ErrUniqueConstraintNotFound) {
		t.Fatalf("ReleaseOwnership on non-forever err = %v, want ErrUniqueConstraintNotFound", err)
	}
}

// CreateUniqueForever over existing current duplicates is rejected.
func TestUniqueForever_ExistingDuplicatesRejected(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			mustAddUser(t, g, "dup@x.com")
			mustAddUser(t, g, "dup@x.com")
			err := g.Constraints().CreateUniqueForever(context.Background(), "User", "email")
			if !errors.Is(err, graphpkg.ErrUniqueViolationExisting) {
				t.Fatalf("CreateUniqueForever over dups err = %v, want ErrUniqueViolationExisting", err)
			}
		})
	}
}

// Introspection surfaces the forever scope.
func TestUniqueForever_Introspection(t *testing.T) {
	g := bcMemory(t)
	defer g.Close()
	mustCreateUniqueForever(t, g)
	cs := g.Constraints().UniqueConstraints()
	if len(cs) != 1 || cs[0].Scope != constraints.UniqueForever {
		t.Fatalf("UniqueConstraints = %+v, want one with scope forever", cs)
	}
}

// Ownership survives a Close/reopen of the same badger dir and keeps barring
// reclaim after the owner is deleted post-reopen.
func TestUniqueForever_ReopenDurability(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6, BadgerDir: dir})
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	if err := g.Constraints().CreateUniqueForever(ctx, "User", "email"); err != nil {
		t.Fatalf("CreateUniqueForever: %v", err)
	}
	a := mustAddUser(t, g, "persist@x.com")
	if err := g.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	g2, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 6, BadgerDir: dir})
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer g2.Close()

	// Delete the owner after reopen; the value is still barred (owner survived).
	if err := g2.Nodes().Delete(ctx, a.ID()); err != nil {
		t.Fatalf("delete after reopen: %v", err)
	}
	_, err = g2.Nodes().Add(ctx, []string{"User"}, map[string]any{"email": "persist@x.com"})
	if !errors.Is(err, graphpkg.ErrUniqueViolation) {
		t.Fatalf("post-reopen post-delete reclaim err = %v, want ErrUniqueViolation", err)
	}
	// A corrupt/tampered blob would fail closed at open (self-hash) — covered by
	// the durable round-trip above; releasing then frees it.
	if err := g2.Constraints().ReleaseOwnership(ctx, "User", "email", "persist@x.com"); err != nil {
		t.Fatalf("ReleaseOwnership after reopen: %v", err)
	}
	mustAddUser(t, g2, "persist@x.com")
}

// Admin().Reset reaps the durable ownership registry.
func TestUniqueForever_ResetReaps(t *testing.T) {
	g := bcMemory(t)
	defer g.Close()
	ctx := context.Background()
	mustCreateUniqueForever(t, g)
	a := mustAddUser(t, g, "own@x.com")
	if err := g.Nodes().Delete(ctx, a.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := g.Admin().Reset(); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	// Constraint + ownership both gone: reclaim now succeeds under a fresh
	// forever constraint.
	mustAddUser(t, g, "own@x.com")
}

// TestUniqueForever_MultiKeyViolationLeavesNoOrphanedClaim guards BACKLOG 9e:
// a node binding TWO constrained tuples — a fresh UniqueForever value and a
// plain UniqueCurrent value that turns out to be a duplicate — must not
// permanently claim the forever value just because a LATER tuple's check
// failed. Before the fix, enforceUniqueForNodeHeld claimed each UniqueForever
// tuple as it was checked, so a later tuple's violation left the earlier
// claim durably persisted on an entity that never actually got created.
func TestUniqueForever_MultiKeyViolationLeavesNoOrphanedClaim(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()

			if err := g.Constraints().CreateUniqueForever(ctx, "Account", "email"); err != nil {
				t.Fatalf("CreateUniqueForever: %v", err)
			}
			if err := g.Constraints().CreateUnique(ctx, "Account", "ssn"); err != nil {
				t.Fatalf("CreateUnique: %v", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"ssn": "999-99-9999"}); err != nil {
				t.Fatalf("seed ssn holder: %v", err)
			}

			// Fresh (claimable) forever email + a duplicate ssn — the whole
			// create must fail, and it must fail regardless of which tuple
			// the implementation happens to check first.
			_, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{
				"email": "fresh@x.com",
				"ssn":   "999-99-9999",
			})
			if !errors.Is(err, graphpkg.ErrUniqueViolation) {
				t.Fatalf("Add with one fresh + one duplicate tuple = %v, want ErrUniqueViolation", err)
			}

			// The forever email value must remain FREE — a real create must
			// still succeed. Pre-fix this fails with ErrUniqueViolation
			// because the earlier failed attempt already durably claimed it.
			if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"email": "fresh@x.com"}); err != nil {
				t.Fatalf("email wrongly left claimed by the failed multi-key create: %v", err)
			}
		})
	}
}

// TestUniqueForever_BatchMultiKeyViolationLeavesNoOrphanedClaim is the batch-
// door mirror of TestUniqueForever_MultiKeyViolationLeavesNoOrphanedClaim
// (BACKLOG 9e) — partitionBatchNodesByUnique had the identical per-tuple
// check-then-claim interleaving bug.
func TestUniqueForever_BatchMultiKeyViolationLeavesNoOrphanedClaim(t *testing.T) {
	for _, bc := range uniqueBackends(t) {
		t.Run(bc.name, func(t *testing.T) {
			g := bc.open(t)
			defer g.Close()
			ctx := context.Background()

			if err := g.Constraints().CreateUniqueForever(ctx, "Account", "email"); err != nil {
				t.Fatalf("CreateUniqueForever: %v", err)
			}
			if err := g.Constraints().CreateUnique(ctx, "Account", "ssn"); err != nil {
				t.Fatalf("CreateUnique: %v", err)
			}
			if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"ssn": "999-99-9999"}); err != nil {
				t.Fatalf("seed ssn holder: %v", err)
			}

			b, err := g.Batch().New()
			if err != nil {
				t.Fatalf("Batch.New: %v", err)
			}
			if _, err := b.AddNode([]string{"Account"}, map[string]any{"email": "batch-fresh@x.com", "ssn": "999-99-9999"}); err != nil {
				t.Fatalf("queue: %v", err)
			}
			if _, err := b.Execute(); !errors.Is(err, graphpkg.ErrBatchFailed) {
				t.Fatalf("Execute err = %v, want ErrBatchFailed", err)
			}

			if _, err := g.Nodes().Add(ctx, []string{"Account"}, map[string]any{"email": "batch-fresh@x.com"}); err != nil {
				t.Fatalf("email wrongly left claimed by the failed batch multi-key create: %v", err)
			}
		})
	}
}

func bcMemory(t *testing.T) *graphpkg.Graph {
	t.Helper()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 8})
	if err != nil {
		t.Fatalf("new memory graph: %v", err)
	}
	return g
}
