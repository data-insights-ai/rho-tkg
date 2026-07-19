package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 9c: CreateUnique's 3-phase install (install PENDING entry -> validate
// existing data -> activate) took no c.mu engagement at all. A standalone
// write's addNodeInternal holds c.mu.RLock() for its ENTIRE duration —
// enforceUniqueForNodeHeld's uniqueness check through the actual store write —
// and that check exits immediately with NO value-stripe lock whenever
// hasUniqueConstraints() is still false. If such a check ran BEFORE
// CreateUnique's Phase 1 installed the pending constraint, but the write's
// actual commit landed AFTER Phase 3 activated it, nothing had ever enforced
// against that write: a duplicate could land under a now-fully-active unique
// constraint.
//
// The fix takes c.mu.Lock() around Phase 1's install. Since EVERY standalone
// write holds c.mu.RLock() for its whole duration, c.mu.Lock() cannot be
// granted while any such write is still in flight — so by the time
// installConstraintLocked runs, every write that checked "no constraint yet"
// has ALREADY fully committed (visible to Phase 2's scan), and every write
// that starts afterward observes the now-installed PENDING entry and
// correctly self-enforces via the stripe lock.
//
// This test reproduces the race deterministically (no timing flakiness) by
// taking c.mu.RLock() directly (package-internal test — direct field access)
// to simulate a write that has already passed its check and is about to
// commit, confirming CreateUnique blocks on the install until that "in-flight
// write" completes, and then simulating its late commit.
func TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes(t *testing.T) {
	ms := memory.New()
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	if _, err := c.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "dup@x.com"}); err != nil {
		t.Fatalf("add first user: %v", err)
	}

	// Simulate a standalone write that already passed its (constraint-free)
	// enforceUniqueForNodeHeld check and is about to commit — it holds
	// c.mu.RLock() for the rest of its addNodeInternal call, exactly like a
	// real in-flight write would.
	c.mu.RLock()

	createDone := make(chan error, 1)
	go func() {
		createDone <- c.Constraints.CreateUnique(ctx, "User", "email")
	}()

	select {
	case err := <-createDone:
		c.mu.RUnlock()
		t.Fatalf("CreateUnique completed (err=%v) while a simulated in-flight write held c.mu.RLock() — BACKLOG 9c regression: the install must block until every in-flight write completes", err)
	case <-time.After(150 * time.Millisecond):
		// Still blocked on the install's c.mu.Lock(), as required.
	}

	// The simulated write's late commit: a SECOND node with the SAME
	// constrained value, written directly to the store — exactly what an
	// in-flight write that already decided "no constraint, safe to proceed"
	// would do.
	dup := types.NewNode(c.nextNodeID(), primaryLabelTokenForTest(t, c, "User"), nil)
	if err := dup.SetProperty("email", "dup@x.com"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ms.PutNode(dup); err != nil {
		t.Fatalf("simulated late commit PutNode: %v", err)
	}

	// Release the simulated in-flight write — CreateUnique's install can now
	// proceed, and its Phase 2 scan (running strictly after) must see BOTH
	// nodes.
	c.mu.RUnlock()

	err = <-createDone
	if !errors.Is(err, ErrUniqueViolationExisting) {
		t.Fatalf("CreateUnique = %v, want ErrUniqueViolationExisting — the late-committing duplicate must be caught, not silently let through under an active constraint", err)
	}
}

// primaryLabelTokenForTest resolves (creating if needed) the token for label,
// mirroring what a real node create does before the property-slice write.
func primaryLabelTokenForTest(t *testing.T, c *Core, label string) uint16 {
	t.Helper()
	tok, err := c.getOrCreateLabelPersisted(label)
	if err != nil {
		t.Fatalf("getOrCreateLabelPersisted(%q): %v", label, err)
	}
	return tok
}
