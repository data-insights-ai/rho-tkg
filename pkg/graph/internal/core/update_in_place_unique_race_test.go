package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// BACKLOG 9j: updateNodeInPlaceInternal decides whether to snapshot the
// pre-mutation node (prevState, needed so enforceUniqueForNode can free the
// OLD value's stripe for a constrained value changed in place) by reading
// c.hasUniqueConstraints.Load() BEFORE the property mutation — but the
// enforcement call that snapshot feeds runs AFTER the mutation, doing its
// OWN independent read of c.uniqueConstraints. The finding worried a
// constraint could be installed BETWEEN these two reads, so prevState stays
// nil (no constraint existed at snapshot time) while enforcement then finds
// one active and enforces it — without the freed-old-value stripe the
// snapshot exists to provide.
//
// Investigation: this exact race is already closed as a side effect of
// BACKLOG 9c. CreateUnique's Phase 1 install (which is the ONLY place
// hasUniqueConstraints transitions false->true) now runs under c.mu.Lock().
// updateNodeInPlaceInternal's ENTIRE body — both the hasUniqueConstraints
// read and the later enforceUniqueForNode call — executes under ONE
// continuously-held c.mu.RLock() (NodeOps.UpdateInPlace wraps it in
// c.runUnderRLock). Since c.mu.Lock() cannot be granted while any RLock is
// outstanding, CreateUnique's install cannot complete anywhere between
// updateNodeInPlaceInternal's two reads — hasUniqueConstraints can only ever
// change value strictly BEFORE or strictly AFTER a given UpdateInPlace call,
// never DURING one. (The reverse direction — a constraint REMOVED mid-call
// via DropUnique, which only needs c.uniqueMu — is harmless: enforcement
// simply no-ops on the now-unconfigured constraint, at worst wasting the
// prevState snapshot.)
//
// This test reproduces the exact interleaving 9j worried about, using the
// same direct c.mu.RLock() simulation technique as
// unique_constraints_toctou_test.go's 9c regression test, and proves
// CreateUnique cannot complete until the simulated in-flight UpdateInPlace
// (which internally observed hasUniqueConstraints==false and skipped the
// snapshot) has fully finished — so its snapshot decision and its
// enforcement call are provably indivisible from CreateUnique's install.
func TestUpdateInPlace_HasUniqueConstraintsSnapshotIndivisibleFromInstall(t *testing.T) {
	ms := memory.New()
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx := context.Background()

	node, err := c.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "keep@x.com"})
	if err != nil {
		t.Fatalf("add node: %v", err)
	}

	// Simulate being INSIDE updateNodeInPlaceInternal, exactly like a real
	// standalone UpdateInPlace call would (NodeOps.UpdateInPlace holds
	// c.mu.RLock() for its whole body via c.runUnderRLock).
	c.mu.RLock()

	createDone := make(chan error, 1)
	go func() {
		createDone <- c.Constraints.CreateUnique(ctx, "User", "email")
	}()

	select {
	case err := <-createDone:
		c.mu.RUnlock()
		t.Fatalf("CreateUnique completed (err=%v) while a simulated in-flight UpdateInPlace held c.mu.RLock() — BACKLOG 9j regression", err)
	case <-time.After(150 * time.Millisecond):
		// Still blocked on the install's c.mu.Lock(), as required.
	}

	// While CreateUnique is still blocked, run the FULL updateNodeInPlaceInternal
	// sequence directly — hasUniqueConstraints.Load() at the snapshot-decision
	// point (before mutation) MUST still observe false (CreateUnique's install
	// cannot have completed), so prevState stays nil; then the mutation
	// changes the value; then enforceUniqueForNode runs and — because
	// hasUniqueConstraints is STILL false at that point too, for the exact
	// same reason — no-ops rather than (incorrectly) enforcing a
	// half-installed constraint without the old-value stripe.
	updated, mutated, err := c.updateNodeInPlaceInternal(ctx, node.ID(), map[string]any{"email": "changed@x.com"})
	if err != nil {
		c.mu.RUnlock()
		t.Fatalf("updateNodeInPlaceInternal (while CreateUnique blocked): %v", err)
	}
	if !mutated {
		c.mu.RUnlock()
		t.Fatal("updateNodeInPlaceInternal reported no mutation")
	}
	if got, _ := updated.GetProperty("email"); got != "changed@x.com" {
		c.mu.RUnlock()
		t.Fatalf("email = %v, want changed@x.com", got)
	}

	// Release the simulated in-flight write. CreateUnique's install can now
	// proceed; its Phase 2 scan runs strictly AFTER the update fully
	// committed, so it sees ONLY the post-update state ("changed@x.com") —
	// no stale "keep@x.com" duplicate from a half-observed transition.
	c.mu.RUnlock()

	if err := <-createDone; err != nil {
		t.Fatalf("CreateUnique = %v, want nil (the update fully committed before the constraint's validation scan ran)", err)
	}

	// Non-regression: the constraint is genuinely active now.
	if _, err := c.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "changed@x.com"}); err == nil {
		t.Fatal("Add with a duplicate now-constrained value succeeded, want ErrUniqueViolation")
	}
}
