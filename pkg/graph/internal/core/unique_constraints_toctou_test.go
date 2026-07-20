package core

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
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

// TestCreateUnique_AdversarialConcurrentRace is the genuinely-concurrent
// counterpart to TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes
// above (BACKLOG 9p): that test proves the fix's mechanism with exactly ONE
// simulated in-flight writer, manipulated serially from the test goroutine.
// This one runs `racers` REAL goroutines simultaneously, each independently
// holding c.mu.RLock() (a genuine concurrent-reader scenario the race
// detector can inspect) to simulate `racers` DIFFERENT standalone writers
// that have all already passed their (constraint-free)
// enforceUniqueForNodeHeld check and are about to commit — then races
// CreateUnique against all of them at once via a barrier, instead of one
// hand-timed interleaving.
//
// A pure "just spawn N concurrent g.Nodes.Add calls and let the scheduler
// decide" version of this test was tried first and reverted: the real
// TOCTOU window (check with no stripe lock -> [gap: build/hash/etc.] ->
// store commit) is narrow enough that Go's scheduler essentially never lands
// a preemption inside it on a fast in-memory store, so that version passed
// even with the BACKLOG 9c fix fully reverted — a non-load-bearing test.
// Forcing every racer to hold c.mu.RLock() across an explicit barrier makes
// the window wide open and deterministic while still exercising REAL
// concurrent goroutines (not serial manual lock manipulation) and running
// under -race.
//
// Asserts the exact invariant: CreateUnique must block until every one of
// the `racers` simulated in-flight writers has committed and released its
// RLock, so Phase 2's scan sees ALL of them (never a subset) and correctly
// rejects with ErrUniqueViolationExisting — never silently activating with
// some of the racers' duplicates unaccounted for.
func TestCreateUnique_AdversarialConcurrentRace(t *testing.T) {
	const racers = 8

	ctx := context.Background()
	ms := memory.New()
	c, err := New(Config{Store: ms})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	if _, err := c.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "race@x.com"}); err != nil {
		t.Fatalf("add genesis user: %v", err)
	}
	labelTok := primaryLabelTokenForTest(t, c, "User")

	// racers goroutines each simulate an independent in-flight writer: take
	// c.mu.RLock() (real concurrent readers), signal arrival, wait for the
	// barrier, then commit their own duplicate node and release.
	arrived := make(chan struct{}, racers)
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(racers)
	for i := 0; i < racers; i++ {
		go func(i int) {
			defer wg.Done()
			c.mu.RLock()
			arrived <- struct{}{}
			<-release
			dup := types.NewNode(c.nextNodeID(), labelTok, nil)
			if err := dup.SetProperty("email", "race@x.com"); err != nil {
				t.Errorf("racer %d SetProperty: %v", i, err)
			}
			if err := ms.PutNode(dup); err != nil {
				t.Errorf("racer %d PutNode: %v", i, err)
			}
			c.mu.RUnlock()
		}(i)
	}
	for i := 0; i < racers; i++ {
		<-arrived
	}

	// Every racer now holds c.mu.RLock() simultaneously. CreateUnique must
	// be unable to proceed past its install while ANY of them still does.
	// Recorded via t.Error (non-fatal) rather than t.Fatal: the racers must
	// be released and joined regardless of outcome below, or a genuine
	// regression here would deadlock the whole test binary — Close() (in
	// t.Cleanup) needs c.mu.Lock(), which cannot be granted while any racer
	// goroutine is still parked on c.mu.RLock() waiting for `release`.
	createDone := make(chan error, 1)
	go func() { createDone <- c.Constraints.CreateUnique(ctx, "User", "email") }()
	select {
	case err := <-createDone:
		t.Errorf("CreateUnique completed (err=%v) while %d simulated in-flight writers still held c.mu.RLock() — BACKLOG 9c/9p regression", err, racers)
	case <-time.After(150 * time.Millisecond):
	}

	// Release all racers at once — genuine concurrent commits, real
	// goroutines, race detector watching. Always runs, so cleanup can never
	// deadlock even if the block-check above already failed.
	close(release)
	wg.Wait()
	if t.Failed() {
		return
	}

	err = <-createDone
	if !errors.Is(err, ErrUniqueViolationExisting) {
		t.Fatalf("CreateUnique = %v, want ErrUniqueViolationExisting — Phase 2 must see all %d racers' duplicates plus genesis", err, racers)
	}

	nodes, err := c.Nodes.ByLabelAndProperty("User", "email", "race@x.com", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(nodes) != racers+1 {
		t.Fatalf("current nodes holding the value = %d, want %d (genesis + every racer accounted for)", len(nodes), racers+1)
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
