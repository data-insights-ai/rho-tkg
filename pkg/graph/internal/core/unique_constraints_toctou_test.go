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

// CreateUnique's 3-phase install (install PENDING entry -> validate
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
// This test reproduces the race deterministically by taking c.mu.RLock()
// directly (package-internal test — direct field access) to simulate a write
// that has already passed its check and is about to commit, waiting until
// CreateUnique is parked on c.mu.Lock() (awaitCoreWriterParked), running the
// in-flight write's late step, then releasing it. Two in-flight writes:
//
//   - late_duplicate: a second node with the same value lands in the store;
//     Phase 2's scan must see it and refuse with ErrUniqueViolationExisting.
//   - update_in_place: updateNodeInPlaceInternal reads hasUniqueConstraints
//     (to decide whether to snapshot the old node for enforceUniqueForNode)
//     before the mutation and enforcement after it. Both reads run under one
//     c.mu.RLock(), so the install cannot land between them: both still see
//     no constraint, and the install's scan afterwards sees only the new
//     value, so CreateUnique succeeds and then enforces it.
func TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		inFlight func(t *testing.T, c *Core, ms *memory.Store, first *types.Node)
		check    func(t *testing.T, c *Core, createErr error)
	}{
		{
			name: "late_duplicate",
			inFlight: func(t *testing.T, c *Core, ms *memory.Store, _ *types.Node) {
				dup := types.NewNode(c.nextNodeID(), primaryLabelTokenForTest(t, c, "User"), nil)
				if err := dup.SetProperty("email", "dup@x.com"); err != nil {
					t.Errorf("SetProperty: %v", err)
				}
				if err := ms.PutNode(dup); err != nil {
					t.Errorf("simulated late commit PutNode: %v", err)
				}
			},
			check: func(t *testing.T, _ *Core, createErr error) {
				if !errors.Is(createErr, ErrUniqueViolationExisting) {
					t.Fatalf("CreateUnique = %v, want ErrUniqueViolationExisting — the late-committing duplicate must be caught, not silently let through under an active constraint", createErr)
				}
			},
		},
		{
			name: "update_in_place",
			inFlight: func(t *testing.T, c *Core, _ *memory.Store, first *types.Node) {
				updated, mutated, err := c.updateNodeInPlaceInternal(context.Background(), first.ID(), map[string]any{"email": "changed@x.com"})
				if err != nil || !mutated {
					t.Errorf("updateNodeInPlaceInternal (while CreateUnique blocked) = (mutated %v, %v)", mutated, err)
					return
				}
				if got, _ := updated.GetProperty("email"); got != "changed@x.com" {
					t.Errorf("email = %v, want changed@x.com", got)
				}
			},
			check: func(t *testing.T, c *Core, createErr error) {
				if createErr != nil {
					t.Fatalf("CreateUnique = %v, want nil (the update fully committed before the constraint's validation scan ran)", createErr)
				}
				_, err := c.Nodes.Add(context.Background(), []string{"User"}, map[string]any{"email": "changed@x.com"})
				if !errors.Is(err, ErrUniqueViolation) {
					t.Fatalf("Add with a duplicate now-constrained value = %v, want ErrUniqueViolation", err)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ms := memory.New()
			c, err := New(Config{Store: ms})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			ctx := context.Background()

			first, err := c.Nodes.Add(ctx, []string{"User"}, map[string]any{"email": "dup@x.com"})
			if err != nil {
				t.Fatalf("add first user: %v", err)
			}

			// Simulate a standalone write that already passed its
			// (constraint-free) check and is about to commit — it holds
			// c.mu.RLock() for the rest of its call, exactly like a real
			// in-flight write would.
			c.mu.RLock()

			createDone := make(chan error, 1)
			go func() {
				createDone <- c.Constraints.CreateUnique(ctx, "User", "email")
			}()
			if err, done := awaitCoreWriterParked(c, createDone); done {
				c.mu.RUnlock()
				t.Fatalf("CreateUnique completed (err=%v) while a simulated in-flight write held c.mu.RLock() — regression: the install must block until every in-flight write completes", err)
			}

			tc.inFlight(t, c, ms, first)

			// Release the simulated in-flight write — CreateUnique's install
			// can now proceed, and its Phase 2 scan runs strictly after.
			c.mu.RUnlock()
			tc.check(t, c, <-createDone)
		})
	}
}

// awaitCoreWriterParked waits until a writer is parked on c.mu.Lock() while
// the caller holds c.mu.RLock(): a pending writer makes a new TryRLock fail. It
// returns (err, true) if done delivers first, meaning the operation finished
// without waiting for the caller's read lock. The wait is positive (it ends
// when the writer parks), so a slow host cannot make it pass vacuously.
func awaitCoreWriterParked(c *Core, done <-chan error) (error, bool) {
	deadline := time.After(5 * time.Second)
	for {
		select {
		case err := <-done:
			return err, true
		case <-deadline:
			return errors.New("no writer parked on c.mu within 5 s"), true
		default:
		}
		// RLock takes stripe 0 and Lock takes every stripe from 0 up, so a
		// writer parks on stripe 0 behind the caller's read lock.
		if !c.mu.stripes[0].mu.TryRLock() {
			return nil, false
		}
		c.mu.stripes[0].mu.RUnlock()
		time.Sleep(50 * time.Microsecond)
	}
}

// TestCreateUnique_AdversarialConcurrentRace is the genuinely-concurrent
// counterpart to TestCreateUnique_BlocksInstallUntilInFlightWriteCompletes
// above: that test proves the fix's mechanism with exactly ONE
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
// even with the fix fully reverted — a non-load-bearing test.
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
	t.Parallel()
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
	if err, done := awaitCoreWriterParked(c, createDone); done {
		t.Errorf("CreateUnique completed (err=%v) while %d simulated in-flight writers still held c.mu.RLock() — regression", err, racers)
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
