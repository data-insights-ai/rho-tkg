package core

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// toggleFailPutNodesBatchStore is the batch-durability battery's failing-store
// decorator (see failPutNodesBatchStore in findings_extra_regression_test.go)
// made TOGGLEABLE so a test can inject a mid-group flush failure and then clear
// it to prove recovery on the next flush. It is NOT *tiered.Store, so core's
// nativeGeneratedCreate declines and node creates route through
// c.store.PutNodesBatch — the override below.
type toggleFailPutNodesBatchStore struct {
	storepkg.Store
	fail atomic.Bool
	err  error
}

func (s *toggleFailPutNodesBatchStore) PutNodesBatch(nodes []*types.Node) error {
	if s.fail.Load() {
		return s.err
	}
	return s.Store.PutNodesBatch(nodes)
}

// TestIngestSyncSubmitVsCloseStress (P1) is the exact probe that hung round 1 at
// iteration 22/200: many producers driving SYNC submits while Close races them.
// The contract (C1): a sync ack ⇒ the group is applied and visible (never
// accepted-then-dropped — which would HANG the sync Submit on <-g.result); a
// submit that loses the race to the fence is rejected cleanly with
// ErrIngestClosed; nothing hangs (a per-iteration timeout catches a hang).
func TestIngestSyncSubmitVsCloseStress(t *testing.T) {
	t.Parallel()
	const iterations = 200
	const producers = 6
	const submitsPerProducer = 8
	ctx := context.Background()

	for iter := 0; iter < iterations; iter++ {
		c, err := New(Config{Store: memory.New()})
		if err != nil {
			t.Fatalf("iter %d New: %v", iter, err)
		}

		var wg sync.WaitGroup
		for p := 0; p < producers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				sess, err := c.Ingest.NewSession(IngestOptions{Sync: true, DeclareLabels: []string{"S"}})
				if err != nil {
					// The graph may already be closing — the only acceptable error.
					if !errors.Is(err, ErrGraphClosed) {
						t.Errorf("iter %d NewSession = %v, want nil or ErrGraphClosed", iter, err)
					}
					return
				}
				for i := 0; i < submitsPerProducer; i++ {
					n, err := sess.AddNode([]string{"S"}, map[string]any{"p": int64(p), "i": int64(i)})
					if err != nil {
						if !errors.Is(err, ErrIngestClosed) && !errors.Is(err, ErrGraphClosed) {
							t.Errorf("iter %d AddNode = %v", iter, err)
						}
						return
					}
					if _, err := sess.Submit(); err != nil {
						// A submit racing Close must reject with the clean sentinel
						// — never a hang (a hang trips the timeout below), never a
						// non-sentinel error.
						if !errors.Is(err, ErrIngestClosed) {
							t.Errorf("iter %d Submit rejection = %v, want ErrIngestClosed", iter, err)
						}
						return
					}
					// A sync ack ⇒ the group was applied. It MUST be visible in the
					// store (never accepted-then-dropped) until the store closes.
					// A closed store answers ErrGraphClosed (acceptable); a missing
					// node (ErrNodeNotFound) is a dropped write.
					got, gerr := c.Nodes.Get(ctx, n.ID())
					if gerr != nil {
						if !errors.Is(gerr, ErrGraphClosed) {
							t.Errorf("iter %d acked submit not visible: err=%v", iter, gerr)
						}
						return
					}
					if got.ID() != n.ID() {
						t.Errorf("iter %d Get returned wrong node", iter)
						return
					}
				}
			}(p)
		}

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		// Close races the producers. stopIngestApplier drains every accepted
		// intent before the store closes.
		if err := c.Close(); err != nil {
			t.Fatalf("iter %d Close: %v", iter, err)
		}
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("iter %d HUNG — a producer blocked on Submit (accepted-then-dropped group)", iter)
		}
	}
}

// TestIngestAsyncWaitAppliedRacingClose (P2): async tokens issued before Close
// must RESOLVE (applied ⇒ nil, or a clean ErrIngestClosed), never block forever.
func TestIngestAsyncWaitAppliedRacingClose(t *testing.T) {
	t.Parallel()
	const iterations = 200
	const submits = 8

	for iter := 0; iter < iterations; iter++ {
		c, err := New(Config{Store: memory.New()})
		if err != nil {
			t.Fatalf("iter %d New: %v", iter, err)
		}

		sess, err := c.Ingest.NewSession(IngestOptions{Sync: false, DeclareLabels: []string{"A"}})
		if err != nil {
			t.Fatalf("iter %d NewSession: %v", iter, err)
		}
		var tokens []SubmitToken
		for i := 0; i < submits; i++ {
			if _, err := sess.AddNode([]string{"A"}, map[string]any{"i": int64(i)}); err != nil {
				t.Fatalf("iter %d AddNode: %v", iter, err)
			}
			tok, err := sess.Submit()
			if err != nil {
				t.Fatalf("iter %d Submit (before Close): %v", iter, err)
			}
			tokens = append(tokens, tok)
		}

		// Every token was issued before Close. Each WaitApplied must resolve —
		// never block forever.
		done := make(chan struct{})
		go func() {
			for _, tok := range tokens {
				if err := c.Ingest.WaitApplied(tok); err != nil && !errors.Is(err, ErrIngestClosed) {
					t.Errorf("iter %d WaitApplied = %v, want nil or ErrIngestClosed", iter, err)
				}
			}
			close(done)
		}()

		if err := c.Close(); err != nil {
			t.Fatalf("iter %d Close: %v", iter, err)
		}
		select {
		case <-done:
		case <-time.After(20 * time.Second):
			t.Fatalf("iter %d HUNG — WaitApplied blocked forever after Close", iter)
		}
	}
}

// TestIngestEnqueueRejectsAfterStop (P3) is the deterministic white-box proof of
// C1: after the applier is stopped, enqueue REJECTS with ErrIngestClosed and
// mints NO seq (round 1 accepted-and-dropped 64 post-stop intents — seqCtr
// advanced while appliedSeq stayed 0).
func TestIngestEnqueueRejectsAfterStop(t *testing.T) {
	t.Parallel()
	c, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer c.Close()

	a, err := c.ensureIngestApplier(defaultIngestGroupSize, 0)
	if err != nil {
		t.Fatalf("ensureIngestApplier: %v", err)
	}

	a.stop()

	seqBefore := a.seqCtr.Load()
	for i := 0; i < 64; i++ {
		g := &ingestGroup{} // contents irrelevant: enqueue rejects before any send
		seq, err := a.enqueue(g, 1)
		if seq != 0 {
			t.Fatalf("enqueue #%d after stop minted seq %d, want 0 (accepted-then-dropped)", i, seq)
		}
		if !errors.Is(err, ErrIngestClosed) {
			t.Fatalf("enqueue #%d after stop err = %v, want ErrIngestClosed", i, err)
		}
	}
	if got := a.seqCtr.Load(); got != seqBefore {
		t.Fatalf("seqCtr advanced %d→%d on rejected enqueues (seq minted for dropped intents)", seqBefore, got)
	}
	if got := a.currentAppliedSeq(); got != 0 {
		t.Fatalf("currentAppliedSeq = %d after stop, want 0 (nothing applied)", got)
	}
}

// TestIngestAsyncUniqueWinnerLoser (P4): two async producers race to create the
// same unique-constrained value. Exactly one wins; the loser's WaitApplied
// returns ErrUniqueViolation (C2 — the async failure is no longer silently
// swallowed), the winner stays visible, and appliedSeq advances past BOTH (a
// rejected group must not wedge waiters behind it).
func TestIngestAsyncUniqueWinnerLoser(t *testing.T) {
	t.Parallel()
	c := newIngestGraph(t)
	ctx := context.Background()
	if err := c.Constraints.CreateUnique(ctx, "U", "k"); err != nil {
		t.Fatalf("CreateUnique: %v", err)
	}

	s1, err := c.Ingest.NewSession(IngestOptions{Sync: false, DeclareLabels: []string{"U"}})
	if err != nil {
		t.Fatalf("NewSession s1: %v", err)
	}
	s2, err := c.Ingest.NewSession(IngestOptions{Sync: false, DeclareLabels: []string{"U"}})
	if err != nil {
		t.Fatalf("NewSession s2: %v", err)
	}

	n1, err := s1.AddNode([]string{"U"}, map[string]any{"k": "dup"})
	if err != nil {
		t.Fatalf("s1.AddNode: %v", err)
	}
	t1, err := s1.Submit()
	if err != nil {
		t.Fatalf("s1.Submit: %v", err)
	}
	n2, err := s2.AddNode([]string{"U"}, map[string]any{"k": "dup"})
	if err != nil {
		t.Fatalf("s2.AddNode: %v", err)
	}
	t2, err := s2.Submit()
	if err != nil {
		t.Fatalf("s2.Submit: %v", err)
	}

	e1 := c.Ingest.WaitApplied(t1)
	e2 := c.Ingest.WaitApplied(t2)

	// appliedSeq advances past BOTH — a rejected group does not wedge waiters.
	maxSeq := t1.Seq
	if t2.Seq > maxSeq {
		maxSeq = t2.Seq
	}
	if got := c.Ingest.AppliedSeq(); got < maxSeq {
		t.Fatalf("AppliedSeq %d < max token %d (rejected group wedged the watermark)", got, maxSeq)
	}

	// Exactly one winner (nil) and one loser (ErrUniqueViolation).
	winners := 0
	if e1 == nil {
		winners++
	} else if !errors.Is(e1, ErrUniqueViolation) {
		t.Fatalf("t1 WaitApplied = %v, want nil or ErrUniqueViolation", e1)
	}
	if e2 == nil {
		winners++
	} else if !errors.Is(e2, ErrUniqueViolation) {
		t.Fatalf("t2 WaitApplied = %v, want nil or ErrUniqueViolation", e2)
	}
	if winners != 1 {
		t.Fatalf("exactly one winner expected; e1=%v e2=%v", e1, e2)
	}

	// The winner's node is visible; the loser's is not; and visibility tracks
	// which token got nil.
	_, err1 := c.Nodes.Get(ctx, n1.ID())
	_, err2 := c.Nodes.Get(ctx, n2.ID())
	if (e1 == nil) != (err1 == nil) {
		t.Fatalf("n1 winner/visibility mismatch: e1=%v getErr=%v", e1, err1)
	}
	if (e2 == nil) != (err2 == nil) {
		t.Fatalf("n2 winner/visibility mismatch: e2=%v getErr=%v", e2, err2)
	}
	if e1 != nil && !errors.Is(err1, storepkg.ErrNodeNotFound) {
		t.Fatalf("loser n1 Get = %v, want ErrNodeNotFound", err1)
	}
	if e2 != nil && !errors.Is(err2, storepkg.ErrNodeNotFound) {
		t.Fatalf("loser n2 Get = %v, want ErrNodeNotFound", err2)
	}

	// Exactly one node carries the constrained value.
	users, err := c.Nodes.ByLabelAndProperty("U", "k", "dup", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabelAndProperty: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("unique admitted %d nodes, want exactly 1", len(users))
	}

	// A second WaitApplied for the loser token returns nil — the failure record
	// is pruned on read (documented retention rule).
	loserTok := t2
	if e1 != nil {
		loserTok = t1
	}
	if err := c.Ingest.WaitApplied(loserTok); err != nil {
		t.Fatalf("second WaitApplied for loser = %v, want nil (pruned on read)", err)
	}
}

// TestIngestFlakyFlushThroughPipeline (C3/F3) drives writes through g.Ingest()
// sessions against the batch-durability battery's failing-store decorator,
// injecting a mid-group flush failure. The batch node-create is all-or-nothing,
// so on failure NO node of the group is visible; the error surfaces to the sync
// submitter and to an async WaitApplied (C2); and clearing the fault recovers on
// the next flush.
func TestIngestFlakyFlushThroughPipeline(t *testing.T) {
	t.Parallel()
	injected := errors.New("injected pipeline flush failure")
	store := &toggleFailPutNodesBatchStore{Store: memory.New(), err: injected}
	c, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx := context.Background()

	// --- Sync: flush fails mid-group; error surfaces to Submit; NO node visible.
	store.fail.Store(true)
	sess, err := c.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatalf("NewSession sync: %v", err)
	}
	var failIDs []types.NodeID
	for i := 0; i < 3; i++ {
		n, err := sess.AddNode([]string{"F"}, map[string]any{"i": int64(i)})
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		failIDs = append(failIDs, n.ID())
	}
	if _, subErr := sess.Submit(); !errors.Is(subErr, injected) {
		t.Fatalf("sync Submit under flush failure = %v, want the injected error", subErr)
	}
	for _, id := range failIDs {
		if _, err := c.Nodes.Get(ctx, id); !errors.Is(err, storepkg.ErrNodeNotFound) {
			t.Fatalf("node %d visible after failed group (partial group leaked): err=%v", id, err)
		}
	}

	// --- Recovery: clear the fault; the next flush commits and is visible.
	store.fail.Store(false)
	rec, err := sess.AddNode([]string{"F"}, map[string]any{"i": int64(99)})
	if err != nil {
		t.Fatalf("AddNode after recovery: %v", err)
	}
	if _, err := sess.Submit(); err != nil {
		t.Fatalf("Submit after recovery: %v", err)
	}
	if _, err := c.Nodes.Get(ctx, rec.ID()); err != nil {
		t.Fatalf("recovered node not visible: %v", err)
	}

	// --- Async: flush fails; WaitApplied returns the error and appliedSeq still
	// advances (C2).
	store.fail.Store(true)
	asess, err := c.Ingest.NewSession(IngestOptions{Sync: false})
	if err != nil {
		t.Fatalf("NewSession async: %v", err)
	}
	an, err := asess.AddNode([]string{"F"}, map[string]any{"a": int64(1)})
	if err != nil {
		t.Fatalf("async AddNode: %v", err)
	}
	tok, err := asess.Submit()
	if err != nil {
		t.Fatalf("async Submit: %v", err)
	}
	if waitErr := c.Ingest.WaitApplied(tok); !errors.Is(waitErr, injected) {
		t.Fatalf("async WaitApplied under flush failure = %v, want the injected error", waitErr)
	}
	if got := c.Ingest.AppliedSeq(); got < tok.Seq {
		t.Fatalf("AppliedSeq %d < rejected token %d (waiters would wedge)", got, tok.Seq)
	}
	if _, err := c.Nodes.Get(ctx, an.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("async-failed node visible: %v", err)
	}
	// Second WaitApplied for the same token returns nil (pruned on read).
	if err := c.Ingest.WaitApplied(tok); err != nil {
		t.Fatalf("second WaitApplied = %v, want nil (pruned on read)", err)
	}
}

// TestIngestApplier_RecordFailureLocked_EvictsOldestAtCap guards BACKLOG
// 11e: the `failures` map's bounded-eviction path (oldest-first at
// ingestFailureCap, counted in failureDrops) had zero direct test coverage
// — driving it through the full async ingest pipeline would need 8192+
// genuinely rejected intents, so this tests recordFailureLocked/
// takeFailureLocked directly against a bare *ingestApplier (no run() loop
// needed — these two methods only touch the failures map under a.mu, which
// the test acquires itself, matching their own "caller holds a.mu"
// contract).
func TestIngestApplier_RecordFailureLocked_EvictsOldestAtCap(t *testing.T) {
	a := newIngestApplier(&Core{}, 0, 0)

	a.mu.Lock()
	for seq := uint64(1); seq <= ingestFailureCap; seq++ {
		a.recordFailureLocked(seq, errors.New("boom"))
	}
	if len(a.failures) != ingestFailureCap {
		t.Fatalf("failures at cap = %d, want %d", len(a.failures), ingestFailureCap)
	}
	if a.failureDrops != 0 {
		t.Fatalf("failureDrops before exceeding cap = %d, want 0", a.failureDrops)
	}

	// One more record must evict the OLDEST (seq=1), not any arbitrary entry.
	a.recordFailureLocked(ingestFailureCap+1, errors.New("boom"))
	if len(a.failures) != ingestFailureCap {
		t.Fatalf("failures after exceeding cap = %d, want still %d", len(a.failures), ingestFailureCap)
	}
	if a.failureDrops != 1 {
		t.Fatalf("failureDrops = %d, want 1", a.failureDrops)
	}
	if _, exists := a.failures[1]; exists {
		t.Fatal("oldest seq=1 was not evicted")
	}
	for seq := uint64(2); seq <= ingestFailureCap+1; seq++ {
		if _, exists := a.failures[seq]; !exists {
			t.Fatalf("seq=%d was evicted, want retained (only the oldest should ever be evicted)", seq)
		}
	}
	a.mu.Unlock()
}

// TestIngestApplier_RecordFailureLocked_KeepsFirstErrorForToken guards the
// "Keeps the FIRST error for a token" contract documented on
// recordFailureLocked — a second record call for an ALREADY-recorded seq
// must be a no-op, not overwrite.
func TestIngestApplier_RecordFailureLocked_KeepsFirstErrorForToken(t *testing.T) {
	a := newIngestApplier(&Core{}, 0, 0)
	first := errors.New("first")
	second := errors.New("second")

	a.mu.Lock()
	a.recordFailureLocked(1, first)
	a.recordFailureLocked(1, second)
	got := a.failures[1]
	a.mu.Unlock()

	if !errors.Is(got, first) {
		t.Fatalf("recorded error = %v, want the FIRST error %v (not overwritten)", got, first)
	}
}

// TestIngestApplier_TakeFailureLocked_PrunesOnRead guards the "prune on
// read" contract directly (the ingest_reliability_test.go tests above only
// exercise this indirectly through WaitApplied).
func TestIngestApplier_TakeFailureLocked_PrunesOnRead(t *testing.T) {
	a := newIngestApplier(&Core{}, 0, 0)
	want := errors.New("boom")

	a.mu.Lock()
	a.recordFailureLocked(1, want)
	got := a.takeFailureLocked(1)
	if !errors.Is(got, want) {
		a.mu.Unlock()
		t.Fatalf("takeFailureLocked = %v, want %v", got, want)
	}
	if _, exists := a.failures[1]; exists {
		a.mu.Unlock()
		t.Fatal("failure record was not pruned after takeFailureLocked")
	}
	// A second take for the same seq returns nil — already pruned.
	got2 := a.takeFailureLocked(1)
	a.mu.Unlock()
	if got2 != nil {
		t.Fatalf("second takeFailureLocked = %v, want nil", got2)
	}
}

// TestIngestApplier_RecordFailureLocked_ConcurrentLoad guards BACKLOG 11e's
// "untested under load or concurrency" half: many goroutines hammering
// record/take concurrently through the SAME a.mu discipline every real
// caller uses, run under -race to confirm the mutex genuinely serializes
// map access (not just "no test crashed by luck").
func TestIngestApplier_RecordFailureLocked_ConcurrentLoad(t *testing.T) {
	a := newIngestApplier(&Core{}, 0, 0)
	const goroutines = 32
	const perGoroutine = 500

	var wg sync.WaitGroup
	var seqCtr atomic.Uint64
	var takenCount atomic.Int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				seq := seqCtr.Add(1)
				a.mu.Lock()
				a.recordFailureLocked(seq, errors.New("boom"))
				a.mu.Unlock()
				if i%3 == 0 {
					a.mu.Lock()
					if a.takeFailureLocked(seq) != nil {
						takenCount.Add(1)
					}
					a.mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	a.mu.Lock()
	finalLen := len(a.failures)
	drops := a.failureDrops
	a.mu.Unlock()

	if finalLen > ingestFailureCap {
		t.Fatalf("failures map grew past the cap under concurrent load: %d > %d", finalLen, ingestFailureCap)
	}
	total := int64(goroutines * perGoroutine)
	if int64(finalLen)+takenCount.Load()+int64(drops) > total {
		t.Fatalf("accounting overflow: remaining(%d) + taken(%d) + dropped(%d) > total inserted(%d)",
			finalLen, takenCount.Load(), drops, total)
	}
}
