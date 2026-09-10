package core

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// groupCommitSpyStore wraps the memory store with a counting
// store.GroupCommitCapability so the tests can see WHICH door opens a window
// and inject an EndGroupCommit failure (the ENOSPC / flush-failure shape).
type groupCommitSpyStore struct {
	*memory.Store        // concrete embed so the memory store's other capabilities stay visible
	begins, ends         atomic.Int64
	failEnd              atomic.Bool
	failErr              error
	panicBegin, panicEnd bool
}

func (s *groupCommitSpyStore) BeginGroupCommit() {
	s.begins.Add(1)
	if s.panicBegin {
		panic("begin")
	}
}
func (s *groupCommitSpyStore) EndGroupCommit() error {
	s.ends.Add(1)
	if s.panicEnd {
		panic("end")
	}
	if s.failEnd.Load() {
		return s.failErr
	}
	return nil
}

func newGroupCommitGraph(t *testing.T) (*Core, *groupCommitSpyStore) {
	t.Helper()
	spy := &groupCommitSpyStore{Store: memory.New(), failErr: errors.New("simulated ENOSPC at group commit")}
	g, err := New(Config{Store: spy})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g, spy
}

// TestGroupCommitWindowIsOpenedOnlyByTheStrongApplier: the strong Sync session
// opens exactly one window per commit group; the plain Batch door, the tx door
// and a concurrent session never do (their durability behaviour is unchanged).
func TestGroupCommitWindowIsOpenedOnlyByTheStrongApplier(t *testing.T) {
	g, spy := newGroupCommitGraph(t)

	// Plain batch door.
	b, _ := NewBatchBuilder(g)
	if _, err := b.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Execute(); err != nil {
		t.Fatal(err)
	}
	// Tx door.
	tx, _ := g.BeginTx()
	if _, err := tx.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Concurrent session.
	cs, err := g.Ingest.NewSession(IngestOptions{Concurrent: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Submit(); err != nil {
		t.Fatal(err)
	}
	if n := spy.begins.Load(); n != 0 {
		t.Fatalf("batch/tx/concurrent doors opened %d group-commit windows, want 0", n)
	}

	// Strong Sync session: one window per commit group, closed exactly once.
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.AddNode([]string{"L"}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Submit(); err != nil {
			t.Fatal(err)
		}
	}
	if b, e := spy.begins.Load(), spy.ends.Load(); b != 3 || e != 3 {
		t.Fatalf("strong door: begins=%d ends=%d, want 3/3 (one per submitted group)", b, e)
	}
}

// TestGroupCommitFailureFailsEverySubmitterAndCannotAck: when the single
// durable operation fails, the Sync submitter gets the error, an async
// submitter's WaitApplied returns it, and AppliedSeq still advances (no wedged
// waiter). Nothing is reported as committed.
func TestGroupCommitFailureFailsEverySubmitterAndCannotAck(t *testing.T) {
	g, spy := newGroupCommitGraph(t)
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	spy.failEnd.Store(true)
	if _, err := s.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	tok, err := s.Submit()
	if err == nil || !errors.Is(err, spy.failErr) {
		t.Fatalf("Sync Submit under a failed group commit = %v, want the group-commit error", err)
	}
	if got := g.Ingest.AppliedSeq(); got < tok.Seq {
		t.Fatalf("AppliedSeq %d did not pass the failed token %d (waiters would wedge)", got, tok.Seq)
	}
	// Async submitter: the failure is the WaitApplied truth.
	as, err := g.Ingest.NewSession(IngestOptions{Sync: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := as.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	atok, err := as.Submit()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Ingest.WaitApplied(atok); err == nil || !errors.Is(err, spy.failErr) {
		t.Fatalf("async WaitApplied under a failed group commit = %v, want the group-commit error", err)
	}
	// Recovery: once the store commits again, a new group succeeds.
	spy.failEnd.Store(false)
	if _, err := s.AddNode([]string{"L"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Submit(); err != nil {
		t.Fatalf("Submit after recovery: %v", err)
	}
}

// TestGroupCommitPartialErrorStillCommitsOnce: a group with one failing intent
// (a relationship whose endpoint was deleted between prepare and Submit) among
// valid ones keeps its survivors (documented keep-survivors semantics), reports
// the per-op error through Submit, and still closes exactly one window —
// partial batch error and physical durability are separate properties.
func TestGroupCommitPartialErrorStillCommitsOnce(t *testing.T) {
	g, spy := newGroupCommitGraph(t)
	s, err := g.Ingest.NewSession(IngestOptions{Sync: true})
	if err != nil {
		t.Fatal(err)
	}
	doomed, err := g.Nodes.Add(t.Context(), []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddRelationship("OK", a, b, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddRelationship("BROKEN", a, doomed, nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.Delete(t.Context(), doomed.ID()); err != nil {
		t.Fatal(err)
	}
	spy.begins.Store(0)
	spy.ends.Store(0)
	if _, err := s.Submit(); err == nil {
		t.Fatal("Submit with a broken relationship returned nil")
	}
	if b, e := spy.begins.Load(), spy.ends.Load(); b != 1 || e != 1 {
		t.Fatalf("begins=%d ends=%d, want 1/1", b, e)
	}
	if n, _ := g.Nodes.CountByLabel("P"); n != 2 {
		t.Fatalf("P count = %d, want 2 (survivors committed once)", n)
	}
	if n, _ := g.Rels.CountByType("OK"); n != 1 {
		t.Fatalf("OK rels = %d, want 1 (survivor)", n)
	}
	if n, _ := g.Rels.CountByType("BROKEN"); n != 0 {
		t.Fatalf("BROKEN rels = %d, want 0", n)
	}
}

// A backend panic must leave the graph usable after recovery, including a
// panic from the optional durability capability itself.
func TestGroupCommitPanicReleasesBatchLocks(t *testing.T) {
	for _, at := range []string{"begin", "end", "begin-and-end"} {
		t.Run(at, func(t *testing.T) {
			g, spy := newGroupCommitGraph(t)
			b, err := NewBatchBuilder(g)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.AddNode([]string{"L"}, nil); err != nil {
				t.Fatal(err)
			}
			b.groupCommit = true
			spy.panicBegin, spy.panicEnd = at != "end", at != "begin"
			func() {
				defer func() {
					if recover() == nil {
						t.Error("backend panic was lost")
					}
				}()
				_, _ = b.Execute()
			}()
			// Probe and repair leaked locks before reporting failure so cleanup cannot
			// hang a red run. These are uncontended after the synchronous Execute.
			if !b.mu.TryLock() {
				t.Error("builder lock retained")
			}
			b.mu.Unlock()
			if !g.txMu.TryLock() {
				t.Error("transaction lock retained")
			}
			g.txMu.Unlock()
			for i := range g.mu.stripes {
				if !g.mu.stripes[i].mu.TryLock() {
					t.Errorf("graph stripe %d retained", i)
				}
				g.mu.stripes[i].mu.Unlock()
			}
			if g.txEventBuffer != nil {
				t.Error("event buffer retained")
				g.txEventBuffer = nil
			}
		})
	}
}
