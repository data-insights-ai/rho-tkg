package core

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// commitLogScopeFailingStore wraps a change-log-enabled badger.Store and lets
// a test force CommitLogScope to fail on demand, to exercise
// Batch.Execute's "the change-log commit itself failed" error path
// independent of any per-op failure.
type commitLogScopeFailingStore struct {
	*badger.Store
	failCommit bool
}

func (s *commitLogScopeFailingStore) CommitLogScope() (uint64, error) {
	if s.failCommit {
		return 0, errCommitLogScopeInjected
	}
	return s.Store.CommitLogScope()
}

var errCommitLogScopeInjected = errors.New("injected: commit log scope failure")

// BACKLOG 11c: Batch.Execute's CommitLogScope error handling used
// `if err != nil && result.Failed == 0` — so a CommitLogScope failure was
// silently DROPPED whenever the batch already had a per-op failure. This is
// backwards: a CommitLogScope failure leaves committed-but-unlogged data (a
// replica-divergence risk, per the function's own comment) — exactly the
// scenario a caller most needs surfaced, not swallowed because something
// unrelated also failed in the same batch.
func TestBatchExecute_CommitLogScopeErrorSurfacedAlongsidePerOpFailure(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	wrapped := &commitLogScopeFailingStore{Store: bs}
	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	// A per-op failure: update on a non-existent node.
	if err := b.UpdateNode(types.NodeID(999999), map[string]any{"name": "Ghost"}); err != nil {
		t.Fatalf("UpdateNode queue: %v", err)
	}

	wrapped.failCommit = true
	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil {
		t.Fatal("Execute returned nil result")
	}

	foundPerOp := false
	foundLogScope := false
	for _, be := range result.Errors {
		if be.Op == "UpdateNode" {
			foundPerOp = true
		}
		if be.Op == "commit-change-log" {
			foundLogScope = true
			if !errors.Is(be.Err, errCommitLogScopeInjected) {
				t.Errorf("commit-change-log error = %v, want wrapping the injected error", be.Err)
			}
		}
	}
	if !foundPerOp {
		t.Fatalf("result.Errors = %+v, missing the pre-existing per-op UpdateNode failure", result.Errors)
	}
	if !foundLogScope {
		t.Fatalf("result.Errors = %+v, missing the commit-change-log failure — BACKLOG 11c regression (silently dropped because a per-op failure already existed)", result.Errors)
	}
	if result.Failed < 2 {
		t.Fatalf("result.Failed = %d, want >= 2 (per-op failure AND commit-log-scope failure both counted)", result.Failed)
	}
}

// TestBatchExecute_CommitLogScopeErrorSurfacedAlone is the non-regression
// counterpart: a CommitLogScope failure with NO per-op failure must still be
// surfaced (the pre-fix `result.Failed == 0` branch already handled this
// case correctly — this pins it stays correct after the fix).
func TestBatchExecute_CommitLogScopeErrorSurfacedAlone(t *testing.T) {
	bs, err := badger.New(badger.Config{InMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	wrapped := &commitLogScopeFailingStore{Store: bs}
	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	b, err := NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := b.AddNode([]string{"Person"}, map[string]any{"name": "Ada"}); err != nil {
		t.Fatalf("AddNode queue: %v", err)
	}

	wrapped.failCommit = true
	result, err := b.Execute()
	if !errors.Is(err, ErrBatchFailed) {
		t.Fatalf("Execute error = %v, want ErrBatchFailed", err)
	}
	if result == nil || len(result.Errors) != 1 || result.Errors[0].Op != "commit-change-log" {
		t.Fatalf("result.Errors = %+v, want exactly one commit-change-log error", result)
	}
}
