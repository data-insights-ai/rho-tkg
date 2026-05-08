package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
)

// TestTxRun_PanicReleasesLock verifies that a panicking callback in TxAPI.Run
// triggers Rollback (releasing the graph write lock) and re-raises the panic.
// Prior behaviour leaked c.mu, deadlocking every subsequent mutation.
func TestTxRun_PanicReleasesLock(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to propagate, got nil")
		}
	}()
	defer func() {
		// After the panic-recovering defer above runs, verify that the lock
		// is released by performing a follow-up mutation under a tight
		// timeout. A leaked lock would block this indefinitely.
		done := make(chan error, 1)
		go func() {
			_, addErr := g.Nodes.Add([]string{"X"}, nil)
			done <- addErr
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("post-panic Nodes.Add: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("post-panic mutation timed out — graph write lock leaked")
		}
	}()

	_ = g.Tx.Run(func(tx *graphpkg.GraphTx) error {
		panic("boom")
	})
}

// TestTxRun_FnErrorRollsBack verifies that fn returning an error rolls back
// without leaking the lock and surfaces the original error to the caller.
func TestTxRun_FnErrorRollsBack(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	want := errors.New("user-defined")
	got := g.Tx.Run(func(tx *graphpkg.GraphTx) error {
		_, _ = tx.AddNode([]string{"Tmp"}, nil)
		return want
	})
	if !errors.Is(got, want) {
		t.Fatalf("Run = %v, want %v wrapped", got, want)
	}
	// Lock released — a follow-up mutation must complete.
	if _, err := g.Nodes.Add([]string{"After"}, nil); err != nil {
		t.Fatalf("post-rollback Add: %v", err)
	}
}

// TestTxRun_ManualRollbackInsideFnDoesNotErrorTwice verifies the deferred
// Rollback in Run silently absorbs the ErrTxDone sentinel that comes from a
// callback that rolled back manually.
func TestTxRun_ManualRollbackInsideFnDoesNotErrorTwice(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	want := errors.New("explicit rollback")
	got := g.Tx.Run(func(tx *graphpkg.GraphTx) error {
		if rbErr := tx.Rollback(); rbErr != nil {
			t.Fatalf("manual Rollback: %v", rbErr)
		}
		return want
	})
	if !errors.Is(got, want) {
		t.Fatalf("Run = %v, want %v", got, want)
	}
	if errors.Is(got, storepkg.ErrTxDone) {
		t.Fatalf("Run leaked ErrTxDone to caller: %v", got)
	}
}

// TestTxRunContext_CtxCancelledAfterFn verifies that a context which is done
// AFTER fn returns nil but BEFORE Commit causes Rollback and returns ctx.Err().
func TestTxRunContext_CtxCancelledAfterFn(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	got := g.Tx.RunContext(ctx, func(tx *graphpkg.GraphTx) error {
		_, _ = tx.AddNode([]string{"Tmp"}, nil)
		cancel() // cancel before commit
		return nil
	})
	if !errors.Is(got, context.Canceled) {
		t.Fatalf("RunContext = %v, want context.Canceled", got)
	}
	// Lock released.
	if _, err := g.Nodes.Add([]string{"After"}, nil); err != nil {
		t.Fatalf("post-cancel Add: %v", err)
	}
}

// TestTxRunContext_PanicReleasesLock mirrors TestTxRun_PanicReleasesLock for
// the context-aware variant.
func TestTxRunContext_PanicReleasesLock(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic to propagate, got nil")
		}
	}()
	defer func() {
		done := make(chan error, 1)
		go func() {
			_, addErr := g.Nodes.Add([]string{"X"}, nil)
			done <- addErr
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("post-panic Nodes.Add: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("post-panic mutation timed out — graph write lock leaked")
		}
	}()

	_ = g.Tx.RunContext(context.Background(), func(tx *graphpkg.GraphTx) error {
		panic("boom-ctx")
	})
}
