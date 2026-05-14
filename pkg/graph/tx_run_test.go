package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type cancelAfterFirstErrContext struct {
	calls int
}

func (c *cancelAfterFirstErrContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterFirstErrContext) Done() <-chan struct{}       { return nil }
func (c *cancelAfterFirstErrContext) Value(any) any               { return nil }
func (c *cancelAfterFirstErrContext) Err() error {
	c.calls++
	if c.calls == 1 {
		return nil
	}
	return context.Canceled
}

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

func TestTxRunRejectsNilCallbackBeforeBegin(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.Tx.Run(nil); !errors.Is(err, graphpkg.ErrNilTxCallback) {
		t.Fatalf("Run(nil) = %v, want ErrNilTxCallback", err)
	}
	if err := g.Tx.RunContext(context.Background(), nil); !errors.Is(err, graphpkg.ErrNilTxCallback) {
		t.Fatalf("RunContext(ctx, nil) = %v, want ErrNilTxCallback", err)
	}

	done := make(chan error, 1)
	go func() {
		_, addErr := g.Nodes.Add([]string{"AfterNilCallback"}, nil)
		done <- addErr
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("post-nil-callback Add: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-nil-callback mutation timed out")
	}
}

func TestGraphTxNilAndZeroReceiversFailClosed(t *testing.T) {
	t.Parallel()

	var nilTx *graphpkg.GraphTx
	var zeroTx graphpkg.GraphTx

	for _, tc := range []struct {
		name string
		tx   *graphpkg.GraphTx
	}{
		{name: "nil", tx: nilTx},
		{name: "zero", tx: &zeroTx},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks := []struct {
				name string
				run  func(*graphpkg.GraphTx) error
			}{
				{name: "Commit", run: func(tx *graphpkg.GraphTx) error { return tx.Commit() }},
				{name: "Rollback", run: func(tx *graphpkg.GraphTx) error { return tx.Rollback() }},
				{name: "GetNode", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.GetNode(types.NodeID(1))
					return err
				}},
				{name: "GetRelationship", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.GetRelationship(types.RelID(1))
					return err
				}},
				{name: "Export", run: func(tx *graphpkg.GraphTx) error {
					var buf bytes.Buffer
					return tx.Export(&buf)
				}},
				{name: "Snapshot", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.Snapshot(1)
					return err
				}},
				{name: "VerifyShard", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.VerifyShard("hot")
					return err
				}},
				{name: "AddNode", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.AddNode([]string{"Node"}, nil)
					return err
				}},
				{name: "AddRelationship", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.AddRelationship("REL", nil, nil, nil)
					return err
				}},
				{name: "AddRelationshipByID", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.AddRelationshipByID("REL", types.NodeID(1), types.NodeID(2), nil)
					return err
				}},
				{name: "AddRelationshipByIDIfAbsent", run: func(tx *graphpkg.GraphTx) error {
					_, _, err := tx.AddRelationshipByIDIfAbsent("REL", types.NodeID(1), types.NodeID(2), nil)
					return err
				}},
				{name: "ImportNodeWithID", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.ImportNodeWithID(context.Background(), types.NodeID(1), []string{"Node"}, nil)
					return err
				}},
				{name: "ImportRelationshipWithID", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.ImportRelationshipWithID(context.Background(), types.RelID(1), "REL", nil, nil, nil)
					return err
				}},
				{name: "UpdateNode", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.UpdateNode(types.NodeID(1), map[string]any{"name": "x"})
					return err
				}},
				{name: "UpdateRelationship", run: func(tx *graphpkg.GraphTx) error {
					_, err := tx.UpdateRelationship(types.RelID(1), map[string]any{"name": "x"})
					return err
				}},
				{name: "SetNodeProperty", run: func(tx *graphpkg.GraphTx) error {
					return tx.SetNodeProperty(types.NodeID(1), "name", "x")
				}},
				{name: "DeleteNodeProperty", run: func(tx *graphpkg.GraphTx) error {
					return tx.DeleteNodeProperty(types.NodeID(1), "name")
				}},
				{name: "SetRelationshipProperty", run: func(tx *graphpkg.GraphTx) error {
					return tx.SetRelationshipProperty(types.RelID(1), "name", "x")
				}},
				{name: "DeleteRelationshipProperty", run: func(tx *graphpkg.GraphTx) error {
					return tx.DeleteRelationshipProperty(types.RelID(1), "name")
				}},
				{name: "DeleteNode", run: func(tx *graphpkg.GraphTx) error {
					return tx.DeleteNode(types.NodeID(1))
				}},
				{name: "DeleteRelationship", run: func(tx *graphpkg.GraphTx) error {
					return tx.DeleteRelationship(types.RelID(1))
				}},
				{name: "AddNodeLabel", run: func(tx *graphpkg.GraphTx) error {
					return tx.AddNodeLabel(types.NodeID(1), "Extra")
				}},
				{name: "RemoveNodeLabel", run: func(tx *graphpkg.GraphTx) error {
					return tx.RemoveNodeLabel(types.NodeID(1), "Extra")
				}},
			}

			for _, check := range checks {
				if err := check.run(tc.tx); !errors.Is(err, graphpkg.ErrNilGraph) {
					t.Fatalf("%s = %v, want ErrNilGraph", check.name, err)
				}
			}
			if ids := tc.tx.CreatedNodeIDs(); len(ids) != 0 {
				t.Fatalf("CreatedNodeIDs = %v, want empty", ids)
			}
			if ids := tc.tx.CreatedRelIDs(); len(ids) != 0 {
				t.Fatalf("CreatedRelIDs = %v, want empty", ids)
			}
		})
	}
}

func TestBatchBuilderNilAndZeroReceiversFailClosed(t *testing.T) {
	t.Parallel()

	var nilBatch *graphpkg.BatchBuilder
	var zeroBatch graphpkg.BatchBuilder

	for _, tc := range []struct {
		name  string
		batch *graphpkg.BatchBuilder
	}{
		{name: "nil", batch: nilBatch},
		{name: "zero", batch: &zeroBatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			checks := []struct {
				name string
				run  func(*graphpkg.BatchBuilder) error
			}{
				{name: "AddNode", run: func(b *graphpkg.BatchBuilder) error {
					_, err := b.AddNode([]string{"Node"}, nil)
					return err
				}},
				{name: "AddRelationship", run: func(b *graphpkg.BatchBuilder) error {
					_, err := b.AddRelationship("REL", nil, nil, nil)
					return err
				}},
				{name: "UpdateNode", run: func(b *graphpkg.BatchBuilder) error {
					return b.UpdateNode(types.NodeID(1), map[string]any{"name": "x"})
				}},
				{name: "UpdateRelationship", run: func(b *graphpkg.BatchBuilder) error {
					return b.UpdateRelationship(types.RelID(1), map[string]any{"name": "x"})
				}},
				{name: "DeleteNode", run: func(b *graphpkg.BatchBuilder) error {
					return b.DeleteNode(types.NodeID(1))
				}},
				{name: "DeleteRelationship", run: func(b *graphpkg.BatchBuilder) error {
					return b.DeleteRelationship(types.RelID(1))
				}},
				{name: "Execute", run: func(b *graphpkg.BatchBuilder) error {
					_, err := b.Execute()
					return err
				}},
			}

			for _, check := range checks {
				if err := check.run(tc.batch); !errors.Is(err, graphpkg.ErrNilGraph) {
					t.Fatalf("%s = %v, want ErrNilGraph", check.name, err)
				}
			}
		})
	}
}

func TestTxRunContextRejectsNilContext(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	var ctx context.Context
	if err := g.Tx.RunContext(ctx, func(*graphpkg.GraphTx) error { return nil }); !errors.Is(err, graphpkg.ErrNilContext) {
		t.Fatalf("RunContext(nil, fn) = %v, want ErrNilContext", err)
	}
}

func TestTxRunContextRejectsCanceledContextBeforeBegin(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := g.Tx.RunContext(ctx, func(*graphpkg.GraphTx) error {
		called = true
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RunContext(canceled, fn) = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("RunContext called callback despite pre-canceled context")
	}
	if _, err := g.Nodes.Add([]string{"AfterCanceled"}, nil); err != nil {
		t.Fatalf("post-canceled-context Add: %v", err)
	}
}

func TestTxRunContextRechecksContextAfterBeginBeforeCallback(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	ctx := &cancelAfterFirstErrContext{}
	called := false
	err = g.Tx.RunContext(ctx, func(*graphpkg.GraphTx) error {
		called = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunContext(cancel after begin) = %v, want context.Canceled", err)
	}
	if called {
		t.Fatal("RunContext called callback after context cancellation became visible post-BeginTx")
	}
	if _, err := g.Nodes.Add([]string{"AfterCanceledPostBegin"}, nil); err != nil {
		t.Fatalf("post-canceled-post-begin Add: %v", err)
	}
}

func TestTxRunContextCommits(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.Tx.RunContext(context.Background(), func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddNode([]string{"Committed"}, nil)
		return err
	}); err != nil {
		t.Fatalf("RunContext commit: %v", err)
	}
	nodes, err := g.Nodes.All(storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("Nodes.All: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("Nodes.All count = %d, want 1", len(nodes))
	}
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
